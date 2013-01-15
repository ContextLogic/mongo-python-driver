import functools
import socket
import warnings
import time

# So that 'setup.py doc' can import this module without Tornado or greenlet
requirements_satisfied = True
try:
    from tornado import ioloop, iostream
except ImportError:
    requirements_satisfied = False
    warnings.warn("Tornado not installed", ImportWarning)

try:
    import greenlet
except ImportError:
    requirements_satisfied = False
    warnings.warn("greenlet module not installed", ImportWarning)


import pymongo
import pymongo.common
import pymongo.errors
import pymongo.mongo_client
import pymongo.mongo_replica_set_client
import pymongo.pool
import pymongo.son_manipulator


def green_sock_method(method):
    """Wrap a GreenletSocket method to pause the current greenlet and arrange
       for the greenlet to be resumed when non-blocking I/O has completed.
    """
    @functools.wraps(method)
    def _green_sock_method(self, *args, **kwargs):
        child_gr = greenlet.getcurrent()
        main = child_gr.parent
        assert main, "Should be on child greenlet"

        # Run on main greenlet
        def closed(gr):
            # The child greenlet might have died, e.g.:
            # - An operation raised an error within PyMongo
            # - PyMongo closed the MotorSocket in response
            # - GreenletSocket.close() closed the IOStream
            # - IOStream scheduled this closed() function on the loop
            # - PyMongo operation completed (with or without error) and
            #       its greenlet terminated
            # - IOLoop runs this function
            if not gr.dead:
                gr.throw(socket.error("error"))

        # send the error to this greenlet if something goes wrong during the
        # query
        self.stream.set_close_callback(functools.partial(closed, child_gr))

        try:
            # method is GreenletSocket.send(), recv(), etc. method() begins a
            # non-blocking operation on an IOStream and arranges for
            # callback() to be executed on the main greenlet once the
            # operation has completed.
            method(self, *args, **kwargs)

            # Pause child greenlet until resumed by main greenlet, which
            # will pass the result of the socket operation (data for recv,
            # number of bytes written for sendall) to us.
            socket_result = main.switch()

            # disable the callback to raise exception in this greenlet on socket
            # close, since the greenlet won't be around to raise the exception
            # in (and it'll be caught on the next query and raise an
            # AutoReconnect, which gets handled properly)
            self.stream.set_close_callback(None)

            return socket_result
        except socket.error:
            raise
        except IOError, e:
            # If IOStream raises generic IOError (e.g., if operation
            # attempted on closed IOStream), then substitute socket.error,
            # since socket.error is what PyMongo's built to handle. For
            # example, PyMongo will catch socket.error, close the socket,
            # and raise AutoReconnect.
            raise socket.error(str(e))

    return _green_sock_method


class GreenletSocket(object):
    """Replace socket with a class that yields from the current greenlet, if
    we're on a child greenlet, when making blocking calls, and uses Tornado
    IOLoop to schedule child greenlet for resumption when I/O is ready.

    We only implement those socket methods actually used by pymongo.
    """
    def __init__(self, sock, io_loop, use_ssl=False):
        self.use_ssl = use_ssl
        self.io_loop = io_loop
        if self.use_ssl:
           raise Exception("SSL isn't supported")
        else:
           self.stream = iostream.IOStream(sock, io_loop=io_loop, priority=0)

    def setsockopt(self, *args, **kwargs):
        self.stream.socket.setsockopt(*args, **kwargs)

    def settimeout(self, timeout):
        # I'm not implementing timeouts here. could be done with a time-delayed
        # callback to the IOLoop, but since we don't use them anywhere, I'm not
        # going to bother.
        #
        # need to implement this method since a non-blocking socket has timeout
        # of None or 0.0, but if anything else is specified, raise exception
        if timeout:
            raise NotImplementedError

    def connect(self, pair):
        # do the connect on the underlying socket synchronously...
        self.stream.connect(pair)

    def sendall(self, data):
        # do the send on the underlying socket synchronously...
        self.stream.write(data)

    def recv(self, num_bytes):
        # if we have enough bytes in our local buffer, don't yield
        if self.stream.can_read_sync(num_bytes):
            return self.stream._consume(num_bytes)
        # else yield while we wait on Mongo to send us more
        else:
            return self.recv_async(num_bytes)

    @green_sock_method
    def recv_async(self, num_bytes):
        # do the recv on the underlying socket... come back to the current
        # greenlet when it's done
        return self.stream.read_bytes(num_bytes, greenlet.getcurrent().switch)

    def close(self):
        # since we're explicitly handling closing here, don't raise an exception
        # via the callback
        self.stream.set_close_callback(None)

        sock = self.stream.socket
        try:
            try:
                self.stream.close()
            except KeyError:
                # Tornado's _impl (epoll, kqueue, ...) has already removed this
                # file descriptor from its dict.
                pass
        finally:
            # Sometimes necessary to avoid ResourceWarnings in Python 3:
            # specifically, if the fd is closed from the OS's view, then
            # stream.close() throws an exception, but the socket still has an
            # fd and so will print a ResourceWarning. In that case, calling
            # sock.close() directly clears the fd and does not raise an error.
            if sock:
                sock.close()

    def fileno(self):
        return self.stream.socket.fileno()


class GreenletPool(pymongo.pool.GreenletPool):
    """A simple connection pool of GreenletSockets.

    Note this inherits from GreenletPool so that when PyMongo internally calls
    start_request, e.g. in Database.authenticate() or
    MongoClient.copy_database(), this pool assigns a socket to the current
    greenlet for the duration of the method. Request semantics are not exposed
    to Motor's users.
    """
    def __init__(self, *args, **kwargs):
        io_loop = kwargs.pop('io_loop', None)
        self.io_loop = io_loop if io_loop else ioloop.IOLoop.instance()
        pymongo.pool.GreenletPool.__init__(self, *args, **kwargs)

    def create_connection(self, pair):
        """Copy of BasePool.connect()
        """
        assert greenlet.getcurrent().parent, "Should be on child greenlet"

        host, port = pair or self.pair

        # Don't try IPv6 if we don't support it. Also skip it if host
        # is 'localhost' (::1 is fine). Avoids slow connect issues
        # like PYTHON-356.
        family = socket.AF_INET
        if socket.has_ipv6 and host != 'localhost':
            family = socket.AF_UNSPEC

        err = None
        for res in socket.getaddrinfo(host, port, family, socket.SOCK_STREAM):
            af, socktype, proto, dummy, sa = res
            green_sock = None
            try:
                sock = socket.socket(af, socktype, proto)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                green_sock = GreenletSocket(
                    sock, self.io_loop, use_ssl=self.use_ssl)

                assert not self.conn_timeout, "Timeouts aren't supported"

                # GreenletSocket will pause the current greenlet and resume it
                # when connection has completed
                green_sock.connect(pair or self.pair)
                return green_sock
            except socket.error, e:
                err = e
                if green_sock is not None:
                    green_sock.close()

        if err is not None:
            raise err
        else:
            # This likely means we tried to connect to an IPv6 only
            # host with an OS/kernel or Python interpeter that doesn't
            # support IPv6.
            raise socket.error('getaddrinfo failed')

    def connect(self, pair):
        """Copy of BasePool.connect(), avoiding call to ssl.wrap_socket

           NOTE [adam Jan/9/12]: not positive why avoiding ssl.wrap_socket is
                needed, but since we don't use SSL, I'm just accepting it
        """
        green_sock = self.create_connection(pair)
        green_sock.settimeout(self.net_timeout)
        resp = pymongo.pool.SocketInfo(green_sock, self.pool_id)
        return resp

class GreenletClient(object):
    client = None

    @classmethod
    def sync_connect(cls, *args, **kwargs):
        """
            Makes a synchronous connection to pymongo using Greenlets

            This is sorta a hack, but it's necessary because there's not a
            great way to do the connect in Tornado asynchronously.
        """

        assert not greenlet.getcurrent().parent, "must be run on root greenlet"

        cls.client = pymongo.mongo_client.MongoClient(*args, **kwargs)
        cls.client.pool_class = GreenletPool

        return cls.client
