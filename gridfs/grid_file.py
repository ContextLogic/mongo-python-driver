# Copyright 2009-2010 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tools for representing files stored in GridFS."""

import datetime
import os
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

from gridfs.errors import (CorruptGridFile,
                           NoFile,
                           UnsupportedAPI)
from pymongo.binary import Binary
from pymongo.collection import Collection
from pymongo.objectid import ObjectId

try:
    _SEEK_SET = os.SEEK_SET
    _SEEK_CUR = os.SEEK_CUR
    _SEEK_END = os.SEEK_END
except AttributeError: # before 2.5
    _SEEK_SET = 0
    _SEEK_CUR = 1
    _SEEK_END = 2


"""Default chunk size, in bytes."""
DEFAULT_CHUNK_SIZE = 256 * 1024


def _create_property(field_name, docstring,
                      read_only=False, closed_only=False):
    """Helper for creating properties to read/write to files.
    """
    def getter(self):
        if closed_only and not self._closed:
            raise AttributeError("can only get %r on a closed file" %
                                 field_name)
        return self._file.get(field_name, None)
    def setter(self, value):
        if self._closed:
            raise AttributeError("cannot set %r on a closed file" %
                                 field_name)
        self._file[field_name] = value

    if read_only:
        docstring = docstring + "\n\nThis attribute is read-only."""
    elif not closed_only:
        docstring = docstring + "\n\nThis attribute can only be set before :meth:`close` has been called."""
    else:
        docstring = docstring + "\n\nThis attribute is read-only and can only be read after :meth:`close` has been called."""

    if not read_only and not closed_only:
        return property(getter, setter, doc=docstring)
    return property(getter, doc=docstring)


class GridIn(object):
    """Class to write data to GridFS.
    """
    def __init__(self, root_collection, **kwargs):
        """Write a file to GridFS

        Application developers should generally not need to
        instantiate this class directly - instead see the methods
        provided by :class:`~gridfs.GridFS`.

        Raises :class:`TypeError` if `root_collection` is not an
        instance of :class:`~pymongo.collection.Collection`.

        Any of the file level options specified in the `GridFS Spec
        <http://dochub.mongodb.org/core/gridfsspec>`_ may be passed as
        keyword arguments. Any additional keyword arguments will be
        set as fields in the ``"metadata"`` embedded document allowed
        by the spec (overwriting any existing ``"metadata"`` field
        with the same name). Valid keyword arguments include:

          - ``"_id"``: unique ID for this file (default:
            :class:`~pymongo.objectid.ObjectId`)

          - ``"filename"``: human name for the file

          - ``"contentType"`` or ``"content_type"``: valid mime-type
            for the file

          - ``"chunkSize"`` or ``"chunk_size"``: size of each of the
            chunks, in bytes (default: 256 kb)

          - ``"aliases"``: array of alias strings

          - ``"metadata"``: document containing arbitrary metadata

        :Parameters:
          - `root_collection`: root collection to write to
          - `**kwargs` (optional): file level options (see above)
        """
        if not isinstance(root_collection, Collection):
            raise TypeError("root_collection must be an instance of Collection")

        # Handle alternative naming
        if "content_type" in kwargs:
            kwargs["contentType"] = kwargs.pop("content_type")
        if "chunk_size" in kwargs:
            kwargs["chunkSize"] = kwargs.pop("chunk_size")

        # Move bonus kwargs into metadata
        to_move = []
        for key in kwargs:
            if key not in ["_id", "filename", "contentType",
                           "chunkSize", "aliases", "metadata"]:
                to_move.append(key)
        if to_move:
            kwargs["metadata"] = kwargs.get("metadata", {})
            for key in to_move:
                kwargs["metadata"][key] = kwargs.pop(key)

        # Defaults
        kwargs["_id"] = kwargs.get("_id", ObjectId())
        kwargs["chunkSize"] = kwargs.get("chunkSize", DEFAULT_CHUNK_SIZE)

        self.__coll = root_collection
        self.__chunks = root_collection.chunks
        self._file = kwargs
        self.__buffer = StringIO()
        self.__position = 0
        self.__chunk_number = 0
        self._closed = False

    @property
    def closed(self):
        """Is this file closed?
        """
        return self._closed

    _id = _create_property("_id", "The ``'_id'`` value for this file.",
                            read_only=True)
    name = _create_property("filename", "Name of this file.")
    content_type = _create_property("contentType", "Mime-type for this file.")
    length = _create_property("length", "Length (in bytes) of this file.",
                               closed_only=True)
    chunk_size = _create_property("chunkSize", "Chunk size for this file.",
                                   read_only=True)
    upload_date = _create_property("uploadDate",
                                    "Date that this file was uploaded.",
                                    closed_only=True)
    aliases = _create_property("aliases", "List of aliases for this file.")
    metadata = _create_property("metadata", "Metadata attached to this file.")
    md5 = _create_property("md5", "MD5 of the contents of this file "
                            "(generated on the server).",
                            closed_only=True)

    def __flush_data(self, data):
        """Flush `data` to a chunk.
        """
        if not data:
            return
        assert(len(data) <= self.chunk_size)

        chunk = {"files_id": self._file["_id"],
                 "n": self.__chunk_number,
                 "data": Binary(data)}

        self.__chunks.insert(chunk)
        self.__chunk_number += 1
        self.__position += len(data)

    def __flush_buffer(self):
        """Flush the buffer contents out to a chunk.
        """
        self.__flush_data(self.__buffer.getvalue())
        self.__buffer.close()
        self.__buffer = StringIO()

    def __flush(self):
        """Flush the file to the database.
        """
        self.__flush_buffer()

        md5 = self.__coll.database.command("filemd5", self._id,
                                           root=self.__coll.name)["md5"]

        self._file["md5"] = md5
        self._file["length"] = self.__position
        self._file["uploadDate"] = datetime.datetime.utcnow()
        return self.__coll.files.insert(self._file)

    def close(self):
        """Flush the file and close it.

        A closed file cannot be written any more. Calling
        :meth:`close` more than once is allowed.
        """
        if not self._closed:
            self.__flush()
        self._closed = True

    # TODO should support writing unicode to a file. this means that files will
    # need to have an encoding attribute.
    def write(self, data):
        """Write a string to the file. There is no return value.

        Due to buffering, the string may not actually be written to
        the database until the :meth:`close` method is called. Raises
        :class:`ValueError` if this file is already closed. Raises
        :class:`TypeErrer` if `data` is not an instance of
        :class:`str`.

        :Parameters:
          - `data`: string of bytes to be written to the file
        """
        if self._closed:
            raise ValueError("cannot write to a closed file")

        if not isinstance(data, str):
            raise TypeError("can only write strings")

        while data:
            space = self.chunk_size - self.__buffer.tell()

            if len(data) <= space:
                self.__buffer.write(data)
                break
            else:
                self.__buffer.write(data[:space])
                self.__flush_buffer()
                data = data[space:]

    def writelines(self, sequence):
        """Write a sequence of strings to the file.

        Does not add seperators.
        """
        for line in sequence:
            self.write(line)

    def __enter__(self):
        """Support for the context manager protocol.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Support for the context manager protocol.

        Close the file and allow exceptions to propogate.
        """
        self.close()
        return False # propogate exceptions


class GridOut(object):
    """Class to read data out of GridFS.
    """
    def __init__(self, root_collection, file_id):
        """Read a file from GridFS

        Application developers should generally not need to
        instantiate this class directly - instead see the methods
        provided by :class:`~gridfs.GridFS`.

        Raises :class:`TypeError` if `root_collection` is not an instance of
        :class:`~pymongo.collection.Collection`.

        :Parameters:
          - `root_collection`: root collection to read from
          - `file_id`: value of ``"_id"`` for the file to read
        """
        if not isinstance(root_collection, Collection):
            raise TypeError("root_collection must be an instance of Collection")

        self.__chunks = root_collection.chunks
        self._file = root_collection.files.find_one({"_id": file_id})

        if not self._file:
            raise NoFile("no file in gridfs collection %r with _id %r" %
                         (root_collection, file_id))

        self.__buffer = ""
        self.__position = 0

    _id = _create_property("_id", "The ``'_id'`` value for this file.", True)
    name = _create_property("filename", "Name of this file.", True)
    content_type = _create_property("contentType", "Mime-type for this file.",
                                     True)
    length = _create_property("length", "Length (in bytes) of this file.",
                               True)
    chunk_size = _create_property("chunkSize", "Chunk size for this file.",
                                   True)
    upload_date = _create_property("uploadDate",
                                    "Date that this file was first uploaded.",
                                    True)
    aliases = _create_property("aliases", "List of aliases for this file.",
                                True)
    metadata = _create_property("metadata", "Metadata attached to this file.",
                                 True)
    md5 = _create_property("md5", "MD5 of the contents of this file "
                            "(generated on the server).", True)

    def read(self, size=-1):
        """Read at most `size` bytes from the file (less if there
        isn't enough data).

        The bytes are returned as an instance of :class:`str`. If
        `size` is negative or omitted all data is read.

        :Parameters:
          - `size` (optional): the number of bytes to read
        """
        if size == 0:
            return ""

        remainder = int(self.length) - self.__position
        if size < 0 or size > remainder:
            size = remainder

        data = self.__buffer
        chunk_number = (len(data) + self.__position) / self.chunk_size

        while len(data) < size:
            chunk = self.__chunks.find_one({"files_id": self._id,
                                            "n": chunk_number})
            if not chunk:
                raise CorruptGridFile("no chunk for n = " + chunk_number)

            if not data:
                data += chunk["data"][self.__position % self.chunk_size:]
            else:
                data += chunk["data"]

            chunk_number += 1

        self.__position += size
        to_return = data[:size]
        self.__buffer = data[size:]
        return to_return

    def tell(self):
        """Return the current position of this file.
        """
        return self.__position

    def seek(self, pos, whence=_SEEK_SET):
        """Set the current position of this file.

        :Parameters:
         - `pos`: the position (or offset if using relative
           positioning) to seek to
         - `whence` (optional): where to seek
           from. :attr:`os.SEEK_SET` (``0``) for absolute file
           positioning, :attr:`os.SEEK_CUR` (``1``) to seek relative
           to the current position, :attr:`os.SEEK_END` (``2``) to
           seek relative to the file's end.
        """
        if whence == _SEEK_SET:
            new_pos = pos
        elif whence == _SEEK_CUR:
            new_pos = self.__position + pos
        elif whence == _SEEK_END:
            new_pos = int(self.length) + pos
        else:
            raise IOError(22, "Invalid value for `whence`")

        if new_pos < 0:
            raise IOError(22, "Invalid value for `pos` - must be positive")

        self.__position = new_pos
        self.__buffer = ""


class GridFile(object):
    """No longer supported.

    .. versionchanged:: 1.5.1+
       The GridFile class is no longer supported.
    """
    def __init__(self, *args, **kwargs):
        raise UnsupportedAPI("The GridFile class is no longer supported. "
                             "Please use GridIn or GridOut instead.")
