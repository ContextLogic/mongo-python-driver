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

"""Test built in connection-pooling."""

import unittest
import threading
import os
import random
import sys
sys.path[0:0] = [""]

from pymongo.connection import Pool
from test_connection import get_connection

N = 50
DB = "pymongo-pooling-tests"

class MongoThread(threading.Thread):

    def __init__(self, test_case):
        threading.Thread.__init__(self)
        self.connection = test_case.c
        self.db = self.connection[DB]
        self.ut = test_case


class SaveAndFind(MongoThread):

    def run(self):
        for _ in xrange(N):
            rand = random.randint(0, N)
            id = self.db.sf.save({"x": rand})
            self.ut.assertEqual(rand, self.db.sf.find_one(id)["x"])
            self.connection.end_request()


class Unique(MongoThread):

    def run(self):
        for _ in xrange(N):
            self.db.unique.insert({})
            self.ut.assertEqual(None, self.db.error())
            self.connection.end_request()


class NonUnique(MongoThread):

    def run(self):
        for _ in xrange(N):
            self.db.unique.insert({"_id": "mike"})
            self.ut.assertNotEqual(None, self.db.error())
            self.connection.end_request()


class Disconnect(MongoThread):

    def run(self):
        for _ in xrange(N):
            self.connection.disconnect()


class NoRequest(MongoThread):

    def run(self):
        errors = 0
        for _ in xrange(N):
            self.db.unique.insert({"_id": "mike"})
            if self.db.error() is None:
                errors += 1

        self.ut.assertEqual(0, errors)


def run_cases(ut, cases):
    threads = []
    for case in cases:
        for i in range(10):
            thread = case(ut)
            thread.start()
            threads.append(thread)

    for t in threads:
        t.join()


class OneOp(threading.Thread):

    def __init__(self, connection):
        threading.Thread.__init__(self)
        self.c = connection

    def run(self):
        assert len(self.c._Connection__pool.sockets) == 1
        self.c.test.test.find_one()
        assert len(self.c._Connection__pool.sockets) == 0
        self.c.end_request()
        assert len(self.c._Connection__pool.sockets) == 1


class TestPooling(unittest.TestCase):

    def setUp(self):
        self.c = get_connection()

        # reset the db
        self.c.drop_database(DB)
        self.c[DB].unique.insert({"_id": "mike"})
        self.c[DB].unique.find_one()

    def test_no_disconnect(self):
        run_cases(self, [NoRequest, NonUnique, Unique, SaveAndFind])

    def test_disconnect(self):
        run_cases(self, [SaveAndFind, Disconnect, Unique])

    def test_independent_pools(self):
        p = Pool(None)
        self.assertEqual([], p.sockets)
        self.c.end_request()
        self.assertEqual([], p.sockets)

        # Sensical values aren't really important here
        p1 = Pool(5)
        self.assertEqual(None, p.socket_factory)
        self.assertEqual(5, p1.socket_factory)

    def test_dependent_pools(self):
        c = get_connection()
        self.assertEqual(0, len(c._Connection__pool.sockets))
        c.test.test.find_one()
        self.assertEqual(0, len(c._Connection__pool.sockets))
        c.end_request()
        self.assertEqual(1, len(c._Connection__pool.sockets))

        t = OneOp(c)
        t.start()
        t.join()

        self.assertEqual(1, len(c._Connection__pool.sockets))
        c.test.test.find_one()
        self.assertEqual(0, len(c._Connection__pool.sockets))


if __name__ == "__main__":
    unittest.main()
