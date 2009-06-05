# Copyright 2009 10gen, Inc.
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

"""Tests for the Code wrapper."""

import unittest
import sys
sys.path[0:0] = [""]

from pymongo.code import Code


class TestCode(unittest.TestCase):

    def setUp(self):
        pass

    def test_types(self):
        self.assertRaises(TypeError, Code, 5)
        self.assertRaises(TypeError, Code, None)
        self.assertRaises(TypeError, Code, "aoeu", 5)
        self.assertRaises(TypeError, Code, u"aoeu", 5)
        self.assert_(Code("aoeu"))
        self.assert_(Code(u"aoeu"))
        self.assert_(Code("aoeu", {}))
        self.assert_(Code(u"aoeu", {}))

    def test_read_only(self):
        c = Code("blah")

        def set_c():
            c.scope = 5
        self.assertRaises(AttributeError, set_c)

    def test_code(self):
        a_string = "hello world"
        a_code = Code("hello world")
        self.assert_(a_code.startswith("hello"))
        self.assert_(a_code.endswith("world"))
        self.assert_(isinstance(a_code, Code))
        self.failIf(isinstance(a_string, Code))
        self.assertEqual(a_code.scope, {})
        a_code.scope["my_var"] = 5
        self.assertEqual(a_code.scope, {"my_var": 5})

    def test_repr(self):
        c = Code("hello world")
        self.assertEqual(repr(c), "Code('hello world', {})")
        c.scope["foo"] = "bar"
        self.assertEqual(repr(c), "Code('hello world', {'foo': 'bar'})")
        c = Code("hello world", {"blah": 3})
        self.assertEqual(repr(c), "Code('hello world', {'blah': 3})")
        c = Code("\x08\xFF")
        self.assertEqual(repr(c), "Code('\\x08\\xff', {})")

if __name__ == "__main__":
    unittest.main()
