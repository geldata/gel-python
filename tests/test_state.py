#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from gel._internal import _testbase as tb

from gel import State


class TestState(tb.TestCase):
    def test_state_default(self):
        self.assertEqual(State.defaults().as_dict(), {})

    def test_state_default_module(self):
        s1 = State.defaults()
        s2 = s1.with_default_module("m2")
        s3 = s2.with_default_module("m3")
        s4 = s3.with_default_module()

        self.assertEqual(s1.as_dict(), {})
        self.assertEqual(s2.as_dict(), {"module": "m2"})
        self.assertEqual(s3.as_dict(), {"module": "m3"})
        self.assertEqual(s4.as_dict(), {})

    def test_state_module_aliaases(self):
        s1 = State.defaults().with_default_module("m")
        s2 = s1.with_module_aliases(x="a", m2="i")
        s3 = s2.with_module_aliases({"x": "b", "m3": "j"})
        s4 = s3.without_module_aliases("x", "m3")
        s5 = s3.without_module_aliases()

        self.assertEqual(s1.as_dict(), {"module": "m"})
        self.assertEqual(s2.as_dict()["module"], "m")
        self.assertListEqual(
            s2.as_dict()["aliases"], [("x", "a"), ("m2", "i")]
        )
        self.assertEqual(s3.as_dict()["module"], "m")
        self.assertListEqual(
            s3.as_dict()["aliases"],
            [("x", "b"), ("m2", "i"), ("m3", "j")],
        )
        self.assertEqual(s4.as_dict()["module"], "m")
        self.assertListEqual(s4.as_dict()["aliases"], [("m2", "i")])
        self.assertEqual(s5.as_dict(), {"module": "m"})

    def test_state_config(self):
        s1 = State.defaults().with_default_module("m")
        s2 = s1.with_config(x="a", m2="i")
        s3 = s2.with_config({"x": "b", "m3": "j"})
        s4 = s3.without_config("x", "m3")
        s5 = s3.without_config()

        self.assertDictEqual(s1.as_dict(), {"module": "m"})
        self.assertEqual(s2.as_dict()["module"], "m")
        self.assertDictEqual(s2.as_dict()["config"], {"x": "a", "m2": "i"})
        self.assertEqual(s3.as_dict()["module"], "m")
        self.assertDictEqual(
            s3.as_dict()["config"],
            {"x": "b", "m2": "i", "m3": "j"},
        )
        self.assertEqual(s4.as_dict()["module"], "m")
        self.assertDictEqual(s4.as_dict()["config"], {"m2": "i"})
        self.assertEqual(s5.as_dict(), {"module": "m"})

    def test_state_globals(self):
        s1 = (
            State.defaults()
            .with_default_module("m")
            .with_module_aliases(a="x", b="y")
        )
        s2 = s1.with_globals({"a::g2": "22"}, i=2)
        s3 = s2.with_globals({"i": 3, "b::g3": "33"})
        s4 = s3.without_globals("i", "y::g3")
        s5 = s3.without_globals()

        self.assertDictEqual(
            s1.as_dict(), {"module": "m", "aliases": [("a", "x"), ("b", "y")]}
        )
        self.assertEqual(s2.as_dict()["module"], "m")
        self.assertListEqual(s2.as_dict()["aliases"], [("a", "x"), ("b", "y")])
        self.assertDictEqual(
            s2.as_dict()["globals"], {"m::i": 2, "x::g2": "22"}
        )
        self.assertEqual(s3.as_dict()["module"], "m")
        self.assertListEqual(s3.as_dict()["aliases"], [("a", "x"), ("b", "y")])
        self.assertDictEqual(
            s3.as_dict()["globals"], {"m::i": 3, "x::g2": "22", "y::g3": "33"}
        )
        self.assertEqual(s4.as_dict()["module"], "m")
        self.assertListEqual(s4.as_dict()["aliases"], [("a", "x"), ("b", "y")])
        self.assertDictEqual(s4.as_dict()["globals"], {"x::g2": "22"})
        self.assertDictEqual(
            s5.as_dict(), {"module": "m", "aliases": [("a", "x"), ("b", "y")]}
        )

        self.assertDictEqual(
            s3.with_default_module("x")
            .with_globals(g2="2222")
            .as_dict()["globals"],
            {"m::i": 3, "x::g2": "2222", "y::g3": "33"},
        )
        self.assertDictEqual(
            s3.with_default_module("x")
            .without_globals("g2")
            .without_module_aliases()
            .with_module_aliases(n="y")
            .with_default_module("y")
            .with_globals({"m::i": 4}, g3="3333")
            .as_dict()["globals"],
            {"m::i": 4, "y::g3": "3333"},
        )
