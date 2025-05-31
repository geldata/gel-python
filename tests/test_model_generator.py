#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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

import dataclasses
import os
import typing
import unittest

if typing.TYPE_CHECKING:
    from typing import reveal_type

from gel import _testbase as tb

from gel._internal._qbmodel._pydantic._models import Pointer, GelModel
from gel._internal._dlist import DistinctList
from gel._internal._edgeql import Cardinality, PointerKind


@dataclasses.dataclass
class Q:
    type: typing.Type
    query: str

    def __edgeql__(self):
        return (self.type, self.query)


class MockPointer(typing.NamedTuple):
    name: str
    cardinality: Cardinality
    computed: bool
    has_props: bool
    kind: PointerKind
    readonly: bool
    type: type


class TestModelGenerator(tb.ModelTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), "dbsetup", "base.esdl")

    SETUP = os.path.join(os.path.dirname(__file__), "dbsetup", "base.edgeql")

    def assert_pointers_match(
        self, obj: type[GelModel], expected: list[MockPointer]
    ):
        ptrs = list(obj.__gel_pointers__().values())
        ptrs.sort(key=lambda x: x.name)

        expected.sort(key=lambda x: x.name)

        with self.subTest(obj=obj):
            for e, p in zip(expected, ptrs, strict=True):
                with self.subTest(prop_name=p.name, test="eq_name"):
                    self.assertEqual(
                        e.name,
                        p.name,
                        f"{obj.__name__} name mismatch",
                    )
                with self.subTest(prop_name=p.name, test="eq_cardinality"):
                    self.assertEqual(
                        e.cardinality,
                        p.cardinality,
                        f"{obj.__name__}.{p.name} cardinality mismatch",
                    )
                with self.subTest(prop_name=p.name, test="eq_computed"):
                    self.assertEqual(
                        e.computed,
                        p.computed,
                        f"{obj.__name__}.{p.name} computed mismatch",
                    )
                with self.subTest(prop_name=p.name, test="eq_has_props"):
                    self.assertEqual(
                        e.has_props,
                        p.has_props,
                        f"{obj.__name__}.{p.name} has_props mismatch",
                    )
                with self.subTest(prop_name=p.name, test="eq_kind"):
                    self.assertEqual(
                        e.kind,
                        p.kind,
                        f"{obj.__name__}.{p.name} kind mismatch",
                    )
                with self.subTest(prop_name=p.name, test="eq_readonly"):
                    self.assertEqual(
                        e.readonly,
                        p.readonly,
                        f"{obj.__name__}.{p.name} readonly mismatch",
                    )
                with self.subTest(prop_name=p.name, test="eq_type"):
                    self.assertTrue(
                        issubclass(p.type, e.type),
                        f"{obj.__name__}.{p.name} eq_type check failed: "
                        f"issubclass({p.type!r}, {e.type!r}) is False",
                    )

    @tb.must_fail
    @tb.typecheck
    def test_modelgen__smoke_test(self):
        from models import default

        self.assertEqual(reveal_type(default.User.groups), "this must fail")

    @tb.typecheck
    def test_modelgen_1(self):
        from models import default

        self.assertEqual(
            reveal_type(default.User.name), "type[models.__variants__.std.str]"
        )

        self.assertEqual(
            reveal_type(default.User.groups), "type[models.default.UserGroup]"
        )

    def test_modelgen_data_unpack_1(self):
        from models import default

        q = Q(
            default.Post,
            """
            select Post {
              body,
              author: {
                *
              }
            } filter .body = 'Hello' limit 1
            """,
        )

        d = self.client.query_single(q)

        self.assertIsInstance(d, default.Post)
        self.assertEqual(d.body, "Hello")
        self.assertIsInstance(d.author, default.User)
        self.assertEqual(d.author.name, "Alice")

    @tb.to_be_fixed
    @tb.typecheck
    def test_modelgen_data_unpack_1b(self):
        from models import default

        q = (
            default.Post.select(
                body=True,
                author=lambda p: p.author.select(name=True),
            )
            .filter(lambda p: p.body == "Hello")
            .limit(1)
        )
        d = self.client.query_single(q)

        self.assertEqual(reveal_type(d), "Union[models.default.Post, None]")
        assert d is not None

        self.assertEqual(
            reveal_type(d.id), "type[models.__variants__.std.uuid]"
        )

        self.assertIsInstance(d, default.Post)
        self.assertEqual(d.body, "Hello")
        self.assertIsInstance(d.author, default.User)

        assert d.author is not None
        self.assertEqual(d.author.name, "Alice")

    def test_modelgen_data_unpack_1c(self):
        from models import default, std

        class MyUser(default.User):
            posts: std.int64

        q = (
            MyUser.select(
                name=True,
                posts=lambda u: std.count(
                    default.Post.filter(lambda p: p.author.id == u.id)
                ),
            )
            .filter(name="Alice")
            .limit(1)
        )
        d = self.client.query_single(q)

        self.assertIsInstance(d, default.User)
        self.assertEqual(d.posts, 2)

        q = (
            MyUser.select(
                name=True,
                posts=lambda u: std.count(
                    default.Post.filter(lambda p: p.author == u)
                ),
            )
            .filter(name="Alice")
            .limit(1)
        )
        d = self.client.query_single(q)

        self.assertIsInstance(d, default.User)
        self.assertEqual(d.posts, 2)

    def test_modelgen_data_unpack_2(self):
        from models import default

        q = default.Post.select().filter(body="Hello")
        d = self.client.query(q)[0]
        self.assertIsInstance(d, default.Post)

    def test_modelgen_data_unpack_3(self):
        from models import default

        q = (
            default.GameSession.select(
                num=True,
                players=lambda s: s.players.select(
                    name=True, groups=lambda p: p.groups.select(name=True)
                ),
            )
            .filter(num=123)
            .limit(1)
        )

        d = self.client.query(q)[0]

        self.assertIsInstance(d, default.GameSession)

        # Test that links are unpacked into a DistinctList, not a vanilla list
        self.assertIsInstance(d.players, DistinctList)

        post = default.Post(author=d.players[0], body="test")

        # Check that validation is enabled for objects created by codecs

        with self.assertRaisesRegex(
            ValueError, r"accepts only values of type.*User.*, got.*Post"
        ):
            d.players.append(post)

        with self.assertRaisesRegex(
            ValueError, r"(?s)xxx.*Object has no attribute 'xxx'"
        ):
            post.xxx = 123

    @tb.typecheck
    def test_modelgen_data_unpack_4(self):
        from models import default

        q = default.Post.select(
            author=True,
        ).limit(1)

        d = self.client.query_required_single(q)

        with self.assertRaisesRegex(AttributeError, r".body. is not set"):
            print(d.body)

    def test_modelgen_data_model_validation_1(self):
        from models import default

        gs = default.GameSession(num=7)
        self.assertIsInstance(gs.players, DistinctList)

        with self.assertRaisesRegex(
            ValueError, r"(?s)players.*Input should be.*instance of User"
        ):
            default.GameSession(num=7, players=[1])

        with self.assertRaisesRegex(
            ValueError, r"(?s)prayers.*Extra inputs are not permitted"
        ):
            default.GameSession(num=7, prayers=[1])

    def test_modelgen_reflection_1(self):
        from models import default, std

        from gel._internal._edgeql import Cardinality, PointerKind
        from gel._internal._qbmodel._pydantic._fields import (
            _UpcastingDistinctList,
        )

        self.assert_pointers_match(
            default.User,
            [
                MockPointer(
                    name="groups",
                    cardinality=Cardinality.One,
                    computed=False,
                    has_props=False,
                    kind=PointerKind.Link,
                    readonly=False,
                    # XXX - there's no need for UpcastingDistinctList here
                    type=_UpcastingDistinctList[
                        default.UserGroup, default.UserGroup
                    ],
                ),
                MockPointer(
                    name="id",
                    cardinality=Cardinality.One,
                    computed=True,
                    has_props=False,
                    kind=PointerKind.Property,
                    readonly=True,
                    type=std.uuid,
                ),
                MockPointer(
                    name="name",
                    cardinality=Cardinality.One,
                    computed=False,
                    has_props=False,
                    kind=PointerKind.Property,
                    readonly=False,
                    type=std.str,
                ),
            ],
        )
