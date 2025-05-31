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
import types
import typing
import unittest

if typing.TYPE_CHECKING:
    from typing import reveal_type

from gel import _testbase as tb

from gel._internal._qbmodel._pydantic._models import Pointer, GelModel
from gel._internal._dlist import DistinctList
from gel._internal._edgeql import Cardinality, PointerKind
from gel._internal._qbmodel._pydantic._fields import (
    _UpcastingDistinctList,
)


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

        for e, p in zip(expected, ptrs, strict=True):
            self.assertEqual(
                e.name,
                p.name,
                f"{obj.__name__} name mismatch",
            )
            self.assertEqual(
                e.cardinality,
                p.cardinality,
                f"{obj.__name__}.{p.name} cardinality mismatch",
            )
            self.assertEqual(
                e.computed,
                p.computed,
                f"{obj.__name__}.{p.name} computed mismatch",
            )
            self.assertEqual(
                e.has_props,
                p.has_props,
                f"{obj.__name__}.{p.name} has_props mismatch",
            )
            self.assertEqual(
                e.kind,
                p.kind,
                f"{obj.__name__}.{p.name} kind mismatch",
            )
            self.assertEqual(
                e.readonly,
                p.readonly,
                f"{obj.__name__}.{p.name} readonly mismatch",
            )

            if isinstance(p.type, type) and isinstance(e.type, type):
                if issubclass(e.type, DistinctList):
                    if not issubclass(p.type, DistinctList):
                        self.fail(
                            f"{obj.__name__}.{p.name} eq_type check failed: "
                            f"p.type is not a DistinctList, but expected "
                            f"type is {e.type!r}",
                        )

                if issubclass(p.type, _UpcastingDistinctList):
                    if not issubclass(e.type, _UpcastingDistinctList):
                        self.fail(
                            f"{obj.__name__}.{p.name} eq_type check "
                            f" failed: p.type is _UpcastingDistinctList, "
                            f"but expected type is {e.type!r}",
                        )
                else:
                    if issubclass(e.type, _UpcastingDistinctList):
                        self.fail(
                            f"{obj.__name__}.{p.name} eq_type check failed: "
                            f"p.type is not a _UpcastingDistinctList, but "
                            f"expected type is {e.type!r}",
                        )

                if not issubclass(p.type.type, e.type.type):
                    self.fail(
                        f"{obj.__name__}.{p.name} eq_type check failed: "
                        f"p.type.type is not a {e.type.type!r} subclass"
                    )
                else:
                    self.assertTrue(
                        issubclass(p.type, e.type),
                        f"{obj.__name__}.{p.name} eq_type check failed: "
                        f"issubclass({p.type!r}, {e.type!r}) is False",
                    )
            else:
                self.assertEqual(
                    e.type,
                    p.type,
                    f"{obj.__name__}.{p.name} eq_type check failed: "
                    f"{p.type!r} != {e.type!r}",
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

        q = self.client.query_required_single(
            default.User.select(groups=True).limit(1)
        )

        self.assertEqual(
            reveal_type(q.groups),
            "builtins.tuple[models.default.UserGroup, ...]",
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

    def test_modelgen_data_unpack_1b(self):
        from models import default

        q = (
            default.Post.select(
                body=True,
                author=True,
            )
            .filter(lambda p: p.body == "Hello")
            .limit(1)
        )
        d = self.client.query_single(q)
        assert d is not None

        self.assertIsInstance(d, default.Post)
        self.assertEqual(d.body, "Hello")
        self.assertIsInstance(d.author, default.User)

        assert d.author is not None
        self.assertEqual(d.author.name, "Alice")

    @tb.to_be_fixed
    @tb.typecheck
    def test_modelgen_data_unpack_1b_tc(self):
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
                    name=True,
                    groups=lambda p: p.groups.select(name=True).order_by(
                        name="asc"
                    ),
                ),
            )
            .filter(num=123)
            .limit(1)
        )

        d = self.client.query(q)[0]

        self.assertIsInstance(d, default.GameSession)

        # Test that links are unpacked into a DistinctList, not a vanilla list
        self.assertIsInstance(d.players, DistinctList)
        self.assertIsInstance(d.players[0].groups, tuple)

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
            d.body

    def test_modelgen_data_model_validation_1(self):
        from models import default

        gs = default.GameSession(num=7)
        self.assertIsInstance(gs.players, DistinctList)

        with self.assertRaisesRegex(
            ValueError, r"(?s)only instances of User are allowed, got .*int"
        ):
            default.GameSession.players(1)

        p = default.Post(body="aaa")
        with self.assertRaisesRegex(
            ValueError, r"(?s)prayers.*Extra inputs are not permitted"
        ):
            default.GameSession(num=7, prayers=[p])

    def test_modelgen_reflection_1(self):
        from models import default, std

        from gel._internal._edgeql import Cardinality, PointerKind

        self.assert_pointers_match(
            default.User,
            [
                MockPointer(
                    name="groups",
                    cardinality=Cardinality.Many,
                    computed=True,
                    has_props=False,
                    kind=PointerKind.Link,
                    readonly=True,
                    type=tuple[default.UserGroup, ...],
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

        self.assert_pointers_match(
            default.GameSession,
            [
                MockPointer(
                    name="num",
                    cardinality=Cardinality.One,
                    computed=False,
                    has_props=False,
                    kind=PointerKind.Property,
                    readonly=False,
                    type=std.int64,
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
                    name="players",
                    cardinality=Cardinality.Many,
                    computed=False,
                    has_props=True,
                    kind=PointerKind.Link,
                    readonly=False,
                    type=_UpcastingDistinctList[
                        default.GameSession.__links__.players, default.User
                    ],
                ),
            ],
        )
