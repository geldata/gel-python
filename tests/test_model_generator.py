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

if typing.TYPE_CHECKING:
    from typing import reveal_type

from gel import _testbase as tb

from gel._internal._qbmodel._pydantic._models import GelModel
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
    SCHEMA = os.path.join(os.path.dirname(__file__), "dbsetup", "orm.gel")

    SETUP = os.path.join(os.path.dirname(__file__), "dbsetup", "orm.edgeql")

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

    def test_modelgen_data_unpack_1a(self):
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
        d = self.client.get(q)

        self.assertEqual(reveal_type(d), "models.default.Post")

        self.assertEqual(reveal_type(d.id), "models.__variants__.std.uuid")

        self.assertIsInstance(d, default.Post)
        self.assertEqual(d.body, "Hello")
        self.assertIsInstance(d.author, default.User)

        assert d.author is not None
        self.assertEqual(d.author.name, "Alice")

    @tb.typecheck
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
        d = self.client.query_required_single(q)

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
        d = self.client.query_required_single(q)

        self.assertIsInstance(d, default.User)
        self.assertEqual(d.posts, 2)

    @tb.typecheck
    def test_modelgen_data_unpack_2(self):
        from models import default

        q = default.Post.select().filter(body="Hello")
        d = self.client.query(q)[0]
        self.assertIsInstance(d, default.Post)

    @tb.typecheck
    def test_modelgen_data_unpack_3(self):
        from models import default
        from gel._internal._dlist import DistinctList

        q = (
            default.GameSession.select(
                num=True,
                players=lambda s: s.players.select(
                    name=True,
                    groups=lambda p: p.groups.select(name=True).order_by(
                        name="asc",
                        id=("desc", "empty first"),
                    ).order_by(
                        lambda u: u.name,
                        (lambda u: u.name, "asc"),
                        (lambda u: u.name, "asc", "empty last"),
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
            d.players.append(post)  # type: ignore [arg-type]

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

    @tb.typecheck
    def test_modelgen_assert_single(self):
        from models import default
        from gel import errors

        q = default.Post.limit(1).__gel_assert_single__()
        d = self.client.query(q)[0]
        self.assertIsInstance(d, default.Post)

        with self.assertRaisesRegex(
            errors.CardinalityViolationError,
            "Post is not single",
        ):
            q = default.Post.__gel_assert_single__(
                message="Post is not single",
            )
            self.client.query(q)

    @tb.typecheck
    def test_modelgen_data_model_validation_1(self):
        from typing import cast
        from gel._internal._dlist import DistinctList
        from models import default
        from models import std

        gs = default.GameSession(num=7)
        self.assertIsInstance(gs.players, DistinctList)

        with self.assertRaisesRegex(
            ValueError, r"(?s)only instances of User are allowed, got .*int"
        ):
            default.GameSession.players(1)  # type: ignore

        p = default.Post(body="aaa")
        with self.assertRaisesRegex(
            ValueError, r"(?s)prayers.*Extra inputs are not permitted"
        ):
            default.GameSession(num=7, prayers=[p])  # type: ignore

        # Check that `groups` is not an allowed keyword-arg for `User.__init__`
        self.assertEqual(
            reveal_type(default.User),
            "def (*, name: builtins.str, nickname: Union[builtins.str, None] =) "
            "-> models.default.User",
        )

        # This also tests that "required computeds" are not "required" as
        # args to `__init__`, and this wasn't straightforward to fix.
        u = self.client.query_required_single(
            default.User.select(
                name=True,
                nickname=True,
                name_len=True,
                nickname_len=True,
            ).limit(1)
        )

        # Check that `groups` is not an allowed keyword-arg for `User.update`
        self.assertEqual(
            reveal_type(u.update),
            "def (*, "
            "name: Union[builtins.str, type[models.__variants__.std.str], "
            "gel._internal._utils.UnspecifiedType] =, "
            "nickname: Union[builtins.str, type[models.__variants__.std.str], "
            "gel._internal._utils.UnspecifiedType] =)"
            " -> type[models.default.User]",
        )

        self.assertEqual(
            reveal_type(u.id),
            "models.__variants__.std.uuid",
        )

        self.assertEqual(
            reveal_type(u.name),
            "models.__variants__.std.str",
        )

        self.assertEqual(
            reveal_type(u.nickname),
            "Union[models.__variants__.std.str, None]",
        )

        self.assertEqual(
            reveal_type(u.name_len),
            "models.__variants__.std.int64",
        )

        self.assertEqual(
            reveal_type(u.nickname_len),
            "Union[models.__variants__.std.int64, None]",
        )

        # Let's test computed link as an arg
        with self.assertRaisesRegex(
            ValueError, r"(?s)cannot set field .groups. on User"
        ):
            default.User(name="aaaa", groups=(1, 2, 3))  # type: ignore

        # Let's test computed property as an arg
        with self.assertRaisesRegex(
            ValueError, r"(?s)cannot set field .name_len. on User"
        ):
            default.User(name="aaaa", name_len=123)  # type: ignore

        u = default.User(name="aaaa")
        u.name = "aaaaaaa"

        with self.assertRaisesRegex(
            AttributeError, r"(?s).name_len. is not set"
        ):
            u.name_len

        with self.assertRaisesRegex(
            ValueError, r"(?s)name_len.*Field is frozen"
        ):
            u.name_len = cast(std.int64, 123)  # type: ignore[assignment]

    def test_modelgen_save_1(self):
        from models import default

        pq = (
            default.Post.select(
                *default.Post,
                author=True,
            )
            .filter(lambda p: p.body == "I'm Alice")
            .limit(1)
        )

        p = self.client.query_required_single(pq)

        self.assertEqual(p.author.name, "Alice")
        self.assertEqual(p.body, "I'm Alice")

        p.author.name = "Alice the 5th"
        p.body = "I'm Alice the 5th"

        self.client.save(p)
        self.client.save(p)  # should be no op

        p2 = self.client.query_required_single("""
            select Post {body, author: {name}}
            filter .author.name = 'Alice the 5th' and
                    .body = "I'm Alice the 5th"
            limit 1
        """)

        self.assertEqual(p2.body, "I'm Alice the 5th")
        self.assertEqual(p2.author.name, "Alice the 5th")

        a = default.User(name="New Alice")
        p.author = a
        self.client.save(p)
        self.client.save(p)  # should be no op

        p2 = self.client.query_required_single("""
            with
                post := assert_single((
                    select Post
                    filter .author.name = 'New Alice' and
                            .body = "I'm Alice the 5th"
                )),
                alice := assert_single((
                    select User {name} filter .name = 'Alice the 5th'
                )),
                new_alice := assert_single((
                    select User {name} filter .name = 'New Alice'
                ))

            select {
                post := post {body, author: {name}},
                alice := alice {name},
                new_alice := new_alice {name},
            }
        """)

        self.assertEqual(p2.post.body, "I'm Alice the 5th")
        self.assertEqual(p2.post.author.name, "New Alice")
        self.assertEqual(p2.alice.name, "Alice the 5th")
        self.assertEqual(p2.new_alice.name, "New Alice")

    @tb.to_be_fixed
    def test_modelgen_linkprops_1(self):
        from models import default

        # Create a new GameSession and add a player
        u = self.client.get(default.User.filter(name='Zoe'))
        gs = default.GameSession(
            num=1001,
            players=[
                default.GameSession.players.link(
                    u, is_tall_enough=True)
            ]
        )
        self.client.save(gs)

        # Now fetch it again
        res = self.client.get(
            default.GameSession.select(
                num=True,
                players=True,
            ).filter(num=1001)
        )
        self.assertEqual(res.num, 1001)
        self.assertEqual(len(res.players), 1)
        p = res.players[0]

        self.assertEqual(p.name, 'Zoe')
        self.assertEqual(p.__linkprops__.is_tall_enough, True)

    @tb.to_be_fixed
    def test_modelgen_linkprops_2(self):
        from models import default

        # Create a new GameSession and add a player
        u = self.client.get(default.User.filter(name='Elsa'))
        gs = default.GameSession(num=1002)
        gs.players.append(u)
        self.client.save(gs)

        # Now fetch it again snd update
        gs = self.client.get(
            default.GameSession.select(
                num=True,
                players=True,
            ).filter(num=1002)
        )
        self.assertEqual(gs.num, 1002)
        self.assertEqual(len(gs.players), 1)
        self.assertEqual(gs.players[0].__linkprops__.is_tall_enough, None)
        gs.players[0].__linkprops__.is_tall_enough = False
        self.client.save(gs)

        # Now fetch after update
        res = self.client.get(
            default.GameSession.select(
                num=True,
                players=True,
            ).filter(num=1002)
        )
        self.assertEqual(res.num, 1002)
        self.assertEqual(len(res.players), 1)
        p = res.players[0]

        self.assertEqual(p.name, 'Elsa')
        self.assertEqual(p.__linkprops__.is_tall_enough, False)

    @tb.to_be_fixed
    def test_modelgen_linkprops_3(self):
        from models import default

        # This one only has a single player
        q = default.GameSession.select(
            num=True,
            players=True,
        ).filter(num=456)
        res = self.client.get(q)

        self.assertEqual(res.num, 456)
        self.assertEqual(len(res.players), 1)
        p0 = res.players[0]

        self.assertEqual(p0.name, 'Dana')
        self.assertEqual(p0.nickname, None)
        self.assertEqual(p0.__linkprops__.is_tall_enough, True)

        p0.name = 'Dana?'
        p0.nickname = 'HACKED'
        p0.__linkprops__.is_tall_enough = False

        self.client.save(res)

        # Now fetch it again
        upd = self.client.get(q)
        self.assertEqual(upd.num, 456)
        self.assertEqual(len(upd.players), 1)
        p1 = upd.players[0]

        self.assertEqual(p1.name, 'Dana?')
        self.assertEqual(p1.nickname, 'HACKED')
        self.assertEqual(p1.__linkprops__.is_tall_enough, False)

    def test_modelgen_reflection_1(self):
        from models import default, std

        from gel._internal._edgeql import Cardinality, PointerKind

        self.assert_pointers_match(
            default.User,
            [
                MockPointer(
                    name="friends",
                    cardinality=Cardinality.Many,
                    computed=False,
                    has_props=True,
                    kind=PointerKind.Link,
                    readonly=False,
                    type=_UpcastingDistinctList[
                        default.User.__links__.friends, default.User
                    ],
                ),
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
                MockPointer(
                    name="name_len",
                    cardinality=Cardinality.One,
                    computed=True,
                    has_props=False,
                    kind=PointerKind.Property,
                    readonly=True,
                    type=std.int64,
                ),
                MockPointer(
                    name="nickname",
                    cardinality=Cardinality.AtMostOne,
                    computed=False,
                    has_props=False,
                    kind=PointerKind.Property,
                    readonly=False,
                    type=std.str,
                ),
                MockPointer(
                    name="nickname_len",
                    cardinality=Cardinality.AtMostOne,
                    computed=True,
                    has_props=False,
                    kind=PointerKind.Property,
                    readonly=True,
                    type=std.int64,
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
