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

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import os
import sys
import typing
import unittest

if typing.TYPE_CHECKING:
    from typing import reveal_type

from gel import MultiRange, Range, errors
from gel import _testbase as tb
from gel._internal import _typing_inspect
from gel._internal._dlist import DistinctList
from gel._internal._edgeql import Cardinality, PointerKind
from gel._internal._qbmodel._pydantic._models import GelModel
from gel._internal._qbmodel._pydantic._pdlist import (
    ProxyDistinctList,
)
from gel._internal._reflection import SchemaPath


@dataclasses.dataclass
class Q:
    type: typing.Type
    query: str

    def __edgeql__(self):
        return (self.type, self.query)


class MockPointer(typing.NamedTuple):
    name: str
    type: SchemaPath
    cardinality: Cardinality
    computed: bool
    kind: PointerKind
    readonly: bool
    properties: dict[str, MockPointer] | None


class TestModelGenerator(tb.ModelTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), "dbsetup", "orm.gel")

    SETUP = os.path.join(os.path.dirname(__file__), "dbsetup", "orm.edgeql")

    ISOLATED_TEST_BRANCHES = True

    def assert_pointers_match(
        self, obj: type[GelModel], expected: list[MockPointer]
    ):
        ptrs = list(obj.__gel_reflection__.pointers.values())
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
                e.kind,
                p.kind,
                f"{obj.__name__}.{p.name} kind mismatch",
            )
            self.assertEqual(
                e.readonly,
                p.readonly,
                f"{obj.__name__}.{p.name} readonly mismatch",
            )
            self.assertEqual(
                bool(e.properties),
                bool(p.properties),
                f"{obj.__name__}.{p.name} has_props mismatch",
            )

            if _typing_inspect.is_valid_isinstance_arg(
                p.type
            ) and _typing_inspect.is_valid_isinstance_arg(e.type):
                if issubclass(e.type, DistinctList):
                    if not issubclass(p.type, DistinctList):
                        self.fail(
                            f"{obj.__name__}.{p.name} eq_type check failed: "
                            f"p.type is not a DistinctList, but expected "
                            f"type is {e.type!r}",
                        )

                if issubclass(p.type, ProxyDistinctList):
                    if not issubclass(e.type, ProxyDistinctList):
                        self.fail(
                            f"{obj.__name__}.{p.name} eq_type check "
                            f" failed: p.type is ProxyDistinctList, "
                            f"but expected type is {e.type!r}",
                        )
                else:
                    if issubclass(e.type, ProxyDistinctList):
                        self.fail(
                            f"{obj.__name__}.{p.name} eq_type check failed: "
                            f"p.type is not a ProxyDistinctList, but "
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

    @unittest.skipIf(
        sys.version_info < (3, 11),
        "dispatch_overload currently broken under Python 3.10",
    )
    @tb.typecheck
    def test_modelgen_data_unpack_1b(self):
        from models import default, std

        q = (
            default.Post.select(
                body=True,
                author=lambda p: p.author.select(name=True),
            )
            .filter(
                lambda p: p.body == "Hello",
                lambda p: 1 * std.len(p.body) == 5,
                lambda p: std.or_(p.body[0] == "H", std.like(p.body, "Hello")),
            )
            .limit(1)
        )
        d = self.client.get(q)

        self.assertEqual(reveal_type(d), "models.default.Post")

        self.assertEqual(reveal_type(d.id), "models.__variants__.std.uuid")

        self.assertIsInstance(d, default.Post)
        self.assertEqual(d.body, "Hello")
        self.assertIsInstance(d.author, default.User)

        self.assertEqual(d.author.name, "Alice")

    @tb.to_be_fixed
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
                    groups=lambda p: p.groups.select(name=True)
                    .order_by(
                        name="asc",
                        id=("desc", "empty first"),
                    )
                    .order_by(
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
    def test_modelgen_pydantic_apis(self):
        # regression test for https://github.com/geldata/gel-python/issues/722

        import pydantic
        from models import default
        from gel._internal._unsetid import UNSET_UUID

        class UserUpdate(pydantic.BaseModel):
            name: str | None = None
            other: int | None = None

        def run_test(user: default.User, upd: UserUpdate) -> None:
            orig_name = user.name
            orig_ch_fields = set(user.__gel_get_changed_fields__())

            user_dct = user.model_dump(exclude_unset=True)

            if user.id is not UNSET_UUID:
                self.assertEqual(user.id, user_dct["id"])
                del user_dct["id"]

            self.assertEqual(
                user_dct,
                {
                    "name": user.name,
                },
            )

            user_upd = user.model_copy(
                update=upd.model_dump(exclude_unset=True)
            )

            self.assertIsNot(user_upd, user)
            self.assertEqual(user_upd, user)
            self.assertEqual(user_upd.id, user.id)

            self.assertEqual(user.name, orig_name)
            self.assertEqual(user_upd.name, upd.name)

            self.assertEqual(
                set(user.__gel_get_changed_fields__()), orig_ch_fields
            )
            self.assertEqual(user_upd.__gel_get_changed_fields__(), {"name"})

        user_loaded = self.client.get(
            default.User.select(name=True).filter(name="Alice").limit(1)
        )
        user_new = default.User(name="Bob")

        run_test(user_loaded, UserUpdate(name="Alice A."))
        run_test(user_new, UserUpdate(name="Bob B."))

    @tb.typecheck
    def test_modelgen_data_unpack_polymorphic(self):
        from models import default

        q = default.Named.select(
            "*",
            *default.UserGroup,
        )

        for item in self.client.query(q):
            if isinstance(item, default.UserGroup):
                self.assertIsNotNone(item.mascot)

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
    def test_modelgen_submodules_and_reexports(self):
        import models

        models.default.Post
        models.std.str

        self.assertEqual(
            reveal_type(models.sub.TypeInSub.post),
            "type[models.default.Post]",
        )

    @tb.typecheck
    def test_modelgen_query_methods_on_instances(self):
        import models

        q = models.default.Post.limit(1).__gel_assert_single__()
        d = self.client.query(q)[0]

        for method in (
            "delete",
            "update",
            "select",
            "filter",
            "order_by",
            "limit",
            "offset",
        ):
            with self.assertRaisesRegex(
                AttributeError,
                "class-only method",
            ):
                getattr(d, method)

    @tb.typecheck
    def test_modelgen_data_model_validation_1(self):
        from typing import cast

        from models import default, std

        from gel._internal._dlist import DistinctList

        gs = default.GameSession(num=7)
        self.assertIsInstance(gs.players, DistinctList)

        with self.assertRaisesRegex(
            ValueError, r"(?s)only instances of User are allowed, got .*int"
        ):
            default.GameSession.players.link(1)  # type: ignore

        u = default.User(name="batman")
        p = default.Post(body="aaa", author=u)
        with self.assertRaisesRegex(
            ValueError, r"(?s)prayers.*Extra inputs are not permitted"
        ):
            default.GameSession(num=7, prayers=[p])  # type: ignore

        gp = default.GameSession.players(name="johny")
        self.assertIsInstance(gp, default.User)
        self.assertIsInstance(gp, default.GameSession.players)
        self.assertIsInstance(gp._p__obj__, default.User)
        self.assertEqual(gp.name, "johny")
        self.assertEqual(gp._p__obj__.name, "johny")
        self.assertIsNotNone(gp.__linkprops__)

        # Check that `groups` is not an allowed keyword-arg for `User.__init__`
        self.assertEqual(
            reveal_type(default.User),
            "def (*, name: builtins.str, nickname: "
            "Union[builtins.str, None] =) -> models.default.User",
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
            reveal_type(default.User.update),
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
            ValueError,
            r"(?s)User model does not accept .groups.*computed field",
        ):
            default.User(name="aaaa", groups=(1, 2, 3))  # type: ignore

        # Let's test computed property as an arg
        with self.assertRaisesRegex(
            ValueError,
            r"(?s)User model does not accept .name_len.*computed field",
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

    @tb.typecheck
    def test_modelgen_save_01(self):
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

        with self.assertRaisesRegex(NotImplementedError, '"del" operation'):
            del p.body

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
                ), message := 'more than one post'),
                alice := assert_single((
                    select User {name} filter .name = 'Alice the 5th'
                ), message := 'more than one alice'),
                new_alice := assert_single((
                    select User {name} filter .name = 'New Alice'
                ), message := 'more than one new_alice')

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

    @tb.typecheck
    def test_modelgen_save_02(self):
        import uuid

        from models import default
        # insert an object with a required multi: no link props, one object
        # added to the link

        party = default.Party(
            name="Solo",
            members=[
                default.User(
                    name="John Smith",
                    nickname="Hannibal",
                ),
            ],
        )
        self.client.save(party)

        # Fetch and verify
        raw_id = uuid.UUID(str(party.id))
        res = self.client.get(
            default.Party.select(
                name=True,
                members=True,
            ).filter(id=raw_id)
        )
        self.assertEqual(res.name, "Solo")
        self.assertEqual(len(res.members), 1)
        m = res.members[0]
        self.assertEqual(m.name, "John Smith")
        self.assertEqual(m.nickname, "Hannibal")

    @tb.typecheck
    def test_modelgen_save_03(self):
        from models import default
        # insert an object with a required multi: no link props, more than one
        # object added to the link

        party = default.Party(
            name="The A-Team",
            members=[
                default.User(
                    name="John Smith",
                    nickname="Hannibal",
                ),
                default.User(
                    name="Templeton Peck",
                    nickname="Faceman",
                ),
                default.User(
                    name="H.M. Murdock",
                    nickname="Howling Mad",
                ),
                default.User(
                    name="Bosco Baracus",
                    nickname="Bad Attitude",
                ),
            ],
        )
        self.client.save(party)

        # Fetch and verify
        res = self.client.get(
            default.Party.select(
                "*",
                members=lambda p: p.members.select("*").order_by(name=True),
            ).filter(name="The A-Team")
        )
        self.assertEqual(res.name, "The A-Team")
        self.assertEqual(len(res.members), 4)
        for m, (name, nickname) in zip(
            res.members,
            [
                ("Bosco Baracus", "Bad Attitude"),
                ("H.M. Murdock", "Howling Mad"),
                ("John Smith", "Hannibal"),
                ("Templeton Peck", "Faceman"),
            ],
            strict=True,
        ):
            self.assertEqual(m.name, name)
            self.assertEqual(m.nickname, nickname)

    @tb.typecheck
    def test_modelgen_save_04(self):
        from models import default
        # insert an object with a required multi: with link props, one object
        # added to the link

        raid = default.Raid(
            name="Solo",
            members=[
                default.Raid.members.link(
                    default.User(
                        name="John Smith",
                        nickname="Hannibal",
                    ),
                    role="everything",
                    rank=1,
                )
            ],
        )
        self.client.save(raid)

        # Fetch and verify
        res = self.client.get(
            default.Raid.select(
                name=True,
                members=lambda r: r.members.select(name=True, nickname=True),
            ).filter(name="Solo")
        )
        self.assertEqual(res.name, "Solo")
        self.assertEqual(len(res.members), 1)
        m = res.members[0]
        self.assertEqual(m.name, "John Smith")
        self.assertEqual(m.nickname, "Hannibal")
        self.assertEqual(m.__linkprops__.role, "everything")
        self.assertEqual(m.__linkprops__.rank, 1)

    @tb.typecheck
    def test_modelgen_save_05(self):
        from models import default
        # insert an object with a required multi: with link props, more than
        # one object added to the link; have one link prop for the first
        # object within the link, and another for the second object within the
        # same link

        raid = default.Raid(
            name="The A-Team",
            members=[
                default.Raid.members.link(
                    default.User(
                        name="John Smith",
                        nickname="Hannibal",
                    ),
                    role="brains",
                    rank=1,
                ),
                default.Raid.members.link(
                    default.User(
                        name="Templeton Peck",
                        nickname="Faceman",
                    ),
                    rank=2,
                ),
                default.Raid.members.link(
                    default.User(
                        name="H.M. Murdock",
                        nickname="Howling Mad",
                    ),
                    role="medic",
                ),
                default.User(
                    name="Bosco Baracus",
                    nickname="Bad Attitude",
                ),
            ],
        )
        self.client.save(raid)

        # Fetch and verify
        res = self.client.get(
            """
            select Raid {
                name,
                members: {
                    name,
                    nickname,
                    @rank,
                    @role
                } order by .name
            } filter .name = "The A-Team"
            limit 1
            """
        )
        self.assertEqual(res.name, "The A-Team")
        self.assertEqual(len(res.members), 4)
        for m, (name, nickname, rank, role) in zip(
            res.members,
            [
                ("Bosco Baracus", "Bad Attitude", None, None),
                ("H.M. Murdock", "Howling Mad", None, "medic"),
                ("John Smith", "Hannibal", 1, "brains"),
                ("Templeton Peck", "Faceman", 2, None),
            ],
            strict=True,
        ):
            self.assertEqual(m.name, name)
            self.assertEqual(m.nickname, nickname)
            self.assertEqual(m["@role"], role)
            self.assertEqual(m["@rank"], rank)

    @tb.typecheck
    def test_modelgen_save_06(self):
        from models import default
        # Update object adding multiple existing objects to an exiting link
        # (no link props)

        gr = self.client.get(
            default.UserGroup.select(
                name=True,
                users=True,
            ).filter(name="blue")
        )
        self.assertEqual(len(gr.users), 0)
        a = self.client.get(default.User.filter(name="Alice"))
        c = self.client.get(default.User.filter(name="Cameron"))
        z = self.client.get(default.User.filter(name="Zoe"))
        gr.users.extend([a, c, z])
        self.client.save(gr)

        # Fetch and verify
        res = self.client.query("""
            select User.name filter "blue" in User.groups.name
        """)
        self.assertEqual(set(res), {"Alice", "Cameron", "Zoe"})

    @tb.typecheck
    def test_modelgen_save_07(self):
        from models import default
        # Update object adding multiple existing objects to an exiting link
        # (no link props)

        gr = self.client.get(
            default.UserGroup.select(
                name=True,
                users=True,
            ).filter(name="green")
        )
        self.assertEqual({u.name for u in gr.users}, {"Alice", "Billie"})
        a = self.client.get(default.User.filter(name="Alice"))
        c = self.client.get(default.User.filter(name="Cameron"))
        z = self.client.get(default.User.filter(name="Zoe"))
        gr.users.extend([a, c, z])
        self.client.save(gr)

        # Fetch and verify
        res = self.client.query("""
            select User.name filter "green" in User.groups.name
        """)
        self.assertEqual(set(res), {"Alice", "Billie", "Cameron", "Zoe"})

    @tb.typecheck
    def test_modelgen_save_08(self):
        from models import default
        # Update object adding multiple existing objects to an exiting link
        # with link props (try variance of props within the same multi link
        # for the same object)

        self.client.save(default.Team(name="test team 8"))
        team = self.client.get(
            default.Team.select(
                name=True,
                members=True,
            ).filter(name="test team 8")
        )
        self.assertEqual(len(team.members), 0)
        a = self.client.get(default.User.filter(name="Alice"))
        b = self.client.get(default.User.filter(name="Billie"))
        c = self.client.get(default.User.filter(name="Cameron"))
        z = self.client.get(default.User.filter(name="Zoe"))
        team.members.extend(
            [
                default.Team.members.link(
                    a,
                    role="lead",
                    rank=1,
                ),
                default.Team.members.link(
                    b,
                    rank=2,
                ),
            ]
        )
        self.client.save(team)

        # Fetch and verify
        res = self.client.query_required_single("""
            select Team {
                members: {
                    @rank,
                    @role,
                    name,
                } order by .name
            }
            filter .name = "test team 8"
        """)
        self.assertEqual(
            [(r.name, r["@rank"], r["@role"]) for r in res.members],
            [
                ("Alice", 1, "lead"),
                ("Billie", 2, None),
            ],
        )

        # Refetch and update it again
        team = self.client.get(
            default.Team.select(
                name=True,
                members=True,
            ).filter(name="test team 8")
        )
        team.members.extend(
            [
                default.Team.members.link(
                    c,
                    role="notes-taker",
                ),
                z,
            ]
        )
        self.client.save(team)

        res = self.client.query_required_single("""
            select Team {
                members: {
                    @rank,
                    @role,
                    name,
                } order by .name
            }
            filter .name = "test team 8"
        """)
        self.assertEqual(
            [(r.name, r["@rank"], r["@role"]) for r in res.members],
            [
                ("Alice", 1, "lead"),
                ("Billie", 2, None),
                ("Cameron", None, "notes-taker"),
                ("Zoe", None, None),
            ],
        )

    @tb.typecheck
    def test_modelgen_save_09(self):
        from models import default
        # Update object removing multiple existing objects from an existing
        # multi link

        gr = self.client.get(
            default.UserGroup.select(
                name=True,
                users=True,
            ).filter(name="red")
        )
        self.assertEqual(gr.name, "red")
        self.assertEqual(
            {u.name for u in gr.users},
            {"Alice", "Billie", "Cameron", "Dana"},
        )
        for u in list(gr.users):
            if u.name in {"Billie", "Cameron"}:
                gr.users.remove(u)
        self.client.save(gr)

        # Fetch and verify
        res = self.client.query("""
            select User.name filter "red" in User.groups.name
        """)
        self.assertEqual(set(res), {"Alice", "Dana"})

    @tb.typecheck
    def test_modelgen_save_10(self):
        from models import default
        # Update object removing multiple existing objects from an existing
        # multi link

        self.client.query("""
            insert Team {
                name := 'test team 10',
                members := assert_distinct((
                    for t in {
                        ('Alice', 'fire', 99),
                        ('Billie', 'ice', 0),
                        ('Cameron', '', 1),
                    }
                    select User {
                        @role := if t.1 = '' then <str>{} else t.1,
                        @rank := if t.2 = 0 then <int64>{} else t.2,
                    }
                    filter .name = t.0
                )),
            }
        """)
        team = self.client.get(
            default.Team.select(
                name=True,
                members=True,
            ).filter(name="test team 10")
        )
        self.assertEqual(team.name, "test team 10")
        self.assertEqual(
            {u.name for u in team.members},
            {"Alice", "Billie", "Cameron"},
        )
        for u in list(team.members):
            if u.name in {"Alice", "Cameron"}:
                team.members.remove(u)
        self.client.save(team)

        # Fetch and verify
        res = self.client.query_required_single("""
            select Team {
                members: {
                    @rank,
                    @role,
                    name,
                } order by .name
            }
            filter .name = "test team 10"
        """)
        self.assertEqual(
            [(r.name, r["@role"], r["@rank"]) for r in res.members],
            [
                ("Billie", "ice", None),
            ],
        )

    @tb.typecheck
    def test_modelgen_save_11(self):
        from models import default
        # Update object adding an existing objecs to an exiting single
        # required link (no link props)

        post = self.client.get(
            default.Post.select(
                body=True,
                author=True,
            ).filter(body="Hello")
        )
        z = self.client.get(default.User.filter(name="Zoe"))
        self.assertEqual(post.author.name, "Alice")
        post.author = z
        self.client.save(post)

        # Fetch and verify
        res = self.client.query("""
            select Post {body, author: {name}}
            filter .body = 'Hello'
        """)
        assert len(res) == 1
        self.assertEqual(res[0].author.name, "Zoe")

    @tb.typecheck
    def test_modelgen_save_12(self):
        from models import default

        # Update object adding an existing object to an exiting single
        # required link (with link props)
        a = self.client.get(default.User.filter(name="Alice"))
        z = self.client.get(default.User.filter(name="Zoe"))
        img_query = default.Image.select(
            file=True,
            author=True,
        ).filter(file="cat.jpg")
        img = self.client.get(img_query)
        self.assertEqual(img.author.name, "Elsa")
        self.assertEqual(img.author.__linkprops__.caption, "made of snow")
        self.assertEqual(img.author.__linkprops__.year, 2025)

        img.author = default.Image.author.link(z, caption="kitty!")
        self.client.save(img)

        # Re-fetch and verify
        img = self.client.get(img_query)
        self.assertEqual(img.author.name, "Zoe")
        self.assertEqual(img.author.__linkprops__.caption, "kitty!")
        self.assertEqual(img.author.__linkprops__.year, None)

        img.author = a
        self.client.save(img)

        # Re-fetch and verify
        img = self.client.get(img_query)
        self.assertEqual(img.author.name, "Alice")
        self.assertEqual(img.author.__linkprops__.caption, None)
        self.assertEqual(img.author.__linkprops__.year, None)

        img.author = default.Image.author.link(z, caption="cute", year=2024)
        self.client.save(img)

        # Re-fetch and verify
        img = self.client.get(img_query)
        self.assertEqual(img.author.name, "Zoe")
        self.assertEqual(img.author.__linkprops__.caption, "cute")
        self.assertEqual(img.author.__linkprops__.year, 2024)

    @tb.typecheck
    def test_modelgen_save_13(self):
        from models import default
        # Update object adding an existing object to an exiting single
        # optional link (no link props)

        loot = self.client.get(
            default.Loot.select(
                name=True,
                owner=True,
            ).filter(name="Cool Hat")
        )
        z = self.client.get(default.User.filter(name="Zoe"))
        assert loot.owner is not None
        self.assertEqual(loot.owner.name, "Billie")
        loot.owner = z
        self.client.save(loot)

        # Fetch and verify
        res = self.client.get("""
            select Loot {name, owner: {name}}
            filter .name = 'Cool Hat'
        """)
        self.assertEqual(res.owner.name, "Zoe")

    @tb.typecheck
    def test_modelgen_save_14(self):
        from models import default
        # Update object adding an existing object to an exiting single
        # optional link (with link props)

        loot = self.client.get(
            default.StackableLoot.select(
                name=True,
                owner=True,
            ).filter(name="Gold Coin")
        )
        a = self.client.get(default.User.filter(name="Alice"))
        z = self.client.get(default.User.filter(name="Zoe"))
        assert loot.owner is not None
        self.assertEqual(loot.owner.name, "Billie")
        self.assertEqual(loot.owner.__linkprops__.count, 34)
        self.assertEqual(loot.owner.__linkprops__.bonus, True)

        loot.owner = default.StackableLoot.owner.link(
            z,
            count=12,
        )
        self.client.save(loot)

        # Re-fetch and verify
        loot = self.client.get(
            default.StackableLoot.select(
                name=True,
                owner=True,
            ).filter(name="Gold Coin")
        )
        assert loot.owner is not None
        self.assertEqual(loot.owner.name, "Zoe")
        self.assertEqual(loot.owner.__linkprops__.count, 12)
        self.assertEqual(loot.owner.__linkprops__.bonus, None)

        loot.owner = a
        self.client.save(loot)

        # Re-fetch and verify
        loot = self.client.get(
            default.StackableLoot.select(
                name=True,
                owner=True,
            ).filter(name="Gold Coin")
        )
        assert loot.owner is not None
        self.assertEqual(loot.owner.name, "Alice")
        self.assertEqual(loot.owner.__linkprops__.count, None)
        self.assertEqual(loot.owner.__linkprops__.bonus, None)

        loot.owner = default.StackableLoot.owner.link(z, count=56, bonus=False)
        self.client.save(loot)

        # Re-fetch and verify
        loot = self.client.get(
            default.StackableLoot.select(
                name=True,
                owner=True,
            ).filter(name="Gold Coin")
        )
        assert loot.owner is not None
        self.assertEqual(loot.owner.name, "Zoe")
        self.assertEqual(loot.owner.__linkprops__.count, 56)
        self.assertEqual(loot.owner.__linkprops__.bonus, False)

    @tb.typecheck
    def test_modelgen_save_15(self):
        from models import default
        # insert an object with a required single: no link props, one object
        # added to the link

        z = self.client.get(default.User.filter(name="Zoe"))
        post = default.Post(
            body="test post 15",
            author=z,
        )
        self.client.save(post)

        # Fetch and verify
        res = self.client.get("""
            select Post {body, author: {name}}
            filter .body = 'test post 15'
            limit 1
        """)
        self.assertEqual(res.body, "test post 15")
        self.assertEqual(res.author.name, "Zoe")

    @tb.typecheck
    def test_modelgen_save_16(self):
        from models import default
        # insert an object with a required single: with link props

        a = self.client.get(default.User.filter(name="Alice"))
        img = default.Image(
            file="puppy.jpg",
            author=default.Image.author.link(
                a,
                caption="woof!",
                year=2000,
            ),
        )
        self.client.save(img)

        # Re-fetch and verify
        img = self.client.get(
            default.Image.select(
                file=True,
                author=True,
            ).filter(file="puppy.jpg")
        )
        self.assertEqual(img.author.name, "Alice")
        self.assertEqual(img.author.__linkprops__.caption, "woof!")
        self.assertEqual(img.author.__linkprops__.year, 2000)

    @tb.typecheck
    def test_modelgen_save_17(self):
        from models import default
        # insert an object with an optional single: no link props, one object
        # added to the link

        z = self.client.get(default.User.filter(name="Zoe"))
        loot = default.Loot(
            name="Pony",
            owner=z,
        )
        self.client.save(loot)

        # Fetch and verify
        res = self.client.get("""
            select Loot {name, owner: {name}}
            filter .name = 'Pony'
        """)
        self.assertEqual(res.name, "Pony")
        self.assertEqual(res.owner.name, "Zoe")

    @tb.typecheck
    def test_modelgen_save_18(self):
        from models import default
        # insert an object with an optional single: with link props

        a = self.client.get(default.User.filter(name="Alice"))
        loot = default.StackableLoot(
            name="Button",
            owner=default.StackableLoot.owner.link(
                a,
                count=5,
                bonus=False,
            ),
        )
        self.client.save(loot)

        # Re-fetch and verify
        loot = self.client.get(
            default.StackableLoot.select(
                name=True,
                owner=True,
            ).filter(name="Button")
        )
        assert loot.owner is not None
        self.assertEqual(loot.owner.name, "Alice")
        self.assertEqual(loot.owner.__linkprops__.count, 5)
        self.assertEqual(loot.owner.__linkprops__.bonus, False)

    @tb.xfail
    @tb.typecheck
    def test_modelgen_save_19(self):
        # FIXME: pydantic might never be happy with the loops that are made by
        # the literally identical object, so this may be invalid, see next
        # test for a workaround
        from models import default
        # insert an object with an optional link to self set to self

        p = default.LinearPath(label="singleton")
        p.next = p
        self.client.save(p)

        # Fetch and verify
        res = self.client.query("""
            select LinearPath {id, label, next: {id, label}}
            order by .label
        """)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].label, "singleton")
        self.assertEqual(res[0].id, res[0].next.id)

    @tb.typecheck
    def test_modelgen_save_20(self):
        from models import default
        # make a self loop in 2 steps

        p = default.LinearPath(label="singleton")
        self.client.save(p)
        # close the loop
        p.next = self.client.get(default.LinearPath.filter(label="singleton"))
        self.client.save(p)

        # Fetch and verify
        res = self.client.query("""
            select LinearPath {id, label, next: {id, label}}
            order by .label
        """)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].label, "singleton")
        self.assertEqual(res[0].id, res[0].next.id)

    @tb.typecheck
    def test_modelgen_save_21(self):
        from models import default
        # insert an object with an optional link to self set to self

        p = default.LinearPath(
            label="start",
            next=default.LinearPath(
                label="step 1", next=default.LinearPath(label="step 2")
            ),
        )
        assert p.next is not None
        assert p.next.next is not None
        p.next.next.next = p
        self.client.save(p)

        # Fetch and verify
        res = self.client.query("""
            select LinearPath {id, label, next: {id, label}}
            order by .label
        """)
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0].label, "start")
        self.assertEqual(res[0].next.label, "step 1")
        self.assertEqual(res[1].label, "step 1")
        self.assertEqual(res[1].next.label, "step 2")
        self.assertEqual(res[2].label, "step 2")
        self.assertEqual(res[2].next.label, "start")

    @tb.typecheck
    def test_modelgen_save_22(self):
        # Test empty object insertion; regression test for
        # https://github.com/geldata/gel-python/issues/720

        from models import default

        from gel._internal._unsetid import UNSET_UUID

        x = default.AllOptional()
        y = default.AllOptional()
        z = default.AllOptional(pointer=x)

        self.client.save(z)
        self.client.save(y)

        self.assertIsNot(x.id, UNSET_UUID)
        self.assertIsNot(y.id, UNSET_UUID)
        self.assertIsNot(z.id, UNSET_UUID)

        self.assertIsNotNone(z.pointer)
        assert z.pointer is not None
        self.assertEqual(z.pointer.id, x.id)
        self.assertIs(z.pointer, x)

    @tb.typecheck
    def test_modelgen_save_23(self):
        from models import default

        p = default.Post(body="save 23", author=default.User(name="Sally"))

        self.client.save(p)

        p2 = self.client.query_required_single("""
            select Post {body, author: {name}}
            filter .author.name = 'Sally'
            limit 1
        """)

        self.assertEqual(p2.body, "save 23")
        self.assertEqual(p2.author.name, "Sally")
        self.assertEqual(p.id, p2.id)
        self.assertEqual(p.author.id, p2.author.id)

    @tb.typecheck
    def test_modelgen_save_24(self):
        from models import default

        z = self.client.get(default.User.filter(name="Zoe"))
        p = self.client.get(
            default.Post.select(body=True).filter(body="Hello")
        )

        self.assertEqual(p.body, "Hello")

        p.body = "Hello world"
        p.author = z
        self.client.save(p)

        p2 = self.client.query_required_single("""
            select Post {body, author: {name}}
            filter .body = 'Hello world'
            limit 1
        """)

        self.assertEqual(p2.body, "Hello world")
        self.assertEqual(p2.author.name, "Zoe")

    @tb.typecheck
    def test_modelgen_save_25(self):
        from models import default

        g = self.client.get(
            default.UserGroup.select(
                name=True,
                mascot=True,
            ).filter(name="red")
        )

        self.assertEqual(g.mascot, "dragon")

        g.mascot = "iguana"
        self.client.save(g)

        g2 = self.client.get(
            default.UserGroup.select(
                name=True,
                mascot=True,
            ).filter(name="red")
        )
        self.assertEqual(g2.mascot, "iguana")

    @tb.xfail
    @tb.typecheck
    def test_modelgen_save_26(self):
        from models import default

        l0 = default.ImpossibleLink0(val="A", il1=default.BaseLink(val="X"))
        l1 = default.ImpossibleLink1(val="2nd", il0=l0)
        # change the prop an dlink of l0
        l0.val = "1st"
        l0.il1 = l1

        # check the state before saving
        self.assertEqual(l0.val, "1st")
        self.assertEqual(l1.il0.val, "1st")
        self.assertEqual(l0.il1.val, "2nd")
        self.assertEqual(l0.il1.il0.val, "1st")

        # this should now be an impossible to save loop
        self.client.save(l0, l1)

    @tb.typecheck
    def test_modelgen_save_27(self):
        from models import default

        a = self.client.get(default.User.filter(name="Alice"))
        b = self.client.get(default.User.filter(name="Billie"))
        p = default.Raid(
            name="2 people",
            members=[
                default.Raid.members.link(a, rank=1),
                default.Raid.members.link(b, rank=2),
            ],
        )
        self.client.save(p)

        # Fetch and verify
        res = self.client.get(
            default.Raid.select(
                name=True,
                members=True,
            ).filter(name="2 people")
        )
        self.assertEqual(res.name, "2 people")
        self.assertEqual(len(res.members), 2)
        self.assertEqual(
            {(m.name, m.__linkprops__.rank) for m in res.members},
            {("Alice", 1), ("Billie", 2)},
        )

        # technically this won't change things
        p.members.extend(p.members)
        self.client.save(p)

        # Fetch and verify
        res2 = self.client.get(
            default.Raid.select(
                name=True,
                members=True,
            ).filter(name="2 people")
        )
        self.assertEqual(res2.name, "2 people")
        self.assertEqual(len(res2.members), 2)
        self.assertEqual(
            {(m.name, m.__linkprops__.rank) for m in res2.members},
            {("Alice", 1), ("Billie", 2)},
        )

    @tb.typecheck
    def test_modelgen_save_28(self):
        from models import default

        a = self.client.get(default.User.filter(name="Alice"))
        p = default.Raid(
            name="mixed raid",
            members=[
                default.Raid.members.link(a, rank=1),
            ],
        )
        self.client.save(p)

        # Fetch and verify
        res = self.client.get(
            default.Raid.select(
                name=True,
                members=True,
            ).filter(name="mixed raid")
        )
        self.assertEqual(res.name, "mixed raid")
        self.assertEqual(len(res.members), 1)
        m = res.members[0]
        self.assertEqual(m.name, "Alice")
        self.assertEqual(m.__linkprops__.rank, 1)

        x = default.CustomUser(name="Xavier")
        p.members.extend([x])
        self.client.save(p)

        res2 = self.client.get(
            default.Raid.select(
                name=True,
                members=True,
            ).filter(name="mixed raid")
        )
        self.assertEqual(res2.name, "mixed raid")
        self.assertEqual(len(res2.members), 2)
        self.assertEqual(
            {(m.name, m.__linkprops__.rank) for m in res2.members},
            {("Alice", 1), ("Xavier", None)},
        )

    @tb.typecheck
    def test_modelgen_save_29(self):
        from models import default

        a = self.client.get(default.User.filter(name="Alice"))
        p = default.Raid(
            name="bad raid",
            members=[
                default.Raid.members.link(a, rank=1),
            ],
        )
        self.client.save(p)

        # Fetch and verify
        res = self.client.get(
            default.Raid.select(
                name=True,
                members=True,
            ).filter(name="bad raid")
        )
        self.assertEqual(res.name, "bad raid")
        self.assertEqual(len(res.members), 1)
        m = res.members[0]
        self.assertEqual(m.name, "Alice")
        self.assertEqual(m.__linkprops__.rank, 1)

        with self.assertRaisesRegex(
            ValueError, r"the list already contains.+User"
        ):
            p.members.extend([a])

    @tb.typecheck(["from gel import errors"])
    def test_modelgen_save_30(self):
        from models import default

        a = self.client.get(default.User.filter(name="Alice"))
        p = default.Raid(
            name="mixed raid",
            members=[
                default.Raid.members.link(a, rank=1),
            ],
        )
        self.client.save(p)

        # Fetch and verify
        res = self.client.get(
            default.Raid.select(
                name=True,
                members=True,
            ).filter(name="mixed raid")
        )
        self.assertEqual(res.name, "mixed raid")
        self.assertEqual(len(res.members), 1)
        m = res.members[0]
        self.assertEqual(m.name, "Alice")
        self.assertEqual(m.__linkprops__.rank, 1)

        p.members.clear()
        with self.assertRaisesRegex(
            errors.MissingRequiredError,
            "missing value for required link 'members'",
        ):
            self.client.save(p)

    @tb.typecheck
    def test_modelgen_save_collections_01(self):
        from models import default
        # insert an object with an optional single: with link props

        ks = default.KitchenSink(
            str="coll_test_1",
            p_multi_str=["1", "222"],
            array=["foo", "bar"],
            p_multi_arr=[["foo"], ["bar"]],
            p_arrtup=[("foo",)],
            p_multi_arrtup=[[("foo",)], [("foo",)]],
            p_tuparr=(["foo"],),
            p_multi_tuparr=[(["foo"],), (["foo"],)],
        )
        self.client.save(ks)

        # Re-fetch and verify
        ks = self.client.get(default.KitchenSink.filter(str="coll_test_1"))
        self.assertEqual(sorted(ks.p_multi_str), ["1", "222"])
        self.assertEqual(ks.array, ["foo", "bar"])
        self.assertEqual(sorted(ks.p_multi_arr), [["bar"], ["foo"]])
        self.assertEqual(ks.p_arrtup, [("foo",)])
        self.assertEqual(sorted(ks.p_multi_arrtup), [[("foo",)], [("foo",)]])
        self.assertEqual(ks.p_tuparr, (["foo"],))
        self.assertEqual(sorted(ks.p_multi_tuparr), [(["foo"],), (["foo"],)])

        ks.p_multi_str.append("zzz")
        ks.p_multi_str.append("zzz")
        ks.p_multi_str.remove("1")
        self.client.save(ks)

        ks3 = self.client.get(default.KitchenSink.filter(str="coll_test_1"))
        self.assertEqual(sorted(ks3.p_multi_str), ["222", "zzz", "zzz"])

    @tb.xfail
    @tb.typecheck
    def test_modelgen_save_collections_02(self):
        from models import default

        ks = default.KitchenSink(
            str="coll_test_2",
            p_multi_str=[""],
            p_opt_str=None,
            array=[],
            p_multi_arr=[[]],
            p_arrtup=[],
            p_multi_arrtup=[[]],
            p_tuparr=([],),
            p_multi_tuparr=[([],)],
        )
        self.client.save(ks)

        # Re-fetch and verify
        ks2 = self.client.get(default.KitchenSink.filter(str="coll_test_2"))
        self.assertEqual(ks2.p_multi_str, [""])
        self.assertEqual(ks2.p_opt_str, None)
        self.assertEqual(ks2.p_opt_multi_str, [])
        self.assertEqual(ks2.array, [])
        self.assertEqual(ks2.p_multi_arr, [[]])
        self.assertEqual(ks2.p_arrtup, [])
        self.assertEqual(ks2.p_multi_arrtup, [[]])
        self.assertEqual(ks2.p_tuparr, ([],))
        self.assertEqual(ks.p_multi_tuparr, [([],)])

        ks.p_opt_str = "hello world"
        ks.p_opt_multi_str.append("hello")
        ks.p_opt_multi_str.append("world")
        self.client.save(ks)

        ks3 = self.client.get(default.KitchenSink.filter(str="coll_test_2"))
        self.assertEqual(ks3.p_opt_str, "hello world")
        self.assertEqual(sorted(ks3.p_opt_multi_str), ["hello", "world"])

        ks.p_opt_str = None
        ks.p_opt_multi_str.clear()
        self.client.save(ks)

        # partially fetch the object
        ks4 = self.client.get(
            default.KitchenSink.select(
                p_opt_str=True,
                p_opt_multi_str=True,
                array=True,
            ).filter(str="coll_test_2")
        )
        self.assertEqual(ks4.p_opt_str, None)
        self.assertEqual(ks4.p_opt_multi_str, [])
        self.assertEqual(ks4.array, [])

        # save the partially fetched object
        ks4.p_opt_str = "hello again"
        ks4.array.append("bye bye")
        self.client.save(ks4)

        ks5 = self.client.get(
            default.KitchenSink.select(
                p_opt_str=True,
                p_opt_multi_str=True,
                array=True,
            ).filter(str="coll_test_2")
        )
        self.assertEqual(ks5.p_opt_str, "hello again")
        self.assertEqual(ks5.array, ["bye bye"])

    # both the typecheck and the expected returned values have errors
    @tb.xfail
    @tb.typecheck
    def test_modelgen_save_collections_03(self):
        from models import default

        ks = self.client.get(default.KitchenSink.filter(str="hello world"))
        self.assertEqual(ks.array, ["foo"])

        ks.array.append("bar")
        self.client.save(ks)

        # Re-fetch and verify
        ks2 = self.client.get(default.KitchenSink.filter(str="hello world"))
        self.assertEqual(ks2.array, ["foo", "bar"])

        ks2.array.remove("foo")
        self.client.save(ks2)

        # Re-fetch and verify
        ks3 = self.client.get(default.KitchenSink.filter(str="hello world"))
        self.assertEqual(ks3.array, ["bar"])

    @tb.typecheck
    def test_modelgen_save_collections_04(self):
        from models import default

        ks = self.client.get(default.KitchenSink.filter(str="hello world"))
        self.assertEqual(sorted(ks.p_multi_arr), [["bar"], ["foo"]])

        ks.p_multi_arr.remove(["foo"])
        self.client.save(ks)

        # Re-fetch and verify
        ks2 = self.client.get(default.KitchenSink.filter(str="hello world"))
        self.assertEqual(sorted(ks2.p_multi_arr), [["bar"]])

    @tb.typecheck
    def test_modelgen_save_collections_05(self):
        from models import default

        ks = self.client.get(default.KitchenSink.filter(str="hello world"))
        self.assertEqual(ks.p_opt_arr, None)

        ks.p_opt_arr = ["silly", "goose"]
        self.client.save(ks)

        # Re-fetch and verify
        ks2 = self.client.get(default.KitchenSink.filter(str="hello world"))
        self.assertEqual(ks2.p_opt_arr, ["silly", "goose"])

    @tb.typecheck
    def test_modelgen_save_collections_06(self):
        from models import default

        ks = self.client.get(
            default.KitchenSink.select(
                p_opt_str=True,
            ).filter(str="hello world")
        )
        ks.p_opt_str = "silly goose"
        self.client.save(ks)

        # Re-fetch and verify
        ks2 = self.client.get(default.KitchenSink.filter(str="hello world"))
        self.assertEqual(ks2.p_opt_str, "silly goose")

    @tb.xfail
    @tb.typecheck(["import datetime as dt", "from gel import Range"])
    def test_modelgen_save_range_01(self):
        from models import default

        r = self.client.get(default.RangeTest.filter(name="test range"))
        self.assertEqual(r.name, "test range")
        self.assertEqual(
            r.int_range,
            Range(23, 45),
        )
        self.assertEqual(
            r.float_range,
            Range(2.5, inc_lower=False),
        )
        self.assertEqual(
            r.date_range,
            Range(dt.date(2025, 1, 6), dt.date(2025, 2, 17)),
        )

        r.int_range = Range(None, 10)
        r.float_range = Range(empty=True)
        self.client.save(r)

        r2 = self.client.get(default.RangeTest.filter(name="test range"))
        self.assertEqual(r2.name, "test range")
        self.assertEqual(
            r2.int_range,
            Range(None, 10),
        )
        self.assertEqual(
            r2.float_range,
            Range(empty=True),
        )
        self.assertEqual(
            r2.date_range,
            Range(dt.date(2025, 1, 6), dt.date(2025, 2, 17)),
        )

    @tb.xfail
    @tb.typecheck(["import datetime as dt", "from gel import Range"])
    def test_modelgen_save_range_02(self):
        from models import default

        r = default.RangeTest(
            name="new range",
            int_range=Range(11),
            float_range=Range(),  # everything
            date_range=Range(dt.date(2025, 3, 4), dt.date(2025, 11, 21)),
        )
        self.client.save(r)

        r2 = self.client.get(default.RangeTest.filter(name="new range"))
        self.assertEqual(r2.name, "new range")
        self.assertEqual(
            r2.int_range,
            Range(11),
        )
        self.assertEqual(
            r2.float_range,
            Range(),
        )
        self.assertEqual(
            r2.date_range,
            Range(dt.date(2025, 3, 4), dt.date(2025, 11, 21)),
        )

    @tb.typecheck(
        ["import datetime as dt", "from gel import MultiRange, Range"]
    )
    def test_modelgen_save_multirange_01(self):
        from models import default

        r = self.client.get(
            default.MultiRangeTest.filter(name="test multirange")
        )
        self.assertEqual(r.name, "test multirange")
        self.assertEqual(
            r.int_mrange, MultiRange([Range(2, 4), Range(23, 45)])
        )
        self.assertEqual(
            r.float_mrange,
            MultiRange(
                [
                    Range(0, 0.5),
                    Range(2.5, inc_lower=False),
                ]
            ),
        )
        self.assertEqual(
            r.date_mrange,
            MultiRange(
                [
                    Range(dt.date(2025, 1, 6), dt.date(2025, 2, 17)),
                    Range(dt.date(2025, 3, 16)),
                ]
            ),
        )

        r.int_mrange = MultiRange()
        r.float_mrange = MultiRange()
        self.client.save(r)

        r2 = self.client.get(
            default.MultiRangeTest.filter(name="test multirange")
        )
        self.assertEqual(r2.name, "test multirange")
        self.assertEqual(
            r2.int_mrange,
            MultiRange(),
        )
        self.assertEqual(
            r2.float_mrange,
            MultiRange(),
        )
        self.assertEqual(
            r2.date_mrange,
            MultiRange(
                [
                    Range(dt.date(2025, 1, 6), dt.date(2025, 2, 17)),
                    Range(dt.date(2025, 3, 16)),
                ]
            ),
        )

    @tb.xfail
    @tb.typecheck(
        ["import datetime as dt", "from gel import MultiRange, Range"]
    )
    def test_modelgen_save_multirange_02(self):
        from models import default

        r = default.MultiRangeTest(
            name="new multirange",
            int_mrange=MultiRange([Range(11)]),
            float_mrange=MultiRange(),  # everything
            date_mrange=MultiRange(
                [Range(dt.date(2025, 3, 4), dt.date(2025, 11, 21))]
            ),
        )
        self.client.save(r)

        r2 = self.client.get(
            default.MultiRangeTest.filter(name="new multirange")
        )
        self.assertEqual(r2.name, "new multirange")
        self.assertEqual(
            r2.int_range,
            MultiRange([Range(11)]),
        )
        self.assertEqual(
            r2.float_range,
            MultiRange(),
        )
        self.assertEqual(
            r2.date_range,
            MultiRange([Range(dt.date(2025, 3, 4), dt.date(2025, 11, 21))]),
        )

    @tb.typecheck
    def test_modelgen_linkprops_1(self):
        from models import default

        # Create a new GameSession and add a player
        u = self.client.get(default.User.filter(name="Zoe"))
        gs = default.GameSession(
            num=1001,
            players=[default.GameSession.players.link(u, is_tall_enough=True)],
            public=True,
        )
        self.client.save(gs)

        # Now fetch it again
        res = self.client.get(
            default.GameSession.select(
                num=True,
                players=True,
            ).filter(num=1001, public=True)
        )
        self.assertEqual(res.num, 1001)
        self.assertEqual(len(res.players), 1)
        p = res.players[0]

        self.assertEqual(p.name, "Zoe")
        self.assertEqual(p.__linkprops__.is_tall_enough, True)

    @tb.typecheck
    def test_modelgen_linkprops_2(self):
        from models import default

        # Create a new GameSession and add a player
        u = self.client.get(default.User.filter(name="Elsa"))
        gs = default.GameSession(num=1002)
        gs.players.append(u)
        self.client.save(gs)

        # Now fetch it again snd update
        gs = self.client.get(
            default.GameSession.select(
                "*",
                players=True,
            ).filter(num=1002)
        )
        self.assertEqual(gs.num, 1002)
        self.assertEqual(gs.public, False)
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

        self.assertEqual(p.name, "Elsa")
        self.assertEqual(p.__linkprops__.is_tall_enough, False)

    @tb.typecheck
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

        self.assertEqual(p0.name, "Dana")
        self.assertEqual(p0.nickname, None)
        self.assertEqual(p0.__linkprops__.is_tall_enough, True)

        p0.name = "Dana?"
        p0.nickname = "HACKED"
        p0.__linkprops__.is_tall_enough = False

        self.client.save(res)

        # Now fetch it again
        upd = self.client.get(q)
        self.assertEqual(upd.num, 456)
        self.assertEqual(len(upd.players), 1)
        p1 = upd.players[0]

        self.assertEqual(p1.name, "Dana?")
        self.assertEqual(p1.nickname, "HACKED")
        self.assertEqual(p1.__linkprops__.is_tall_enough, False)

    def test_modelgen_reflection_1(self):
        from models import default

        from gel._internal._edgeql import Cardinality, PointerKind

        self.assert_pointers_match(
            default.User,
            [
                MockPointer(
                    name="__type__",
                    cardinality=Cardinality.One,
                    computed=False,
                    properties=None,
                    kind=PointerKind.Link,
                    readonly=True,
                    type=SchemaPath("schema", "ObjectType"),
                ),
                MockPointer(
                    name="groups",
                    cardinality=Cardinality.Many,
                    computed=True,
                    properties=None,
                    kind=PointerKind.Link,
                    readonly=False,
                    type=SchemaPath("default", "UserGroup"),
                ),
                MockPointer(
                    name="id",
                    cardinality=Cardinality.One,
                    computed=False,
                    properties=None,
                    kind=PointerKind.Property,
                    readonly=True,
                    type=SchemaPath("std", "uuid"),
                ),
                MockPointer(
                    name="name",
                    cardinality=Cardinality.One,
                    computed=False,
                    properties=None,
                    kind=PointerKind.Property,
                    readonly=False,
                    type=SchemaPath("std", "str"),
                ),
                MockPointer(
                    name="name_len",
                    cardinality=Cardinality.One,
                    computed=True,
                    properties=None,
                    kind=PointerKind.Property,
                    readonly=False,
                    type=SchemaPath("std", "int64"),
                ),
                MockPointer(
                    name="nickname",
                    cardinality=Cardinality.AtMostOne,
                    computed=False,
                    properties=None,
                    kind=PointerKind.Property,
                    readonly=False,
                    type=SchemaPath("std", "str"),
                ),
                MockPointer(
                    name="nickname_len",
                    cardinality=Cardinality.AtMostOne,
                    computed=True,
                    properties=None,
                    kind=PointerKind.Property,
                    readonly=False,
                    type=SchemaPath("std", "int64"),
                ),
            ],
        )

        self.assert_pointers_match(
            default.GameSession,
            [
                MockPointer(
                    name="__type__",
                    cardinality=Cardinality.One,
                    computed=False,
                    properties=None,
                    kind=PointerKind.Link,
                    readonly=True,
                    type=SchemaPath("schema", "ObjectType"),
                ),
                MockPointer(
                    name="num",
                    cardinality=Cardinality.One,
                    computed=False,
                    properties=None,
                    kind=PointerKind.Property,
                    readonly=False,
                    type=SchemaPath("std", "int64"),
                ),
                MockPointer(
                    name="id",
                    cardinality=Cardinality.One,
                    computed=False,
                    properties=None,
                    kind=PointerKind.Property,
                    readonly=True,
                    type=SchemaPath("std", "uuid"),
                ),
                MockPointer(
                    name="players",
                    cardinality=Cardinality.Many,
                    computed=False,
                    properties={
                        "is_tall_enough": MockPointer(
                            name="is_tall_enough",
                            cardinality=Cardinality.AtMostOne,
                            computed=False,
                            properties=None,
                            kind=PointerKind.Property,
                            readonly=False,
                            type=SchemaPath("std", "bool"),
                        )
                    },
                    kind=PointerKind.Link,
                    readonly=False,
                    type=SchemaPath("default", "User"),
                ),
                MockPointer(
                    name="public",
                    cardinality=Cardinality.One,
                    computed=False,
                    properties=None,
                    kind=PointerKind.Property,
                    readonly=False,
                    type=SchemaPath("std", "bool"),
                ),
            ],
        )

        ntup_t = default.sub.TypeInSub.__gel_reflection__.pointers["ntup"].type
        self.assertEqual(
            ntup_t.as_schema_name(),
            "tuple<a:std::str, b:tuple<c:std::int64, d:std::str>>",
        )


class TestEmptyAiModelGenerator(tb.ModelTestCase):
    DEFAULT_MODULE = "default"
    SCHEMA = os.path.join(os.path.dirname(__file__), "dbsetup", "empty_ai.gel")

    @tb.typecheck
    def test_modelgen_empty_ai_schema_1(self):
        # This is it, we're just testing empty import.
        import models

        self.assertEqual(
            models.sys.ExtensionPackage.__name__, "ExtensionPackage"
        )

    @tb.typecheck
    def test_modelgen_empty_ai_schema_2(self):
        # This is it, we're just testing empty import.
        from models.ext import ai  # noqa: F401


# TODO: currently the schema and the tests here are broken in a way that makes
# it hard to integrate them with the main tests suite without breaking many
# tests in it as well. Presumably after fixing the issues, the schema and
# tests can just be merged with the main suite.
class TestModelGeneratorOther(tb.ModelTestCase):
    SCHEMA = os.path.join(
        os.path.dirname(__file__), "dbsetup", "orm_other.gel"
    )

    SETUP = os.path.join(
        os.path.dirname(__file__), "dbsetup", "orm_other.edgeql"
    )

    ISOLATED_TEST_BRANCHES = True

    @tb.xfail
    def test_modelgen_scalars_01(self):
        from models import default

        # Get the object with non-trivial scalars
        s = self.client.get(default.AssortedScalars.filter(name="hello world"))
        self.assertEqual(s.name, "hello world")
        self.assertEqual(
            json.loads(s.json),
            [
                "hello",
                {
                    "age": 42,
                    "name": "John Doe",
                    "special": None,
                },
                False,
            ],
        )
        self.assertEqual(s.bstr, b"word\x00\x0b")
        self.assertEqual(s.time, dt.time(20, 13, 45, 678000))
        self.assertEqual(
            [(t[0], json.loads(t[1])) for t in s.nested_mixed],
            [
                (
                    [1, 1, 2, 3],
                    {"next": 5, "label": "Fibonacci sequence"},
                ),
                (
                    [123, 0, 0, 3],
                    "simple JSON",
                ),
                (
                    [],
                    None,
                ),
            ],
        )
        self.assertEqual(s.positive, 123)

    def _compare_scalars(self, tname, name, prop):
        self.assertTrue(
            self.client.query_single(f"""
                with
                    A := assert_single((
                        select {tname}
                        filter .name = 'hello world'
                    )),
                    B := assert_single((
                        select {tname}
                        filter .name = {name!r}
                    )),
                select A.{prop} = B.{prop}
            """),
            f"property {prop} value does not match",
        )

    @tb.xfail
    def test_modelgen_scalars_02(self):
        from models import default

        # Create a new AssortedScalars object with all fields same as the
        # existing one, except the name. This makes it easy to verify the
        # result.
        s = default.AssortedScalars(name="scalars test 1")
        s.json = json.dumps(
            [
                "hello",
                {
                    "age": 42,
                    "name": "John Doe",
                    "special": None,
                },
                False,
            ],
        )
        self.client.save(s)
        self._compare_scalars("AssortedScalars", "scalars test 1", "json")

        s.bstr = b"word\x00\x0b"
        self.client.save(s)
        self._compare_scalars("AssortedScalars", "scalars test 1", "bstr")

        s.time = dt.time(20, 13, 45, 678000)
        s.date = dt.date(2025, 1, 26)
        s.ts = dt.datetime(2025, 1, 26, 20, 13, 45, tzinfo=dt.timezone.utc)
        s.lts = dt.datetime(2025, 1, 26, 20, 13, 45)
        self.client.save(s)
        self._compare_scalars("AssortedScalars", "scalars test 1", "time")
        self._compare_scalars("AssortedScalars", "scalars test 1", "date")
        self._compare_scalars("AssortedScalars", "scalars test 1", "ts")
        self._compare_scalars("AssortedScalars", "scalars test 1", "lts")

        s.positive = 123
        self.client.save(s)
        self._compare_scalars("AssortedScalars", "scalars test 1", "positive")

    @tb.xfail
    def test_modelgen_scalars_03(self):
        from models import default

        # Test deeply nested mixed collections.
        s = default.AssortedScalars(name="scalars test 2")
        s.nested_mixed = [
            (
                [1, 1, 2, 3],
                json.dumps({"next": 5, "label": "Fibonacci sequence"}),
            ),
            (
                [123, 0, 0, 3],
                '"simple JSON"',
            ),
            (
                [],
                "null",
            ),
        ]
        self.client.save(s)
        self._compare_scalars(
            "AssortedScalars", "scalars test 2", "nested_mixed"
        )

    @tb.xfail
    def test_modelgen_escape_01(self):
        from models import default
        # insert an object that needs a lot of escaping

        a = self.client.get(default.User.filter(name="Alice"))
        obj = default.limit(
            alter=False,
            like="like this",
            commit=a,
            configure=[default.limit.configure.link(a, create=True)],
        )
        self.client.save(obj)

        # Fetch and verify
        res = self.client.get(
            default.limit.select(
                alter=True,
                like=True,
                commit=True,
                configure=True,
            )
        )
        self.assertEqual(res.alter, False)
        self.assertEqual(res.like, "like this")
        self.assertEqual(res.commit.name, "Alice")
        self.assertEqual(len(res.configure), 1)
        self.assertEqual(res.configure[0].name, "Alice")
        self.assertEqual(res.configure[0].__linkprops__.create, True)

    @tb.xfail
    def test_modelgen_escape_02(self):
        from models import default
        # insert and update an object that needs a lot of escaping

        a = self.client.get(default.User.filter(name="Alice"))
        obj = default.limit(
            alter=False,
        )
        self.client.save(obj)
        obj.like = "like this"
        self.client.save(obj)
        obj.commit = a
        self.client.save(obj)
        obj.configure.append(default.limit.configure.link(a, create=True))
        self.client.save(obj)

        # Fetch and verify
        res = self.client.get(
            default.limit.select(
                alter=True,
                like=True,
                commit=True,
                configure=True,
            )
        )
        self.assertEqual(res.alter, False)
        self.assertEqual(res.like, "like this")
        self.assertEqual(res.commit.name, "Alice")
        self.assertEqual(len(res.configure), 1)
        self.assertEqual(res.configure[0].name, "Alice")
        self.assertEqual(res.configure[0].__linkprops__.create, True)

    @tb.xfail
    def test_modelgen_enum_01(self):
        from models import default

        res = self.client.query(default.EnumTest.order_by(color=True))

        self.assertEqual(len(res), 3)
        self.assertEqual(
            [r.color for r in res],
            [
                default.Color.Red,
                default.Color.Green,
                default.Color.Blue,
            ],
        )

    @tb.xfail
    def test_modelgen_enum_02(self):
        from models import default

        e = default.EnumTest(name="color test 1", color="Orange")
        self.client.save(e)

        e2 = self.client.get(default.EnumTest.filter(name="color test 1"))
        self.assertEqual(e2.color, default.Color.Orange)

        e.color = default.Color.Indigo
        self.client.save(e)

        e2 = self.client.get(default.EnumTest.filter(name="color test 1"))
        self.assertEqual(e2.color, default.Color.Indigo)
