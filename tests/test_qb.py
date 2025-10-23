#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2025-present MagicStack Inc. and the EdgeDB authors.
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

import os

from gel._internal._testbase import _models as tb


@tb.typecheck
class TestQueryBuilder(tb.ModelTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), "dbsetup", "orm_qb.gel")

    SETUP = os.path.join(os.path.dirname(__file__), "dbsetup", "orm_qb.edgeql")

    ISOLATED_TEST_BRANCHES = False

    def test_implicit_select_01(self):
        # Schema Set

        from models.orm_qb import default

        # mypy complains if this doesn't have type annotation???
        users: list[default.User] = self.client.query(default.User)

        self._assertObjectsWithFields(
            users,
            "name",
            [
                (
                    default.User,
                    {
                        'name': "Alice",
                        'name_len': 5,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
                (
                    default.User,
                    {
                        'name': "Billie",
                        'name_len': 6,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
                (
                    default.User,
                    {
                        'name': "Cameron",
                        'name_len': 7,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
                (
                    default.User,
                    {
                        'name': "Dana",
                        'name_len': 4,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
                (
                    default.User,
                    {
                        'name': "Elsa",
                        'name_len': 4,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
                (
                    default.User,
                    {  # Is not converted to default.CustomUser
                        'name': "Zoe",
                        'name_len': 3,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
            ],
        )

    def test_implicit_select_02(self):
        # Schema Set + filter

        from models.orm_qb import default

        users = self.client.query(default.User.filter(name="Alice"))
        self._assertObjectsWithFields(
            users,
            "name",
            [
                (
                    default.User,
                    {
                        'name': "Alice",
                        'name_len': 5,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
            ],
        )

    def test_implicit_select_03(self):
        # Schema Set + Path

        from models.orm_qb import default

        authors = self.client.query(default.Post.author)

        self._assertObjectsWithFields(
            authors,
            "name",
            [
                (
                    default.User,
                    {
                        'name': "Alice",
                        'name_len': 5,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
                (
                    default.User,
                    {
                        'name': "Cameron",
                        'name_len': 7,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
                (
                    default.User,
                    {
                        'name': "Elsa",
                        'name_len': 4,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
            ],
        )

    def test_implicit_select_04(self):
        # Schema Set + Filter + Path

        from models.orm_qb import default

        authors = self.client.query(
            default.Post.filter(body="I'm Alice").author
        )

        self._assertObjectsWithFields(
            authors,
            "name",
            [
                (
                    default.User,
                    {
                        'name': "Alice",
                        'name_len': 5,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
            ],
        )

    def test_implicit_select_05(self):
        # Schema Set + Path + Filter

        from models.orm_qb import default

        authors = self.client.query(default.Post.author.filter(name="Alice"))

        self._assertObjectsWithFields(
            authors,
            "name",
            [
                (
                    default.User,
                    {
                        'name': "Alice",
                        'name_len': 5,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
            ],
        )

    def test_implicit_select_06(self):
        # Schema Set + Filter + Path + Filter

        from models.orm_qb import default

        authors = self.client.query(
            default.Post.filter(body="I'm Alice").author.filter(name="Alice")
        )

        self._assertObjectsWithFields(
            authors,
            "name",
            [
                (
                    default.User,
                    {
                        'name': "Alice",
                        'name_len': 5,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
            ],
        )

    def test_implicit_select_07(self):
        # Ensure select without a schema path doesn't use a splat
        from models.orm_qb import default

        groups = self.client.query(
            default.UserGroup.select(users=lambda x: x.users).filter(
                name="green"
            )
        )
        self.assertEqual(len(groups), 1)

        for user in groups[0].users:
            self._assertNotHasFields(user, {"name", "nickname"})

    def test_implicit_select_08(self):
        # In shape: Schema Set
        from models.orm_qb import default

        groups = self.client.query(
            default.UserGroup.select(users=default.User).filter(name="green")
        )
        self.assertEqual(len(groups), 1)

        users = groups[0].users
        self._assertObjectsWithFields(
            users,
            "name",
            [
                (
                    default.User,
                    {
                        'name': "Alice",
                        'name_len': 5,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
                (
                    default.User,
                    {
                        'name': "Billie",
                        'name_len': 6,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
                (
                    default.User,
                    {
                        'name': "Cameron",
                        'name_len': 7,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
                (
                    default.User,
                    {
                        'name': "Dana",
                        'name_len': 4,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
                (
                    default.User,
                    {
                        'name': "Elsa",
                        'name_len': 4,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
                (
                    default.User,
                    {  # Is not converted to default.CustomUser
                        'name': "Zoe",
                        'name_len': 3,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
            ],
        )

    def test_implicit_select_09(self):
        # In shape: Schema Set
        from models.orm_qb import default

        groups = self.client.query(
            default.UserGroup.select(
                users=default.User.filter(name="Alice")
            ).filter(name="green")
        )
        self.assertEqual(len(groups), 1)

        users = groups[0].users
        self._assertObjectsWithFields(
            users,
            "name",
            [
                (
                    default.User,
                    {
                        'name': "Alice",
                        'name_len': 5,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
            ],
        )

    def test_implicit_select_10(self):
        # In shape: Schema Set + Path
        from models.orm_qb import default

        groups = self.client.query(
            default.UserGroup.select(users=default.Post.author).filter(
                name="green"
            )
        )
        self.assertEqual(len(groups), 1)

        users = groups[0].users
        self._assertObjectsWithFields(
            users,
            "name",
            [
                (
                    default.User,
                    {
                        'name': "Alice",
                        'name_len': 5,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
                (
                    default.User,
                    {
                        'name': "Cameron",
                        'name_len': 7,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
                (
                    default.User,
                    {
                        'name': "Elsa",
                        'name_len': 4,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
            ],
        )

    def test_implicit_select_11(self):
        # In shape: Schema Set + Filter + Path
        from models.orm_qb import default

        groups = self.client.query(
            default.UserGroup.select(
                users=default.Post.filter(body="I'm Alice").author
            ).filter(name="green")
        )
        self.assertEqual(len(groups), 1)

        users = groups[0].users
        self._assertObjectsWithFields(
            users,
            "name",
            [
                (
                    default.User,
                    {
                        'name': "Alice",
                        'name_len': 5,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
            ],
        )

    def test_implicit_select_12(self):
        # In shape: Schema Set + Filter + Path
        from models.orm_qb import default

        groups = self.client.query(
            default.UserGroup.select(
                users=default.Post.author.filter(name="Alice")
            ).filter(name="green")
        )
        self.assertEqual(len(groups), 1)

        users = groups[0].users
        self._assertObjectsWithFields(
            users,
            "name",
            [
                (
                    default.User,
                    {
                        'name': "Alice",
                        'name_len': 5,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
            ],
        )

    def test_implicit_select_13(self):
        # In shape: Schema Set + Filter + Path
        from models.orm_qb import default

        groups = self.client.query(
            default.UserGroup.select(
                users=default.Post.filter(body="I'm Alice").author.filter(
                    name="Alice"
                )
            ).filter(name="green")
        )
        self.assertEqual(len(groups), 1)

        users = groups[0].users
        self._assertObjectsWithFields(
            users,
            "name",
            [
                (
                    default.User,
                    {
                        'name': "Alice",
                        'name_len': 5,
                        'nickname': None,
                        'nickname_len': None,
                    },
                ),
            ],
        )

    def test_qb_computed_01(self):
        """Replace an existing field with a computed literal value"""
        from models.orm_qb import default, std

        res1 = self.client.get(
            default.User.select(
                name=True,
                nickname="hello",
            ).filter(name="Alice")
        )
        self.assertEqual(res1.name, "Alice")
        self.assertEqual(res1.nickname, "hello")

        res2 = self.client.get(
            default.User.select(
                name=True,
                nickname=std.str("hello"),
            ).filter(name="Alice")
        )
        self.assertEqual(res2.name, "Alice")
        self.assertEqual(res2.nickname, "hello")

    def test_qb_computed_02(self):
        from models.orm_qb import default

        res = self.client.get(
            default.User.select(
                name=True,
                nickname=lambda u: "Little " + u.name,
            ).filter(name="Alice")
        )
        self.assertEqual(res.name, "Alice")
        self.assertEqual(res.nickname, "Little Alice")

    def test_qb_computed_03(self):
        from models.orm_qb import default, std

        res = self.client.get(
            default.User.select(
                name=True,
                nickname=lambda u: u.name + std.str.cast(std.len(u.name)),
            ).filter(name="Alice")
        )
        self.assertEqual(res.name, "Alice")
        self.assertEqual(res.nickname, "Alice5")

    def test_qb_computed_04(self):
        from models.orm_qb import default, std

        class MyUser(default.User):
            foo: std.str

        res = self.client.get(
            MyUser.select(
                name=True,
                foo="hello",
            ).filter(name="Alice")
        )
        self.assertEqual(res.name, "Alice")
        self.assertEqual(res.foo, "hello")

    def test_qb_computed_05(self):
        from models.orm_qb import default

        res = self.client.get(
            default.User.select(
                name=True,
                name_len=True,
            ).filter(name="Alice")
        )
        self.assertEqual(res.name, "Alice")
        self.assertEqual(res.name_len, 5)

        res2 = self.client.get(
            default.User.select(
                name=True,
                name_len=lambda u: u.name_len * 3,
            ).filter(name="Alice")
        )
        self.assertEqual(res2.name, "Alice")
        self.assertEqual(res2.name_len, 15)

    def test_qb_computed_06(self):
        from models.orm_qb import default

        res = self.client.get(
            default.User.select(
                name=lambda u: u.name[0],
            ).filter(name="A")
        )
        self.assertEqual(res.name, "A")

        # Switch the order: filter first, then select
        res2 = self.client.get(
            default.User.filter(name="Alice").select(
                name=lambda u: u.name[0],
            )
        )
        self.assertEqual(res2.name, "A")

        res3 = self.client.get(
            default.User.filter(name="Alice")
            .select(
                name=lambda u0: u0.name[0],
            )
            .select(
                name=lambda u1: u1.name + "!",
            )
        )
        self.assertEqual(res3.name, "A!")

    def test_qb_order_01(self):
        from models.orm_qb import default

        res = self.client.query(default.User.order_by(name=True))
        self.assertEqual(
            [u.name for u in res],
            ["Alice", "Billie", "Cameron", "Dana", "Elsa", "Zoe"],
        )

    def test_qb_order_02(self):
        from models.orm_qb import default

        res = self.client.get(
            default.GameSession.select(
                num=True,
                # Use lambda in order_by
                players=lambda g: g.players.select("*").order_by(
                    lambda u: u.name[0]
                ),
            ).filter(num=123)
        )
        self.assertEqual(res.num, 123)
        self.assertEqual(
            [(u.name, u.__linkprops__.is_tall_enough) for u in res.players],
            [("Alice", False), ("Billie", True)],
        )

    def test_qb_order_03(self):
        from models.orm_qb import default

        res = self.client.get(
            default.GameSession.select(
                num=True,
                players=lambda g: g.players.select(
                    name=True,
                ).order_by(lambda u: u.__linkprops__.is_tall_enough),
            ).filter(num=123)
        )
        self.assertEqual(res.num, 123)
        self.assertEqual(
            [(u.name, u.__linkprops__.is_tall_enough) for u in res.players],
            [("Alice", False), ("Billie", True)],
        )

    def test_qb_order_04(self):
        from models.orm_qb import default, std

        res = self.client.query(
            default.UserGroup.select(
                name=True,
            )
            .filter(
                lambda g: std.count(g.users.filter(lambda u: u.name_len > 5))
                >= 0
            )
            .order_by(name=True)
        )
        names = [x.name for x in res]
        self.assertEqual(names, sorted(names))

    def test_qb_filter_01(self):
        from models.orm_qb import default, std

        res = self.client.query(
            default.User.filter(lambda u: std.like(u.name, "%e%")).order_by(
                name=True
            )
        )
        self.assertEqual(
            [u.name for u in res], ["Alice", "Billie", "Cameron", "Zoe"]
        )

        # Test with std.contains instead of std.like
        res2 = list(
            self.client.query(
                default.User.filter(
                    lambda u: std.contains(u.name, "e")
                ).order_by(name=True)
            )
        )
        self.assertEqual(
            [u.name for u in res2], ["Alice", "Billie", "Cameron", "Zoe"]
        )

        # Compare the objects
        self.assertEqual(list(res), list(res2))

    def test_qb_filter_02(self):
        from models.orm_qb import default

        res = self.client.get(
            default.UserGroup.select("*", users=True).filter(name="red")
        )
        self.assertEqual(res.name, "red")
        self.assertEqual(res.mascot, "dragon")
        self.assertEqual(
            list(sorted(u.name for u in res.users)),
            ["Alice", "Billie", "Cameron", "Dana"],
        )

    def test_qb_filter_03(self):
        from models.orm_qb import default

        res = self.client.get(
            default.UserGroup.select(
                "*",
                # Skip explicit select clause
                users=lambda g: g.users.order_by(name=True),
            ).filter(name="red")
        )
        self.assertEqual(res.name, "red")
        self.assertEqual(res.mascot, "dragon")

        # We didn't fetch name as part of that query so we need to
        # fetch them separately to check that the order still worked.
        users = []
        for u in res.users:
            ures = self.client.get(
                default.User.select(name=True).filter(id=u.id)
            )
            users.append(ures.name)

        self.assertEqual(users, ["Alice", "Billie", "Cameron", "Dana"])

    def test_qb_filter_04(self):
        from models.orm_qb import default, std

        res = self.client.get(
            default.UserGroup.select(
                "*",
                users=lambda g: g.users.select("*")
                .filter(lambda u: std.like(u.name, "%e%"))
                .order_by(name="desc"),
            ).filter(name="red")
        )
        self.assertEqual(res.name, "red")
        self.assertEqual(res.mascot, "dragon")
        self.assertEqual(
            [u.name for u in res.users], ["Cameron", "Billie", "Alice"]
        )

    @tb.xfail_unimplemented('''
        I think supporting *typing* this will need a PEP.
        We could make it work dynamically if we wanted, though
        or add a **kwargs.
    ''')
    def test_qb_filter_05(self):
        from models.orm_qb import default, std

        # Test filter by ad-hoc computed
        res = self.client.get(
            default.UserGroup.select(
                name=True,
                user_count=lambda g: std.count(g.users),
            ).filter(user_count=4)
        )
        self.assertEqual(res.name, "red")
        self.assertEqual(res.user_count, 4)

    @tb.xfail_unimplemented('''
        I think supporting *typing* this will need a PEP.
        We could make it work dynamically if we wanted, though.
    ''')
    def test_qb_filter_06(self):
        from models.orm_qb import default, std

        # Test filter by compex expression
        res = self.client.get(
            default.UserGroup.select(
                name=True,
                count=lambda g: std.count(
                    g.users.filter(lambda u: u.name_len > 5)
                ),
            ).filter(
                lambda g: std.count(g.users.filter(lambda u: u.name_len > 5))
                == 2
            )
        )
        self.assertEqual(res.name, "red")
        self.assertEqual(res.count, 2)

    def test_qb_filter_07(self):
        from models.orm_qb import default

        # Test filter with nested property expression
        res = self.client.query(
            default.Post.select("**")
            .filter(lambda p: p.author.groups.name == "green")
            .order_by(body=True)
        )
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0].author.name, "Alice")
        self.assertEqual(res[0].body, "Hello")
        self.assertEqual(res[1].author.name, "Alice")
        self.assertEqual(res[1].body, "I'm Alice")

    def test_qb_filter_08(self):
        from models.orm_qb import default, std

        # Test filter with nested property expression
        res = self.client.query(
            default.Post.select("**").filter(
                lambda p: std.not_in("red", p.author.groups.name)
            )
        )
        self.assertEqual(len(res), 1)
        post = res[0]
        self.assertEqual(post.author.name, "Elsa")
        self.assertEqual(post.body, "*magic stuff*")

    def test_qb_filter_09(self):
        from models.orm_qb import default, std

        # Find GameSession with same players as the green group
        green = default.UserGroup.select(users=True).filter(name="green")
        q = default.GameSession.select(
            "*",
            players=True,
        ).filter(
            lambda g: std.array_agg(g.players.order_by(id=True).id)
            == std.array_agg(green.users.order_by(id=True).id)
        )

        res = self.client.get(q)
        green_res = self.client.get(green)
        self.assertEqual(res.num, 123)
        self.assertEqual(
            {u.id for u in res.players},
            {u.id for u in green_res.users},
        )

    def test_qb_filter_10(self):
        from models.orm_qb import default, std

        # Find GameSession with same *number* of players as the green group
        green = default.UserGroup.select(users=True).filter(name="green")
        q = default.GameSession.select(
            "*",
            players=True,
        ).filter(lambda g: std.count(g.players) == std.count(green.users))

        res = self.client.get(q)
        green_res = self.client.get(green)
        self.assertEqual(res.num, 123)
        self.assertEqual(
            {u.id for u in res.players},
            {u.id for u in green_res.users},
        )

    def test_qb_filter_11(self):
        from models.orm_qb import default

        sess_client = self.client.with_globals(
            {"default::current_game_session_num": 123}
        )

        q = (
            default.User.select(
                name=True,
            )
            .filter(
                # want to use std::in_, but that fails dynamically...
                lambda g: g == default.CurrentGameSession.players
            )
            .order_by(name=True)
        )
        res = sess_client.query(q)
        self.assertEqual(len(res), 2)

    def test_qb_filter_12(self):
        # Same as above but with an extra .select() in the filter
        from models.orm_qb import default

        sess_client = self.client.with_globals(
            {"default::current_game_session_num": 123}
        )

        q = (
            default.User.select(
                name=True,
            )
            .filter(
                # want to use std::in_, but that fails dynamically...
                lambda g: g.select() == default.CurrentGameSession.players
            )
            .order_by(name=True)
        )
        res = sess_client.query(q)
        self.assertEqual(len(res), 2)

    def test_qb_filter_13(self):
        from models.orm_qb import default, std

        # Create a complex filter expression using some std functions and an
        # unrelated subquery
        dana = default.User.filter(name="Dana")
        res = self.client.get(
            default.UserGroup.filter(
                lambda g: std.count(std.distinct(std.union(g.users, dana)))
                == 3
            )
        )
        self.assertEqual(res.name, "green")

    def test_qb_link_property_01(self):
        from models.orm_qb import default

        # Test fetching GameSession with players multi-link
        res = self.client.get(
            default.GameSession.select(
                num=True,
                public=True,
                players=lambda g: g.players.select("*").order_by(name=True),
            ).filter(num=123)
        )
        self.assertEqual(res.num, 123)
        self.assertTrue(res.public)
        self.assertEqual(
            [(u.name, u.__linkprops__.is_tall_enough) for u in res.players],
            [("Alice", False), ("Billie", True)],
        )

    def test_qb_link_property_02(self):
        from models.orm_qb import default

        # Test filtering players based on link property
        res = self.client.get(
            default.GameSession.select(
                num=True,
                players=lambda g: g.players.select(name=True)
                .filter(lambda u: u.__linkprops__.is_tall_enough)
                .order_by(name=True),
            ).filter(num=123)
        )
        self.assertEqual(res.num, 123)
        self.assertEqual([u.name for u in res.players], ["Billie"])

    def test_qb_multiprop_01(self):
        from models.orm_qb import default

        res = self.client.query(
            default.KitchenSink.select(
                str=True,
                p_multi_str=True,
            ).order_by(str=True)
        )

        self.assertEqual(len(res), 2)
        self.assertEqual(res[0].str, "another one")
        self.assertEqual(set(res[0].p_multi_str), {"quick", "fox", "jumps"})
        self.assertEqual(res[1].str, "hello world")
        self.assertEqual(set(res[1].p_multi_str), {"brown", "fox"})

    def test_qb_multiprop_02(self):
        from models.orm_qb import default, std

        res = self.client.get(
            default.KitchenSink.select(
                str=True,
                p_multi_str=True,
            ).filter(lambda k: std.in_("quick", k.p_multi_str))
        )
        self.assertEqual(res.str, "another one")
        self.assertEqual(set(res.p_multi_str), {"quick", "fox", "jumps"})

    def test_qb_multiprop_03(self):
        from models.orm_qb import default

        res = self.client.get(
            default.KitchenSink.select(
                str=True,
                p_multi_str=True,
                # In filters == and in behave similarly for multi props
            ).filter(lambda k: k.p_multi_str == "quick")
        )
        self.assertEqual(res.str, "another one")
        self.assertEqual(set(res.p_multi_str), {"quick", "fox", "jumps"})

    @tb.xfail_unimplemented('''
        We don't support .order_by on a multi prop.
        We might want *some* way to do it.
    ''')
    def test_qb_multiprop_04(self):
        from models.orm_qb import default

        res = self.client.get(
            default.KitchenSink.select(
                str=True,
                # FIXME: Not sure how to express ordering a multi prop
                p_multi_str=lambda k: k.p_multi_str.order_by(k.p_multi_str),
            ).filter(str="another one")
        )
        self.assertEqual(res.str, "another one")
        self.assertEqual(set(res.p_multi_str), {"brown", "jumps"})

    @tb.xfail_unimplemented('''
        We don't support .filter on a multi prop.
        We might want *some* way to do it.
    ''')
    def test_qb_multiprop_05(self):
        from models.orm_qb import default, std

        res = self.client.get(
            default.KitchenSink.select(
                str=True,
                # FIXME: Not sure how to express filtering a multi prop
                p_multi_str=lambda k: k.p_multi_str.filter(
                    lambda s: std.len(s) == 5
                ),
            ).filter(str="another one")
        )
        self.assertEqual(res.str, "another one")
        self.assertEqual(set(res.p_multi_str), {"brown", "jumps"})

    def test_qb_limit_offset_01(self):
        from models.orm_qb import default, std

        res = self.client.get(
            default.User.select(name=True)
            .filter(lambda u: std.contains(u.name, "li"))
            .order_by(lambda u: u.name)
            .offset(1)
            .limit(1)
        )
        self.assertEqual(
            res.model_dump(exclude={"id"}),
            {
                "name": "Billie",
                tb.TNAME: "default::User",
            },
        )

    def test_qb_boolean_operator_error_01(self):
        from models.orm_qb import default

        # Test that using 'and' operator raises TypeError
        with self.assertRaisesRegex(TypeError, "use std.and_"):
            default.User.filter(
                lambda u: u.name == "Alice" and u.nickname == "Al"
            )

        # Test that using 'or' operator raises TypeError
        with self.assertRaisesRegex(TypeError, "use std.or_"):
            default.User.filter(lambda u: u.name == "Alice" or u.name == "Bob")

        # Test that using 'not' operator raises TypeError
        with self.assertRaisesRegex(TypeError, "use std.not_"):
            default.User.filter(lambda u: not u.name)  # type: ignore [arg-type, return-value]

        # Test that using 'in'/'not in' operator raises TypeError
        with self.assertRaisesRegex(TypeError, "use std.in_"):
            default.User.filter(lambda u: "blue" in u.groups.name)  # type: ignore [arg-type, operator, return-value]

        with self.assertRaisesRegex(TypeError, "use std.in_"):
            default.User.filter(lambda u: "blue" not in u.groups.name)  # type: ignore [arg-type, operator, return-value]

        # Test that using bool() conversion raises TypeError
        with self.assertRaisesRegex(TypeError, "use std.exists"):
            default.User.filter(lambda u: bool(u.name))  # type: ignore [arg-type, return-value]

        # Test that using if statement raises TypeError
        with self.assertRaisesRegex(TypeError, "use std.if_"):
            default.User.filter(lambda u: u.name if u.name else "default")  # type: ignore [arg-type, return-value]

        # Test that using '== None' comparison raises TypeError
        with self.assertRaisesRegex(TypeError, r"use std.not_\(std.exists"):
            default.User.filter(lambda u: u.name == None)  # type: ignore  # noqa: E711

        # Test that using '!= None' comparison raises TypeError
        with self.assertRaisesRegex(TypeError, "use std.exists"):
            default.User.filter(lambda u: u.name != None)  # type: ignore  # noqa: E711

        # Test that using 'is None' comparison raises TypeError
        with self.assertRaisesRegex(TypeError, r"use std.not_\(std.exists"):
            default.User.filter(lambda u: u.name is None)  # type: ignore

        # Test that using 'is not None' comparison raises TypeError
        with self.assertRaisesRegex(TypeError, "use std.exists"):
            default.User.filter(lambda u: u.name is not None)  # type: ignore

    def test_qb_enum_01(self):
        from models.orm_qb import default

        e = self.client.get(default.EnumTest.filter(color=default.Color.Red))

        self.assertEqual(e.color, default.Color.Red)
        self.assertEqual(e.name, "red")

    def test_qb_for_01(self):
        from models.orm_qb import default, std

        res = self.client.query(
            std.for_(
                std.range_unpack(std.range(std.int64(1), std.int64(10))),
                lambda x: x * 2,
            )
        )
        self.assertEqual(set(res), {i * 2 for i in range(1, 10)})

        res = self.client.query(
            std.for_(
                std.range_unpack(std.range(std.int64(1), std.int64(3))),
                lambda x: std.for_(
                    std.range_unpack(std.range(std.int64(1), std.int64(3))),
                    lambda y: x * 10 + y,
                ),
            )
        )

        self.assertEqual(set(res), {11, 12, 21, 22})

        res2 = self.client.query(
            std.for_(
                default.User,
                lambda x: x.name,
            )
        )
        self.assertEqual(
            set(res2),
            {'Alice', 'Zoe', 'Billie', 'Dana', 'Cameron', 'Elsa'},
        )

        res3 = self.client.query(
            default.User.filter(
                lambda u: std.for_(
                    std.assert_exists(std.int64(0)),
                    # HMMMMM
                    lambda x: x == x,
                )
            )
        )
        self.assertEqual(len(res3), 6)

        res4 = self.client.query(
            default.User.filter(
                lambda u: std.for_(
                    u.name,
                    lambda x: x == "Alice",
                )
            )
        )
        self.assertEqual(len(res4), 1)

    def test_qb_poly_01(self):
        from models.orm_qb import default

        p = self.client.get(
            default.Person.select(
                "*",
                item=lambda p: p.item.select(
                    "*",
                    contents=lambda i: i.contents.select(
                        "*",
                    ).order_by(game_id=True),
                ),
            ).filter(
                game_id=1,
            )
        )

        self.assertEqual(p.name, "Alice")
        self.assertIsNone(p.item)

    def test_qb_poly_02(self):
        from models.orm_qb import default

        p = self.client.get(
            default.Person.select(
                "*",
                item=lambda p: p.item.select(
                    "*",
                    contents=lambda i: i.contents.select(
                        "*",
                    ).order_by(game_id=True),
                ),
            ).filter(
                game_id=2,
            )
        )

        self.assertEqual(p.name, "Billie")
        assert p.item
        self.assertEqual(p.item.name, "nice bag")
        self.assertEqual(p.item.contents, [])
        self.assertIsInstance(p.item, default.Bag)

    def test_qb_poly_03(self):
        from models.orm_qb import default

        p = self.client.get(
            default.Person.select(
                "*",
                item=lambda p: p.item.select(
                    "*",
                    contents=lambda i: i.contents.select(
                        "*",
                    ).order_by(game_id=True),
                ),
            ).filter(
                game_id=3,
            )
        )

        self.assertEqual(p.name, "Cameron")
        assert p.item
        self.assertEqual(p.item.name, "big box")
        self.assertIsInstance(p.item, default.Box)

        for c, (name, t) in zip(
            p.item.contents,
            [
                ("cotton candy", default.Candy),
                ("candy corn", default.Candy),
            ],
            strict=False,
        ):
            self.assertEqual(c.name, name)
            self.assertIsInstance(c, t)

    def test_qb_poly_04(self):
        from models.orm_qb import default

        p = self.client.get(
            default.Person.select(
                "*",
                item=lambda p: p.item.select(
                    "*",
                    contents=lambda i: i.contents.select(
                        "*",
                    ).order_by(game_id=True),
                ),
            ).filter(
                game_id=4,
            )
        )

        self.assertEqual(p.name, "Dana")
        assert p.item
        self.assertEqual(p.item.name, "round tin")
        self.assertIsInstance(p.item, default.Tin)

        for c, (name, t) in zip(
            p.item.contents,
            [
                ("milk", default.Chocolate),
                ("dark", default.Chocolate),
            ],
            strict=False,
        ):
            self.assertEqual(c.name, name)
            self.assertIsInstance(c, t)

    def test_qb_poly_05(self):
        from models.orm_qb import default

        p = self.client.get(
            default.Person.select(
                "*",
                item=lambda p: p.item.select(
                    "*",
                    contents=lambda i: i.contents.select(
                        "*",
                    ).order_by(game_id=True),
                ),
            ).filter(
                game_id=5,
            )
        )

        self.assertEqual(p.name, "Elsa")
        assert p.item
        self.assertEqual(p.item.name, "package")
        self.assertIsInstance(p.item, default.Box)

        for c, (name, t) in zip(
            p.item.contents,
            [
                ("lemon drop", default.Candy),
                ("blue bear", default.Gummy),
                ("sour worm", default.GummyWorm),
                ("almond", default.Chocolate),
            ],
            strict=False,
        ):
            self.assertEqual(c.name, name)
            self.assertIsInstance(c, t)

    def test_qb_poly_06(self):
        from models.orm_qb import default

        p = self.client.get(
            default.Person.select(
                "*",
                item=lambda p: p.item.select(
                    "*",
                    contents=lambda i: i.contents.select(
                        "*",
                    ).order_by(game_id=True),
                ),
            ).filter(
                game_id=6,
            )
        )

        self.assertEqual(p.name, "Zoe")
        assert p.item
        self.assertEqual(p.item.name, "fancy")
        self.assertIsInstance(p.item, default.GiftBox)

        for c, (name, t) in zip(
            p.item.contents,
            [
                ("sour worm", default.GummyWorm),
            ],
            strict=False,
        ):
            self.assertEqual(c.name, name)
            self.assertIsInstance(c, t)

    def test_qb_poly_07(self):
        from models.orm_qb import default

        tin = self.client.get(
            default.Tin.select(
                "*",
                contents=lambda i: i.contents.select(
                    "*",
                ).order_by(game_id=True),
            ).filter(
                game_id=12,
            )
        )

        self.assertEqual(tin.name, "round tin")
        self.assertIsInstance(tin, default.Tin)

        for c, (name, kind) in zip(
            tin.contents,
            [
                ("milk", "bar"),
                ("dark", "truffle"),
            ],
            strict=False,
        ):
            self.assertEqual(c.name, name)
            self.assertEqual(c.kind, kind)
            self.assertIsInstance(c, default.Chocolate)

    def test_qb_array_agg_01(self):
        from models.orm_qb import default, std

        agg = std.array_agg(default.User)
        unpack = std.array_unpack(agg)

        res = self.client.query(unpack)
        self.assertEqual(len(res), 6)

    def test_qb_cast_scalar_01(self):
        # scalar to scalar
        from models.orm_qb import std

        result = self.client.get(std.str.cast(std.int64(1)))
        self.assertEqual(result, "1")

    def test_qb_cast_scalar_02(self):
        # enum to scalar
        from models.orm_qb import default, std

        result = self.client.get(std.str.cast(default.Color.Red))
        self.assertEqual(result, "Red")

    def test_qb_cast_scalar_03(self):
        # scalar to enum
        from models.orm_qb import default, std

        result = self.client.get(default.Color.cast(std.str("Red")))
        self.assertEqual(result, default.Color.Red)

    def test_qb_cast_array_01(self):
        # array[scalar] to array[scalar]
        from models.orm_qb import std

        result = self.client.get(
            std.array[std.str].cast(
                std.array[std.int64](
                    [std.int64(1), std.int64(2), std.int64(3)]
                )
            )
        )
        self.assertEqual(result, ["1", "2", "3"])

    def test_qb_cast_array_02(self):
        # array[enum] to array[scalar]
        from models.orm_qb import default, std

        result = self.client.get(
            std.array[std.str].cast(
                std.array[default.Color](
                    [
                        default.Color.Red,
                        default.Color.Green,
                        default.Color.Blue,
                    ]
                )
            )
        )
        self.assertEqual(result, ["Red", "Green", "Blue"])

    def test_qb_cast_array_03(self):
        # array[scalar] to array[enum]
        from models.orm_qb import default, std

        result = self.client.get(
            std.array[default.Color].cast(
                std.array[std.str](
                    [std.str("Red"), std.str("Green"), std.str("Blue")]
                )
            )
        )
        self.assertEqual(
            result,
            [default.Color.Red, default.Color.Green, default.Color.Blue],
        )

    def test_qb_cast_array_04(self):
        # array[tuple] to array[tuple]
        from models.orm_qb import default, std

        result = self.client.get(
            std.array[std.tuple[std.int64, default.Color]].cast(
                std.array[std.tuple[std.str, std.str]](
                    [
                        std.tuple[std.str, std.str](
                            (std.str("1"), std.str("Red"))
                        ),
                        std.tuple[std.str, std.str](
                            (std.str("2"), std.str("Green"))
                        ),
                        std.tuple[std.str, std.str](
                            (std.str("3"), std.str("Blue"))
                        ),
                    ]
                )
            )
        )
        self.assertEqual(
            result,
            [
                (1, default.Color.Red),
                (2, default.Color.Green),
                (3, default.Color.Blue),
            ],
        )

    def test_qb_cast_tuple_01(self):
        # unnamed tuple to unnamed tuple
        from models.orm_qb import default, std

        result = self.client.get(
            std.tuple[
                std.int64, default.Color, std.str, std.array[std.int64]
            ].cast(
                std.tuple[std.str, std.str, default.Color, std.array[std.str]](
                    (
                        std.str("1"),
                        std.str("Red"),
                        default.Color.Green,
                        std.array[std.str](
                            [std.str("2"), std.str("3"), std.str("4")]
                        ),
                    )
                )
            )
        )
        self.assertEqual(result, (1, default.Color.Red, "Green", [2, 3, 4]))

    def test_qb_cast_range_01(self):
        # range to range
        from gel.datatypes import range as _range
        from models.orm_qb import std

        result = self.client.get(
            std.range[std.int64].cast(
                std.range[std.int32](std.int32(1), std.int32(9))
            )
        )
        self.assertEqual(result, _range.Range(std.int64(1), std.int64(9)))

    def test_qb_is_type_basic_01(self):
        # Simple TypeIntersection
        from models.orm_qb import default

        result = self.client.query(default.Inh_A.is_(default.Inh_B))

        self._assertObjectsWithFields(
            result,
            "a",
            [
                (
                    default.Inh_AB,
                    {
                        "a": 4,
                        "b": 5,
                    },
                ),
                (
                    default.Inh_ABC,
                    {
                        "a": 13,
                        "b": 14,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 17,
                        "b": 18,
                    },
                ),
            ],
            excluded_fields={'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )

    def test_qb_is_type_basic_02(self):
        # Chained TypeIntersection
        from models.orm_qb import default

        result = self.client.query(
            default.Inh_A.is_(default.Inh_B).is_(default.Inh_C)
        )

        self._assertObjectsWithFields(
            result,
            "a",
            [
                (
                    default.Inh_ABC,
                    {
                        "a": 13,
                        "b": 14,
                        "c": 15,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 17,
                        "b": 18,
                        "c": 19,
                    },
                ),
            ],
            excluded_fields={'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )

    def test_qb_is_type_basic_03(self):
        # TypeIntersection Select
        from models.orm_qb import default

        result = self.client.query(
            default.Inh_A.is_(default.Inh_B).select(a=True)
        )

        self._assertObjectsWithFields(
            result,
            "a",
            [
                (
                    default.Inh_AB,
                    {
                        "a": 4,
                    },
                ),
                (
                    default.Inh_ABC,
                    {
                        "a": 13,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 17,
                    },
                ),
            ],
            excluded_fields={'b', 'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )

    def test_qb_is_type_basic_04(self):
        # Model Select
        # with computed single prop using type intersection
        from models.orm_qb import default

        result = self.client.query(
            default.Inh_AB.select(a=lambda x: x.is_(default.Inh_C).c)
        )

        self._assertObjectsWithFields(
            result,
            "a",
            [
                (
                    default.Inh_AB,
                    {
                        "a": None,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 19,
                    },
                ),
            ],
            excluded_fields={'b', 'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )

    def test_qb_is_type_basic_05(self):
        # TypeIntersection Select
        # with computed single prop using type intersection
        from models.orm_qb import default

        result = self.client.query(
            default.Inh_A.is_(default.Inh_B).select(
                ab=lambda x: x.is_(default.Inh_AB).ab
            )
        )

        self._assertObjectsWithFields(
            result,
            "ab",
            [
                (
                    default.Inh_AB,
                    {
                        "ab": 6,
                    },
                ),
                (
                    default.Inh_ABC,
                    {
                        "ab": None,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "ab": 20,
                    },
                ),
            ],
            excluded_fields={'a', 'b', 'c', 'ac', 'bc', 'abc', 'ab_ac'},
        )

    def test_qb_is_type_basic_06(self):
        # TypeIntersection Select
        # with computed multi prop using type intersection
        from models.orm_qb import default, std

        result = self.client.query(
            default.Inh_A.is_(default.Inh_B).select(
                a=True,
                abc=lambda x: std.union(
                    x.is_(default.Inh_AB).ab,
                    x.is_(default.Inh_AC).ac,
                ),
            )
        )

        self._assertObjectsWithFields(
            result,
            "a",
            [
                (
                    default.Inh_AB,
                    {
                        "a": 4,
                        "abc": [6],
                    },
                ),
                (
                    default.Inh_ABC,
                    {
                        "a": 13,
                        "abc": [],
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 17,
                        "abc": [20, 21],
                    },
                ),
            ],
            excluded_fields={'b', 'c', 'ab', 'ac', 'bc', 'ab_ac'},
        )

    def test_qb_is_type_basic_07(self):
        # Link TypeIntersection
        from models.orm_qb import default

        result = self.client.query(default.Link_Inh_A.l.is_(default.Inh_B))

        self._assertObjectsWithFields(
            result,
            "a",
            [
                (
                    default.Inh_AB,
                    {
                        "a": 4,
                        "b": 5,
                    },
                ),
                (
                    default.Inh_ABC,
                    {
                        "a": 13,
                        "b": 14,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 17,
                        "b": 18,
                    },
                ),
            ],
            excluded_fields={'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )

    def test_qb_is_type_basic_08(self):
        # Link TypeIntersection Select
        # with computed single prop using type intersection
        from models.orm_qb import default

        result = self.client.query(
            default.Link_Inh_A.l.is_(default.Inh_B).select(
                a=True,
                ab=lambda x: x.is_(default.Inh_AB).ab,
            )
        )

        self._assertObjectsWithFields(
            result,
            "a",
            [
                (
                    default.Inh_AB,
                    {
                        "a": 4,
                        "ab": 6,
                    },
                ),
                (
                    default.Inh_ABC,
                    {
                        "a": 13,
                        "ab": None,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 17,
                        "ab": 20,
                    },
                ),
            ],
            excluded_fields={'b', 'c', 'ac', 'bc', 'abc', 'ab_ac'},
        )

    def test_qb_is_type_basic_09(self):
        # Model Select
        # with computed single link using type intersection
        from models.orm_qb import default

        inh_a_objs = self.client.query(default.Inh_A.select(a=True))
        possible_targets = {obj.a: obj for obj in inh_a_objs}

        result = self.client.query(
            default.Link_Inh_A.select(
                n=True, l=lambda x: x.l.is_(default.Inh_B)
            )
        )

        self._assertObjectsWithFields(
            result,
            "n",
            [
                (
                    default.Link_Inh_A,
                    {
                        "n": 1,
                        "l": None,
                    },
                ),
                (
                    default.Link_Inh_A,
                    {
                        "n": 4,
                        "l": possible_targets[4],
                    },
                ),
                (
                    default.Link_Inh_A,
                    {
                        "n": 7,
                        "l": None,
                    },
                ),
                (
                    default.Link_Inh_A,
                    {
                        "n": 13,
                        "l": possible_targets[13],
                    },
                ),
                (
                    default.Link_Inh_A,
                    {
                        "n": 17,
                        "l": possible_targets[17],
                    },
                ),
            ],
        )

        for r in result:
            if r.l is not None:
                self._assertNotHasFields(
                    r.l, {'a', 'b', 'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'}
                )

    def test_qb_is_type_basic_10(self):
        # Model Select
        # with computed single link using type intersection
        # with select
        from models.orm_qb import default

        inh_a_objs = self.client.query(default.Inh_A.select(a=True))
        possible_targets = {obj.a: obj for obj in inh_a_objs}

        result = self.client.query(
            default.Link_Inh_A.select(
                n=True, l=lambda x: x.l.is_(default.Inh_B).select(a=True)
            )
        )

        self._assertObjectsWithFields(
            result,
            "n",
            [
                (
                    default.Link_Inh_A,
                    {
                        "n": 1,
                        "l": None,
                    },
                ),
                (
                    default.Link_Inh_A,
                    {
                        "n": 4,
                        "l": possible_targets[4],
                    },
                ),
                (
                    default.Link_Inh_A,
                    {
                        "n": 7,
                        "l": None,
                    },
                ),
                (
                    default.Link_Inh_A,
                    {
                        "n": 13,
                        "l": possible_targets[13],
                    },
                ),
                (
                    default.Link_Inh_A,
                    {
                        "n": 17,
                        "l": possible_targets[17],
                    },
                ),
            ],
        )

        for r in result:
            if r.l is not None:
                self._assertHasFields(r.l, {'a'})
                self._assertNotHasFields(
                    r.l, {'b', 'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'}
                )

    def test_qb_is_type_for_01(self):
        # TypeIntersection in iterator
        from models.orm_qb import default, std

        result = self.client.query(
            std.for_(default.Inh_A.is_(default.Inh_B), lambda x: x).select(
                a=True
            )
        )

        self._assertObjectsWithFields(
            result,
            "a",
            [
                (
                    default.Inh_AB,
                    {
                        "a": 4,
                    },
                ),
                (
                    default.Inh_ABC,
                    {
                        "a": 13,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 17,
                    },
                ),
            ],
            excluded_fields={'b', 'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )

    @tb.xfail(
        '''ISE when applying shape to for loop with type intersection
        https://github.com/geldata/gel/issues/9092
        '''
    )
    def test_qb_is_type_for_02(self):
        # TypeIntersection in body
        from models.orm_qb import default, std

        result = self.client.query(
            std.for_(
                default.Inh_A,
                lambda x: x.is_(default.Inh_B).select(a=True),
            )
        )

        self._assertObjectsWithFields(
            result,
            "a",
            [
                (
                    default.Inh_AB,
                    {
                        "a": 4,
                    },
                ),
                (
                    default.Inh_ABC,
                    {
                        "a": 13,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 17,
                    },
                ),
            ],
            excluded_fields={'b', 'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )

    @tb.xfail(
        '''ISE when applying shape to for loop with type intersection
        https://github.com/geldata/gel/issues/9092
        '''
    )
    def test_qb_is_type_for_03(self):
        # TypeIntersection on entire statement
        from models.orm_qb import default, std

        result = self.client.query(
            std.for_(default.Inh_A, lambda x: x)
            .is_(default.Inh_B)
            .select(a=True)
        )

        self._assertObjectsWithFields(
            result,
            "a",
            [
                (
                    default.Inh_AB,
                    {
                        "a": 4,
                    },
                ),
                (
                    default.Inh_ABC,
                    {
                        "a": 13,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 17,
                    },
                ),
            ],
            excluded_fields={'b', 'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )

    def test_qb_is_type_as_function_arg_01(self):
        # Test that type exprs produced by is_ can be passed as function args
        from models.orm_qb import default, std

        result = self.client.query(
            std.distinct(default.Inh_A.is_(default.Inh_B)).select('*')
        )

        self._assertObjectsWithFields(
            result,
            "a",
            [
                (
                    default.Inh_AB,
                    {
                        "a": 4,
                        "b": 5,
                    },
                ),
                (
                    default.Inh_ABC,
                    {
                        "a": 13,
                        "b": 14,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 17,
                        "b": 18,
                    },
                ),
            ],
            excluded_fields={'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )

    def test_qb_is_type_as_function_arg_02(self):
        # Test that complex type exprs produced by is_ can be passed as
        # function args
        from models.orm_qb import default, std

        result = self.client.query(
            std.distinct(
                default.Inh_A.is_(default.Inh_B).is_(default.Inh_C)
            ).select('*')
        )

        self._assertObjectsWithFields(
            result,
            "a",
            [
                (
                    default.Inh_ABC,
                    {
                        "a": 13,
                        "b": 14,
                        "c": 15,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 17,
                        "b": 18,
                        "c": 19,
                    },
                ),
            ],
            excluded_fields={'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )


class TestQueryBuilderModify(tb.ModelTestCase):
    """This test suite is for data manipulation using QB."""

    SCHEMA = os.path.join(os.path.dirname(__file__), "dbsetup", "orm_qb.gel")

    SETUP = os.path.join(os.path.dirname(__file__), "dbsetup", "orm_qb.edgeql")

    ISOLATED_TEST_BRANCHES = True

    def test_qb_update_01(self):
        from models.orm_qb import default

        self.client.query(
            default.User.filter(name="Alice").update(
                name="Cooper",
                nickname="singer",
            )
        )

        res = self.client.get(default.User.filter(name="Cooper"))
        self.assertEqual(res.name, "Cooper")
        self.assertEqual(res.nickname, "singer")

    def test_qb_update_02(self):
        from models.orm_qb import default, std

        self.client.query(
            default.UserGroup.filter(name="blue").update(
                users=default.User.filter(
                    lambda u: std.in_(u.name, {"Zoe", "Dana"})
                )
            )
        )

        res = self.client.get(
            default.UserGroup.select("**").filter(name="blue")
        )
        self.assertEqual(res.name, "blue")
        self.assertEqual({u.name for u in res.users}, {"Zoe", "Dana"})

    def test_qb_update_03(self):
        from models.orm_qb import default, std

        # Combine update and select of the updated object
        res = self.client.get(
            default.Post.filter(body="Hello")
            .update(
                author=std.assert_single(default.User.filter(name="Billie"))  # type: ignore
            )
            .select("*", author=lambda p: p.author.select("**"))
        )

        self.assertEqual(res.body, "Hello")
        self.assertEqual(res.author.name, "Billie")
        self.assertEqual({g.name for g in res.author.groups}, {"red", "green"})

    # Fails at typecheck time because update's *types* dont't
    # support callbacks, though runtime does.
    @tb.skip_typecheck
    def test_qb_update_04(self):
        from models.orm_qb import default, std

        self.client.query(
            default.UserGroup.filter(name="blue").update(
                users=default.User.filter(
                    lambda u: std.in_(u.name, {"Zoe", "Dana"})
                )
            )
        )

        res0 = self.client.get(
            default.UserGroup.select("**").filter(name="blue")
        )
        self.assertEqual(res0.name, "blue")
        self.assertEqual({u.name for u in res0.users}, {"Zoe", "Dana"})

        # Add Alice to the group
        self.client.query(
            default.UserGroup.filter(name="blue").update(
                users=lambda g: std.assert_distinct(
                    std.union(g.users, default.User.filter(name="Alice"))
                )
            )
        )

        res1 = self.client.get(
            default.UserGroup.select("**").filter(name="blue")
        )
        self.assertEqual(res1.name, "blue")
        self.assertEqual(
            {u.name for u in res1.users}, {"Zoe", "Dana", "Alice"}
        )

        # Remove Dana from the group
        self.client.query(
            default.UserGroup.filter(name="blue").update(
                users=lambda g: std.except_(
                    g.users, default.User.filter(name="Dana")
                )
            )
        )

        res2 = self.client.get(
            default.UserGroup.select("**").filter(name="blue")
        )
        self.assertEqual(res2.name, "blue")
        self.assertEqual({u.name for u in res2.users}, {"Zoe", "Alice"})

    def test_qb_delete_01(self):
        from models.orm_qb import default

        before = self.client.query(
            default.Post.select(body=True).order_by(body=True)
        )
        self.assertEqual(
            [p.body for p in before],
            ["*magic stuff*", "Hello", "I'm Alice", "I'm Cameron"],
        )

        # Delete a specific post
        self.client.query(default.Post.filter(body="I'm Cameron").delete())

        after = self.client.query(
            default.Post.select(body=True).order_by(body=True)
        )
        self.assertEqual(
            [p.body for p in after], ["*magic stuff*", "Hello", "I'm Alice"]
        )

    def test_qb_delete_02(self):
        from models.orm_qb import default

        before = self.client.query(
            default.Post.select(body=True).order_by(body=True)
        )
        self.assertEqual(
            [p.body for p in before],
            ["*magic stuff*", "Hello", "I'm Alice", "I'm Cameron"],
        )

        # Delete posts by Alice
        self.client.query(
            default.Post.filter(lambda p: p.author.name == "Alice").delete()
        )

        after = self.client.query(
            default.Post.select(body=True).order_by(body=True)
        )
        self.assertEqual(
            [p.body for p in after], ["*magic stuff*", "I'm Cameron"]
        )

    def test_qb_delete_03(self):
        from models.orm_qb import default

        # Delete posts by Alice and fetch the deleted stuff
        res = self.client.query(
            default.Post.filter(lambda p: p.author.name == "Alice")
            .delete()
            .select("**")
            .order_by(body=True)
        )

        self.assertEqual(res[0].body, "Hello")
        self.assertEqual(res[0].author.name, "Alice")
        self.assertEqual(res[1].body, "I'm Alice")
        self.assertEqual(res[1].author.name, "Alice")

    def test_qb_enum_edit_01(self):
        from models.orm_qb import default

        e = self.client.get(
            default.EnumTest.filter(
                name="red",
            )
            .update(color=default.Color.Orange)
            .select("*")
        )

        self.assertEqual(e.color, default.Color.Orange)
        self.assertEqual(e.name, "red")

    def test_qb_enum_edit_02(self):
        from models.orm_qb import default

        e = self.client.get(
            default.EnumTest.filter(
                name="red",
            )
            .update(color=lambda e: default.Color("Violet"))
            .select("*")
        )

        self.assertEqual(e.color, default.Color.Violet)
        self.assertEqual(e.name, "red")

    def test_qb_update_is_type_01(self):
        # Type Intersection Update
        from models.orm_qb import default

        result = self.client.query(
            default.Inh_A.is_(default.Inh_B)
            .update(a=lambda x: x.a + 1000)
            .select(a=True)
        )
        self._assertObjectsWithFields(
            result,
            "a",
            [
                (
                    default.Inh_AB,
                    {
                        "a": 1004,
                    },
                ),
                (
                    default.Inh_ABC,
                    {
                        "a": 1013,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 1017,
                    },
                ),
            ],
            excluded_fields={'b', 'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )

        updated = self.client.query(default.Inh_A)
        self._assertObjectsWithFields(
            updated,
            "a",
            [
                (
                    default.Inh_A,
                    {
                        "a": 1,
                    },
                ),
                (
                    default.Inh_AB,
                    {
                        "a": 1004,
                    },
                ),
                (
                    default.Inh_AC,
                    {
                        "a": 7,
                    },
                ),
                (
                    default.Inh_ABC,
                    {
                        "a": 1013,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 1017,
                    },
                ),
                (
                    default.Inh_AXA,
                    {
                        "a": 1001,
                    },
                ),
            ],
            excluded_fields={'b', 'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )

    def test_qb_update_is_type_02(self):
        # Update Type Intersection
        from models.orm_qb import default

        result = self.client.query(
            default.Inh_A.update(a=lambda x: x.a + 1000)
            .is_(default.Inh_B)
            .select(a=True)
        )
        self._assertObjectsWithFields(
            result,
            "a",
            [
                (
                    default.Inh_AB,
                    {
                        "a": 1004,
                    },
                ),
                (
                    default.Inh_ABC,
                    {
                        "a": 1013,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 1017,
                    },
                ),
            ],
            excluded_fields={'b', 'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )

        updated = self.client.query(default.Inh_A)
        self._assertObjectsWithFields(
            updated,
            "a",
            [
                (
                    default.Inh_A,
                    {
                        "a": 1001,
                    },
                ),
                (
                    default.Inh_AB,
                    {
                        "a": 1004,
                    },
                ),
                (
                    default.Inh_AC,
                    {
                        "a": 1007,
                    },
                ),
                (
                    default.Inh_ABC,
                    {
                        "a": 1013,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 1017,
                    },
                ),
                (
                    default.Inh_AXA,
                    {
                        "a": 2001,
                    },
                ),
            ],
            excluded_fields={'b', 'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )

    def test_qb_delete_is_type_01(self):
        # Type Intersection Delete
        from models.orm_qb import default

        result = self.client.query(
            default.Inh_A.is_(default.Inh_B).delete().select(a=True)
        )
        self._assertObjectsWithFields(
            result,
            "a",
            [
                (
                    default.Inh_AB,
                    {
                        "a": 4,
                    },
                ),
                (
                    default.Inh_ABC,
                    {
                        "a": 13,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 17,
                    },
                ),
            ],
            excluded_fields={'b', 'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )

        updated = self.client.query(default.Inh_A)
        self._assertObjectsWithFields(
            updated,
            "a",
            [
                (
                    default.Inh_A,
                    {
                        "a": 1,
                    },
                ),
                (
                    default.Inh_AC,
                    {
                        "a": 7,
                    },
                ),
                (
                    default.Inh_AXA,
                    {
                        "a": 1001,
                    },
                ),
            ],
            excluded_fields={'b', 'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )

    def test_qb_delete_is_type_02(self):
        # Delete Type Intersection
        from models.orm_qb import default

        result = self.client.query(
            default.Inh_A.delete().is_(default.Inh_B).select(a=True)
        )
        self._assertObjectsWithFields(
            result,
            "a",
            [
                (
                    default.Inh_AB,
                    {
                        "a": 4,
                    },
                ),
                (
                    default.Inh_ABC,
                    {
                        "a": 13,
                    },
                ),
                (
                    default.Inh_AB_AC,
                    {
                        "a": 17,
                    },
                ),
            ],
            excluded_fields={'b', 'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )

        updated = self.client.query(default.Inh_A)
        self._assertObjectsWithFields(
            updated,
            "a",
            [],
            excluded_fields={'b', 'c', 'ab', 'ac', 'bc', 'abc', 'ab_ac'},
        )
