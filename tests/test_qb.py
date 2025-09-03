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

from gel import _testbase as tb


@tb.typecheck
class TestQueryBuilder(tb.ModelTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), "dbsetup", "orm.gel")

    SETUP = os.path.join(os.path.dirname(__file__), "dbsetup", "orm_qb.edgeql")

    ISOLATED_TEST_BRANCHES = False

    def test_qb_computed_01(self):
        """Replace an existing field with a computed literal value"""
        from models.orm import default, std

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
        from models.orm import default

        res = self.client.get(
            default.User.select(
                name=True,
                nickname=lambda u: "Little " + u.name,
            ).filter(name="Alice")
        )
        self.assertEqual(res.name, "Alice")
        self.assertEqual(res.nickname, "Little Alice")

    @tb.xfail_unimplemented('''
        Needs casts. Issue #672.
        Medium priority.
        (We might do different syntax and need to change the test.)
    ''')
    def test_qb_computed_03(self):
        from models.orm import default, std

        res = self.client.get(
            default.User.select(
                name=True,
                nickname=lambda u: u.name + std.str(std.len(u.name)),
            ).filter(name="Alice")
        )
        self.assertEqual(res.name, "Alice")
        self.assertEqual(res.nickname, "Alice5")

    def test_qb_computed_04(self):
        from models.orm import default, std

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
        from models.orm import default

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
        from models.orm import default

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
        from models.orm import default

        res = self.client.query(default.User.order_by(name=True))
        self.assertEqual(
            [u.name for u in res],
            ["Alice", "Billie", "Cameron", "Dana", "Elsa", "Zoe"],
        )

    def test_qb_order_02(self):
        from models.orm import default

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
        from models.orm import default

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
        from models.orm import default, std

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
        from models.orm import default, std

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
        from models.orm import default

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
        from models.orm import default

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

        self.assertEqual(
            users, ["Alice", "Billie", "Cameron", "Dana"]
        )

    def test_qb_filter_04(self):
        from models.orm import default, std

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
        from models.orm import default, std

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
        from models.orm import default, std

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
        from models.orm import default

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
        from models.orm import default, std

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

    @tb.xfail('''
        == on Array seems busted both for the typechecker and runtime
        Issue #896
    ''')
    def test_qb_filter_09(self):
        from models.orm import default, std

        # Find GameSession with same players as the green group
        green = default.UserGroup.select(users=True).filter(name="green")
        q = default.GameSession.select(
            "*",
            players=True,
        ).filter(
            lambda g: std.array_agg(g.players.id)
            == std.array_agg(green.users.id)
        )

        self.assertEqual(1, 2)
        res = self.client.get(q)
        green_res = self.client.get(green)
        self.assertEqual(res.num, 123)
        self.assertEqual(
            {u.id for u in res.players},
            {u.id for u in green_res.users},
        )

    def test_qb_filter_10(self):
        from models.orm import default, std

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
        from models.orm import default

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
        from models.orm import default

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

    def test_qb_link_property_01(self):
        from models.orm import default

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
        from models.orm import default

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
        from models.orm import default

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
        from models.orm import default, std

        res = self.client.get(
            default.KitchenSink.select(
                str=True,
                p_multi_str=True,
            ).filter(lambda k: std.in_("quick", k.p_multi_str))
        )
        self.assertEqual(res.str, "another one")
        self.assertEqual(set(res.p_multi_str), {"quick", "fox", "jumps"})

    def test_qb_multiprop_03(self):
        from models.orm import default

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
        from models.orm import default

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
        from models.orm import default, std

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
        from models.orm import default, std

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
            },
        )

    def test_qb_boolean_operator_error_01(self):
        from models.orm import default

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

    @tb.xfail('''
        Filtering on enums broken. And Enums need __gel_type_class__?
        See #635
    ''')
    @tb.skip_typecheck
    def test_qb_enum_01(self):
        from models.orm import default

        e = self.client.get(default.EnumTest.filter(color=default.Color.Red))

        self.assertEqual(e.color, default.Color.Red)
        self.assertEqual(e.name, "red")


class TestQueryBuilderModify(tb.ModelTestCase):
    """This test suite is for data manipulation using QB."""

    SCHEMA = os.path.join(os.path.dirname(__file__), "dbsetup", "orm.gel")

    SETUP = os.path.join(os.path.dirname(__file__), "dbsetup", "orm_qb.edgeql")

    ISOLATED_TEST_BRANCHES = True

    def test_qb_update_01(self):
        from models.orm import default

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
        from models.orm import default, std

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

    @tb.xfail('''
       Runtime failure because assert_single doesn't work.
       Bug #897.

       Also fails at typecheck time because assert_single can return None?
    ''')
    def test_qb_update_03(self):
        from models.orm import default, std

        # Combine update and select of the updated object
        res = self.client.get(
            default.Post.filter(body="Hello")
            .update(
                author=std.assert_single(default.User.filter(name="Billie"))
            )
            .select("*", author=lambda p: p.author.select("**"))
        )

        self.assertEqual(res.body, "Hello")
        self.assertEqual(res.author.name, "Zoe")
        self.assertEqual({g.name for g in res.author.groups}, {"redgreen"})

    @tb.xfail('''
       Runtime failure because assert_single doesn't work.
       Bug #897.

       Also fails at typecheck time because update's *types* dont't
       support callbacks, though runtime does.
    ''')
    def test_qb_update_04(self):
        from models.orm import default, std

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
                users=lambda g: std.union(
                    g.users, default.User.filter(name="Alice")
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
        from models.orm import default

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
        from models.orm import default

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
        from models.orm import default

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

    @tb.xfail('''
        Filtering on enums broken. And Enums need __gel_type_class__?
        See #635
    ''')
    def test_qb_enum_edit_01(self):
        from models.orm import default

        e = self.client.get(
            default.EnumTest.filter(
                name="red",
            )
            .update(color=default.Color.Orange)
            .select("*")
        )

        self.assertEqual(e.color, default.Color.Orange)
        self.assertEqual(e.name, "red")

    @tb.xfail('''
        Filtering on enums broken. And Enums need __gel_type_class__?
        See #635
    ''')
    def test_qb_enum_edit_02(self):
        from models.orm import default

        e = self.client.get(
            default.EnumTest.filter(
                name="red",
            )
            .update(color=lambda e: default.Color("Violet"))
            .select("*")
        )

        self.assertEqual(e.color, default.Color.Violet)
        self.assertEqual(e.name, "red")
