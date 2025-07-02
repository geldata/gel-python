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


class TestQueryBuilder(tb.ModelTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), "dbsetup", "orm.gel")

    SETUP = os.path.join(os.path.dirname(__file__), "dbsetup", "orm_qb.edgeql")

    ISOLATED_TEST_BRANCHES = False

    @tb.xfail
    @tb.typecheck
    def test_qb_computed_01(self):
        """Replace an existing field with a computed literal value"""
        from models import default, std

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

    @tb.typecheck
    def test_qb_computed_02(self):
        from models import default

        res = self.client.get(
            default.User.select(
                name=True,
                nickname=lambda u: "Little " + u.name,
            ).filter(name="Alice")
        )
        self.assertEqual(res.name, "Alice")
        self.assertEqual(res.nickname, "Little Alice")

    @tb.xfail
    @tb.typecheck
    def test_qb_computed_03(self):
        from models import default, std

        res = self.client.get(
            default.User.select(
                name=True,
                nickname=lambda u: u.name + std.str(std.len(u.name)),
            ).filter(name="Alice")
        )
        self.assertEqual(res.name, "Alice")
        self.assertEqual(res.nickname, "Alice5")

    @tb.xfail
    @tb.typecheck
    def test_qb_computed_04(self):
        from models import default, std

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

    @tb.typecheck
    def test_qb_computed_05(self):
        from models import default

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

    @tb.typecheck
    def test_qb_computed_06(self):
        from models import default

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

    @tb.typecheck
    def test_qb_order_01(self):
        from models import default

        res = self.client.query(default.User.order_by(name=True))
        self.assertEqual(
            [u.name for u in res],
            ["Alice", "Billie", "Cameron", "Dana", "Elsa", "Zoe"],
        )

    @tb.typecheck
    def test_qb_order_02(self):
        from models import default

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

    @tb.xfail
    @tb.typecheck
    def test_qb_order_03(self):
        from models import default

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
            [(u.name, u.is_tall_enough) for u in res.players],
            [("Alice", False), ("Billie", True)],
        )

    @tb.typecheck
    def test_qb_filter_01(self):
        from models import default, std

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

    @tb.typecheck
    def test_qb_filter_02(self):
        from models import default

        res = self.client.get(
            default.UserGroup.select("*", users=True).filter(name="red")
        )
        self.assertEqual(res.name, "red")
        self.assertEqual(res.mascot, "dragon")
        self.assertEqual(
            list(sorted(u.name for u in res.users)),
            ["Alice", "Billie", "Cameron", "Dana"],
        )

    @tb.xfail
    @tb.typecheck
    def test_qb_filter_03(self):
        from models import default

        res = self.client.get(
            default.UserGroup.select(
                "*",
                # Skip explicit select clause
                users=lambda g: g.users.order_by(name=True),
            ).filter(name="red")
        )
        self.assertEqual(res.name, "red")
        self.assertEqual(res.mascot, "dragon")
        self.assertEqual(
            [u.name for u in res.users], ["Alice", "Billie", "Cameron", "Dana"]
        )

    @tb.typecheck
    def test_qb_filter_04(self):
        from models import default, std

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

    @tb.xfail
    @tb.typecheck
    def test_qb_filter_05(self):
        from models import default, std

        # Test filter by ad-hoc computed
        res = self.client.get(
            default.UserGroup.select(
                name=True,
                user_count=lambda g: std.count(g.users),
            ).filter(user_count=4)
        )
        self.assertEqual(res.name, "red")
        self.assertEqual(res.user_count, 4)

    @tb.xfail
    @tb.typecheck
    def test_qb_filter_06(self):
        from models import default, std

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

    @tb.xfail
    @tb.typecheck
    def test_qb_filter_07(self):
        from models import default

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

    @tb.xfail
    @tb.typecheck
    def test_qb_filter_08(self):
        from models import default

        # Test filter with nested property expression
        res = self.client.query(
            default.Post.select("**").filter(
                lambda p: "red" not in p.author.groups.name
            )
        )
        self.assertEqual(len(res), 1)
        post = res[0]
        self.assertEqual(post.author.name, "Elsa")
        self.assertEqual(post.body, "*magic stuff*")

    @tb.typecheck
    def test_qb_link_property_01(self):
        from models import default

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

    @tb.xfail
    @tb.typecheck
    def test_qb_link_property_02(self):
        from models import default

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

    @tb.typecheck
    def test_qb_multiprop_01(self):
        from models import default

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

    @tb.xfail
    @tb.typecheck
    def test_qb_multiprop_02(self):
        from models import default

        # FIXME: This straight up hangs
        raise Exception("This filter will hang")
        res = self.client.get(
            default.KitchenSink.select(
                str=True,
                p_multi_str=True,
            ).filter(lambda k: "quick" in k.p_multi_str)
        )
        self.assertEqual(res.str, "another one")
        self.assertEqual(set(res.p_multi_str), {"quick", "fox", "jumps"})

    @tb.xfail
    @tb.typecheck
    def test_qb_multiprop_03(self):
        from models import default

        res = self.client.get(
            default.KitchenSink.select(
                str=True,
                p_multi_str=True,
                # In filters == and in behave similarly for multi props
            ).filter(lambda k: "quick" == k.p_multi_str)
        )
        self.assertEqual(res.str, "another one")
        self.assertEqual(set(res.p_multi_str), {"quick", "fox", "jumps"})

    @tb.xfail
    @tb.typecheck
    def test_qb_multiprop_04(self):
        from models import default

        res = self.client.get(
            default.KitchenSink.select(
                str=True,
                # FIXME: Not sure how to express filtering a multi prop
                p_multi_str=lambda k: k.p_multi_str.order_by(k.p_multi_str),
            ).filter(str="another one")
        )
        self.assertEqual(res.str, "another one")
        self.assertEqual(set(res.p_multi_str), {"brown", "jumps"})

    @tb.xfail
    @tb.typecheck
    def test_qb_multiprop_05(self):
        from models import default, std

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


class TestQueryBuilderModify(tb.ModelTestCase):
    """This test suite is for data manipulation using QB."""

    SCHEMA = os.path.join(os.path.dirname(__file__), "dbsetup", "orm.gel")

    SETUP = os.path.join(os.path.dirname(__file__), "dbsetup", "orm_qb.edgeql")

    ISOLATED_TEST_BRANCHES = True

    @tb.xfail
    @tb.typecheck
    def test_qb_update_01(self):
        from models import default

        self.client.query(
            default.User.filter(name="Alice").update(
                name="Cooper",
                nickname="singer",
            )
        )

        res = self.client.get(default.User.filter(name="Cooper"))
        self.assertEqual(res.name, "Cooper")
        self.assertEqual(res.nickname, "singer")

    @tb.xfail
    @tb.typecheck
    def test_qb_update_02(self):
        from models import default

        self.client.query(
            default.UserGroup.filter(name="blue").update(
                users=default.User.filter(lambda u: u.name in {"Zoe", "Dana"})
            )
        )

        res = self.client.get(
            default.UserGroup.select("**").filter(name="blue")
        )
        self.assertEqual(res.name, "blue")
        self.assertEqual({u.name for u in res.users}, {"Zoe", "Dana"})

    @tb.xfail
    @tb.typecheck
    def test_qb_update_03(self):
        from models import default, std

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

    @tb.xfail
    @tb.typecheck
    def test_qb_update_04(self):
        from models import default, std

        self.client.query(
            default.UserGroup.filter(name="blue").update(
                users=default.User.filter(lambda u: u.name in {"Zoe", "Dana"})
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

    @tb.typecheck
    def test_qb_delete_01(self):
        from models import default

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

    @tb.typecheck
    def test_qb_delete_02(self):
        from models import default

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

    @tb.xfail
    @tb.typecheck
    def test_qb_delete_03(self):
        from models import default

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
