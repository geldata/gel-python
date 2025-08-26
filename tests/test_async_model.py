from __future__ import annotations

import asyncio
import os
import typing

if typing.TYPE_CHECKING:
    from typing import reveal_type

from gel._internal._testbase import _models as tb


@tb.typecheck
class TestAsyncModelGenerator(tb.AsyncModelTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), "dbsetup", "orm.gel")

    SETUP = os.path.join(os.path.dirname(__file__), "dbsetup", "orm.edgeql")

    ISOLATED_TEST_BRANCHES = True

    @tb.must_fail  # this test ensures that @typecheck is working
    async def test_async_modelgen__smoke_test_01(self):
        from models.orm import default

        self.assertEqual(reveal_type(default.User.groups), "this must fail")

    @tb.must_fail  # this test ensures that @typecheck is working
    async def test_async_modelgen__smoke_test_02(self):
        await asyncio.sleep(0)
        raise AssertionError("this must fail")

    async def test_async_modelgen_01(self):
        from models.orm import default

        alice = await self.client.query_required_single(
            default.User.select(
                groups=default.User.groups.select("*").filter(name="green")
            )
            .filter(name="Alice")
            .limit(1)
        )

        self.assertIn(
            "ComputedLinkSet[models.orm.default.UserGroup]",
            reveal_type(alice.groups),
        )

        self.assertEqual(next(iter(alice.groups)).name, "green")

    async def test_async_modelgen_02(self):
        import uuid

        from models.orm import default
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
        await self.client.sync(party)

        # Fetch and verify
        raw_id = uuid.UUID(str(party.id))
        res = await self.client.get(
            default.Party.select(
                name=True,
                members=True,
            ).filter(id=raw_id)
        )
        self.assertEqual(res.name, "Solo")
        self.assertEqual(len(res.members), 1)
        m = next(iter(res.members))
        self.assertEqual(m.name, "John Smith")
        self.assertEqual(m.nickname, "Hannibal")

    async def test_async_modelgen_save_refetch_modes(self):
        from models.orm import default

        u1 = default.User(name="Al")
        await self.client.sync(u1)
        self.assertTrue(hasattr(u1, "name_len"))

        u2 = default.User(name="Al")
        await self.client.save(u2)
        self.assertFalse(hasattr(u2, "name_len"))

    async def test_async_modelgen_save_reload_links_07(self):
        # This is a copy if test_async_modelgen_save_reload_links_07
        # ensuring the sync() is fully supported in async mode.

        from models.orm import default

        # Test backlink invalidation when adding a user to a different group
        # Fetch red and blue groups with nested user data and their groups

        red = await self.client.get(
            default.UserGroup.select(
                name=True,
                users=lambda g: g.users.select(
                    name=True,
                    groups=True,
                ),
            ).filter(name="red")
        )

        blue = await self.client.get(
            default.UserGroup.select(
                name=True,
                users=lambda g: g.users.select(
                    name=True,
                    groups=True,
                ),
            ).filter(name="blue")
        )

        self.assertEqual(red.name, "red")
        self.assertEqual(blue.name, "blue")
        self.assertEqual(len(blue.users), 0)  # Should be empty initially

        orig_ids = {u.id for u in red.users}
        # Find Alice in the red group
        alice = [u for u in red.users if u.name == "Alice"][0]
        self.assertIn("red", {g.name for g in alice.groups})

        # Add Alice to the blue group
        blue.users.add(alice)
        await self.client.sync(blue)

        # Check that the Alice's groups computed backlink is updated
        # after sync()
        self.assertEqual(
            {g.id for g in alice.groups},
            set(
                await self.client.query(
                    """
                        with w := (select User{groups} filter .id=<uuid>$0)
                        select w.groups.id
                    """,
                    alice.id,
                )
            ),
        )

        # But we should still have some valid data
        self.assertEqual(alice.name, "Alice")
        self.assertEqual({u.id for u in red.users}, orig_ids)
        self.assertEqual({u.id for u in blue.users}, {alice.id})

    async def test_async_modelgen_sync_warning(self):
        from models.orm import default

        g = default.UserGroup(
            name="Pickle Pirates",
            users=[default.User(name="{i}") for i in range(200)],
        )

        with self.assertWarns(msg_part="`sync()` is creating") as fn:
            await self.client.sync(g)
            self.assertEqual(fn, __file__)  # just a sanity check

        for u in g.users:
            u.name += "aaa"

        with self.assertWarns(msg_part="`sync()` is refetching"):
            await self.client.sync(g)

        for u in g.users:
            u.name += "bbb"

        with self.assertNotWarns():
            await self.client.sync(g, warn_on_large_sync=False)
