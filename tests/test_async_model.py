from __future__ import annotations

import asyncio
import os
import typing

if typing.TYPE_CHECKING:
    from typing import reveal_type

from gel import _testbase as tb


@tb.typecheck
class TestAsyncModelGenerator(tb.AsyncModelTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), "dbsetup", "orm.gel")

    SETUP = os.path.join(os.path.dirname(__file__), "dbsetup", "orm.edgeql")

    ISOLATED_TEST_BRANCHES = True

    @tb.must_fail  # this test ensures that @typecheck is working
    async def test_async_modelgen__smoke_test_01(self):
        from models import default

        self.assertEqual(reveal_type(default.User.groups), "this must fail")

    @tb.must_fail  # this test ensures that @typecheck is working
    async def test_async_modelgen__smoke_test_02(self):
        await asyncio.sleep(0)
        raise AssertionError("this must fail")

    async def test_async_modelgen_01(self):
        from models import default

        alice = await self.client.query_required_single(
            default.User.select(
                groups=default.User.groups.select("*").filter(name="green")
            )
            .filter(name="Alice")
            .limit(1)
        )

        self.assertEqual(
            reveal_type(alice.groups),
            "builtins.tuple[models.default.UserGroup, ...]",
        )

        self.assertEqual(alice.groups[0].name, "green")

    async def test_async_modelgen_02(self):
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
        from models import default

        u1 = default.User(name="Al")
        await self.client.sync(u1)
        self.assertTrue(hasattr(u1, "name_len"))

        u2 = default.User(name="Al")
        await self.client.save(u2)
        self.assertFalse(hasattr(u2, "name_len"))
