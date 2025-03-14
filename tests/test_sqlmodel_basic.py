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

import datetime as dt
import os
import uuid
import unittest

try:
    from sqlmodel import create_engine, Session, select
except ImportError:
    NO_ORM = True
else:
    NO_ORM = False

from gel import _testbase as tb
from gel.orm import sqlmodel as sqlm


class TestSQLModelBasic(tb.SQLModelTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'dbsetup',
                          'base.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'dbsetup',
                         'base.edgeql')

    MODEL_PACKAGE = 'sqlmbase'

    @classmethod
    def setUpClass(cls):
        if NO_ORM:
            raise unittest.SkipTest("sqlmodel is not installed")

        super().setUpClass()

        cls.engine = create_engine(cls.get_dsn_for_sqla())
        cls.sess = Session(cls.engine)

        from sqlmbase import default
        cls.sm = default

    def setUp(self):
        super().setUp()

        if self.client.query_required_single('''
            select sys::get_version().major < 6
        '''):
            self.skipTest("Test needs SQL DML queries")

        self.sess.begin()

    def tearDown(self):
        super().tearDown()
        self.sess.rollback()

    def test_sqlmodel_read_models_01(self):
        vals = {r.name for r in self.sess.exec(select(self.sm.User))}
        self.assertEqual(
            vals, {'Alice', 'Billie', 'Cameron', 'Dana', 'Elsa', 'Zoe'})

        vals = {r.name for r in self.sess.exec(select(self.sm.UserGroup))}
        self.assertEqual(
            vals, {'red', 'green', 'blue'})

        vals = {r.num for r in self.sess.exec(select(self.sm.GameSession))}
        self.assertEqual(vals, {123, 456})

        vals = {r.body for r in self.sess.exec(select(self.sm.Post))}
        self.assertEqual(
            vals, {'Hello', "I'm Alice", "I'm Cameron", '*magic stuff*'})

        # Read from the abstract type
        vals = {r.name for r in self.sess.exec(select(self.sm.Named))}
        self.assertEqual(
            vals,
            {
                'Alice', 'Billie', 'Cameron', 'Dana', 'Elsa', 'Zoe',
                'red', 'green', 'blue',
            }
        )

    def test_sqlmodel_read_models_02(self):
        # test single link and the one-to-many backlink
        # using load-on-demand

        res = self.sess.exec(select(self.sm.Post))
        vals = {(p.author.name, p.body) for p in res}
        self.assertEqual(
            vals,
            {
                ('Alice', 'Hello'),
                ('Alice', "I'm Alice"),
                ('Cameron', "I'm Cameron"),
                ('Elsa', '*magic stuff*'),
            }
        )

        # use backlink
        res = self.sess.exec(
            select(self.sm.User).order_by('name')
        )
        vals = [
            (u.name, {p.body for p in u._author_Post})
            for u in res
        ]
        self.assertEqual(
            vals,
            [
                ('Alice', {'Hello', "I'm Alice"}),
                ('Billie', set()),
                ('Cameron', {"I'm Cameron"}),
                ('Dana', set()),
                ('Elsa', {'*magic stuff*'}),
                ('Zoe', set()),
            ]
        )

    def test_sqlmodel_read_models_03(self):
        # test single link and the one-to-many backlink

        res = self.sess.exec(
            select(self.sm.Post).join(self.sm.Post.author)
        )
        vals = {(p.author.name, p.body) for p in res}
        self.assertEqual(
            vals,
            {
                ('Alice', 'Hello'),
                ('Alice', "I'm Alice"),
                ('Cameron', "I'm Cameron"),
                ('Elsa', '*magic stuff*'),
            }
        )

        # prefetch via backlink
        res = self.sess.exec(
            select(self.sm.User).join(
                self.sm.User._author_Post, isouter=True
            ).order_by(self.sm.Post.body)
        )
        vals = {
            (u.name, tuple(p.body for p in u._author_Post))
            for u in res
        }
        self.assertEqual(
            vals,
            {
                ('Alice', ('Hello', "I'm Alice")),
                ('Billie', ()),
                ('Cameron', ("I'm Cameron",)),
                ('Dana', ()),
                ('Elsa', ('*magic stuff*',)),
                ('Zoe', ()),
            }
        )

    def test_sqlmodel_read_models_04(self):
        # test exclusive multi link and its backlink
        # using load-on-demand

        res = self.sess.exec(
            select(self.sm.GameSession).order_by('num')
        )
        vals = [(g.num, {u.name for u in g.players}) for g in res]
        self.assertEqual(
            vals,
            [
                (123, {'Alice', 'Billie'}),
                (456, {'Dana'}),
            ]
        )

        # use backlink
        res = self.sess.exec(select(self.sm.User))
        vals = {
            (u.name, tuple(g.num for g in u._players_GameSession))
            for u in res
        }
        self.assertEqual(
            vals,
            {
                ('Alice', (123,)),
                ('Billie', (123,)),
                ('Cameron', ()),
                ('Dana', (456,)),
                ('Elsa', ()),
                ('Zoe', ()),
            }
        )

    def test_sqlmodel_read_models_05(self):
        # test exclusive multi link and its backlink

        res = self.sess.exec(
            select(self.sm.GameSession).join(
                self.sm.GameSession.players, isouter=True,
            )
        )
        vals = {
            (g.num, tuple(sorted(u.name for u in g.players)))
            for g in res
        }
        self.assertEqual(
            vals,
            {
                (123, ('Alice', 'Billie')),
                (456, ('Dana',)),
            }
        )

        # prefetch via backlink
        res = self.sess.exec(
            select(self.sm.User).join(
                self.sm.User._players_GameSession, isouter=True,
            )
        )
        vals = {
            (u.name, tuple(g.num for g in u._players_GameSession))
            for u in res
        }
        self.assertEqual(
            vals,
            {
                ('Alice', (123,)),
                ('Billie', (123,)),
                ('Cameron', ()),
                ('Dana', (456,)),
                ('Elsa', ()),
                ('Zoe', ()),
            }
        )

    def test_sqlmodel_read_models_06(self):
        # test multi link and its backlink
        # using load-on-demand

        res = self.sess.exec(
            select(self.sm.UserGroup).order_by('name')
        )
        vals = [(g.name, {u.name for u in g.users}) for g in res]
        self.assertEqual(
            vals,
            [
                ('blue', set()),
                ('green', {'Alice', 'Billie'}),
                ('red', {'Alice', 'Billie', 'Cameron', 'Dana'}),
            ]
        )

        # use backlink
        res = self.sess.exec(
            select(self.sm.User).order_by('name')
        )
        vals = [
            (u.name, {g.name for g in u._users_UserGroup})
            for u in res
        ]
        self.assertEqual(
            vals,
            [
                ('Alice', {'red', 'green'}),
                ('Billie', {'red', 'green'}),
                ('Cameron', {'red'}),
                ('Dana', {'red'}),
                ('Elsa', set()),
                ('Zoe', set()),
            ]
        )

    def test_sqlmodel_read_models_07(self):
        # test exclusive multi link and its backlink

        res = self.sess.exec(
            select(self.sm.UserGroup).join(
                self.sm.UserGroup.users, isouter=True,
            )
        )
        vals = {
            (g.name, tuple(sorted(u.name for u in g.users)))
            for g in res
        }
        self.assertEqual(
            vals,
            {
                ('blue', ()),
                ('green', ('Alice', 'Billie')),
                ('red', ('Alice', 'Billie', 'Cameron', 'Dana')),
            }
        )

        # prefetch via backlink
        res = self.sess.exec(
            select(self.sm.User).join(
                self.sm.User._users_UserGroup, isouter=True,
            )
        )
        vals = {
            (u.name, tuple(sorted(g.name for g in u._users_UserGroup)))
            for u in res
        }
        self.assertEqual(
            vals,
            {
                ('Alice', ('green', 'red')),
                ('Billie', ('green', 'red')),
                ('Cameron', ('red',)),
                ('Dana', ('red',)),
                ('Elsa', ()),
                ('Zoe', ()),
            }
        )

    def test_sqlmodel_read_models_08(self):
        # test arrays, bytes and various date/time scalars

        res = self.sess.exec(
            select(self.sm.AssortedScalars)
        ).one()

        self.assertEqual(res.name, 'hello world')
        self.assertEqual(res.vals, ['brown', 'fox'])
        self.assertEqual(res.bstr, b'word\x00\x0b')
        self.assertEqual(
            res.time,
            dt.time(20, 13, 45, 678_000),
        )
        self.assertEqual(
            res.date,
            dt.date(2025, 1, 26),
        )
        # time zone aware
        self.assertEqual(
            res.ts,
            dt.datetime.fromisoformat('2025-01-26T20:13:45+00:00'),
        )
        # naive datetime
        self.assertEqual(
            res.lts,
            dt.datetime.fromisoformat('2025-01-26T20:13:45'),
        )

    def test_sqlmodel_create_models_01(self):
        vals = self.sess.exec(
            select(self.sm.User).where(
                self.sm.User.name == 'Yvonne'
            )
        )
        self.assertEqual(list(vals), [])

        self.sess.add(self.sm.User(name='Yvonne'))
        self.sess.flush()

        user = self.sess.exec(
            select(self.sm.User).where(
                self.sm.User.name == 'Yvonne'
            )
        ).one()
        self.assertEqual(user.name, 'Yvonne')
        self.assertIsInstance(user.id, uuid.UUID)

    def test_sqlmodel_create_models_02(self):
        cyan = self.sm.UserGroup(
            name='cyan',
            users=[
                self.sm.User(name='Yvonne'),
                self.sm.User(name='Xander'),
            ],
        )

        self.sess.add(cyan)
        self.sess.flush()

        for name in ['Yvonne', 'Xander']:
            user = self.sess.exec(
                select(self.sm.User).filter_by(name=name)
            ).one()

            self.assertEqual(user.name, name)
            self.assertEqual(user._users_UserGroup[0].name, 'cyan')
            self.assertIsInstance(user.id, uuid.UUID)

    def test_sqlmodel_create_models_03(self):
        user0 = self.sm.User(name='Yvonne')
        user1 = self.sm.User(name='Xander')
        cyan = self.sm.UserGroup(name='cyan')

        user0._users_UserGroup.append(cyan)
        user1._users_UserGroup.append(cyan)

        self.sess.add(cyan)
        self.sess.flush()

        for name in ['Yvonne', 'Xander']:
            user = self.sess.exec(
                select(self.sm.User).filter_by(name=name)
            ).one()

            self.assertEqual(user.name, name)
            self.assertEqual(user._users_UserGroup[0].name, 'cyan')
            self.assertIsInstance(user.id, uuid.UUID)

    def test_sqlmodel_create_models_04(self):
        user = self.sm.User(name='Yvonne')
        self.sm.Post(body='this is a test', author=user)
        self.sm.Post(body='also a test', author=user)

        self.sess.add(user)
        self.sess.flush()

        res = self.sess.exec(
            select(self.sm.Post)
            .join(self.sm.Post.author, isouter=True)
            .where(self.sm.User.name == 'Yvonne')
        )
        self.assertEqual(
            {p.body for p in res},
            {'this is a test', 'also a test'},
        )

    def test_sqlmodel_delete_models_01(self):
        user = self.sess.exec(
            select(self.sm.User).filter_by(name='Zoe')
        ).one()
        self.assertEqual(user.name, 'Zoe')
        self.assertIsInstance(user.id, uuid.UUID)

        self.sess.delete(user)
        self.sess.flush()

        vals = self.sess.exec(
            select(self.sm.User).filter_by(name='Zoe')
        )
        self.assertEqual(list(vals), [])

    def test_sqlmodel_delete_models_02(self):
        post = self.sess.exec(
            select(self.sm.Post)
            .join(self.sm.Post.author, isouter=True)
            .filter(self.sm.User.name == 'Elsa')
        ).one()
        user_id = post.author.id

        self.sess.delete(post)
        self.sess.flush()

        vals = self.sess.exec(
            select(self.sm.Post)
            .join(self.sm.Post.author, isouter=True)
            .filter(self.sm.User.name == 'Elsa')
        )
        self.assertEqual(list(vals), [])

        user = self.sess.get(self.sm.User, user_id)
        self.assertEqual(user.name, 'Elsa')

    def test_sqlmodel_delete_models_03(self):
        post = self.sess.exec(
            select(self.sm.Post)
            .join(self.sm.Post.author, isouter=True)
            .filter(self.sm.User.name == 'Elsa')
        ).one()
        user = post.author

        self.sess.delete(post)
        self.sess.delete(user)
        self.sess.flush()

        vals = self.sess.exec(
            select(self.sm.Post)
            .join(self.sm.Post.author, isouter=True)
            .filter(self.sm.User.name == 'Elsa')
        )
        self.assertEqual(list(vals), [])

        vals = self.sess.exec(
            select(self.sm.User).filter_by(name='Elsa')
        )
        self.assertEqual(list(vals), [])

    def test_sqlmodel_delete_models_04(self):
        group = self.sess.exec(
            select(self.sm.UserGroup).filter_by(name='green')
        ).one()
        names = {u.name for u in group.users}

        self.sess.delete(group)
        self.sess.flush()

        vals = self.sess.exec(
            select(self.sm.UserGroup).filter_by(name='green')
        )
        self.assertEqual(list(vals), [])

        users = list(self.sess.exec(select(self.sm.User)))
        for name in names:
            self.assertIn(name, {u.name for u in users})

    def test_sqlmodel_delete_models_05(self):
        group = self.sess.exec(
            select(self.sm.UserGroup).filter_by(name='green')
        ).one()
        for u in group.users:
            if u.name == 'Billie':
                user = u
                break

        self.sess.delete(group)
        self.sess.delete(user)
        self.sess.flush()

        vals = self.sess.exec(
            select(self.sm.UserGroup).filter_by(name='green')
        )
        self.assertEqual(list(vals), [])

        users = self.sess.exec(select(self.sm.User))
        self.assertNotIn('Billie', {u.name for u in users})

    def test_sqlmodel_update_models_01(self):
        user = self.sess.exec(
            select(self.sm.User).filter_by(name='Alice')
        ).one()
        self.assertEqual(user.name, 'Alice')
        self.assertIsInstance(user.id, uuid.UUID)

        user.name = 'Xander'
        self.sess.add(user)
        self.sess.flush()

        vals = self.sess.exec(
            select(self.sm.User).filter_by(name='Alice')
        )
        self.assertEqual(list(vals), [])
        other = self.sess.exec(
            select(self.sm.User).filter_by(name='Xander')
        ).one()
        self.assertEqual(user, other)

    def test_sqlmodel_update_models_02(self):
        red = self.sess.exec(
            select(self.sm.UserGroup).filter_by(name='red')
        ).one()
        blue = self.sess.exec(
            select(self.sm.UserGroup).filter_by(name='blue')
        ).one()
        user = self.sm.User(name='Yvonne')

        self.sess.add(user)
        red.users.append(user)
        blue.users.append(user)
        self.sess.flush()

        self.assertEqual(
            {g.name for g in user._users_UserGroup},
            {'red', 'blue'},
        )
        self.assertEqual(user.name, 'Yvonne')
        self.assertIsInstance(user.id, uuid.UUID)

        group = [g for g in user._users_UserGroup if g.name == 'red'][0]
        self.assertEqual(
            {u.name for u in group.users},
            {'Alice', 'Billie', 'Cameron', 'Dana', 'Yvonne'},
        )

    def test_sqlmodel_update_models_03(self):
        user0 = self.sess.exec(
            select(self.sm.User).filter_by(name='Elsa')
        ).one()
        user1 = self.sess.exec(
            select(self.sm.User).filter_by(name='Zoe')
        ).one()
        # Replace the author or a post
        post = user0._author_Post[0]
        body = post.body
        post.author = user1

        self.sess.add(post)
        self.sess.flush()

        res = self.sess.exec(
            select(self.sm.Post)
            .join(self.sm.Post.author, isouter=True)
            .where(self.sm.User.name == 'Zoe')
        )
        self.assertEqual(
            {p.body for p in res},
            {body},
        )

    def test_sqlmodel_update_models_04(self):
        user = self.sess.exec(
            select(self.sm.User).filter_by(name='Zoe')
        ).one()
        post = self.sess.exec(
            select(self.sm.Post)
            .join(self.sm.Post.author, isouter=True)
            .filter(self.sm.User.name == 'Elsa')
        ).one()
        # Replace the author or a post
        post_id = post.id
        post.author = user

        self.sess.add(post)
        self.sess.flush()

        post = self.sess.get(self.sm.Post, post_id)
        self.assertEqual(post.author.name, 'Zoe')

    def test_sqlmodel_update_models_05(self):
        # test arrays, bytes and various date/time scalars
        #
        # For the purpose of sending data creating and updating a model are
        # both testing accurate data transfer.

        res = self.sess.exec(
            select(self.sm.AssortedScalars)
        ).one()

        res.name = 'New Name'
        res.vals.append('jumped')
        res.bstr = b'\x01success\x02'
        res.time = dt.time(8, 23, 54, 999_000)
        res.date = dt.date(2020, 2, 14)
        res.ts = res.ts - dt.timedelta(days=6)
        res.lts = res.lts + dt.timedelta(days=6)

        self.sess.add(res)
        self.sess.flush()

        upd = self.sess.exec(
            select(self.sm.AssortedScalars)
        ).one()

        self.assertEqual(upd.name, 'New Name')
        self.assertEqual(upd.vals, ['brown', 'fox', 'jumped'])
        self.assertEqual(upd.bstr, b'\x01success\x02')
        self.assertEqual(
            upd.time,
            dt.time(8, 23, 54, 999_000),
        )
        self.assertEqual(
            upd.date,
            dt.date(2020, 2, 14),
        )
        # time zone aware
        self.assertEqual(
            upd.ts,
            dt.datetime.fromisoformat('2025-01-20T20:13:45+00:00'),
        )
        # naive datetime
        self.assertEqual(
            upd.lts,
            dt.datetime.fromisoformat('2025-02-01T20:13:45'),
        )

    def test_sqlmodel_sorting(self):
        # Test the natural sorting function used for ordering fields, etc.

        unsorted = [
            {'name': 'zoo'},
            {'name': 'apple'},
            {'name': 'potato'},
            {'name': 'grape10'},
            {'name': 'grape1'},
            {'name': 'grape5'},
            {'name': 'grape2'},
            {'name': 'grape20'},
            {'name': 'grape25'},
            {'name': 'grape12'},
        ]

        self.assertEqual(
            list(sorted(unsorted, key=sqlm.field_name_sort)),
            [
                {'name': 'apple'},
                {'name': 'grape1'},
                {'name': 'grape2'},
                {'name': 'grape5'},
                {'name': 'grape10'},
                {'name': 'grape12'},
                {'name': 'grape20'},
                {'name': 'grape25'},
                {'name': 'potato'},
                {'name': 'zoo'},
            ],
        )
