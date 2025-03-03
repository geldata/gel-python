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

import datetime as dt
import os
import uuid
import unittest

try:
    import django
    from django.db import transaction
except ImportError:
    NO_ORM = True
else:
    NO_ORM = False

from gel import _testbase as tb
from gel.orm.django import generator


class TestDjangoBasic(tb.DjangoTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'dbsetup',
                          'base.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'dbsetup',
                         'base.edgeql')

    MODEL_PACKAGE = 'djangobase'

    @classmethod
    def setUpClass(cls):
        if NO_ORM:
            raise unittest.SkipTest("django is not installed")

        super().setUpClass()

        from django.conf import settings
        from djangobase.settings import mysettings

        settings.configure(**mysettings)
        django.setup()

        from djangobase import models
        cls.m = models
        transaction.set_autocommit(False)

    def setUp(self):
        super().setUp()

        if self.client.query_required_single('''
            select sys::get_version().major < 6
        '''):
            self.skipTest("Test needs SQL DML queries")

        transaction.savepoint()

    def tearDown(self):
        super().tearDown()
        transaction.rollback()

    def test_django_read_models_01(self):
        vals = {r.name for r in self.m.User.objects.all()}
        self.assertEqual(
            vals, {'Alice', 'Billie', 'Cameron', 'Dana', 'Elsa', 'Zoe'})

        vals = {r.name for r in self.m.UserGroup.objects.all()}
        self.assertEqual(
            vals, {'red', 'green', 'blue'})

        vals = {r.num for r in self.m.GameSession.objects.all()}
        self.assertEqual(vals, {123, 456})

        vals = {r.body for r in self.m.Post.objects.all()}
        self.assertEqual(
            vals, {'Hello', "I'm Alice", "I'm Cameron", '*magic stuff*'})

        # Read from the abstract type
        vals = {r.name for r in self.m.Named.objects.all()}
        self.assertEqual(
            vals,
            {
                'Alice', 'Billie', 'Cameron', 'Dana', 'Elsa', 'Zoe',
                'red', 'green', 'blue',
            }
        )

    def test_django_read_models_02(self):
        # test single link and the one-to-many backlink
        # using load-on-demand

        res = self.m.Post.objects.all()
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
        res = self.m.User.objects.order_by('name').all()
        vals = [
            (u.name, {p.body for p in u._author_Post.all()})
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

    def test_django_read_models_03(self):
        # test single link and the one-to-many backlink

        res = self.m.Post.objects.select_related('author')
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
        res = self.m.User.objects.prefetch_related('_author_Post') \
                  .order_by('_author_Post__body')
        vals = {
            (u.name, tuple(p.body for p in u._author_Post.all()))
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

    def test_django_read_models_04(self):
        # test exclusive multi link and its backlink
        # using load-on-demand

        res = self.m.GameSession.objects.order_by('num').all()
        vals = [(g.num, {u.name for u in g.players.all()}) for g in res]
        self.assertEqual(
            vals,
            [
                (123, {'Alice', 'Billie'}),
                (456, {'Dana'}),
            ]
        )

        # use backlink
        res = self.m.User.objects.all()
        vals = {
            (u.name, tuple(g.num for g in u._players_GameSession.all()))
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

    def test_django_read_models_05(self):
        # test exclusive multi link and its backlink

        res = self.m.GameSession.objects.prefetch_related('players')
        vals = {
            (g.num, tuple(sorted(u.name for u in g.players.all())))
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
        res = self.m.User.objects.prefetch_related('_players_GameSession')
        vals = {
            (u.name, tuple(g.num for g in u._players_GameSession.all()))
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

    def test_django_read_models_06(self):
        # test multi link and its backlink
        # using load-on-demand

        res = self.m.UserGroup.objects.order_by('name').all()
        vals = [(g.name, {u.name for u in g.users.all()}) for g in res]
        self.assertEqual(
            vals,
            [
                ('blue', set()),
                ('green', {'Alice', 'Billie'}),
                ('red', {'Alice', 'Billie', 'Cameron', 'Dana'}),
            ]
        )

        # use backlink
        res = self.m.User.objects.order_by('name').all()
        vals = [
            (u.name, {g.name for g in u._users_UserGroup.all()})
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

    def test_django_read_models_07(self):
        # test exclusive multi link and its backlink

        res = self.m.UserGroup.objects.prefetch_related('users')
        vals = {
            (g.name, tuple(sorted(u.name for u in g.users.all())))
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
        res = self.m.User.objects.prefetch_related('_users_UserGroup')
        vals = {
            (u.name, tuple(sorted(g.name for g in u._users_UserGroup.all())))
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

    def test_django_read_models_08(self):
        # test arrays, bytes and various date/time scalars

        res = self.m.AssortedScalars.objects.all()[0]

        self.assertEqual(res.name, 'hello world')
        self.assertEqual(res.vals, ['brown', 'fox'])
        self.assertEqual(bytes(res.bstr), b'word\x00\x0b')
        self.assertEqual(
            res.time,
            dt.time(20, 13, 45, 678_000),
        )
        self.assertEqual(
            res.date,
            dt.date(2025, 1, 26),
        )
        # time zone aware (default for Django)
        self.assertEqual(
            res.ts,
            dt.datetime.fromisoformat('2025-01-26T20:13:45+00:00'),
        )

    def test_django_create_models_01(self):
        vals = self.m.User.objects.filter(name='Yvonne').all()
        self.assertEqual(list(vals), [])

        user = self.m.User(name='Yvonne')
        user.save()

        self.assertEqual(user.name, 'Yvonne')
        self.assertIsInstance(user.id, uuid.UUID)

    def test_django_create_models_02(self):
        x = self.m.User(name='Xander')
        y = self.m.User(name='Yvonne')
        cyan = self.m.UserGroup(name='cyan')

        x.save()
        y.save()
        cyan.save()
        cyan.users.set([x, y])

        for name in ['Yvonne', 'Xander']:
            user = self.m.User.objects.get(name=name)

            self.assertEqual(user.name, name)
            self.assertEqual(user._users_UserGroup.all()[0].name, 'cyan')
            self.assertIsInstance(user.id, uuid.UUID)

    def test_django_create_models_03(self):
        x = self.m.User(name='Xander')
        y = self.m.User(name='Yvonne')
        cyan = self.m.UserGroup(name='cyan')

        x.save()
        y.save()
        cyan.save()

        x._users_UserGroup.add(cyan)
        y._users_UserGroup.add(cyan)

        group = self.m.UserGroup.objects.get(name='cyan')
        self.assertEqual(group.name, 'cyan')
        self.assertEqual(
            {u.name for u in group.users.all()},
            {'Xander', 'Yvonne'},
        )

    def test_django_create_models_04(self):
        user = self.m.User(name='Yvonne')
        user.save()
        self.m.Post(body='this is a test', author=user).save()
        self.m.Post(body='also a test', author=user).save()

        res = self.m.Post.objects.select_related('author') \
                  .filter(author__name='Yvonne')
        self.assertEqual(
            {p.body for p in res},
            {'this is a test', 'also a test'},
        )

    def test_django_delete_models_01(self):
        user = self.m.User.objects.get(name='Zoe')
        self.assertEqual(user.name, 'Zoe')
        self.assertIsInstance(user.id, uuid.UUID)

        user.delete()

        vals = self.m.User.objects.filter(name='Zoe').all()
        self.assertEqual(list(vals), [])

    def test_django_delete_models_02(self):
        post = self.m.Post.objects.select_related('author') \
                   .get(author__name='Elsa')
        user_id = post.author.id

        post.delete()

        vals = self.m.Post.objects.select_related('author') \
                   .filter(author__name='Elsa')
        self.assertEqual(list(vals), [])

        user = self.m.User.objects.get(id=user_id)
        self.assertEqual(user.name, 'Elsa')

    def test_django_delete_models_03(self):
        post = self.m.Post.objects.select_related('author') \
                   .get(author__name='Elsa')
        user = post.author

        post.delete()
        user.delete()

        vals = self.m.Post.objects.select_related('author') \
                   .filter(author__name='Elsa')
        self.assertEqual(list(vals), [])

        vals = self.m.User.objects.filter(name='Elsa')
        self.assertEqual(list(vals), [])

    def test_django_delete_models_04(self):
        group = self.m.UserGroup.objects.get(name='green')
        names = {u.name for u in group.users.all()}

        group.delete()

        vals = self.m.UserGroup.objects.filter(name='green').all()
        self.assertEqual(list(vals), [])

        users = self.m.User.objects.all()
        for name in names:
            self.assertIn(name, {u.name for u in users})

    def test_django_delete_models_05(self):
        group = self.m.UserGroup.objects.get(name='green')
        for u in group.users.all():
            if u.name == 'Billie':
                user = u
                break

        group.delete()
        # make sure the user object is no longer a link target
        user._users_UserGroup.clear()
        user._players_GameSession.clear()
        user.delete()

        vals = self.m.UserGroup.objects.filter(name='green').all()
        self.assertEqual(list(vals), [])

        users = self.m.User.objects.all()
        self.assertNotIn('Billie', {u.name for u in users})

    def test_django_update_models_01(self):
        user = self.m.User.objects.get(name='Alice')
        self.assertEqual(user.name, 'Alice')
        self.assertIsInstance(user.id, uuid.UUID)

        user.name = 'Xander'
        user.save()

        vals = self.m.User.objects.filter(name='Alice').all()
        self.assertEqual(list(vals), [])
        other = self.m.User.objects.get(name='Xander')
        self.assertEqual(user, other)

    def test_django_update_models_02(self):
        red = self.m.UserGroup.objects.get(name='red')
        blue = self.m.UserGroup.objects.get(name='blue')
        user = self.m.User(name='Yvonne')

        user.save()
        red.users.add(user)
        blue.users.add(user)

        self.assertEqual(
            {g.name for g in user._users_UserGroup.all()},
            {'red', 'blue'},
        )
        self.assertEqual(user.name, 'Yvonne')
        self.assertIsInstance(user.id, uuid.UUID)

        group = [g for g in user._users_UserGroup.all()
                 if g.name == 'red'][0]
        self.assertEqual(
            {u.name for u in group.users.all()},
            {'Alice', 'Billie', 'Cameron', 'Dana', 'Yvonne'},
        )

    def test_django_update_models_03(self):
        user0 = self.m.User.objects.get(name='Elsa')
        user1 = self.m.User.objects.get(name='Zoe')
        # Replace the author or a post
        post = user0._author_Post.all()[0]
        body = post.body
        post.author = user1
        post.save()

        res = self.m.Post.objects.select_related('author') \
                  .filter(author__name='Zoe')
        self.assertEqual(
            {p.body for p in res},
            {body},
        )

    def test_django_update_models_04(self):
        user = self.m.User.objects.get(name='Zoe')
        post = self.m.Post.objects.select_related('author') \
                   .get(author__name='Elsa')
        # Replace the author or a post
        post_id = post.id
        post.author = user
        post.save()

        post = self.m.Post.objects.get(id=post_id)
        self.assertEqual(post.author.name, 'Zoe')

    def test_django_update_models_05(self):
        # test arrays, bytes and various date/time scalars
        #
        # For the purpose of sending data creating and updating a model are
        # both testing accurate data transfer.

        res = self.m.AssortedScalars.objects.all()[0]

        res.name = 'New Name'
        res.vals.append('jumped')
        res.bstr = b'\x01success\x02'
        res.time = dt.time(8, 23, 54, 999_000)
        res.date = dt.date(2020, 2, 14)
        res.ts = res.ts - dt.timedelta(days=6)

        res.save()

        upd = self.m.AssortedScalars.objects.all()[0]

        self.assertEqual(upd.name, 'New Name')
        self.assertEqual(upd.vals, ['brown', 'fox', 'jumped'])
        self.assertEqual(bytes(upd.bstr), b'\x01success\x02')
        self.assertEqual(
            upd.time,
            dt.time(8, 23, 54, 999_000),
        )
        self.assertEqual(
            upd.date,
            dt.date(2020, 2, 14),
        )
        # time zone aware (default for Django)
        self.assertEqual(
            upd.ts,
            dt.datetime.fromisoformat('2025-01-20T20:13:45+00:00'),
        )

    def test_django_sorting(self):
        # Test the natural sorting function used for ordering fields, etc.

        unsorted = {
            'zoo': 1,
            'apple': 1,
            'potato': 1,
            'grape10': 1,
            'grape1': 1,
            'grape5': 1,
            'grape2': 1,
            'grape20': 1,
            'grape25': 1,
            'grape12': 1,
        }

        self.assertEqual(
            list(sorted(unsorted.items(), key=generator.field_name_sort)),
            [
                ('apple', 1),
                ('grape1', 1),
                ('grape2', 1),
                ('grape5', 1),
                ('grape10', 1),
                ('grape12', 1),
                ('grape20', 1),
                ('grape25', 1),
                ('potato', 1),
                ('zoo', 1),
            ],
        )
