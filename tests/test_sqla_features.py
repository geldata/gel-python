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

import os
import unittest

try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
except ImportError:
    NO_ORM = True
else:
    NO_ORM = False

from gel import _testbase as tb


class TestSQLAFeatures(tb.SQLATestCase):
    SCHEMA = os.path.join(
        os.path.dirname(__file__), 'dbsetup', 'features_default.esdl')

    SCHEMA_OTHER = os.path.join(
        os.path.dirname(__file__), 'dbsetup', 'features_other.esdl')

    SCHEMA_OTHER_NESTED = os.path.join(
        os.path.dirname(__file__), 'dbsetup', 'features_other_nested.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'dbsetup',
                         'features.edgeql')

    MODEL_PACKAGE = 'fmodels'

    @classmethod
    def setUpClass(cls):
        if NO_ORM:
            raise unittest.SkipTest("sqlalchemy is not installed")

        super().setUpClass()
        cls.engine = create_engine(cls.get_dsn_for_sqla())
        cls.sess = Session(cls.engine, autobegin=False)

        from fmodels import default, other
        from fmodels.other import nested
        cls.sm = default
        cls.sm_o = other
        cls.sm_on = nested

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

    def test_sqla_linkprops_01(self):
        val = self.sess.query(self.sm.HasLinkPropsA).one()
        self.assertEqual(val.child.target.num, 0)
        self.assertEqual(val.child.a, 'single')

    def test_sqla_linkprops_02(self):
        val = self.sess.query(self.sm.HasLinkPropsA).one()
        self.assertEqual(val.child.target.num, 0)
        self.assertEqual(val.child.a, 'single')

        # replace the single child with a different one
        ch = self.sess.query(self.sm.Child).filter_by(num=1).one()
        val.child = self.sm.HasLinkPropsA_child_link(a='replaced', target=ch)
        self.sess.flush()

        val = self.sess.query(self.sm.HasLinkPropsA).one()
        self.assertEqual(val.child.target.num, 1)
        self.assertEqual(val.child.a, 'replaced')

        # make sure there's only one link object still
        vals = self.sess.query(self.sm.HasLinkPropsA_child_link).all()
        self.assertEqual(
            [(val.a, val.target.num) for val in vals],
            [('replaced', 1)]
        )

    def test_sqla_linkprops_03(self):
        val = self.sess.query(self.sm.HasLinkPropsA).one()
        self.assertEqual(val.child.target.num, 0)
        self.assertEqual(val.child.a, 'single')

        # delete the child object
        val = self.sess.query(self.sm.Child).filter_by(num=0).one()
        self.sess.delete(val)
        self.sess.flush()

        val = self.sess.query(self.sm.HasLinkPropsA).one()
        self.assertEqual(val.child, None)

        # make sure the link object is removed
        vals = self.sess.query(self.sm.HasLinkPropsA_child_link).all()
        self.assertEqual(vals, [])

    def test_sqla_linkprops_04(self):
        val = self.sess.query(self.sm.HasLinkPropsB).one()
        self.assertEqual(
            {(c.b, c.target.num) for c in val.children},
            {('hello', 0), ('world', 1)},
        )

    def test_sqla_linkprops_05(self):
        val = self.sess.query(self.sm.HasLinkPropsB).one()
        self.assertEqual(
            {(c.b, c.target.num) for c in val.children},
            {('hello', 0), ('world', 1)},
        )

        # Remove one of the children
        for t in list(val.children):
            if t.b != 'hello':
                val.children.remove(t)
        self.sess.flush()

        val = self.sess.query(self.sm.HasLinkPropsB).one()
        self.assertEqual(
            {(c.b, c.target.num) for c in val.children},
            {('hello', 0)},
        )

    def test_sqla_linkprops_06(self):
        val = self.sess.query(self.sm.HasLinkPropsB).one()
        self.assertEqual(
            {(c.b, c.target.num) for c in val.children},
            {('hello', 0), ('world', 1)},
        )

        # Remove one of the children
        val = self.sess.query(self.sm.Child).filter_by(num=0).one()
        self.sess.delete(val)
        self.sess.flush()

        val = self.sess.query(self.sm.HasLinkPropsB).one()
        self.assertEqual(
            {(c.b, c.target.num) for c in val.children},
            {('world', 1)},
        )

    def test_sqla_module_01(self):
        vals = self.sess.query(self.sm_o.Branch).all()
        self.assertEqual(
            {(r.val, tuple(sorted(lf.num for lf in r.leaves))) for r in vals},
            {
                ('big', (20, 30)),
                ('small', (10,)),
            },
        )

        vals = self.sess.query(self.sm_on.Leaf).all()
        self.assertEqual(
            {r.num for r in vals},
            {10, 20, 30},
        )

        vals = self.sess.query(self.sm.Theme).all()
        self.assertEqual(
            {
                (r.color, r.branch.note, r.branch.target.val)
                for r in vals
            },
            {
                ('green', 'fresh', 'big'),
                ('orange', 'fall', 'big'),
            },
        )

    def test_sqla_module_02(self):
        val = self.sess.query(self.sm.Theme).filter_by(color='orange').one()
        self.assertEqual(
            (val.color, val.branch.note, val.branch.target.val),
            ('orange', 'fall', 'big'),
        )

        # swap the branch for 'small'
        br = self.sess.query(self.sm_o.Branch).filter_by(val='small').one()
        # can't update link tables (Gel limitation), so we will create a new
        # one
        val.branch = self.sm.Theme_branch_link(
            note='swapped', target=br)
        self.sess.add(val)
        self.sess.flush()

        vals = self.sess.query(self.sm.Theme).all()
        self.assertEqual(
            {
                (r.color, r.branch.note, r.branch.target.val)
                for r in vals
            },
            {
                ('green', 'fresh', 'big'),
                ('orange', 'swapped', 'small'),
            },
        )

    def test_sqla_bklink_01(self):
        # test backlink name collisions
        foo = self.sess.query(self.sm.Foo).filter_by(name='foo').one()
        oof = self.sess.query(self.sm.Foo).filter_by(name='oof').one()

        # only one link from Bar 123 to foo
        self.assertEqual(
            [obj.n for obj in foo._foo_Bar],
            [123],
        )
        # only one link from Who 456 to oof
        self.assertEqual(
            [obj.x for obj in oof._foo_Who],
            [456],
        )

        # foo is linked via `many_foo` from both Bar and Who
        self.assertEqual(
            [obj.n for obj in foo._many_foo_Bar],
            [123],
        )
        self.assertEqual(
            [(obj.note, obj.source.x) for obj in foo._many_foo_Who],
            [('just one', 456)],
        )
