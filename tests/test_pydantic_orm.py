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

import json
import os

from gel import _testbase as tb
from gel.compatibility.pydmodels import commit


class TestPydantic(tb.PydanticTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'dbsetup',
                          'pydantic.gel')

    MODEL_PACKAGE = 'pymodels'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        from pymodels import default
        cls.m = default

    def tearDown(self):
        self.client.query('delete Object')
        super().tearDown()

    def test_pydantic_insert_models_01(self):
        # insert a bunch of disconnected objects
        data = [
            self.m.ToDoList(name='1st'),
            self.m.ToDoList(name='2nd'),
            self.m.ToDoList(name='3rd'),
            self.m.ToDoList(name='last'),
        ]

        commit(self.client, data)
        vals = json.loads(self.client.query_json('''
            select ToDoList.name;
        '''))

        self.assertEqual(
            set(vals),
            {'1st', '2nd', '3rd', 'last'},
        )

    def test_pydantic_insert_models_02(self):
        # insert a bunch of object structures
        l = self.m.ToDoList(name='mylist')
        data = [
            self.m.Item(num=0, text='first!!!', done=True, list=l),
            self.m.Item(num=1, text='do something', done=True, list=l),
            self.m.Item(num=2, text='coffee', done=False, list=l),
            self.m.Item(num=10, text='last', done=False,
                        list=self.m.ToDoList(name='otherlist')),
        ]

        commit(self.client, data)
        vals = json.loads(self.client.query_json('''
            select ToDoList {
                name,
                items := (
                    select .<list[is Item] {
                        num,
                        text,
                        done,
                    } order by .num
                )
            }
        '''))

        self.assertEqual(
            vals,
            [
                {
                    "name": "mylist",
                    "items": [
                        {
                            "num": 0,
                            "text": "first!!!",
                            "done": True,
                        },
                        {
                            "num": 1,
                            "text": "do something",
                            "done": True,
                        },
                        {
                            "num": 2,
                            "text": "coffee",
                            "done": False,
                        }
                    ]
                },
                {
                    "name": "otherlist",
                    "items": [
                        {
                            "num": 10,
                            "text": "last",
                            "done": False,
                        }
                    ]
                },
            ]
        )

    def test_pydantic_insert_models_03(self):
        # insert a bunch of object structures, but use exclusive fields to
        # merge same object
        data = [
            self.m.Item(
                num=0, text='first!!!', done=True,
                list=self.m.ToDoList(name='mylist'),
            ),
            self.m.Item(
                num=1, text='do something', done=True,
                list=self.m.ToDoList(name='mylist'),
            ),
            self.m.Item(
                num=2, text='coffee', done=False,
                list=self.m.ToDoList(name='mylist'),
            ),
            self.m.Item(
                num=10, text='last', done=False,
                list=self.m.ToDoList(name='otherlist'),
            ),
        ]

        commit(self.client, data, identity_merge=True)
        vals = json.loads(self.client.query_json('''
            select ToDoList {
                name,
                items := (
                    select .<list[is Item] {
                        num,
                        text,
                        done,
                    } order by .num
                )
            }
        '''))

        self.assertEqual(
            vals,
            [
                {
                    "name": "mylist",
                    "items": [
                        {
                            "num": 0,
                            "text": "first!!!",
                            "done": True,
                        },
                        {
                            "num": 1,
                            "text": "do something",
                            "done": True,
                        },
                        {
                            "num": 2,
                            "text": "coffee",
                            "done": False,
                        }
                    ]
                },
                {
                    "name": "otherlist",
                    "items": [
                        {
                            "num": 10,
                            "text": "last",
                            "done": False,
                        }
                    ]
                },
            ]
        )

    def test_pydantic_insert_models_05(self):
        # insert a linked list, also tests multi prop
        data = [
            self.m.LinkedList(
                data='0',
                next=self.m.LinkedList(
                    data='1',
                    ints=[2, 2, 33, -1],
                    next=self.m.LinkedList(
                        data='tail',
                    ),
                ),
            ),
        ]

        commit(self.client, data, identity_merge=True)
        vals = json.loads(self.client.query_json('''
            select LinkedList {
                data,
                ints := (select _ := .ints order by _),
                next: {
                    data
                }
            } order by .data
        '''))

        self.assertEqual(
            vals,
            [
                {
                    "data": "0",
                    "ints": [],
                    "next": {"data": "1"},
                },
                {
                    "data": "1",
                    "ints": [-1, 2, 2, 33],
                    "next": {"data": "tail"},
                },
                {
                    "data": "tail",
                    "ints": [],
                    "next": None,
                },
            ]
        )

    def test_pydantic_insert_models_06(self):
        # insert a looped linked list
        data = [
            self.m.LinkedList(
                data='0',
                next=self.m.LinkedList(
                    data='1',
                    ints=[2, 2, 33, -1],
                    next=self.m.LinkedList(
                        data='tail',
                        next=self.m.LinkedList(
                            data='0',
                        )
                    ),
                ),
            ),
        ]

        commit(self.client, data, identity_merge=True)
        vals = json.loads(self.client.query_json('''
            select LinkedList {
                data,
                ints := (select _ := .ints order by _),
                next: {
                    data
                }
            } order by .data
        '''))

        self.assertEqual(
            vals,
            [
                {
                    "data": "0",
                    "ints": [],
                    "next": {"data": "1"},
                },
                {
                    "data": "1",
                    "ints": [-1, 2, 2, 33],
                    "next": {"data": "tail"},
                },
                {
                    "data": "tail",
                    "ints": [],
                    "next": {"data": "0"},
                },
            ]
        )

    def test_pydantic_update_models_01(self):
        # insert and then update a Tree
        data = [
            self.m.Tree(
                data='root',
                branches=[
                    self.m.Tree(data='l0'),
                    self.m.Tree(data='l1'),
                    self.m.Tree(
                        data='l2',
                        branches=[
                            self.m.Tree(data='l20'),
                            self.m.Tree(data='l21'),
                        ],
                    ),
                    self.m.Tree(
                        data='l3',
                        branches=[
                            self.m.Tree(data='l30'),
                            self.m.Tree(
                                data='l31',
                                branches=[
                                    self.m.Tree(data='l310'),
                                ]
                            ),
                        ],
                    ),
                ],
            ),
        ]

        commit(self.client, data, identity_merge=True)
        vals = json.loads(self.client.query_single_json('''
            select Tree {
                data,
                branches: {
                    data,
                    branches: {
                        data,
                        branches: {
                            data
                        } order by .data,
                    } order by .data,
                } order by .data,
            } filter .data = 'root'
        '''))

        self.assertEqual(
            vals,
            {
                "data": "root",
                "branches": [
                    {
                        "data": "l0",
                        "branches": [],
                    },
                    {
                        "data": "l1",
                        "branches": [],
                    },
                    {
                        "data": "l2",
                        "branches": [
                            {
                                "data": "l20",
                                "branches": [],
                            },
                            {
                                "data": "l21",
                                "branches": [],
                            },
                        ],
                    },
                    {
                        "data": "l3",
                        "branches": [
                            {
                                "data": "l30",
                                "branches": [],
                            },
                            {
                                "data": "l31",
                                "branches": [
                                    {
                                        "data": "l310",
                                    },
                                ],
                            },
                        ],
                    },
                ],
            }
        )

        # Now update some of the tree
        updates = []
        for res in self.client.query('''
            select Tree {id, data}
            order by .data
        '''):
            if res.data[-1] == '0':
                updates.append(
                    self.m.Tree.update(
                        id=res.id,
                        data=f'updated {res.data}',
                    )
                )
        updates[0].branches = [self.m.Tree(data='new leaf')]
        commit(self.client, updates)

        vals = json.loads(self.client.query_single_json('''
            select Tree {
                data,
                branches: {
                    data,
                    branches: {
                        data,
                        branches: {
                            data
                        } order by .data,
                    } order by .data,
                } order by .data,
            } filter .data = 'root'
        '''))
        self.assertEqual(
            vals,
            {
                "data": "root",
                "branches": [
                    {
                        "data": "l1",
                        "branches": [],
                    },
                    {
                        "data": "l2",
                        "branches": [
                            {
                                "data": "l21",
                                "branches": [],
                            },
                            {
                                "data": "updated l20",
                                "branches": [],
                            },
                        ],
                    },
                    {
                        "data": "l3",
                        "branches": [
                            {
                                "data": "l31",
                                "branches": [
                                    {
                                        "data": "updated l310",
                                    },
                                ],
                            },
                            {
                                "data": "updated l30",
                                "branches": [],
                            },
                        ],
                    },
                    {
                        "data": "updated l0",
                        "branches": [
                            {
                                "data": "new leaf",
                                "branches": [],
                            },
                        ],
                    },
                ]
            }
        )
