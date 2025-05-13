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
import typing
import unittest

if typing.TYPE_CHECKING:
    from typing import reveal_type

from gel import _testbase as tb


class TestModelGenerator(tb.ModelTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'dbsetup',
                          'base.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'dbsetup',
                         'base.edgeql')

    @unittest.expectedFailure
    @tb.typecheck
    def test_modelgen__smoke_test(self):
        from models import default
        self.assertEqual(
            reveal_type(default.User.groups),
            'this must fail'
        )

    @tb.typecheck
    def test_modelgen_1(self):
        from models import default

        self.assertEqual(
            reveal_type(default.User.name),
            'models.__variants__.std.str'
        )

        self.assertEqual(
            reveal_type(default.User.groups),
            'models.default.UserGroup'
        )
