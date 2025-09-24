#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


import string
import typing_extensions

import os


from gel._internal._testbase import _models as tb


@tb.typecheck
class TestLinkSetModels(tb.ModelTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), "dbsetup", "link_set.gel")

    SETUP = """
        INSERT House { name := 'Griffindor' };
        INSERT House { name := 'Hufflepuff' };
        INSERT House { name := 'Ravenclaw' };
        INSERT House { name := 'Slytherin' };

        INSERT Class { name := 'Charms' };
        INSERT Class { name := 'Potions' };
        INSERT Class { name := 'Divination' };

        CREATE ALIAS Griffindor := assert_exists((
            select House
            filter .name = 'Griffindor'
            limit 1
        ));

        INSERT Person {
            name := 'Harry Potter',
            house := Griffindor {
                @rank := 'seeker'
            },
            pet := (
                INSERT Pet {
                    name := 'Hedwig',
                }
            ),
            classes := (
                select Class
                filter .name in {'Charms', 'Potions'}
            ),
        };
        INSERT Person {
            name := 'Hermione Granger',
            house := Griffindor,
            pet := (
                INSERT Pet {
                    name := 'Crookshanks',
                }
            ),
        };
        INSERT Person {
            name := 'Ron Weasley',
            house := Griffindor,
            pet := (
                INSERT Pet {
                    name := 'Scabbers',
                }
            ),
        };
        INSERT Person {
            name := 'Neville Longbottom',
            house := Griffindor,
            pet := (
                INSERT Pet {
                    name := 'Trevor',
                }
            ),
        };
        UPDATE Person
        filter .name = 'Harry Potter'
        set {
            friends := assert_distinct((
                with friend_data := {
                    ('Hermione Granger', 'smart'),
                    ('Ron Weasley', 'reliable'),
                }
                for data in friend_data union (
                    select detached Person {
                        @opinion := data.1
                    }
                    filter .name = data.0
                )
            ))
        };
    """

    def _format_type_name(self, name: str) -> str:
        return name.translate(
            str.maketrans("", "", string.whitespace)
        ).replace(",", ", ")

    def test_link_set_model_types_01(self):
        from models.link_set import default

        typing_extensions.assert_type(
            default.House.members,
            type[default.Person],
        )
        typing_extensions.assert_type(
            default.Person.house,
            type[default.Person.__links__.house],
        )
        typing_extensions.assert_type(
            default.Person.friends,
            type[default.Person.__links__.friends],
        )
        typing_extensions.assert_type(
            default.Person.pet,
            type[default.Pet],
        )
        typing_extensions.assert_type(
            default.Person.classes,
            type[default.Class],
        )
        typing_extensions.assert_type(
            default.Pet.owner,
            type[default.Person],
        )

    def test_link_set_model_query_single_link_01(self):
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                pet=True,
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hedwig = self.client.query_required_single(
            default.Pet.filter(name="Hedwig").limit(1)
        )

        self.assertEqual(harry.pet, hedwig)
        typing_extensions.assert_type(
            harry.pet,
            default.Pet | None,
        )

    def test_link_set_model_query_single_link_02(self):
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                pet=lambda p: p.pet.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hedwig = self.client.query_required_single(
            default.Pet.filter(name="Hedwig").limit(1)
        )

        self.assertEqual(harry.pet, hedwig)
        typing_extensions.assert_type(
            harry.pet,
            default.Pet | None,
        )

    def test_link_set_model_query_single_link_03(self):
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                "*",
            )
            .filter(name="Harry Potter")
            .limit(1)
        )

        self.assertFalse(hasattr(harry, 'pet'))

    def test_link_set_model_query_multi_link_01(self):
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=True,
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_query_multi_link_02(self):
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_query_multi_link_03(self):
        from gel._internal import _tracked_list
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                "*",
            )
            .filter(name="Harry Potter")
            .limit(1)
        )

        self.assertTrue(hasattr(harry, "classes"))
        self.assertEqual(harry.classes._mode, _tracked_list.Mode.Write)
        self.assertEqual(harry.classes._items, [])
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_query_single_link_with_props_01(self):
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                house=True,
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        griffindor = self.client.query_required_single(
            default.House.filter(name="Griffindor").limit(1)
        )

        self.assertEqual(harry.house, griffindor)
        typing_extensions.assert_type(
            harry.house,
            default.Person.__links__.house | None,
        )
        self.assertEqual(
            (
                harry.house.__linkprops__.rank
                if harry.house is not None
                else ""
            ),
            "seeker",
        )
        typing_extensions.assert_type(
            (
                harry.house.__linkprops__.rank
                if harry.house is not None
                else None
            ),
            str | None,
        )

    def test_link_set_model_query_single_link_with_props_02(self):
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                house=lambda p: p.house.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        griffindor = self.client.query_required_single(
            default.House.filter(name="Griffindor").limit(1)
        )

        self.assertEqual(harry.house, griffindor)
        typing_extensions.assert_type(
            harry.house,
            default.Person.__links__.house | None,
        )
        self.assertEqual(
            (
                harry.house.__linkprops__.rank
                if harry.house is not None
                else ""
            ),
            "seeker",
        )
        typing_extensions.assert_type(
            (
                harry.house.__linkprops__.rank
                if harry.house is not None
                else None
            ),
            str | None,
        )

    def test_link_set_model_query_single_link_with_props_03(self):
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                "*",
            )
            .filter(name="Harry Potter")
            .limit(1)
        )

        self.assertFalse(hasattr(harry, 'house'))

    def test_link_set_model_query_multi_link_with_props_01(self):
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=True,
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )

        self.assertEqual(harry.friends, {hermione, ron})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: "reliable"},
        )
        typing_extensions.assert_type(
            list(harry.friends)[0].__linkprops__.opinion,
            str | None,
        )

    def test_link_set_model_query_multi_link_with_props_02(self):
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )

        self.assertEqual(harry.friends, {hermione, ron})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: "reliable"},
        )
        typing_extensions.assert_type(
            list(harry.friends)[0].__linkprops__.opinion,
            str | None,
        )

    def test_link_set_model_query_multi_link_with_props_03(self):
        from gel._internal import _tracked_list
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                "*",
            )
            .filter(name="Harry Potter")
            .limit(1)
        )

        self.assertTrue(hasattr(harry, "friends"))
        self.assertEqual(harry.friends._mode, _tracked_list.Mode.Write)
        self.assertEqual(harry.friends._items, [])
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )

    def test_link_set_model_query_computed_single_link_01(self):
        from models.link_set import default

        hedwig = self.client.query_required_single(
            default.Pet.select(owner=True).filter(name="Hedwig").limit(1)
        )
        harry = self.client.query_required_single(
            default.Person.filter(name="Harry Potter").limit(1)
        )

        self.assertEqual(hedwig.owner, harry)
        typing_extensions.assert_type(hedwig.owner, default.Person | None)

    def test_link_set_model_query_computed_single_link_02(self):
        from models.link_set import default

        hedwig = self.client.query_required_single(
            default.Pet.select(owner=lambda p: p.owner.select(name=True))
            .filter(name="Hedwig")
            .limit(1)
        )
        harry = self.client.query_required_single(
            default.Person.filter(name="Harry Potter").limit(1)
        )

        self.assertEqual(hedwig.owner, harry)
        typing_extensions.assert_type(hedwig.owner, default.Person | None)

    def test_link_set_model_query_computed_single_link_03(self):
        from models.link_set import default

        hedwig = self.client.query_required_single(
            default.Pet.select(
                "*",
            )
            .filter(name="Hedwig")
            .limit(1)
        )

        self.assertFalse(hasattr(hedwig, "owner"))

    @tb.xfail  # expected ForwardRef to be a path alias
    def test_link_set_model_query_computed_multi_link_01(self):
        from gel._internal._qbmodel._abstract._link_set import ComputedLinkSet
        from models.link_set import default

        griffindor = self.client.query_required_single(
            default.House.select(
                members=True,
            )
            .filter(name="Griffindor")
            .limit(1)
        )
        harry = self.client.query_required_single(
            default.Person.filter(name="Harry Potter").limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )

        self.assertEqual(griffindor.members, {harry, hermione, ron})
        typing_extensions.assert_type(
            griffindor.members,
            ComputedLinkSet[default.Person],
        )

    def test_link_set_model_query_computed_multi_link_02(self):
        from gel._internal._qbmodel._abstract._link_set import ComputedLinkSet
        from models.link_set import default

        griffindor = self.client.query_required_single(
            default.House.select(
                members=lambda h: h.members.select(name=True),
            )
            .filter(name="Griffindor")
            .limit(1)
        )
        harry = self.client.query_required_single(
            default.Person.filter(name="Harry Potter").limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        neville = self.client.query_required_single(
            default.Person.filter(name="Neville Longbottom").limit(1)
        )

        self.assertEqual(griffindor.members, {harry, hermione, ron, neville})
        typing_extensions.assert_type(
            griffindor.members,
            ComputedLinkSet[default.Person],
        )

    def test_link_set_model_query_computed_multi_link_03(self):
        from models.link_set import default

        griffindor = self.client.query_required_single(
            default.House.select(
                "*",
            )
            .filter(name="Griffindor")
            .limit(1)
        )

        self.assertFalse(hasattr(griffindor, "members"))

    def test_link_set_model_modify_multi_link_add_01(self):
        # Add existing item
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )

        harry.classes.add(potions)

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_modify_multi_link_add_02(self):
        # Add new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        divination = self.client.query_required_single(
            default.Class.filter(name="Divination").limit(1)
        )

        harry.classes.add(divination)

        self.assertEqual(harry.classes, {charms, potions, divination})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_modify_multi_link_add_03(self):
        # Add new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        herbology = default.Class(name="Herbology")

        harry.classes.add(herbology)

        # Compare to list, since there are unhashable items
        self.assertEqual(harry.classes, [charms, potions, herbology])
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_modify_multi_link_remove_01(self):
        # Remove existing item
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )

        harry.classes.remove(potions)

        # Compare to list, tracking indexes and sets not synchronized
        self.assertEqual(harry.classes, [charms])
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_modify_multi_link_remove_02(self):
        # Remove new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        divination = self.client.query_required_single(
            default.Class.filter(name="Divination").limit(1)
        )

        with self.assertRaises(KeyError):
            harry.classes.remove(divination)

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_modify_multi_link_remove_03(self):
        # Remove new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        herbology = default.Class(name="Herbology")

        with self.assertRaises(KeyError):
            harry.classes.remove(herbology)

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_modify_multi_link_discard_01(self):
        # Discard existing item
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )

        harry.classes.discard(potions)

        # Compare to list, tracking indexes and sets not synchronized
        self.assertEqual(harry.classes, [charms])
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_modify_multi_link_discard_02(self):
        # Discard new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        divination = self.client.query_required_single(
            default.Class.filter(name="Divination").limit(1)
        )

        harry.classes.discard(divination)

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_modify_multi_link_discard_03(self):
        # Discard new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        herbology = default.Class(name="Herbology")

        harry.classes.discard(herbology)

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_modify_multi_link_clear_01(self):
        # Discard existing item
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )

        harry.classes.clear()

        self.assertEqual(harry.classes, set())
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_modify_multi_link_update_01(self):
        # Update existing item
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )

        harry.classes.update([potions])

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_modify_multi_link_update_02(self):
        # Update new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        divination = self.client.query_required_single(
            default.Class.filter(name="Divination").limit(1)
        )

        harry.classes.update([divination])

        self.assertEqual(harry.classes, {charms, potions, divination})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_modify_multi_link_update_03(self):
        # Update new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        herbology = default.Class(name="Herbology")

        harry.classes.update([herbology])

        # Compare to list, since there are unhashable items
        self.assertEqual(harry.classes, [charms, potions, herbology])
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    @tb.xfail  # mypy seems to be checking + instead of +=
    def test_link_set_model_modify_multi_link_op_iadd_01(self):
        # Operator iadd existing item
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )

        harry.classes += [potions]

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    @tb.xfail  # mypy seems to be checking + instead of +=
    def test_link_set_model_modify_multi_link_op_iadd_02(self):
        # Operator iadd new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        divination = self.client.query_required_single(
            default.Class.filter(name="Divination").limit(1)
        )

        harry.classes += [divination]

        self.assertEqual(harry.classes, {charms, potions, divination})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    @tb.xfail  # mypy seems to be checking + instead of +=
    def test_link_set_model_modify_multi_link_op_iadd_03(self):
        # Update new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        herbology = default.Class(name="Herbology")

        harry.classes += [herbology]

        # Compare to list, since there are unhashable items
        self.assertEqual(harry.classes, [charms, potions, herbology])
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    @tb.xfail  # mypy seems to be checking - instead of -=
    def test_link_set_model_modify_multi_link_op_isub_01(self):
        # Operator isub existing item
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )

        harry.classes -= [potions]

        self.assertEqual(harry.classes, {charms})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    @tb.xfail  # mypy seems to be checking - instead of -=
    def test_link_set_model_modify_multi_link_op_isub_02(self):
        # Operator isub new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                classes=lambda p: p.classes.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        divination = self.client.query_required_single(
            default.Class.filter(name="Divination").limit(1)
        )

        harry.classes -= [divination]

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    # Tests for friends property (multi link with props) instead of classes
    def test_link_set_model_modify_multi_link_with_props_add_01(self):
        # Add existing item
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )

        harry.friends.add(ron)

        self.assertEqual(harry.friends, {hermione, ron})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: "reliable"},
        )

    def test_link_set_model_modify_multi_link_with_props_add_02(self):
        # Add new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        neville = self.client.query_required_single(
            default.Person.filter(name="Neville Longbottom").limit(1)
        )

        harry.friends.add(neville)

        self.assertEqual(harry.friends, {hermione, ron, neville})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {
                f: (
                    f.__linkprops__.opinion
                    if hasattr(f.__linkprops__, "opinion")
                    else None
                )
                for f in harry.friends
            },
            {hermione: "smart", ron: "reliable", neville: None},
        )

    def test_link_set_model_modify_multi_link_with_props_add_03(self):
        # Add new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        luna = default.Person(name="Luna Lovegood")

        harry.friends.add(luna)

        # Compare to list, since there are unhashable items
        self.assertEqual(harry.friends, [hermione, ron, luna])
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {
                f.name: (
                    f.__linkprops__.opinion
                    if hasattr(f.__linkprops__, "opinion")
                    else None
                )
                for f in harry.friends
            },
            {
                "Hermione Granger": "smart",
                "Ron Weasley": "reliable",
                "Luna Lovegood": None,
            },
        )

    def test_link_set_model_modify_multi_link_with_props_remove_01(self):
        # Remove existing item
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )

        harry.friends.remove(ron)

        # Compare to list, tracking indexes and sets not synchronized
        self.assertEqual(harry.friends, [hermione])
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart"},
        )

    def test_link_set_model_modify_multi_link_with_props_remove_02(self):
        # Remove new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        neville = self.client.query_required_single(
            default.Person.filter(name="Neville Longbottom").limit(1)
        )

        with self.assertRaises(KeyError):
            harry.friends.remove(neville)

        self.assertEqual(harry.friends, {hermione, ron})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: "reliable"},
        )

    def test_link_set_model_modify_multi_link_with_props_remove_03(self):
        # Remove new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        luna = default.Person(name="Luna Lovegood")

        with self.assertRaises(KeyError):
            harry.friends.remove(luna)

        self.assertEqual(harry.friends, {hermione, ron})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: "reliable"},
        )

    def test_link_set_model_modify_multi_link_with_props_discard_01(self):
        # Discard existing item
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )

        harry.friends.discard(ron)

        # Compare to list, tracking indexes and sets not synchronized
        self.assertEqual(harry.friends, [hermione])
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart"},
        )

    def test_link_set_model_modify_multi_link_with_props_discard_02(self):
        # Discard new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        neville = self.client.query_required_single(
            default.Person.filter(name="Neville Longbottom").limit(1)
        )

        harry.friends.discard(neville)

        self.assertEqual(harry.friends, {hermione, ron})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: "reliable"},
        )

    def test_link_set_model_modify_multi_link_with_props_discard_03(self):
        # Discard new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        luna = default.Person(name="Luna Lovegood")

        harry.friends.discard(luna)

        self.assertEqual(harry.friends, {hermione, ron})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: "reliable"},
        )

    def test_link_set_model_modify_multi_link_with_props_clear_01(self):
        # Clear existing items
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )

        harry.friends.clear()

        self.assertEqual(harry.friends, set())
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {},
        )

    def test_link_set_model_modify_multi_link_with_props_update_01(self):
        # Update existing item
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )

        harry.friends.update([ron])

        self.assertEqual(harry.friends, {hermione, ron})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: None},
        )

    def test_link_set_model_modify_multi_link_with_props_update_02(self):
        # Update new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        neville = self.client.query_required_single(
            default.Person.filter(name="Neville Longbottom").limit(1)
        )

        harry.friends.update([neville])

        self.assertEqual(harry.friends, {hermione, ron, neville})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {
                f: (
                    f.__linkprops__.opinion
                    if hasattr(f.__linkprops__, "opinion")
                    else None
                )
                for f in harry.friends
            },
            {hermione: "smart", ron: "reliable", neville: None},
        )

    def test_link_set_model_modify_multi_link_with_props_update_03(self):
        # Update new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        luna = default.Person(name="Luna Lovegood")

        harry.friends.update([luna])

        # Compare to list, since there are unhashable items
        self.assertEqual(harry.friends, [hermione, ron, luna])
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {
                f.name: (
                    f.__linkprops__.opinion
                    if hasattr(f.__linkprops__, "opinion")
                    else None
                )
                for f in harry.friends
            },
            {
                "Hermione Granger": "smart",
                "Ron Weasley": "reliable",
                "Luna Lovegood": None,
            },
        )

    def test_link_set_model_modify_multi_link_with_props_op_iadd_01(self):
        # Operator iadd existing item
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )

        harry.friends += [ron]

        self.assertEqual(harry.friends, {hermione, ron})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: None},
        )

    def test_link_set_model_modify_multi_link_with_props_op_iadd_02(self):
        # Operator iadd new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        neville = self.client.query_required_single(
            default.Person.filter(name="Neville Longbottom").limit(1)
        )

        harry.friends += [neville]

        self.assertEqual(harry.friends, {hermione, ron, neville})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {
                f: (
                    f.__linkprops__.opinion
                    if hasattr(f.__linkprops__, "opinion")
                    else None
                )
                for f in harry.friends
            },
            {hermione: "smart", ron: "reliable", neville: None},
        )

    def test_link_set_model_modify_multi_link_with_props_op_iadd_03(self):
        # Operator iadd new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        luna = default.Person(name="Luna Lovegood")

        harry.friends += [luna]

        # Compare to list, since there are unhashable items
        self.assertEqual(harry.friends, [hermione, ron, luna])
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {
                f.name: (
                    f.__linkprops__.opinion
                    if hasattr(f.__linkprops__, "opinion")
                    else None
                )
                for f in harry.friends
            },
            {
                "Hermione Granger": "smart",
                "Ron Weasley": "reliable",
                "Luna Lovegood": None,
            },
        )

    @tb.xfail  # mypy seems to be checking - instead of -=
    def test_link_set_model_modify_multi_link_with_props_op_isub_01(self):
        # Operator isub existing item
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )

        harry.friends -= [ron]

        self.assertEqual(harry.friends, {hermione})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart"},
        )

    @tb.xfail  # mypy seems to be checking - instead of -=
    def test_link_set_model_modify_multi_link_with_props_op_isub_02(self):
        # Operator isub new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        harry = self.client.query_required_single(
            default.Person.select(
                friends=lambda p: p.friends.select(name=True),
            )
            .filter(name="Harry Potter")
            .limit(1)
        )
        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        neville = self.client.query_required_single(
            default.Person.filter(name="Neville Longbottom").limit(1)
        )

        harry.friends -= [neville]

        self.assertEqual(harry.friends, {hermione, ron})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: "reliable"},
        )

    def test_link_set_model_fresh_multi_link_add_01(self):
        # Add existing item
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        harry.classes.add(potions)

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_fresh_multi_link_add_02(self):
        # Add new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        divination = self.client.query_required_single(
            default.Class.filter(name="Divination").limit(1)
        )

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        harry.classes.add(divination)

        self.assertEqual(harry.classes, {charms, potions, divination})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_fresh_multi_link_add_03(self):
        # Add new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        herbology = default.Class(name="Herbology")

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        harry.classes.add(herbology)

        # Compare to list, since there are unhashable items
        self.assertEqual(harry.classes, [charms, potions, herbology])
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_fresh_multi_link_remove_01(self):
        # Remove existing item
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        harry.classes.remove(potions)

        # Compare to list, tracking indexes and sets not synchronized
        self.assertEqual(harry.classes, [charms])
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_fresh_multi_link_remove_02(self):
        # Remove new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        divination = self.client.query_required_single(
            default.Class.filter(name="Divination").limit(1)
        )

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        with self.assertRaises(KeyError):
            harry.classes.remove(divination)

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_fresh_multi_link_remove_03(self):
        # Remove new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        herbology = default.Class(name="Herbology")

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        with self.assertRaises(KeyError):
            harry.classes.remove(herbology)

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_fresh_multi_link_discard_01(self):
        # Discard existing item
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        harry.classes.discard(potions)

        # Compare to list, tracking indexes and sets not synchronized
        self.assertEqual(harry.classes, [charms])
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_fresh_multi_link_discard_02(self):
        # Discard new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        divination = self.client.query_required_single(
            default.Class.filter(name="Divination").limit(1)
        )

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        harry.classes.discard(divination)

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_fresh_multi_link_discard_03(self):
        # Discard new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        herbology = default.Class(name="Herbology")

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        harry.classes.discard(herbology)

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_fresh_multi_link_clear_01(self):
        # Clear existing items
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        harry.classes.clear()

        self.assertEqual(harry.classes, set())
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_fresh_multi_link_update_01(self):
        # Update existing item
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        harry.classes.update([potions])

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_fresh_multi_link_update_02(self):
        # Update new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        divination = self.client.query_required_single(
            default.Class.filter(name="Divination").limit(1)
        )

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        harry.classes.update([divination])

        self.assertEqual(harry.classes, {charms, potions, divination})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_fresh_multi_link_update_03(self):
        # Update new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        herbology = default.Class(name="Herbology")

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        harry.classes.update([herbology])

        # Compare to list, since there are unhashable items
        self.assertEqual(harry.classes, [charms, potions, herbology])
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    @tb.xfail  # mypy seems to be checking + instead of +=
    def test_link_set_model_fresh_multi_link_op_iadd_01(self):
        # Operator iadd existing item
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        harry.classes += [potions]

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    @tb.xfail  # mypy seems to be checking + instead of +=
    def test_link_set_model_fresh_multi_link_op_iadd_02(self):
        # Operator iadd new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        divination = self.client.query_required_single(
            default.Class.filter(name="Divination").limit(1)
        )

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        harry.classes += [divination]

        self.assertEqual(harry.classes, {charms, potions, divination})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    @tb.xfail  # mypy seems to be checking + instead of +=
    def test_link_set_model_fresh_multi_link_op_iadd_03(self):
        # Operator iadd new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        herbology = default.Class(name="Herbology")

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        harry.classes += [herbology]

        # Compare to list, since there are unhashable items
        self.assertEqual(harry.classes, [charms, potions, herbology])
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    @tb.xfail  # mypy seems to be checking - instead of -=
    def test_link_set_model_fresh_multi_link_op_isub_01(self):
        # Operator isub existing item
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        harry.classes -= [potions]

        self.assertEqual(harry.classes, {charms})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    @tb.xfail  # mypy seems to be checking - instead of -=
    def test_link_set_model_fresh_multi_link_op_isub_02(self):
        # Operator isub new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkSet
        from models.link_set import default

        charms = self.client.query_required_single(
            default.Class.filter(name="Charms").limit(1)
        )
        potions = self.client.query_required_single(
            default.Class.filter(name="Potions").limit(1)
        )
        divination = self.client.query_required_single(
            default.Class.filter(name="Divination").limit(1)
        )

        harry = default.Person(name="Harry Potter", classes=[charms, potions])

        harry.classes -= [divination]

        self.assertEqual(harry.classes, {charms, potions})
        typing_extensions.assert_type(
            harry.classes,
            LinkSet[default.Class],
        )

    def test_link_set_model_fresh_multi_link_with_props_add_01(self):
        # Add existing item
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        harry.friends.add(ron)

        self.assertEqual(harry.friends, {hermione, ron})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: "reliable"},
        )

    def test_link_set_model_fresh_multi_link_with_props_add_02(self):
        # Add new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        neville = self.client.query_required_single(
            default.Person.filter(name="Neville Longbottom").limit(1)
        )

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        harry.friends.add(neville)

        self.assertEqual(harry.friends, {hermione, ron, neville})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {
                f: (
                    f.__linkprops__.opinion
                    if hasattr(f.__linkprops__, "opinion")
                    else None
                )
                for f in harry.friends
            },
            {hermione: "smart", ron: "reliable", neville: None},
        )

    def test_link_set_model_fresh_multi_link_with_props_add_03(self):
        # Add new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        luna = default.Person(name="Luna Lovegood")

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        harry.friends.add(luna)

        # Compare to list, since there are unhashable items
        self.assertEqual(harry.friends, [hermione, ron, luna])
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {
                f.name: (
                    f.__linkprops__.opinion
                    if hasattr(f.__linkprops__, "opinion")
                    else None
                )
                for f in harry.friends
            },
            {
                "Hermione Granger": "smart",
                "Ron Weasley": "reliable",
                "Luna Lovegood": None,
            },
        )

    def test_link_set_model_fresh_multi_link_with_props_remove_01(self):
        # Remove existing item
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        harry.friends.remove(ron)

        # Compare to list, tracking indexes and sets not synchronized
        self.assertEqual(harry.friends, [hermione])
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart"},
        )

    def test_link_set_model_fresh_multi_link_with_props_remove_02(self):
        # Remove new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        neville = self.client.query_required_single(
            default.Person.filter(name="Neville Longbottom").limit(1)
        )

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        with self.assertRaises(KeyError):
            harry.friends.remove(neville)

        self.assertEqual(harry.friends, {hermione, ron})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: "reliable"},
        )

    def test_link_set_model_fresh_multi_link_with_props_remove_03(self):
        # Remove new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        luna = default.Person(name="Luna Lovegood")

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        with self.assertRaises(KeyError):
            harry.friends.remove(luna)

        self.assertEqual(harry.friends, {hermione, ron})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: "reliable"},
        )

    def test_link_set_model_fresh_multi_link_with_props_discard_01(self):
        # Discard existing item
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        harry.friends.discard(ron)

        # Compare to list, tracking indexes and sets not synchronized
        self.assertEqual(harry.friends, [hermione])
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart"},
        )

    def test_link_set_model_fresh_multi_link_with_props_discard_02(self):
        # Discard new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        neville = self.client.query_required_single(
            default.Person.filter(name="Neville Longbottom").limit(1)
        )

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        harry.friends.discard(neville)

        self.assertEqual(harry.friends, {hermione, ron})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: "reliable"},
        )

    def test_link_set_model_fresh_multi_link_with_props_discard_03(self):
        # Discard new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        luna = default.Person(name="Luna Lovegood")

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        harry.friends.discard(luna)

        self.assertEqual(harry.friends, {hermione, ron})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: "reliable"},
        )

    def test_link_set_model_fresh_multi_link_with_props_clear_01(self):
        # Clear existing items
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        harry.friends.clear()

        self.assertEqual(harry.friends, set())
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {},
        )

    def test_link_set_model_fresh_multi_link_with_props_update_01(self):
        # Update existing item
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        harry.friends.update([ron])

        self.assertEqual(harry.friends, {hermione, ron})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: None},
        )

    def test_link_set_model_fresh_multi_link_with_props_update_02(self):
        # Update new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        neville = self.client.query_required_single(
            default.Person.filter(name="Neville Longbottom").limit(1)
        )

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        harry.friends.update([neville])

        self.assertEqual(harry.friends, {hermione, ron, neville})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {
                f: (
                    f.__linkprops__.opinion
                    if hasattr(f.__linkprops__, "opinion")
                    else None
                )
                for f in harry.friends
            },
            {hermione: "smart", ron: "reliable", neville: None},
        )

    def test_link_set_model_fresh_multi_link_with_props_update_03(self):
        # Update new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        luna = default.Person(name="Luna Lovegood")

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        harry.friends.update([luna])

        # Compare to list, since there are unhashable items
        self.assertEqual(harry.friends, [hermione, ron, luna])
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {
                f.name: (
                    f.__linkprops__.opinion
                    if hasattr(f.__linkprops__, "opinion")
                    else None
                )
                for f in harry.friends
            },
            {
                "Hermione Granger": "smart",
                "Ron Weasley": "reliable",
                "Luna Lovegood": None,
            },
        )

    def test_link_set_model_fresh_multi_link_with_props_op_iadd_01(self):
        # Operator iadd existing item
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        harry.friends += [ron]

        self.assertEqual(harry.friends, {hermione, ron})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: None},
        )

    def test_link_set_model_fresh_multi_link_with_props_op_iadd_02(self):
        # Operator iadd new item with id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        neville = self.client.query_required_single(
            default.Person.filter(name="Neville Longbottom").limit(1)
        )

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        harry.friends += [neville]

        self.assertEqual(harry.friends, {hermione, ron, neville})
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {
                f: (
                    f.__linkprops__.opinion
                    if hasattr(f.__linkprops__, "opinion")
                    else None
                )
                for f in harry.friends
            },
            {hermione: "smart", ron: "reliable", neville: None},
        )

    def test_link_set_model_fresh_multi_link_with_props_op_iadd_03(self):
        # Operator iadd new item without id
        from gel._internal._qbmodel._abstract._link_set import LinkWithPropsSet
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        luna = default.Person(name="Luna Lovegood")

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        harry.friends += [luna]

        # Compare to list, since there are unhashable items
        self.assertEqual(harry.friends, [hermione, ron, luna])
        typing_extensions.assert_type(
            harry.friends,
            LinkWithPropsSet[
                default.Person.__links__.friends,
                default.Person,
            ],
        )
        self.assertEqual(
            {
                f.name: (
                    f.__linkprops__.opinion
                    if hasattr(f.__linkprops__, "opinion")
                    else None
                )
                for f in harry.friends
            },
            {
                "Hermione Granger": "smart",
                "Ron Weasley": "reliable",
                "Luna Lovegood": None,
            },
        )

    @tb.skip_typecheck  # mypy seems to be checking - instead of -=
    def test_link_set_model_fresh_multi_link_with_props_op_isub_01(self):
        # Operator isub existing item
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        harry.friends -= [ron]

        self.assertEqual(harry.friends, {hermione})
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart"},
        )

    @tb.skip_typecheck  # mypy seems to be checking - instead of -=
    def test_link_set_model_fresh_multi_link_with_props_op_isub_02(self):
        # Operator isub new item with id
        from models.link_set import default

        hermione = self.client.query_required_single(
            default.Person.filter(name="Hermione Granger").limit(1)
        )
        ron = self.client.query_required_single(
            default.Person.filter(name="Ron Weasley").limit(1)
        )
        neville = self.client.query_required_single(
            default.Person.filter(name="Neville Longbottom").limit(1)
        )

        harry = default.Person(
            name="Harry Potter",
            friends=[
                default.Person.friends.link(hermione, opinion="smart"),
                default.Person.friends.link(ron, opinion="reliable"),
            ],
        )

        harry.friends -= [neville]

        self.assertEqual(harry.friends, {hermione, ron})
        self.assertEqual(
            {f: f.__linkprops__.opinion for f in harry.friends},
            {hermione: "smart", ron: "reliable"},
        )
