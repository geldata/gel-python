#
# This source file is part of the Gel open source project.
#
# Copyright 2024-present MagicStack Inc. and the Gel authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import unittest
import uuid
from gel import _testbase as tb
from gel.ai.vectorstore import (
    VectorStore,
    BaseEmbeddingModel,
    Item,
    Record,
    SearchResult,
)
from gel.ai.metadata_filter import (
    MetadataFilter,
    CompositeFilter,
    FilterOperator,
)


# records to be reused in tests
records = [
    Item(
        text="""EdgeQL is a next-generation query language designed
                to match SQL in power and surpass it in terms of clarity,
                brevity, and intuitiveness. It's used to query the database,
                insert/update/delete data, modify/introspect the schema,
                manage transactions, and more.""",
        metadata={"category": "edgeql"},
    ),
    Item(
        text="""Gel schemas are declared using SDL (Gel's
                Schema Definition Language). Your schema is defined inside
                .esdl files. It's common to define your entire schema in a
                single file called default.esdl, but you can split it across
                multiple files if you wish.""",
        metadata={"category": "schema"},
    ),
    Item(
        text="""Object types can contain computed properties and
                links. Computed properties and links are not persisted in the
                database. Instead, they are evaluated on the fly whenever
                that field is queried""",
        metadata={"category": "schema"},
    ),
]


class MockEmbeddingModel(BaseEmbeddingModel):
    """Mocked embedding model returns fixed embeddings."""

    def __call__(self, item):
        return [0.1] * 1536

    @property
    def dimensions(self):
        return 1536

    @property
    def target_type(self):
        return str


class TestAIVectorStore(tb.SyncQueryTestCase):
    VECTORSTORE_VER = None

    SCHEMA = os.path.join(
        os.path.dirname(__file__), "schema", "vectorstore.esdl"
    )

    SETUP = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.VECTORSTORE_VER = cls.client.query_single(
            """
            select assert_single((
              select sys::ExtensionPackage filter .name = 'vectorstore'
            )).version
        """
        )

        if cls.VECTORSTORE_VER is None:
            raise unittest.SkipTest("feature not implemented")

        cls.client.execute(
            """
            create extension pgvector;
            create extension vectorstore;
        """
        )

    @classmethod
    def tearDownClass(cls):
        try:
            cls.client.execute(
                """
                drop extension vectorstore;
                drop extension pgvector;
            """
            )
        finally:
            super().tearDownClass()

    def setUp(self):
        super().setUp()
        self.vectorstore = VectorStore(
            embedding_model=MockEmbeddingModel(),
            client_config={
                "host": "localhost",
                "port": 5656,
                "user": "edgedb",
                "database": "main",
                "tls_security": "insecure",
            },
        )

    def tearDown(self):
        try:
            self._clean_vectorstore()
        finally:
            if hasattr(self, "vectorstore"):
                self.vectorstore.gel_client.close()
            super().tearDown()

    def _clean_vectorstore(self):
        """Helper method to remove all records from the vectorstore."""

        self.vectorstore.gel_client.execute(
            f"""
            delete {self.vectorstore.record_type}
            filter .collection = <str>$collection_name;
            """,
            collection_name=self.vectorstore.collection_name,
        )

    # will be used in tests for comparing embeddings
    def assertListAlmostEqual(self, first, second, places=7):
        """Assert that two lists of floats are almost equal."""
        self.assertEqual(len(first), len(second))
        for a, b in zip(first, second):
            self.assertAlmostEqual(a, b, places=places)

    def test_add_get_and_delete(self):
        text = """Gel is an open-source database engineered to advance SQL
        into a sophisticated graph data model, supporting composable
        hierarchical queries and accelerated development cycles."""

        # insert a record
        ids = self.vectorstore.add_items(Item(text=text))
        record_id = ids[0]
        self.assertIsNotNone(ids)
        self.assertEqual(len(ids), 1)
        self.assertIsInstance(ids[0], uuid.UUID)

        # verify the inserted record
        records = self.vectorstore.get(record_id)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].id, record_id)
        self.assertIsInstance(records[0], Record)

        # delete the record
        deleted_records_ids = self.vectorstore.delete(record_id)
        self.assertEqual(deleted_records_ids[0], record_id)
        self.assertIsInstance(deleted_records_ids[0], uuid.UUID)

        # verify that the record is deleted
        records = self.vectorstore.get(record_id)
        self.assertEqual(len(records), 0)

    def test_add_multiple(self):
        ids = self.vectorstore.add_items(*records)
        self.assertEqual(len(ids), 3)

    def test_search_no_filters(self):
        self.vectorstore.add_items(*records)

        query = "Tell me about edgeql"
        results = self.vectorstore.search_by_item(item=query, limit=2)

        self.assertEqual(len(results), 2)
        # since we're using a mock embedding model that returns the same vector
        # for all inputs, the results will be ordered by insertion order
        for result in results:
            self.assertIsInstance(result, SearchResult)
            self.assertIsNotNone(result.text)
            self.assertIsNotNone(result.cosine_similarity)
            self.assertIsNotNone(result.metadata)

    def test_search_with_filters(self):
        self.vectorstore.add_items(*records)

        filters = CompositeFilter(
            filters=[
                MetadataFilter(
                    key="category", operator=FilterOperator.EQ, value="schema"
                )
            ]
        )

        query = "How do I use computed properties?"
        results = self.vectorstore.search_by_item(
            item=query, filters=filters, limit=3
        )

        self.assertEqual(len(results), 2)
        # verify all results are from the schema category
        for result in results:
            self.assertIsInstance(result, SearchResult)
            self.assertEqual(result.metadata["category"], "schema")

    def test_update_vector(self):
        # insert a record
        ids = self.vectorstore.add(Record(embedding=[0.1] * 1536))
        record_id = ids[0]

        # verify the inserted record
        record = self.vectorstore.get(record_id)[0]
        self.assertIsInstance(record, Record)
        self.assertIsNone(record.metadata)
        self.assertIsNone(record.text)
        self.assertListAlmostEqual(record.embedding, [0.1] * 1536)

        # update just metadata
        new_metadata = {"test": "test"}
        updated_id = self.vectorstore.update(id=record_id, metadata=new_metadata)
        self.assertEqual(updated_id, record_id)

        # verify the updated record
        record = self.vectorstore.get(record_id)[0]
        self.assertIsNone(record.text)
        self.assertEqual(record.metadata, new_metadata)

        # update both text & embedding
        new_text = "Update text content and embedding"
        new_embedding = [0.0] * 1536
        self.vectorstore.update(id=record_id, text=new_text, embedding=new_embedding)

        # verify the updated record
        record = self.vectorstore.get(record_id)[0]
        self.assertEqual(record.metadata, new_metadata)
        self.assertEqual(record.text, new_text)
        self.assertListAlmostEqual(record.embedding, new_embedding)

        # update just text: embedding should be auto-generated
        new_text = "Update just text content"
        self.vectorstore.update(id=record_id, text=new_text)

        # verify the update
        record = self.vectorstore.get(record_id)[0]
        self.assertEqual(record.text, new_text)
        self.assertEqual(record.metadata, new_metadata)

        # remove text and metadata
        self.vectorstore.update(id=record_id, text=None, metadata=None)

        # verify the update
        record = self.vectorstore.get(record_id)[0]
        self.assertIsNone(record.text)
        self.assertIsNone(record.metadata)

    def test_update_nonexistent_record(self):
        fake_id = uuid.uuid4()
        updated = self.vectorstore.update(id=fake_id, text="This shouldn't work")
        self.assertIsNone(updated)

    def test_update_no_fields_specified(self):
        with self.assertRaises(ValueError):
            self.vectorstore.update(id=uuid.uuid4())
