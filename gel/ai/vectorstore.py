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
# Extension VectorStore Binding
# ----------------------------
#
# `VectorStore` is designed to integrate with vector databases following
# LangChain-LlamaIndex conventions. It enables interaction with embedding models
# (both within and outside of Gel) through a simple interface.
#
# This binding does not assume a specific data type, allowing it to support
# text, images, or any other embeddings. For example, CLIP can be wrapped into
# this interface to generate and store image embeddings.

from __future__ import annotations
from typing import (
    Optional,
    TypeVar,
    Generic,
    Union,
    Any,
    List,
    Dict,
    TYPE_CHECKING,
)

import gel
import json
import uuid
import abc
import array
import textwrap
import dataclasses

from gel import quote
from .metadata_filter import (
    get_filter_clause,
    CompositeFilter,
)


if TYPE_CHECKING:
    try:
        import numpy as np
        import numpy.typing as npt

        Vector = Union[List[float], array.array[float], npt.NDArray[np.float32]]
    except ImportError:
        Vector = Union[List[float], array.array[float]]


def create_vstore(client: gel.Client, **kwargs) -> VectorStore:
    """Create a new vector store instance."""

    client.ensure_connected()
    return VectorStore(client, **kwargs)


async def create_async_vstore(
    client: gel.AsyncIOClient, **kwargs
) -> AsyncVectorStore:
    """Create a new async vector store instance."""

    await client.ensure_connected()
    return AsyncVectorStore(client, **kwargs)


BATCH_ADD_QUERY = textwrap.dedent(
    """
    with items := json_array_unpack(<json>$items)
      for item in items union (
          insert {record_type} {{
              collection := <str>$collection_name,
              text := <str>item['text'],
              embedding := <array<float32>>item['embedding'],
              metadata := to_json(<str>item['metadata'])
          }}
      )
    """
)


DELETE_BY_IDS_QUERY = textwrap.dedent(
    """
    delete {record_type}
        filter .id in array_unpack(<array<uuid>>$ids)
        and .collection = <str>$collection_name;
    """
)


SEARCH_QUERY = """
    with collection_records := (
        select {record_type}
        filter .collection = <str>$collection_name
        and exists(.embedding)
    )
    select collection_records {{
        id,
        text,
        embedding,
        metadata,
        cosine_similarity := 1 - ext::pgvector::cosine_distance(
            .embedding, <ext::pgvector::vector>$query_embedding),
    }}
    {filter_expression}
    order by .cosine_similarity desc empty last
    limit <optional int64>$limit;
    """


GET_BY_IDS_QUERY = """
    select {record_type} {{
        id, 
        text,
        embedding,
        metadata,
    }}
    filter .id in array_unpack(<array<uuid>>$ids)
    and .collection = <str>$collection_name;
    """.strip()


UPDATE_QUERY = """
    with updates := array_unpack(<array<str>>$updates)
    update {record_type}
    filter .id = <uuid>$id and .collection = <str>$collection_name
    set {{
        text := <optional str>$text if 'text' in updates else .text,
        embedding := <optional ext::pgvector::vector>$embedding 
            if 'embedding' in updates 
            else .embedding,
        metadata := to_json(<optional str>$metadata)
            if 'metadata' in updates 
            else .metadata,
    }};
    """.strip()


@dataclasses.dataclass
class Vector:
    """A vector (embeddings) along with its text and metadata.
    If id is None, it is considered a new record to be inserted. Use
    this when you have pre-calculated embeddings.
    """

    id: Optional[uuid.UUID] = None
    embedding: Optional[Vector] = None
    text: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclasses.dataclass
class Record:
    """A record to be inserted into the vector store, where its
    embedding will be automatically generated in the vectorstore.
    Use this when you expect the vectorstore to generate the
    embeddings using the embedding model you provided.
    """

    text: str
    metadata: Optional[Dict[str, Any]] = None

    def to_vector(self, embedding_model: EmbeddingModel) -> Vector:
        """Convert this item to a Record using the provided embedding model."""

        return Vector(
            text=self.text,
            embedding=embedding_model.generate(self.text),
            metadata=self.metadata,
        )

    async def ato_vector(self, embedding_model: AsyncEmbeddingModel) -> Vector:
        """
        Convert this item to a Vector using the provided async embedding model.
        """

        return Vector(
            text=self.text,
            embedding=await embedding_model.generate(self.text),
            metadata=self.metadata,
        )


@dataclasses.dataclass
class SearchResult:
    """A search result from the vector store."""

    id: uuid.UUID
    text: Optional[str] = None
    embedding: Optional[Vector] = None
    metadata: Optional[Dict[str, Any]] = None
    cosine_similarity: float = 0.0


def _serialize_metadata(metadata: Optional[Dict[str, Any]]) -> Optional[str]:
    """Helper to serialize metadata to JSON string."""

    return json.dumps(metadata) if metadata else None


def _deserialize_metadata(metadata: Optional[str]) -> Optional[Dict[str, Any]]:
    """Helper to deserialize metadata from JSON string."""

    return json.loads(metadata) if metadata else None


_sentinel = object()

T = TypeVar("T")


class EmbeddingModel(abc.ABC, Generic[T]):
    """
    Embedding model. Any embedding model used with `VectorStore`
    must implement this interface. The model is expected to convert input data
    (text, images, etc.) into a numerical vector representation.
    """

    @abc.abstractmethod
    def generate(self, item: T) -> Vector:
        """Convert an input item into a list of floating-point values."""
        ...


class BaseVectorStore(Generic[T]):
    """
    Base class for vector store.
    T: The type of items that can be embedded (e.g., str for text...)
    """

    def __init__(
        self,
        client: Union[gel.Client, gel.AsyncIOClient],
        collection_name: str = "default",
        record_type: str = "ext::vectorstore::DefaultRecord",
    ):
        """Initialize a new vector store instance.

        Args:
            client (Union[gel.Client, gel.AsyncIOClient]): Gel client instance,
            collection_name (str): Collection name,
            record_type (str): Schema type (table name) for storing records,
            embedding_model (EmbeddingModel[T]): Embedding model.
        """

        self.client = client
        self.collection_name = collection_name
        self.record_type = record_type


class VectorStore(BaseVectorStore[T]):
    """
    A framework-agnostic interface for interacting with Gel's ext::vectorstore.

    This class provides methods for storing, retrieving, and searching
    vector embeddings. It follows vector database conventions and supports
    different embedding models.
    """

    def __init__(
        self,
        embedding_model: Optional[EmbeddingModel] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.embedding_model = embedding_model

    def add_records(self, *records: Record) -> List[uuid.UUID]:
        """
        Add multiple items to the vector store in a single transaction.
        Embeddinsg will be generated and stored for all items.

        Args:
            *records (Record): Records to insert. Each contains:
                - text (str): The text content to be embedded,
                - metadata (Optional[Dict[str, Any]]): Additional data to store.

        Returns:
            List[uuid.UUID]: List of database record IDs for the inserted items.
        """

        if not self.embedding_model:
            raise ValueError("Embedding model is not set.")

        vectors = [record.to_vector(self.embedding_model) for record in records]
        return self.add_vectors(*vectors)

    def add_vectors(self, *vectors: Vector) -> List[uuid.UUID]:
        """Add pre-computed vector embeddings to the store.

        Use this method when you have already generated embeddings and want to
        store them directly without re-computing them.

        Args:
            *vectors (Vector): Vectors to insert. Each contains:
                - embedding (Optional[Vector]): Pre-computed embeddings,
                - text (Optional[str]): Original text content,
                - metadata (Optional[Dict[str, Any]]): Additional data to store.

        Returns:
            List[uuid.UUID]: List of database record IDs for the inserted items.
        """

        results = self.client.query(
            query=BATCH_ADD_QUERY.format(
                record_type=quote.quote_ident(self.record_type)
            ),
            collection_name=self.collection_name,
            items=json.dumps(
                [
                    {
                        "text": vector.text,
                        "embedding": vector.embedding,
                        "metadata": _serialize_metadata(vector.metadata),
                    }
                    for vector in vectors
                ]
            ),
        )
        return [result.id for result in results]

    def delete(self, *ids: uuid.UUID) -> List[uuid.UUID]:
        """Delete records from the vector store by their IDs.

        Args:
            *ids (uuid.UUID): Ids of records to delete.

        Returns:
            List[uuid.UUID]: List of deleted record IDs.
        """

        results = self.client.query(
            query=DELETE_BY_IDS_QUERY.format(
                record_type=quote.quote_ident(self.record_type)
            ),
            collection_name=self.collection_name,
            ids=ids,
        )
        return [result.id for result in results]

    def get_by_ids(self, *ids: uuid.UUID) -> List[Vector]:
        """Retrieve specific vectors by their IDs.

        Args:
            *ids (uuid.UUID): IDs of vectors to retrieve.

        Returns:
            List[Vector]: List of retrieved vectors. Each contains:
                - id (uuid.UUID): Vector's unique identifier,
                - text (Optional[str]): Text content,
                - embedding (Optional[Vector]): Stored vector embedding,
                - metadata (Optional[Dict[str, Any]]): Any associated metadata.
        """

        results = self.client.query(
            query=GET_BY_IDS_QUERY.format(
                record_type=quote.quote_ident(self.record_type)
            ),
            collection_name=self.collection_name,
            ids=ids,
        )

        return [
            Record(
                id=result.id,
                text=result.text,
                embedding=result.embedding,
                metadata=_deserialize_metadata(result.metadata),
            )
            for result in results
        ]

    def search_by_record(
        self,
        item: T,
        filters: Optional[CompositeFilter] = None,
        limit: Optional[int] = 4,
    ) -> List[SearchResult]:
        """Search for similar vectors in the vector store.

        This method:
        1. Generates an embedding for the input item,
        2. Finds items with similar embeddings,
        3. Optionally filters results based on metadata,
        4. Returns the most similar items up to the specified limit.

        Args:
            item (T): The query item to find similar matches for.
              Must be compatible with the embedding model's type.
            filters (Optional[CompositeFilter]): Metadata-based filters to use.
            limit (Optional[int]): Max number of results to return.
              Defaults to 4.

        Returns:
            List[SearchResult]: List of similar items, ordered by similarity.
                Each contains:
                - id (uuid.UUID): Item's unique identifier,
                - text (Optional[str]): Text content,
                - embedding (Vector): Stored vector embedding,
                - metadata (Optional[Dict[str, Any]]): Any associated metadata,
                - cosine_similarity (float): Similarity score.
        """

        vector = self.embedding_model.generate(item)
        filter_expression = (
            f"filter {get_filter_clause(filters)}" if filters else ""
        )
        return self.search_by_vector(
            vector=vector, filter_expression=filter_expression, limit=limit
        )

    def search_by_vector(
        self,
        vector: Vector,
        filter_expression: str = "",
        limit: Optional[int] = 4,
    ) -> List[SearchResult]:
        """Search using a pre-computed vector embedding.

        Useful when you have already computed the embedding or want to search
        with a modified/combined embedding vector.

        Args:
            vector (Vector): The query embedding to search with.
              Must match the dimensionality of stored embeddings.
            filter_expression (str): Filter expression for metadata filtering.
            limit (Optional[int]): Max num of results to return. Defaults to 4.

        Returns:
            List[SearchResult]: List of similar items, ordered by similarity.
                Each contains:
                - id (uuid.UUID): Item's unique identifier,
                - text (Optional[str]): Text content,
                - embedding (Vector): Stored vector embedding,
                - metadata (Optional[Dict[str, Any]]): Any associated metadata,
                - cosine_similarity (float): Similarity score.
        """

        results = self.client.query(
            query=SEARCH_QUERY.format(
                record_type=quote.quote_ident(self.record_type),
                filter_expression=filter_expression,
            ),
            collection_name=self.collection_name,
            query_embedding=vector,
            limit=limit,
        )
        return [
            SearchResult(
                id=result.id,
                text=result.text,
                embedding=list(result.embedding) if result.embedding else None,
                metadata=_deserialize_metadata(result.metadata),
                cosine_similarity=result.cosine_similarity,
            )
            for result in results
        ]

    def update_record(
        self,
        id: uuid.UUID,
        text: Union[str, None, object] = _sentinel,
        embedding: Union[Vector, None, object] = _sentinel,
        metadata: Union[Dict[str, Any], None, object] = _sentinel,
    ) -> Optional[uuid.UUID]:
        """Update an existing item in the vector store.

        Only specified fields will be updated. If text is provided
        but not embedding, a new embedding will be automatically
        generated using the embedding model you provided.

        Args:
            - id (uuid.UUID): ID of the item to update,
            - text (Optional[str]): New text content. If provided without
              embedding, a new embedding will be generated.
            - embedding (Optional[Vector]): New vector embedding.
            - metadata (Optional[Dict[str, Any]]): New metadata to store.
              Completely replaces existing metadata.
        Returns:
            Optional[uuid.UUID]: Updated record's ID if found and updated.
        Raises:
            ValueError: If no fields are specified for update.
        """

        updates = []

        if text is not _sentinel:
            updates.append("text")
        if embedding is not _sentinel:
            updates.append("embedding")
        if metadata is not _sentinel:
            updates.append("metadata")

        if not updates:
            raise ValueError("No fields specified for update.")

        if (
            "text" in updates
            and text is not None
            and "embedding" not in updates
        ):
            updates.append("embedding")
            embedding = self.embedding_model.generate(text)

        result = self.client.query_single(
            query=UPDATE_QUERY.format(
                record_type=quote.quote_ident(self.record_type)
            ),
            collection_name=self.collection_name,
            id=id,
            updates=list(updates),
            text=text if text is not _sentinel else None,
            embedding=embedding if embedding is not _sentinel else None,
            metadata=(
                _serialize_metadata(metadata)
                if metadata is not _sentinel
                else None
            ),
        )
        return result.id if result else None


class AsyncEmbeddingModel(abc.ABC, Generic[T]):
    """
    Async embedding model. Any async embedding model used with `VectorStore`
    must implement this interface. The model is expected to convert input data
    (text, images, etc.) into a numerical vector representation.
    """

    @abc.abstractmethod
    async def generate(self, item: T) -> Vector: ...


class AsyncVectorStore(BaseVectorStore[T]):
    """
    A framework-agnostic interface for interacting with Gel's ext::vectorstore.

    This class provides methods for storing, retrieving, and searching
    vector embeddings. It follows vector database conventions and supports
    different embedding models.
    """

    def __init__(
        self,
        embedding_model: Optional[AsyncEmbeddingModel] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.embedding_model = embedding_model

    async def add_records(self, *records: Record) -> List[uuid.UUID]:
        if not self.embedding_model:
            raise ValueError("Embedding model is not set")

        vectors = [
            record.ato_vector(self.embedding_model) for record in records
        ]
        return self.add_vectors(*vectors)

    async def add_vectors(self, *vectors: Vector) -> List[uuid.UUID]:
        results = await self.client.query(
            query=BATCH_ADD_QUERY.format(
                record_type=quote.quote_ident(self.record_type)
            ),
            collection_name=self.collection_name,
            items=json.dumps(
                [
                    {
                        "text": vector.text,
                        "embedding": vector.embedding,
                        "metadata": _serialize_metadata(vector.metadata),
                    }
                    for vector in vectors
                ]
            ),
        )
        return [result.id for result in results]

    async def delete(self, *ids: uuid.UUID) -> List[uuid.UUID]:
        results = await self.client.query(
            query=DELETE_BY_IDS_QUERY.format(
                record_type=quote.quote_ident(self.record_type)
            ),
            collection_name=self.collection_name,
            ids=ids,
        )
        return [result.id for result in results]

    async def get_by_ids(self, *ids: uuid.UUID) -> List[Record]:
        results = await self.client.query(
            query=GET_BY_IDS_QUERY.format(
                record_type=quote.quote_ident(self.record_type)
            ),
            collection_name=self.collection_name,
            ids=ids,
        )

        return [
            Record(
                id=result.id,
                text=result.text,
                embedding=result.embedding,
                metadata=_deserialize_metadata(result.metadata),
            )
            for result in results
        ]

    async def search_by_record(
        self,
        item: T,
        filters: Optional[CompositeFilter] = None,
        limit: Optional[int] = 4,
    ) -> List[SearchResult]:
        vector = await self.embedding_model.generate(item)
        filter_expression = (
            f"filter {get_filter_clause(filters)}" if filters else ""
        )
        return self.search_by_vector(
            vector=vector, filter_expression=filter_expression, limit=limit
        )

    async def search_by_vector(
        self,
        vector: Vector,
        filter_expression: str = "",
        limit: Optional[int] = 4,
    ) -> List[SearchResult]:
        results = await self.client.query(
            query=SEARCH_QUERY.format(
                record_type=quote.quote_ident(self.record_type),
                filter_expression=filter_expression,
            ),
            collection_name=self.collection_name,
            query_embedding=vector,
            limit=limit,
        )
        return [
            SearchResult(
                id=result.id,
                text=result.text,
                embedding=list(result.embedding) if result.embedding else None,
                metadata=_deserialize_metadata(result.metadata),
                cosine_similarity=result.cosine_similarity,
            )
            for result in results
        ]

    async def update_record(
        self,
        id: uuid.UUID,
        *,
        text: Union[str, None, object] = _sentinel,
        embedding: Union[Vector, None, object] = _sentinel,
        metadata: Union[Dict[str, Any], None, object] = _sentinel,
    ) -> Optional[uuid.UUID]:
        updates = []

        if text is not _sentinel:
            updates.append("text")
        if embedding is not _sentinel:
            updates.append("embedding")
        if metadata is not _sentinel:
            updates.append("metadata")

        if not updates:
            raise ValueError("No fields specified for update.")

        if (
            "text" in updates
            and text is not None
            and "embedding" not in updates
        ):
            updates.append("embedding")
            embedding = await self.embedding_model.generate(text)

        result = await self.client.query_single(
            query=UPDATE_QUERY.format(
                record_type=quote.quote_ident(self.record_type)
            ),
            collection_name=self.collection_name,
            id=id,
            updates=list(updates),
            text=text if text is not _sentinel else None,
            embedding=embedding if embedding is not _sentinel else None,
            metadata=(
                _serialize_metadata(metadata)
                if metadata is not _sentinel
                else None
            ),
        )
        return result.id if result else None
