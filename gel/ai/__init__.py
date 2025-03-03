#
# This source file is part of the Gel open source project.
#
# Copyright 2024-present MagicStack Inc. and the Gel authors.
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

from .types import RAGOptions, ChatParticipantRole, Prompt, QueryContext
from .core import create_rag_client, RAGClient
from .core import create_async_rag_client, AsyncRAGClient
from .vectorstore import (
    create_vstore,
    create_async_vstore,
    Record,
    Vector,
    SearchResult,
    EmbeddingModel,
    AsyncEmbeddingModel,
)

__all__ = [
    "RAGOptions",
    "ChatParticipantRole",
    "Prompt",
    "QueryContext",
    "create_rag_client",
    "RAGClient",
    "create_async_rag_client",
    "AsyncRAGClient",
    "create_vstore",
    "create_async_vstore",
    "Record",
    "Vector",
    "SearchResult",
    "EmbeddingModel",
    "AsyncEmbeddingModel",

]
