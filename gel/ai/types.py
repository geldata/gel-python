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

import typing

import dataclasses as dc
import enum


class ChatParticipantRole(enum.Enum):
    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"
    TOOL = "tool"


class Custom(typing.TypedDict):
    role: ChatParticipantRole
    content: str


class Prompt:
    name: typing.Optional[str]
    id: typing.Optional[str]
    custom: typing.Optional[typing.List[Custom]]


@dc.dataclass
class RAGOptions:
    model: str
    prompt: typing.Optional[Prompt] = None

    def derive(self, kwargs):
        return RAGOptions(**{**dc.asdict(self), **kwargs})


@dc.dataclass
class QueryContext:
    query: str = ""
    variables: typing.Optional[typing.Dict[str, typing.Any]] = None
    globals: typing.Optional[typing.Dict[str, typing.Any]] = None
    max_object_count: typing.Optional[int] = None

    def derive(self, kwargs):
        return QueryContext(**{**dc.asdict(self), **kwargs})


@dc.dataclass
class RAGRequest:
    model: str
    prompt: typing.Optional[Prompt]
    context: QueryContext
    query: str
    stream: typing.Optional[bool]

    def to_httpx_request(self) -> typing.Dict[str, typing.Any]:
        return dict(
            url="/rag",
            headers={
                "Content-Type": "application/json",
                "Accept": (
                    "text/event-stream" if self.stream else "application/json"
                ),
            },
            json=dc.asdict(self),
        )
