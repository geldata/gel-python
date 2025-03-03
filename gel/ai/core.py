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

from __future__ import annotations
import typing

import gel
import httpx
import httpx_sse

from . import types


def create_rag_client(client: gel.Client, **kwargs) -> RAGClient:
    client.ensure_connected()
    return RAGClient(client, types.RAGOptions(**kwargs))


async def create_async_rag_client(
    client: gel.AsyncIOClient, **kwargs
) -> AsyncRAGClient:
    await client.ensure_connected()
    return AsyncRAGClient(client, types.RAGOptions(**kwargs))


class BaseRAGClient:
    options: types.RAGOptions
    context: types.QueryContext
    client_cls = NotImplemented

    def __init__(
        self,
        client: typing.Union[gel.Client, gel.AsyncIOClient],
        options: types.RAGOptions,
        **kwargs,
    ):
        pool = client._impl
        host, port = pool._working_addr
        params = pool._working_params
        proto = "http" if params.tls_security == "insecure" else "https"
        branch = params.branch
        self.options = options
        self.context = types.QueryContext(**kwargs)
        args = dict(
            base_url=f"{proto}://{host}:{port}/branch/{branch}/ext/ai",
            verify=params.ssl_ctx,
        )
        if params.password is not None:
            args["auth"] = (params.user, params.password)
        elif params.secret_key is not None:
            args["headers"] = {"Authorization": f"Bearer {params.secret_key}"}
        self._init_client(**args)

    def _init_client(self, **kwargs):
        raise NotImplementedError

    def with_config(self, **kwargs) -> typing.Self:
        cls = type(self)
        rv = cls.__new__(cls)
        rv.options = self.options.derive(kwargs)
        rv.context = self.context
        rv.client = self.client
        return rv

    def with_context(self, **kwargs) -> typing.Self:
        cls = type(self)
        rv = cls.__new__(cls)
        rv.options = self.options
        rv.context = self.context.derive(kwargs)
        rv.client = self.client
        return rv

    def _make_rag_request(
        self,
        *,
        message: str,
        context: typing.Optional[types.QueryContext] = None,
        stream: bool,
    ) -> types.RAGRequest:
        if context is None:
            context = self.context
        return types.RAGRequest(
            model=self.options.model,
            prompt=self.options.prompt,
            context=context,
            query=message,
            stream=stream,
        )


class RAGClient(BaseRAGClient):
    client: httpx.Client

    def _init_client(self, **kwargs):
        self.client = httpx.Client(**kwargs)

    def query_rag(
        self, message: str, context: typing.Optional[types.QueryContext] = None
    ) -> str:
        resp = self.client.post(
            **self._make_rag_request(
                context=context,
                message=message,
                stream=False,
            ).to_httpx_request()
        )
        resp.raise_for_status()
        return resp.json()["response"]

    def stream_rag(
        self, message: str, context: typing.Optional[types.QueryContext] = None
    ) -> typing.Iterator[str]:
        with httpx_sse.connect_sse(
            self.client,
            "post",
            **self._make_rag_request(
                context=context,
                message=message,
                stream=True,
            ).to_httpx_request(),
        ) as event_source:
            event_source.response.raise_for_status()
            for sse in event_source.iter_sse():
                yield sse.data

    def generate_embeddings(self, *inputs: str, model: str) -> list[float]:
        resp = self.client.post(
            "/embeddings", json={"input": inputs, "model": model}
        )
        resp.raise_for_status()
        return resp.json()["data"][0]["embedding"]


class AsyncRAGClient(BaseRAGClient):
    client: httpx.AsyncClient

    def _init_client(self, **kwargs):
        self.client = httpx.AsyncClient(**kwargs)

    async def query_rag(
        self, message: str, context: typing.Optional[types.QueryContext] = None
    ) -> str:
        resp = await self.client.post(
            **self._make_rag_request(
                context=context,
                message=message,
                stream=False,
            ).to_httpx_request()
        )
        resp.raise_for_status()
        return resp.json()["response"]

    async def stream_rag(
        self, message: str, context: typing.Optional[types.QueryContext] = None
    ) -> typing.Iterator[str]:
        async with httpx_sse.aconnect_sse(
            self.client,
            "post",
            **self._make_rag_request(
                context=context,
                message=message,
                stream=True,
            ).to_httpx_request(),
        ) as event_source:
            event_source.response.raise_for_status()
            async for sse in event_source.aiter_sse():
                yield sse.data

    async def generate_embeddings(
        self, *inputs: str, model: str
    ) -> list[float]:
        resp = await self.client.post(
            "/embeddings", json={"input": inputs, "model": model}
        )
        resp.raise_for_status()
        return resp.json()["data"][0]["embedding"]
