# SPDX-PackageName: gel-python
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright Gel Data Inc. and the contributors.

from __future__ import annotations
from typing import Optional, TYPE_CHECKING

import asyncio
import email.message
import email.parser
import email.policy

import gel
from fastapi_cli.utils.cli import get_rich_toolkit

if TYPE_CHECKING:
    import rich_toolkit


class SMTPServerProtocol(asyncio.Protocol):
    _transport: asyncio.Transport
    _mail_from: Optional[str]
    _rcpt_to: list[str]
    _parser: email.parser.BytesFeedParser
    _in_data: bool = False

    def __init__(self, cli: rich_toolkit.RichToolkit):
        self._cli = cli
        self._buffer = bytearray()
        self._reset()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        assert isinstance(transport, asyncio.Transport)
        self._transport = transport
        transport.write(b"220 localhost Simple SMTP server\r\n")

    def connection_lost(self, exc: Optional[Exception]) -> None:
        del self._transport

    def data_received(self, data: bytes) -> None:
        self._buffer.extend(data)

        while True:
            newline_index = self._buffer.find(b"\r\n")
            if newline_index == -1:
                break

            line = self._buffer[:newline_index]
            self._buffer = self._buffer[newline_index + 2 :]

            self._handle_line(bytes(line))

    def _handle_line(self, line: bytes) -> None:
        if self._in_data:
            if line == b".":  # End of DATA mode
                message = self._parser.close()
                assert isinstance(message, email.message.EmailMessage)
                self._handle_message(message)
                self._reset()
                self._transport.write(b"250 OK\r\n")
            else:
                self._parser.feed(line + b"\r\n")
            return

        # Handle SMTP commands
        upper = line.upper()
        if upper.startswith((b"HELO", b"EHLO")):
            self._transport.write(b"250 Hello\r\n")
        elif upper.startswith(b"MAIL FROM:"):
            self._mail_from = line[10:].strip().decode()
            self._transport.write(b"250 OK\r\n")
        elif upper.startswith(b"RCPT TO:"):
            self._rcpt_to.append(line[8:].strip().decode())
            self._transport.write(b"250 OK\r\n")
        elif upper == b"DATA":
            self._transport.write(b"354 End data with <CR><LF>.<CR><LF>\r\n")
            self._in_data = True
        elif upper == b"QUIT":
            self._transport.write(b"221 Bye\r\n")
            self._transport.close()
        else:
            self._transport.write(b"500 Unrecognized command\r\n")

    def _handle_message(self, message: email.message.EmailMessage) -> None:
        self._cli.print("Received email:", tag="gel")
        self._cli.print(f"  From: {self._mail_from}", tag="gel")
        self._cli.print(f"  To: {', '.join(self._rcpt_to)}", tag="gel")
        self._cli.print(f"  Subject: {message.get('Subject')}", tag="gel")
        for key in message:
            if key.lower().startswith("x-gel-"):
                self._cli.print(f"  {key}: {message[key]}", tag="gel")

    def _reset(self) -> None:
        self._mail_from = None
        self._rcpt_to = []
        self._parser = email.parser.BytesFeedParser(policy=email.policy.SMTP)
        self._in_data = False
        self._buffer.clear()


class SMTPServer:
    _server: asyncio.Server

    async def maybe_start(
        self,
        client: gel.AsyncIOClient,
    ) -> None:
        with get_rich_toolkit() as toolkit:
            try:
                config = await client.query_single("""
                    select cfg::SMTPProviderConfig {
                        host,
                        port,
                        security
                    } filter .name =
                        assert_single(cfg::Config).current_email_provider_name;
                """)
            except gel.QueryError as ex:
                toolkit.print(
                    f"Skipping SMTP server startup due to "
                    f"error reading configuration: {ex}",
                    tag="gel",
                )
                return None

            if config is None:
                toolkit.print(
                    "No SMTP configuration found, "
                    "skipping SMTP server startup",
                    tag="gel",
                )
                return None
            if config.security not in {"PlainText", "STARTTLSOrPlainText"}:
                toolkit.print(
                    "SMTP server only supports security=PlainText or "
                    "STARTTLSOrPlainText, skipping SMTP server startup",
                    tag="gel",
                )
                return None

            try:
                self._server = await asyncio.get_running_loop().create_server(
                    lambda: SMTPServerProtocol(toolkit),
                    host=config.host,
                    port=config.port,
                )
            except Exception as ex:
                toolkit.print(
                    f"Skipping SMTP server startup due to error: {ex}",
                    tag="gel",
                )
            else:
                toolkit.print(
                    f"Started SMTP server on {config.host}:{config.port} "
                    f"for testing purposes.",
                    tag="gel",
                )

    async def stop(self) -> None:
        if hasattr(self, "_server"):
            self._server.close()
            await self._server.wait_closed()
