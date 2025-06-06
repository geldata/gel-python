#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

"""Helpers for SCRAM authentication."""

import base64
import hashlib
import hmac
import os
import typing

from .saslprep import saslprep


RAW_NONCE_LENGTH = 18

# Per recommendations in RFC 7677.
DEFAULT_SALT_LENGTH = 16
DEFAULT_ITERATIONS = 4096


def generate_salt(length: int = DEFAULT_SALT_LENGTH) -> bytes:
    return os.urandom(length)


def generate_nonce(length: int = RAW_NONCE_LENGTH) -> str:
    return B64(os.urandom(length))


def build_verifier(
    password: str,
    *,
    salt: typing.Optional[bytes] = None,
    iterations: int = DEFAULT_ITERATIONS,
) -> str:
    """Build the SCRAM verifier for the given password.

    Returns a string in the following format:

        "<MECHANISM>$<iterations>:<salt>$<StoredKey>:<ServerKey>"

    The salt and keys are base64-encoded values.
    """
    password = saslprep(password).encode("utf-8")

    if salt is None:
        salt = generate_salt()

    salted_password = get_salted_password(password, salt, iterations)
    client_key = get_client_key(salted_password)
    stored_key = H(client_key)
    server_key = get_server_key(salted_password)

    return (
        f"SCRAM-SHA-256${iterations}:{B64(salt)}$"
        f"{B64(stored_key)}:{B64(server_key)}"
    )


class SCRAMVerifier(typing.NamedTuple):
    mechanism: str
    iterations: int
    salt: bytes
    stored_key: bytes
    server_key: bytes


def parse_verifier(verifier: str) -> SCRAMVerifier:
    parts = verifier.split("$")
    if len(parts) != 3:
        raise ValueError("invalid SCRAM verifier")

    mechanism = parts[0]
    if mechanism != "SCRAM-SHA-256":
        raise ValueError("invalid SCRAM verifier")

    iterations, _, salt = parts[1].partition(":")
    stored_key, _, server_key = parts[2].partition(":")
    if not salt or not server_key:
        raise ValueError("invalid SCRAM verifier")

    try:
        iterations = int(iterations)
    except ValueError:
        raise ValueError("invalid SCRAM verifier") from None

    return SCRAMVerifier(
        mechanism=mechanism,
        iterations=iterations,
        salt=base64.b64decode(salt),
        stored_key=base64.b64decode(stored_key),
        server_key=base64.b64decode(server_key),
    )


def parse_client_first_message(resp: bytes):
    # Relevant bits of RFC 5802:
    #
    #    saslname        = 1*(value-safe-char / "=2C" / "=3D")
    #                      ;; Conforms to <value>.
    #
    #    authzid         = "a=" saslname
    #                      ;; Protocol specific.
    #
    #    cb-name         = 1*(ALPHA / DIGIT / "." / "-")
    #                       ;; See RFC 5056, Section 7.
    #                       ;; E.g., "tls-server-end-point" or
    #                       ;; "tls-unique".
    #
    #    gs2-cbind-flag  = ("p=" cb-name) / "n" / "y"
    #                      ;; "n" -> client doesn't support channel binding.
    #                      ;; "y" -> client does support channel binding
    #                      ;;        but thinks the server does not.
    #                      ;; "p" -> client requires channel binding.
    #                      ;; The selected channel binding follows "p=".
    #
    #    gs2-header      = gs2-cbind-flag "," [ authzid ] ","
    #                      ;; GS2 header for SCRAM
    #                      ;; (the actual GS2 header includes an optional
    #                      ;; flag to indicate that the GSS mechanism is not
    #                      ;; "standard", but since SCRAM is "standard", we
    #                      ;; don't include that flag).
    #
    #    username        = "n=" saslname
    #                      ;; Usernames are prepared using SASLprep.
    #
    #    reserved-mext  = "m=" 1*(value-char)
    #                      ;; Reserved for signaling mandatory extensions.
    #                      ;; The exact syntax will be defined in
    #                      ;; the future.
    #
    #    nonce           = "r=" c-nonce [s-nonce]
    #                      ;; Second part provided by server.
    #
    #    c-nonce         = printable
    #
    #    client-first-message-bare =
    #                      [reserved-mext ","]
    #                      username "," nonce ["," extensions]
    #
    #    client-first-message =
    #                      gs2-header client-first-message-bare

    attrs = resp.split(b",")

    cb_attr = attrs[0]
    if cb_attr == b"y":
        cb = True
    elif cb_attr == b"n":
        cb = False
    elif cb_attr[0:1] == b"p":
        _, _, cb = cb_attr.partition(b"=")
        if not cb:
            raise ValueError("malformed SCRAM message")
    else:
        raise ValueError("malformed SCRAM message")

    authzid_attr = attrs[1]
    if authzid_attr:
        if authzid_attr[0:1] != b"a":
            raise ValueError("malformed SCRAM message")
        _, _, authzid = authzid_attr.partition(b"=")
    else:
        authzid = None

    user_attr = attrs[2]
    if user_attr[0:1] == b"m":
        raise ValueError("unsupported SCRAM extensions in message")
    elif user_attr[0:1] != b"n":
        raise ValueError("malformed SCRAM message")

    _, _, user = user_attr.partition(b"=")

    nonce_attr = attrs[3]
    if nonce_attr[0:1] != b"r":
        raise ValueError("malformed SCRAM message")

    _, _, nonce_bin = nonce_attr.partition(b"=")
    nonce = nonce_bin.decode("ascii")
    if not nonce.isprintable():
        raise ValueError("invalid characters in client nonce")

    # ["," extensions] are ignored

    return len(cb_attr) + 2, cb, authzid, user, nonce


def parse_client_final_message(
    msg: bytes, client_nonce: str, server_nonce: str
):
    # Relevant bits of RFC 5802:
    #
    #   gs2-header       = gs2-cbind-flag "," [ authzid ] ","
    #                       ;; GS2 header for SCRAM
    #                       ;; (the actual GS2 header includes an optional
    #                       ;; flag to indicate that the GSS mechanism is not
    #                       ;; "standard", but since SCRAM is "standard", we
    #                       ;; don't include that flag).
    #
    #   cbind-input     = gs2-header [ cbind-data ]
    #                       ;; cbind-data MUST be present for
    #                       ;; gs2-cbind-flag of "p" and MUST be absent
    #                       ;; for "y" or "n".
    #
    #   channel-binding = "c=" base64
    #                       ;; base64 encoding of cbind-input.
    #
    #   proof           = "p=" base64
    #
    #   client-final-message-without-proof =
    #                       channel-binding "," nonce [","
    #                       extensions]
    #
    #   client-final-message =
    #                       client-final-message-without-proof "," proof

    attrs = msg.split(b",")

    cb_attr = attrs[0]
    if cb_attr[0:1] != b"c":
        raise ValueError("malformed SCRAM message")

    _, _, cb_data = cb_attr.partition(b"=")

    nonce_attr = attrs[1]
    if nonce_attr[0:1] != b"r":
        raise ValueError("malformed SCRAM message")

    _, _, nonce_bin = nonce_attr.partition(b"=")
    nonce = nonce_bin.decode("ascii")

    expected_nonce = f"{client_nonce}{server_nonce}"

    if nonce != expected_nonce:
        raise ValueError(
            "invalid SCRAM client-final message: nonce does not match"
        )

    proof = None

    for attr in attrs[2:]:
        if attr[0:1] == b"p":
            _, _, proof = attr.partition(b"=")
            proof_attr_len = len(attr)
            proof = base64.b64decode(proof)
        elif proof is not None:
            raise ValueError("malformed SCRAM message")

    if proof is None:
        raise ValueError("malformed SCRAM message")

    return cb_data, proof, proof_attr_len + 1


def build_client_first_message(client_nonce: str, username: str) -> str:
    bare = f"n={saslprep(username)},r={client_nonce}"
    return f"n,,{bare}", bare


def build_server_first_message(
    server_nonce: str, client_nonce: str, salt: bytes, iterations: int
) -> str:
    return f"r={client_nonce}{server_nonce},s={B64(salt)},i={iterations}"


def build_auth_message(
    client_first_bare: bytes, server_first: bytes, client_final: bytes
) -> bytes:
    return b"%b,%b,%b" % (client_first_bare, server_first, client_final)


def build_client_final_message(
    password: str,
    salt: bytes,
    iterations: int,
    client_first_bare: bytes,
    server_first: bytes,
    server_nonce: str,
) -> str:
    client_final = f"c=biws,r={server_nonce}"

    AuthMessage = build_auth_message(
        client_first_bare, server_first, client_final.encode("utf-8")
    )

    SaltedPassword = get_salted_password(
        saslprep(password).encode("utf-8"), salt, iterations
    )

    ClientKey = get_client_key(SaltedPassword)
    StoredKey = H(ClientKey)
    ClientSignature = HMAC(StoredKey, AuthMessage)
    ClientProof = XOR(ClientKey, ClientSignature)

    ServerKey = get_server_key(SaltedPassword)
    ServerProof = HMAC(ServerKey, AuthMessage)

    return f"{client_final},p={B64(ClientProof)}", ServerProof


def build_server_final_message(
    client_first_bare: bytes,
    server_first: bytes,
    client_final: bytes,
    server_key: bytes,
) -> str:
    AuthMessage = build_auth_message(
        client_first_bare, server_first, client_final
    )
    ServerSignature = HMAC(server_key, AuthMessage)
    return f"v={B64(ServerSignature)}"


def parse_server_first_message(msg: bytes):
    attrs = msg.split(b",")

    nonce_attr = attrs[0]
    if nonce_attr[0:1] != b"r":
        raise ValueError("malformed SCRAM message")

    _, _, nonce_bin = nonce_attr.partition(b"=")
    nonce = nonce_bin.decode("ascii")
    if not nonce.isprintable():
        raise ValueError("malformed SCRAM message")

    salt_attr = attrs[1]
    if salt_attr[0:1] != b"s":
        raise ValueError("malformed SCRAM message")

    _, _, salt_b64 = salt_attr.partition(b"=")
    salt = base64.b64decode(salt_b64)

    iter_attr = attrs[2]
    if iter_attr[0:1] != b"i":
        raise ValueError("malformed SCRAM message")

    _, _, iterations = iter_attr.partition(b"=")

    try:
        itercount = int(iterations)
    except ValueError:
        raise ValueError("malformed SCRAM message") from None

    return nonce, salt, itercount


def parse_server_final_message(msg: bytes):
    attrs = msg.split(b",")

    nonce_attr = attrs[0]
    if nonce_attr[0:1] != b"v":
        raise ValueError("malformed SCRAM message")

    _, _, signature_b64 = nonce_attr.partition(b"=")
    signature = base64.b64decode(signature_b64)

    return signature


def verify_password(password: bytes, verifier: str) -> bool:
    """Check the given password against a verifier.

    Returns True if the password is OK, False otherwise.
    """

    password = saslprep(password).encode("utf-8")
    v = parse_verifier(verifier)
    salted_password = get_salted_password(password, v.salt, v.iterations)
    computed_key = get_server_key(salted_password)
    return v.server_key == computed_key


def verify_client_proof(
    client_first: bytes,
    server_first: bytes,
    client_final: bytes,
    StoredKey: bytes,
    ClientProof: bytes,
) -> bool:
    AuthMessage = build_auth_message(client_first, server_first, client_final)
    ClientSignature = HMAC(StoredKey, AuthMessage)
    ClientKey = XOR(ClientProof, ClientSignature)
    return H(ClientKey) == StoredKey


def B64(val: bytes) -> str:
    """Return base64-encoded string representation of input binary data."""
    return base64.b64encode(val).decode()


def HMAC(key: bytes, msg: bytes) -> bytes:
    return hmac.new(key, msg, digestmod=hashlib.sha256).digest()


def XOR(a: bytes, b: bytes) -> bytes:
    if len(a) != len(b):
        raise ValueError("scram.XOR received operands of unequal length")
    xint = int.from_bytes(a, "big") ^ int.from_bytes(b, "big")
    return xint.to_bytes(len(a), "big")


def H(s: bytes) -> bytes:
    return hashlib.sha256(s).digest()


def get_salted_password(
    password: bytes, salt: bytes, iterations: int
) -> bytes:
    # U1 := HMAC(str, salt + INT(1))
    H_i = U_i = HMAC(password, salt + b"\x00\x00\x00\x01")

    for _ in range(iterations - 1):
        U_i = HMAC(password, U_i)
        H_i = XOR(H_i, U_i)

    return H_i


def get_client_key(salted_password: bytes) -> bytes:
    return HMAC(salted_password, b"Client Key")


def get_server_key(salted_password: bytes) -> bytes:
    return HMAC(salted_password, b"Server Key")
