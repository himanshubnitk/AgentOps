from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

from agentops_api.config import Settings
from agentops_api.errors import ApiError


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _b64decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    iterations = 390_000
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    return f"pbkdf2_sha256${iterations}${_b64encode(salt)}${_b64encode(digest)}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algorithm, iterations, salt, expected = password_hash.split("$", 3)
    except ValueError:
        return False
    if algorithm != "pbkdf2_sha256":
        return False
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode(),
        _b64decode(salt),
        int(iterations),
    )
    return hmac.compare_digest(_b64encode(digest), expected)


def create_token(
    *,
    subject: str,
    token_type: str,
    settings: Settings,
    expires_delta: timedelta,
    claims: dict[str, Any] | None = None,
) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "sub": subject,
        "typ": token_type,
        "exp": int((datetime.now(UTC) + expires_delta).timestamp()),
    }
    if claims:
        payload.update(
            {key: value for key, value in claims.items() if key not in {"sub", "typ", "exp"}}
        )
    signing_input = ".".join(
        [
            _b64encode(json.dumps(header, separators=(",", ":")).encode()),
            _b64encode(json.dumps(payload, separators=(",", ":")).encode()),
        ]
    )
    signature = hmac.new(
        settings.jwt_secret.encode(), signing_input.encode(), hashlib.sha256
    ).digest()
    return f"{signing_input}.{_b64encode(signature)}"


def decode_token(token: str, *, settings: Settings, token_type: str) -> dict[str, Any]:
    try:
        header, payload, signature = token.split(".")
    except ValueError as exc:
        raise ApiError("INVALID_TOKEN", "Invalid token.", 401) from exc

    signing_input = f"{header}.{payload}"
    expected = hmac.new(
        settings.jwt_secret.encode(), signing_input.encode(), hashlib.sha256
    ).digest()
    if not hmac.compare_digest(_b64encode(expected), signature):
        raise ApiError("INVALID_TOKEN", "Invalid token.", 401)

    data = json.loads(_b64decode(payload))
    if not isinstance(data, dict):
        raise ApiError("INVALID_TOKEN", "Invalid token.", 401)
    if data.get("typ") != token_type:
        raise ApiError("INVALID_TOKEN", "Invalid token type.", 401)
    if int(data.get("exp", 0)) < int(datetime.now(UTC).timestamp()):
        raise ApiError("TOKEN_EXPIRED", "Token expired.", 401)
    return data
