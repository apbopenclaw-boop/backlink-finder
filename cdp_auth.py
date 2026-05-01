"""CDP API key authentication for x402 facilitator.

Uses PyJWT with Ed25519 signing (the CDP recommended default).
Generates short-lived JWTs with uri claim for each facilitator endpoint.
"""

import base64
import os
import uuid
import time

import jwt
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from x402.http import CreateHeadersAuthProvider

CDP_HOST = "api.cdp.coinbase.com"
CDP_BASE_PATH = "/platform/v2/x402"


def _build_cdp_jwt(key_id: str, private_key: Ed25519PrivateKey, method: str, path: str) -> str:
    """Build a CDP JWT token using Ed25519 (EdDSA) with uri claim."""
    now = int(time.time())
    return jwt.encode(
        {
            "iss": "cdp",
            "sub": key_id,
            "nbf": now,
            "exp": now + 120,
            "uri": f"{method.upper()} {CDP_HOST}{CDP_BASE_PATH}{path}",
        },
        private_key,
        algorithm="EdDSA",
        headers={"kid": key_id, "nonce": uuid.uuid4().hex},
    )


def create_cdp_auth_provider() -> CreateHeadersAuthProvider | None:
    """Create a CDP auth provider from environment variables.

    Requires CDP_API_KEY_NAME and CDP_API_KEY_SECRET env vars.
    Returns None if not configured.
    """
    key_id = os.getenv("CDP_API_KEY_NAME")
    key_secret = os.getenv("CDP_API_KEY_SECRET")

    if not key_id or not key_secret:
        return None

    # Decode Ed25519 private key from base64 (first 32 bytes = seed)
    seed = base64.b64decode(key_secret)[:32]
    private_key = Ed25519PrivateKey.from_private_bytes(seed)

    def _create_headers():
        return {
            "verify": {"Authorization": f"Bearer {_build_cdp_jwt(key_id, private_key, 'POST', '/verify')}"},
            "settle": {"Authorization": f"Bearer {_build_cdp_jwt(key_id, private_key, 'POST', '/settle')}"},
            "supported": {"Authorization": f"Bearer {_build_cdp_jwt(key_id, private_key, 'GET', '/supported')}"},
        }

    return CreateHeadersAuthProvider(create_headers=_create_headers)
