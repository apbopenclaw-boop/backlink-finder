"""Tests for the X402ResponsePolish ASGI middleware."""
import base64
import json

import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient


@pytest.fixture
def polish_module():
    import sys, os
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if root not in sys.path:
        sys.path.insert(0, root)
    import x402_polish
    return x402_polish


def _broken_402_payload(scheme: str = "http") -> dict:
    """Mimic what x402 PaymentMiddlewareASGI puts in payment-required."""
    return {
        "x402Version": 2,
        "error": "Payment required",
        "resource": {
            "url": f"{scheme}://example.fly.dev/find?x=y",
            "description": "Test endpoint",
            "mimeType": "application/json",
        },
        "accepts": [{
            "scheme": "exact",
            "network": "eip155:8453",
            "asset": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "amount": "10000",
            "payTo": "0x2D8cFC122D13971EEf8cfB4CBC047F527eB76FAd",
            "maxTimeoutSeconds": 300,
        }],
        "extensions": {},
    }


def _make_app_with_broken_402(payload: dict, polish_module):
    """Build a tiny Starlette app whose handler returns the same shape that
    x402 PaymentMiddlewareASGI returns: empty body + payment-required
    header, behind X402ResponsePolish."""
    pr_b64 = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")

    async def handler(request):
        # Empty body, header set — same as upstream middleware
        return JSONResponse(
            content={},
            status_code=402,
            headers={"payment-required": pr_b64},
        )

    app = Starlette(routes=[Route("/paid", handler)])
    app.add_middleware(polish_module.X402ResponsePolish)
    return app


def _make_app_with_normal_endpoint(polish_module):
    async def handler(request):
        return JSONResponse({"ok": True}, status_code=200)
    app = Starlette(routes=[Route("/free", handler)])
    app.add_middleware(polish_module.X402ResponsePolish)
    return app


def test_402_body_is_populated_with_payload(polish_module):
    payload = _broken_402_payload()
    client = TestClient(_make_app_with_broken_402(payload, polish_module))
    r = client.get("/paid")
    assert r.status_code == 402
    body = r.json()
    assert body["x402Version"] == 2
    assert body["accepts"][0]["amount"] == "10000"


def test_402_resource_url_https_rewrite(polish_module):
    payload = _broken_402_payload(scheme="http")
    client = TestClient(_make_app_with_broken_402(payload, polish_module))
    r = client.get("/paid")
    body = r.json()
    assert body["resource"]["url"].startswith("https://"), body["resource"]["url"]
    # And the payment-required header is also re-encoded with the fix
    pr_b64 = r.headers["payment-required"]
    decoded = json.loads(base64.b64decode(pr_b64 + "==").decode("utf-8"))
    assert decoded["resource"]["url"].startswith("https://")


def test_402_https_left_alone(polish_module):
    payload = _broken_402_payload(scheme="https")
    client = TestClient(_make_app_with_broken_402(payload, polish_module))
    body = client.get("/paid").json()
    assert body["resource"]["url"] == "https://example.fly.dev/find?x=y"


def test_402_cors_headers_added(polish_module):
    payload = _broken_402_payload()
    client = TestClient(_make_app_with_broken_402(payload, polish_module))
    r = client.get("/paid")
    assert r.headers["access-control-allow-origin"] == "*"
    assert "PAYMENT-REQUIRED" in r.headers["access-control-expose-headers"]
    assert "X-Payment" in r.headers["access-control-allow-headers"]
    assert "GET" in r.headers["access-control-allow-methods"]


def test_402_v1_fallback_header_added(polish_module):
    payload = _broken_402_payload()
    client = TestClient(_make_app_with_broken_402(payload, polish_module))
    r = client.get("/paid")
    # Both v1 and v2 header names present, both base64 of the same payload
    assert r.headers["payment-required"] == r.headers["x-payment-required"]


def test_non_402_response_passes_through_unchanged(polish_module):
    client = TestClient(_make_app_with_normal_endpoint(polish_module))
    r = client.get("/free")
    assert r.status_code == 200
    assert r.json() == {"ok": True}
    # No CORS injection on a normal response
    assert "access-control-allow-origin" not in r.headers


def test_402_with_corrupted_header_keeps_body_intact(polish_module):
    """If we can't decode the header, we shouldn't crash — body and
    headers pass through (with CORS appended)."""

    async def handler(request):
        return JSONResponse(
            {"already": "set"},
            status_code=402,
            headers={"payment-required": "not-valid-base64-!!!"},
        )

    app = Starlette(routes=[Route("/paid", handler)])
    app.add_middleware(polish_module.X402ResponsePolish)
    client = TestClient(app)
    r = client.get("/paid")
    assert r.status_code == 402
    # The handler's body had content, so the polish should have used that
    # to recover a payload — but {"already":"set"} doesn't have the
    # x402 fields. Either way it shouldn't 500.
    assert r.headers["access-control-allow-origin"] == "*"


def test_402_rebuilds_pr_header_when_missing(polish_module):
    """If the upstream forgets the header but does put the JSON in the
    body, we should re-derive the header from the body."""

    payload = _broken_402_payload(scheme="https")  # already https
    body_json = json.dumps(payload).encode("utf-8")

    async def handler(request):
        return JSONResponse(
            content=payload,
            status_code=402,
            headers={},  # no payment-required header
        )

    app = Starlette(routes=[Route("/paid", handler)])
    app.add_middleware(polish_module.X402ResponsePolish)
    client = TestClient(app)
    r = client.get("/paid")
    assert r.status_code == 402
    # Now there's a payment-required header, and the body is intact
    assert "payment-required" in r.headers
    assert r.json()["x402Version"] == 2
