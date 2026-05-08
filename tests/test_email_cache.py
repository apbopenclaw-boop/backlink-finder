"""Tests for /find caching behavior, including negative caching."""
import asyncio
import os
import sys
import tempfile

import pytest


@pytest.fixture(scope="module")
def main_module():
    """Import main.py with stubbed env so it doesn't try to talk to anything real."""
    os.environ.setdefault("EVM_ADDRESS", "0xTEST")
    os.environ["EMAIL_DB_PATH"] = tempfile.mkstemp(suffix=".db")[1]
    os.environ.setdefault("MONTHLY_LIMIT", "1000")
    os.environ.setdefault("APOLLO_API_KEY", "stub")

    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    # Starlette's StaticFiles checks the directory exists at import time
    os.makedirs(os.path.join(root, ".well-known"), exist_ok=True)
    os.chdir(root)
    sys.path.insert(0, root)
    import main
    return main


def test_cache_miss_returns_none(main_module):
    assert main_module._check_email_cache("alice", "smith", "example.com") is None


def test_positive_cache_roundtrip(main_module):
    main_module._save_email_lookup("alice", "smith", "example.com", "alice@example.com", "high", "CTO", "Example Inc")
    row = main_module._check_email_cache("alice", "smith", "example.com")
    assert row is not None
    assert row["email"] == "alice@example.com"


def test_no_person_negative_cache(main_module):
    main_module._save_email_lookup("ghost", "person", "void.com", "", "", "", "")
    row = main_module._check_email_cache("ghost", "person", "void.com")
    assert row is not None
    assert not row["email"]
    assert not row["company"]
    assert not row["title"]


def test_person_no_email_negative_cache(main_module):
    main_module._save_email_lookup("bob", "jones", "acme.com", "", "", "VP Sales", "Acme Corp")
    row = main_module._check_email_cache("bob", "jones", "acme.com")
    assert row is not None
    assert not row["email"]
    assert row["company"] == "Acme Corp"


def _fake_request():
    """Build a request object whose state has no payment_payload, exercising
    the no-payer fallback path in find_email."""
    class _State: pass
    class _Req: pass
    r = _Req()
    r.state = _State()
    return r


def test_handler_caches_definitive_negative_then_skips_apollo(main_module):
    from fastapi import HTTPException

    async def fake_apollo(fn, ln, d):
        return {"error": "No match found", "definitive": True}
    main_module._apollo_people_match = fake_apollo

    async def run():
        with pytest.raises(HTTPException) as exc:
            await main_module.find_email(_fake_request(), first_name="Nobody", last_name="Here", domain="missing.com")
        assert exc.value.status_code == 404

        # Negative row was saved
        row = main_module._check_email_cache("Nobody", "Here", "missing.com")
        assert row is not None and not row["email"] and not row["company"]

        # Subsequent call must not invoke Apollo
        called = [0]
        async def counting_apollo(fn, ln, d):
            called[0] += 1
            return {"error": "shouldn't be called"}
        main_module._apollo_people_match = counting_apollo
        with pytest.raises(HTTPException) as exc:
            await main_module.find_email(_fake_request(), first_name="Nobody", last_name="Here", domain="missing.com")
        assert exc.value.status_code == 404
        assert called[0] == 0

    asyncio.run(run())


def test_handler_does_not_cache_transient_errors(main_module):
    from fastapi import HTTPException

    async def transient_apollo(fn, ln, d):
        return {"error": "Apollo returned 500"}  # no "definitive": True
    main_module._apollo_people_match = transient_apollo

    async def run():
        with pytest.raises(HTTPException):
            await main_module.find_email(_fake_request(), first_name="Maybe", last_name="Real", domain="real.com")

    asyncio.run(run())
    assert main_module._check_email_cache("Maybe", "Real", "real.com") is None
