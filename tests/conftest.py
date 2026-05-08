"""Shared pytest fixtures for the backlink-finder test suite."""
import os
import sys
import tempfile

import pytest


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(scope="session")
def main_module():
    """Import the FastAPI app with stubbed env so it doesn't talk to anything real.

    Sets EVM_ADDRESS, isolates EMAIL_DB_PATH to a temp file, ensures the
    .well-known/ directory exists (Starlette's StaticFiles checks it eagerly
    at import time on older versions of main.py), then imports `main`.
    """
    os.environ.setdefault("EVM_ADDRESS", "0xTEST0000000000000000000000000000000000")
    os.environ["EMAIL_DB_PATH"] = tempfile.mkstemp(suffix=".db")[1]
    os.environ.setdefault("MONTHLY_LIMIT", "1000")
    # Setting APOLLO_API_KEY makes main.py initialize the email DB schema on import.
    os.environ.setdefault("APOLLO_API_KEY", "stub-for-tests")

    well_known = os.path.join(REPO_ROOT, ".well-known")
    if not os.path.isdir(well_known):
        os.makedirs(well_known, exist_ok=True)

    os.chdir(REPO_ROOT)
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)

    import main
    return main


@pytest.fixture
def crawler_module():
    """Import crawler.py — pure module, no env dependencies."""
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)
    import crawler
    return crawler
