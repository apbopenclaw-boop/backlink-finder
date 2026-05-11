"""Tests for ENDPOINT_CATALOG and the manifests it drives."""
import asyncio
import os
import sys
import tempfile

import pytest


@pytest.fixture(scope="module")
def main_module():
    os.environ.setdefault("EVM_ADDRESS", "0xTEST")
    os.environ["EMAIL_DB_PATH"] = tempfile.mkstemp(suffix=".db")[1]
    os.environ.setdefault("MONTHLY_LIMIT", "1000")
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    os.chdir(root)
    sys.path.insert(0, root)
    import main
    return main


def test_catalog_has_required_keys(main_module):
    required = {"method", "path", "route_pattern", "description", "price_usd",
                "amount_atomic", "query_params", "output_example"}
    for e in main_module.ENDPOINT_CATALOG:
        missing = required - set(e.keys())
        assert not missing, f"{e['path']} missing {missing}"


def test_paid_routes_have_atomic_amounts_consistent_with_price(main_module):
    for e in main_module.ENDPOINT_CATALOG:
        if e["price_usd"] is None:
            assert e["amount_atomic"] is None
            assert e["route_pattern"] is None
        else:
            assert e["amount_atomic"] is not None
            # USDC has 6 decimals
            usd = float(e["price_usd"].replace("$", ""))
            expected_atomic = str(int(round(usd * 10**6)))
            assert e["amount_atomic"] == expected_atomic, (
                f"{e['path']}: {e['price_usd']} → expected amount_atomic {expected_atomic}, got {e['amount_atomic']}"
            )


def test_routes_dict_built_from_paid_entries(main_module):
    paid = [e for e in main_module.ENDPOINT_CATALOG if e["route_pattern"] is not None]
    assert set(main_module.routes.keys()) == {e["route_pattern"] for e in paid}
    assert len(main_module.routes) == 5


def test_services_manifest_uses_catalog(main_module):
    resp = asyncio.run(main_module.services_manifest())
    paths_in_manifest = [e["path"] for e in resp["endpoints"]]
    paths_in_catalog = [e["path"] for e in main_module.ENDPOINT_CATALOG]
    assert paths_in_manifest == paths_in_catalog


def test_x402_manifest_structure(main_module):
    resp = asyncio.run(main_module.x402_manifest())
    assert resp["x402Version"] == 2
    assert resp["payment"]["payTo"] == os.environ["EVM_ADDRESS"]

    paid = [e for e in resp["endpoints"] if e["accepts"]]
    free = [e for e in resp["endpoints"] if not e["accepts"]]
    assert len(paid) == 5
    assert len(free) == 4

    # Each paid endpoint declares an exact-scheme accept on Base mainnet
    for e in paid:
        a = e["accepts"][0]
        assert a["scheme"] == "exact"
        assert a["network"] == "eip155:8453"
        assert a["asset"] == "USDC"
        assert int(a["amount"]) > 0


def test_llms_txt_lists_all_catalog_entries(main_module):
    resp = asyncio.run(main_module.llms_txt())
    body = resp.body.decode()
    for e in main_module.ENDPOINT_CATALOG:
        assert e["path"] in body, f"missing {e['path']} in /llms.txt"
