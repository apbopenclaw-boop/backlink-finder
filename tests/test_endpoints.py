"""Behaviour tests for the free observability and discovery endpoints."""
import asyncio
import os
import tempfile


def _run(coro):
    return asyncio.run(coro)


def test_parquet_status_with_no_parquet(main_module, monkeypatch, tmp_path):
    # Point PARQUET_DIR at an empty temp dir so _parquet_available is False
    from crawler import _parquet_available
    monkeypatch.setattr("crawler.PARQUET_DIR", str(tmp_path))
    resp = _run(main_module.parquet_status())
    assert resp["available"] is False
    assert resp["edge_bucket_count"] == 0
    assert resp["vertices_parquet_mb"] is None
    assert resp["edges_total_gb"] is None
    assert resp["edges_last_modified_unix"] is None


def test_parquet_status_with_populated_parquet(main_module, monkeypatch, tmp_path):
    # Build a fake parquet layout that satisfies _parquet_available()
    edges_dir = tmp_path / "edges"
    edges_dir.mkdir()
    for i in range(1000):
        (edges_dir / f"bucket={i}.parquet").write_bytes(b"fake parquet content")
    vertices = tmp_path / "vertices.parquet"
    vertices.write_bytes(b"x" * 1024 * 1024)  # 1 MB

    monkeypatch.setattr("crawler.PARQUET_DIR", str(tmp_path))
    resp = _run(main_module.parquet_status())
    assert resp["available"] is True
    assert resp["edge_bucket_count"] == 1000
    assert resp["vertices_parquet_mb"] == 1.0
    assert resp["edges_total_gb"] is not None
    assert resp["edges_last_modified_unix"] is not None


def test_x402_manifest_route_uses_live_env(main_module, monkeypatch):
    """The dynamic .well-known/x402.json must reflect EVM_ADDRESS / FACILITATOR_URL
    from the running process, not a hardcoded value."""
    resp = _run(main_module.x402_manifest())
    assert resp["x402Version"] == 2
    assert resp["payment"]["payTo"] == os.environ["EVM_ADDRESS"]
    # endpoints' payTo must match too
    paid = [e for e in resp["endpoints"] if e["accepts"]]
    for e in paid:
        assert e["accepts"][0]["payTo"] == os.environ["EVM_ADDRESS"]


def test_x402_manifest_paid_endpoints_are_consistent_with_catalog(main_module):
    resp = _run(main_module.x402_manifest())
    catalog_paid = {
        e["path"]: e["amount_atomic"]
        for e in main_module.ENDPOINT_CATALOG
        if e["amount_atomic"] is not None
    }
    manifest_paid = {
        e["path"]: e["accepts"][0]["amount"]
        for e in resp["endpoints"]
        if e["accepts"]
    }
    assert manifest_paid == catalog_paid


def test_x402_manifest_includes_parquet_status_as_free(main_module):
    resp = _run(main_module.x402_manifest())
    parquet_status = next(
        (e for e in resp["endpoints"] if e["path"] == "/parquet-status"), None
    )
    assert parquet_status is not None
    assert parquet_status["accepts"] == []


def test_health_endpoint(main_module):
    resp = _run(main_module.health())
    assert resp["status"] == "ok"
    assert "version" in resp
