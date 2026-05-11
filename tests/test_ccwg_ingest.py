"""Tests for the Common Crawl Web Graph ingest: parser, host_rev reversal,
and DR-score derivation."""
import os
import sqlite3
import sys
import tempfile

import pytest


@pytest.fixture(scope="module")
def ccwg():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    sys.path.insert(0, root)
    import ingest_ccwg
    return ingest_ccwg


# ── host_rev reversal ────────────────────────────────────────────────


def test_host_rev_simple(ccwg):
    assert ccwg.host_rev_to_domain("com.facebook") == "facebook.com"


def test_host_rev_three_label(ccwg):
    assert ccwg.host_rev_to_domain("uk.co.bbc") == "bbc.co.uk"


def test_host_rev_punycode(ccwg):
    # IDN domains are stored in punycode by CC; reversal is purely structural.
    assert ccwg.host_rev_to_domain("com.xn--mnchen-3ya") == "xn--mnchen-3ya.com"


# ── DR score derivation ──────────────────────────────────────────────


def test_dr_rank_1_is_100(ccwg):
    assert ccwg.derive_dr(1) == 100


def test_dr_rank_total_is_0(ccwg):
    assert ccwg.derive_dr(ccwg.TOTAL_DOMAINS) == 0


def test_dr_top_10_above_85(ccwg):
    # Top 10 domains land around DR 87-90.
    for rank in range(1, 11):
        assert ccwg.derive_dr(rank) >= 85


def test_dr_rank_1000_in_mid_60s(ccwg):
    # Sanity-check that rank ~1k lands in the same DR band as Ahrefs typically
    # gives well-known sites (around DR 60-65).
    assert 58 <= ccwg.derive_dr(1000) <= 68


def test_dr_monotonic(ccwg):
    """Rank goes up → DR goes down."""
    last = 101
    for rank in [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 5_000_000]:
        score = ccwg.derive_dr(rank)
        assert score < last, f"rank {rank} → dr {score} not less than previous {last}"
        last = score


def test_dr_clamped(ccwg):
    assert ccwg.derive_dr(0) == 100        # nonsense rank → clamp to top
    assert ccwg.derive_dr(-5) == 100
    assert ccwg.derive_dr(10**12) == 0     # absurdly large → clamp to bottom


# ── Line parser ──────────────────────────────────────────────────────


def test_parse_line_skips_header(ccwg):
    assert ccwg.parse_line("#harmonicc_pos\t#harmonicc_val\t...") is None


def test_parse_line_skips_blank(ccwg):
    assert ccwg.parse_line("") is None
    assert ccwg.parse_line("\n") is None


def test_parse_line_skips_short(ccwg):
    assert ccwg.parse_line("1\t2.0\t3") is None


def test_parse_line_real_row(ccwg):
    """A literal row from the 2025 CCWG file."""
    row = ccwg.parse_line("2\t3.2672742E7\t3\t0.00895321110515939\tcom.facebook\t3608\n")
    assert row is not None
    domain, h_rank, h_val, pr_rank, pr_val, n_hosts = row
    assert domain == "facebook.com"
    assert h_rank == 2
    assert pr_rank == 3
    assert n_hosts == 3608
    assert abs(h_val - 3.2672742e7) < 1
    assert abs(pr_val - 0.00895321110515939) < 1e-12


def test_parse_line_malformed_returns_none(ccwg):
    assert ccwg.parse_line("not\ta\tnumber\there\tcom.example\t10") is None


# ── End-to-end ingest against a tiny fake gzip ──────────────────────


def test_ingest_writes_filtered_rows(ccwg, tmp_path, monkeypatch):
    """Build a 4-row fake CCWG file, point ingest at it via file://, and
    verify only the top N land in pagerank_cache with correct DR scores."""
    import gzip
    rows = (
        "#harmonicc_pos\t#harmonicc_val\t#pr_pos\t#pr_val\t#host_rev\t#n_hosts\n"
        "1\t3.3e7\t1\t0.014\tcom.google\t15559\n"
        "2\t3.2e7\t2\t0.014\tcom.facebook\t3608\n"
        "3\t3.1e7\t3\t0.013\tcom.twitter\t697\n"
        "4\t3.0e7\t4\t0.012\tcom.linkedin\t702\n"
    ).encode()
    gz_path = tmp_path / "fake.txt.gz"
    with gzip.open(gz_path, "wb") as f:
        f.write(rows)
    db_path = str(tmp_path / "bl.db")
    ccwg.ingest(db_path, limit=2, url=f"file://{gz_path}")

    con = sqlite3.connect(db_path)
    got = con.execute(
        "SELECT domain, harmonic_rank, dr_score FROM pagerank_cache ORDER BY harmonic_rank"
    ).fetchall()
    con.close()
    assert got == [("google.com", 1, 100), ("facebook.com", 2, ccwg.derive_dr(2))]
