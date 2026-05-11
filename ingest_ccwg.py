"""Stream-ingest the Common Crawl Web Graph domain-level rank file into
pagerank_cache.

Usage (on the Fly machine):

    DB_PATH=/data/backlinks.db python ingest_ccwg.py [--limit 5000000]

The file is ~2.88 GB compressed (~12 GB uncompressed text) and contains
roughly 109 million domains. We stream + filter to top N by harmonic
centrality rank — the headline metric, equivalent to Ahrefs DR.

Format (tab-separated, with a single header row prefixed by '#'):
    harmonicc_pos  harmonicc_val  pr_pos  pr_val  host_rev  n_hosts

host_rev is reverse-DNS-style at PSL domain level: 'com.facebook' → facebook.com.

DR derivation: dr = round(100 * (1 − log10(rank) / log10(total))).
This is a log-rank mapping that mirrors Ahrefs' published DR shape — top
~10 domains land at 100, rank ~1k at ~62, rank ~1M at ~25.
"""
import argparse
import gzip
import math
import os
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone


CCWG_URL = (
    "https://data.commoncrawl.org/projects/hyperlinkgraph/"
    "cc-main-2025-feb-mar-apr/domain/"
    "cc-main-2025-feb-mar-apr-domain-ranks.txt.gz"
)
# Total domain count used for DR log-scale denominator. From the 2025 release
# (~109M). Worth recomputing when the source file changes substantially, but
# tiny drift here doesn't materially shift DR scores.
TOTAL_DOMAINS = 109_000_000
BATCH_SIZE = 10_000


def host_rev_to_domain(host_rev: str) -> str:
    """'com.facebook' → 'facebook.com'; 'uk.co.bbc' → 'bbc.co.uk'."""
    return ".".join(reversed(host_rev.split(".")))


def derive_dr(rank: int, total: int = TOTAL_DOMAINS) -> int:
    """Log-rank mapping to a 0-100 DR-equivalent score.

    rank=1 → 100. rank=total → 0. Clamped at 0-100.
    """
    if rank <= 0:
        return 100
    if rank >= total:
        return 0
    score = 100.0 * (1.0 - math.log10(rank) / math.log10(total))
    return max(0, min(100, int(round(score))))


def init_pagerank_cache(con: sqlite3.Connection) -> None:
    """Recreate pagerank_cache with the CCWG schema. Safe — table was always
    empty before this ingest existed."""
    con.execute("DROP TABLE IF EXISTS pagerank_cache")
    con.execute("""
        CREATE TABLE pagerank_cache (
            domain         TEXT PRIMARY KEY,
            harmonic_rank  INTEGER NOT NULL,
            harmonic_val   REAL,
            pr_rank        INTEGER,
            pr_val         REAL,
            n_hosts        INTEGER,
            dr_score       INTEGER,
            fetched_at     TEXT NOT NULL
        )
    """)
    con.execute("CREATE INDEX idx_pagerank_harmonic ON pagerank_cache(harmonic_rank)")
    con.commit()


def parse_line(line: str):
    """Return (domain, harmonic_rank, harmonic_val, pr_rank, pr_val, n_hosts)
    or None on header / malformed input."""
    line = line.rstrip("\n")
    if not line or line.startswith("#"):
        return None
    parts = line.split("\t")
    if len(parts) < 6:
        return None
    try:
        h_rank = int(parts[0])
        h_val = float(parts[1])
        pr_rank = int(parts[2])
        pr_val = float(parts[3])
        host_rev = parts[4]
        n_hosts = int(parts[5])
    except (ValueError, IndexError):
        return None
    return (host_rev_to_domain(host_rev), h_rank, h_val, pr_rank, pr_val, n_hosts)


def ingest(db_path: str, limit: int, url: str = CCWG_URL) -> tuple[int, float]:
    """Stream the URL, parse, filter to top `limit`, write to pagerank_cache.

    Returns (rows_inserted, elapsed_seconds).
    """
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    init_pagerank_cache(con)

    now = datetime.now(timezone.utc).isoformat()
    started = time.time()
    rows_inserted = 0
    batch: list[tuple] = []

    print(f"[ccwg] streaming {url}", flush=True)
    req = urllib.request.Request(url, headers={"User-Agent": "bhrefs-ccwg-ingest/1.0"})
    with urllib.request.urlopen(req) as resp:
        with gzip.GzipFile(fileobj=resp) as gz:
            for raw in gz:
                # gzip.GzipFile yields bytes; decode latin-1 for raw safety
                # (host_rev is ASCII or punycode, never multi-byte).
                row = parse_line(raw.decode("latin-1", errors="replace"))
                if row is None:
                    continue
                h_rank = row[1]
                if h_rank > limit:
                    # File is sorted by harmonic_rank ascending — we're done.
                    break
                batch.append((row[0], h_rank, row[2], row[3], row[4], row[5], derive_dr(h_rank), now))
                if len(batch) >= BATCH_SIZE:
                    con.executemany(
                        "INSERT OR REPLACE INTO pagerank_cache "
                        "(domain, harmonic_rank, harmonic_val, pr_rank, pr_val, n_hosts, dr_score, fetched_at) "
                        "VALUES (?,?,?,?,?,?,?,?)",
                        batch,
                    )
                    con.commit()
                    rows_inserted += len(batch)
                    batch.clear()
                    if rows_inserted % 100_000 == 0:
                        elapsed = time.time() - started
                        rate = rows_inserted / elapsed
                        print(f"[ccwg] inserted {rows_inserted:>9,d}  ({rate:>6.0f}/s)", flush=True)

    if batch:
        con.executemany(
            "INSERT OR REPLACE INTO pagerank_cache "
            "(domain, harmonic_rank, harmonic_val, pr_rank, pr_val, n_hosts, dr_score, fetched_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            batch,
        )
        con.commit()
        rows_inserted += len(batch)

    elapsed = time.time() - started
    print(f"[ccwg] done: {rows_inserted:,} rows in {elapsed:.0f}s", flush=True)
    con.close()
    return rows_inserted, elapsed


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    p.add_argument("--limit", type=int, default=5_000_000,
                   help="Top-N domains by harmonic centrality to keep (default 5,000,000).")
    p.add_argument("--db-path", default=os.environ.get("DB_PATH", "/data/backlinks.db"),
                   help="SQLite DB path (defaults to $DB_PATH or /data/backlinks.db).")
    p.add_argument("--url", default=CCWG_URL, help="CCWG domain-ranks .txt.gz URL.")
    args = p.parse_args()
    ingest(args.db_path, args.limit, args.url)
    return 0


if __name__ == "__main__":
    sys.exit(main())
