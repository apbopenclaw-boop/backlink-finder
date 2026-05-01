"""
On-demand Common Crawl backlink crawler.

Downloads CC hyperlink graph data and queries it with DuckDB.
Results are stored in the SQLite database for instant re-queries.
"""

import os
import re
import sqlite3
import urllib.request
from datetime import datetime, timezone

import duckdb

DEFAULT_RELEASE = "cc-main-2026-jan-feb-mar"
BASE_URL = "https://data.commoncrawl.org/projects/hyperlinkgraph/{release}/domain"
CACHE_DIR = os.getenv("CC_CACHE_DIR", "/data/cc-cache")
PARQUET_DIR = os.getenv("CC_PARQUET_DIR", "/data/cc-parquet")

DOMAIN_RE = re.compile(
    r"^[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?)*$"
)


def validate_domain(domain: str) -> str:
    domain = domain.strip().lower().rstrip(".")
    if not DOMAIN_RE.match(domain) or len(domain) > 253:
        raise ValueError(f"Invalid domain: {domain!r}")
    return domain


def reverse_domain(domain: str) -> str:
    return ".".join(reversed(domain.split(".")))


def download(url: str, dest: str) -> None:
    """Download a file if not already cached."""
    if os.path.exists(dest):
        return
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    urllib.request.urlretrieve(url, dest)


def _parquet_available() -> bool:
    """Check if pre-built parquet files exist (vertices + ≥1000 edge buckets)."""
    if not os.path.isfile(os.path.join(PARQUET_DIR, "vertices.parquet")):
        return False
    edges_dir = os.path.join(PARQUET_DIR, "edges")
    if not os.path.isdir(edges_dir):
        return False
    bucket_count = sum(1 for f in os.listdir(edges_dir) if f.endswith(".parquet"))
    return bucket_count >= 1000


def _query_parquet(domain: str) -> list[dict]:
    """Fast path: query pre-built parquet with bucket pruning."""
    rev_domain = reverse_domain(domain)
    vertices_path = os.path.join(PARQUET_DIR, "vertices.parquet")
    edges_dir = os.path.join(PARQUET_DIR, "edges")

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit = '1GB'")

    # Look up target vertex ID
    row = con.execute(
        "SELECT id FROM read_parquet($1) WHERE rev_domain = $2",
        [vertices_path, rev_domain],
    ).fetchone()
    if not row:
        con.close()
        return []

    target_id = row[0]
    bucket = target_id % 1000
    bucket_path = os.path.join(edges_dir, f"bucket={bucket}.parquet")

    results = con.execute(
        """
        SELECT
            array_to_string(list_reverse(string_split(v.rev_domain, '.')), '.') AS linking_domain,
            v.num_hosts
        FROM read_parquet($1) e
        JOIN read_parquet($2) v ON v.id = e.from_id
        WHERE e.to_id = $3
        ORDER BY v.num_hosts DESC, linking_domain
        """,
        [bucket_path, vertices_path, target_id],
    ).fetchall()
    con.close()

    return [{"domain": r[0], "num_hosts": r[1]} for r in results]


def _query_raw(domain: str, release: str) -> list[dict]:
    """Slow fallback: query raw gzipped text files."""
    rev_domain = reverse_domain(domain)

    cache = os.path.join(CACHE_DIR, release)
    vertices = os.path.join(cache, f"{release}-domain-vertices.txt.gz")
    edges = os.path.join(cache, f"{release}-domain-edges.txt.gz")

    base = BASE_URL.format(release=release)
    download(f"{base}/{release}-domain-vertices.txt.gz", vertices)
    download(f"{base}/{release}-domain-edges.txt.gz", edges)

    con = duckdb.connect(":memory:")
    results = con.execute(
        """
        WITH vertices AS (
            SELECT * FROM read_csv($1, delim='\t', header=false,
                columns={'id':'BIGINT','rev_domain':'VARCHAR','num_hosts':'BIGINT'})
        ),
        target AS (
            SELECT id FROM vertices WHERE rev_domain = $3
        ),
        inbound AS (
            SELECT from_id FROM read_csv($2, delim='\t', header=false,
                columns={'from_id':'BIGINT','to_id':'BIGINT'},
                ignore_errors=true)
            WHERE to_id = (SELECT id FROM target)
        )
        SELECT
            array_to_string(list_reverse(string_split(v.rev_domain, '.')), '.') AS linking_domain,
            v.num_hosts
        FROM inbound i
        JOIN vertices v ON v.id = i.from_id
        ORDER BY v.num_hosts DESC, linking_domain
        """,
        [vertices, edges, rev_domain],
    ).fetchall()
    con.close()

    return [{"domain": row[0], "num_hosts": row[1]} for row in results]


def query_backlinks(domain: str, release: str = DEFAULT_RELEASE) -> list[dict]:
    """Query CC hyperlink graph for all domains linking to target."""
    domain = validate_domain(domain)
    if _parquet_available():
        return _query_parquet(domain)
    return _query_raw(domain, release)


def init_db(db_path: str) -> sqlite3.Connection:
    """Initialize SQLite database with schema."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("""
        CREATE TABLE IF NOT EXISTS crawls (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            target      TEXT NOT NULL,
            release     TEXT NOT NULL,
            crawled_at  TEXT NOT NULL,
            result_count INTEGER NOT NULL,
            UNIQUE(target, release)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS backlinks (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            crawl_id        INTEGER NOT NULL REFERENCES crawls(id),
            linking_domain  TEXT NOT NULL,
            num_hosts       INTEGER NOT NULL,
            page_rank       REAL,
            UNIQUE(crawl_id, linking_domain)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS pagerank_cache (
            domain      TEXT PRIMARY KEY,
            page_rank   REAL,
            fetched_at  TEXT NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS majestic_cache (
            domain          TEXT PRIMARY KEY,
            global_rank     INTEGER,
            tld_rank        INTEGER,
            ref_subnets     INTEGER,
            ref_ips         INTEGER,
            fetched_at      TEXT NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS tranco_cache (
            domain      TEXT PRIMARY KEY,
            tranco_rank INTEGER,
            fetched_at  TEXT NOT NULL
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_backlinks_domain ON backlinks(linking_domain)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_backlinks_crawl ON backlinks(crawl_id)")
    con.commit()
    return con


def store_results(domain: str, release: str, results: list[dict], db_path: str) -> int:
    """Store crawl results in SQLite. Returns crawl_id."""
    con = init_db(db_path)
    now = datetime.now(timezone.utc).isoformat()

    # Replace any existing data for this domain+release
    con.execute(
        "DELETE FROM backlinks WHERE crawl_id IN "
        "(SELECT id FROM crawls WHERE target = ? AND release = ?)",
        (domain, release),
    )
    con.execute(
        "DELETE FROM crawls WHERE target = ? AND release = ?",
        (domain, release),
    )
    cur = con.execute(
        "INSERT INTO crawls (target, release, crawled_at, result_count) VALUES (?, ?, ?, ?)",
        (domain, release, now, len(results)),
    )
    crawl_id = cur.lastrowid
    con.executemany(
        "INSERT INTO backlinks (crawl_id, linking_domain, num_hosts) VALUES (?, ?, ?)",
        [(crawl_id, r["domain"], r["num_hosts"]) for r in results],
    )
    con.commit()
    con.close()
    return crawl_id


def crawl_and_store(domain: str, db_path: str, release: str = DEFAULT_RELEASE) -> tuple[list[dict], int]:
    """Full pipeline: download CC data, query, store results."""
    results = query_backlinks(domain, release)
    crawl_id = store_results(domain, release, results, db_path)
    return results, crawl_id
