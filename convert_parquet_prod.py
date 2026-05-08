#!/usr/bin/env python3
"""Convert Common Crawl hyperlink graph to partitioned parquet for fast queries.

Two-phase approach designed to survive container restarts and fit in 4 GB RAM:

  Phase 1: gzip → single edges_raw.parquet (one streaming gzip read)
  Phase 2: read raw parquet, partition into 1000 buckets by (to_id % 1000)

Configuration (env vars, all optional):

  CC_RELEASE          source release name (default: cc-main-2026-jan-feb-mar)
  CC_CACHE_DIR        where source .gz files live  (default: /data/cc-cache)
  CC_PARQUET_DIR      where output parquet files go (default: /data/cc-parquet)
  CC_TMP_DIR          DuckDB spill dir              (default: /data/tmp_duckdb)
  CC_PART_DIR         intermediate partition dir    (default: /data/tmp_edges_part)
  CC_AUTO_DOWNLOAD    fetch source if missing (1/0) (default: 0)
  CC_THREADS          DuckDB thread count           (default: 2)
  CC_MEMORY_LIMIT     DuckDB memory cap             (default: 2GB)
  CC_PASS_SIZE        buckets per Phase 2 pass      (default: 200)

Modes:

  python convert_parquet_prod.py            # full conversion
  python convert_parquet_prod.py --check    # report status, exit 0/1
"""
import os
import shutil
import sys
import time
import urllib.request

import duckdb

CC_RELEASE = os.getenv("CC_RELEASE", "cc-main-2026-jan-feb-mar")
CACHE_DIR = os.getenv("CC_CACHE_DIR", "/data/cc-cache")
PARQUET_DIR = os.getenv("CC_PARQUET_DIR", "/data/cc-parquet")
TMP_DIR = os.getenv("CC_TMP_DIR", "/data/tmp_duckdb")
PART_DIR = os.getenv("CC_PART_DIR", "/data/tmp_edges_part")
AUTO_DOWNLOAD = os.getenv("CC_AUTO_DOWNLOAD", "0") == "1"
THREADS = int(os.getenv("CC_THREADS", "2"))
MEMORY_LIMIT = os.getenv("CC_MEMORY_LIMIT", "2GB")
PASS_SIZE = int(os.getenv("CC_PASS_SIZE", "200"))

CC_DIR = os.path.join(CACHE_DIR, CC_RELEASE)
EDGES_GZ = os.path.join(CC_DIR, f"{CC_RELEASE}-domain-edges.txt.gz")
VERTS_GZ = os.path.join(CC_DIR, f"{CC_RELEASE}-domain-vertices.txt.gz")
SOURCE_BASE = f"https://data.commoncrawl.org/projects/hyperlinkgraph/{CC_RELEASE}/domain"

EDGES_RAW_PQ = os.path.join(PARQUET_DIR, "edges_raw.parquet")
EDGES_DIR = os.path.join(PARQUET_DIR, "edges")
VERTS_PQ = os.path.join(PARQUET_DIR, "vertices.parquet")


def log(msg: str) -> None:
    print(f"PARQUET: {msg}", flush=True)


def edge_bucket_count() -> int:
    if not os.path.isdir(EDGES_DIR):
        return 0
    return sum(1 for f in os.listdir(EDGES_DIR) if f.endswith(".parquet"))


def status() -> dict:
    return {
        "release": CC_RELEASE,
        "edges_gz_exists": os.path.exists(EDGES_GZ),
        "vertices_gz_exists": os.path.exists(VERTS_GZ),
        "edges_raw_parquet_gb": (
            round(os.path.getsize(EDGES_RAW_PQ) / 1024**3, 2)
            if os.path.exists(EDGES_RAW_PQ) else None
        ),
        "vertices_parquet_mb": (
            round(os.path.getsize(VERTS_PQ) / 1024**2, 1)
            if os.path.exists(VERTS_PQ) else None
        ),
        "edge_bucket_count": edge_bucket_count(),
        "ready": edge_bucket_count() >= 1000 and os.path.exists(VERTS_PQ),
    }


def print_status() -> bool:
    s = status()
    log(f"release={s['release']}")
    log(f"source: edges.gz={'OK' if s['edges_gz_exists'] else 'MISSING'} "
        f"vertices.gz={'OK' if s['vertices_gz_exists'] else 'MISSING'}")
    log(f"output: vertices.parquet={s['vertices_parquet_mb']} MB, "
        f"edges_raw.parquet={s['edges_raw_parquet_gb']} GB, "
        f"edge buckets={s['edge_bucket_count']}/1000")
    log(f"ready: {s['ready']}")
    return s["ready"]


def download(url: str, dest: str) -> None:
    """Download with progress logging; atomic via .partial rename."""
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        log(f"already cached: {dest} ({os.path.getsize(dest)/1024**3:.2f} GB)")
        return
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    log(f"downloading {url}")
    tmp = f"{dest}.partial"
    t0 = time.time()
    last_log = [t0]

    def reporthook(blocks_done: int, block_size: int, total: int) -> None:
        now = time.time()
        if now - last_log[0] < 5:
            return
        last_log[0] = now
        done = blocks_done * block_size
        if total > 0:
            pct = 100 * done / total
            speed = done / (now - t0) / 1024**2
            eta = (total - done) / max(1, done / (now - t0))
            log(f"  ... {done/1024**3:.2f}/{total/1024**3:.2f} GB "
                f"({pct:.1f}%, {speed:.1f} MB/s, ETA {eta:.0f}s)")

    urllib.request.urlretrieve(url, tmp, reporthook=reporthook)
    os.rename(tmp, dest)
    elapsed = time.time() - t0
    log(f"downloaded {os.path.getsize(dest)/1024**3:.2f} GB in {elapsed:.0f}s")


def ensure_source_data() -> bool:
    """Make sure both source files are present. Returns False if missing and not allowed to download."""
    have_edges = os.path.exists(EDGES_GZ)
    have_verts = os.path.exists(VERTS_GZ)
    if have_edges and have_verts:
        return True
    if not AUTO_DOWNLOAD:
        log("source data missing.")
        log(f"  expected: {EDGES_GZ}")
        log(f"  expected: {VERTS_GZ}")
        log("  set CC_AUTO_DOWNLOAD=1 to fetch automatically, or place files manually from:")
        log(f"  {SOURCE_BASE}/")
        return False
    download(f"{SOURCE_BASE}/{CC_RELEASE}-domain-vertices.txt.gz", VERTS_GZ)
    download(f"{SOURCE_BASE}/{CC_RELEASE}-domain-edges.txt.gz", EDGES_GZ)
    return True


def duck(memory: str = MEMORY_LIMIT, threads: int = THREADS) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute(f"SET threads TO {threads}")
    con.execute(f"SET memory_limit = '{memory}'")
    con.execute(f"SET temp_directory = '{TMP_DIR}'")
    return con


def convert_vertices() -> None:
    if os.path.exists(VERTS_PQ) and os.path.getsize(VERTS_PQ) >= 800_000_000:
        log(f"vertices already done ({os.path.getsize(VERTS_PQ)/1024**2:.0f} MB)")
        return
    log("converting vertices...")
    t0 = time.time()
    con = duck()
    try:
        con.execute(f"""
            COPY (
                SELECT * FROM read_csv('{VERTS_GZ}',
                    delim='\t', header=false,
                    columns={{'id':'BIGINT','rev_domain':'VARCHAR','num_hosts':'BIGINT'}},
                    ignore_errors=true)
            ) TO '{VERTS_PQ}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)
        """)
    finally:
        con.close()
    log(f"vertices done: {os.path.getsize(VERTS_PQ)/1024**2:.0f} MB in {time.time()-t0:.0f}s")


def phase1_gzip_to_raw_parquet() -> None:
    if os.path.exists(EDGES_RAW_PQ) and os.path.getsize(EDGES_RAW_PQ) >= 1_000_000_000:
        log(f"phase 1 already done ({os.path.getsize(EDGES_RAW_PQ)/1024**3:.2f} GB)")
        return
    log("phase 1 — gzip → raw parquet (single-threaded gzip stream)...")
    t0 = time.time()
    # Phase 1 is single-stream gzip decompression; threads > 1 don't help.
    con = duck(memory=os.getenv("CC_PHASE1_MEMORY", "1GB"), threads=1)
    try:
        con.execute(f"""
            COPY (
                SELECT from_id, to_id
                FROM read_csv('{EDGES_GZ}',
                    delim='\t', header=false,
                    columns={{'from_id':'BIGINT','to_id':'BIGINT'}},
                    ignore_errors=true)
            ) TO '{EDGES_RAW_PQ}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)
        """)
    finally:
        con.close()
    log(f"phase 1 done: {os.path.getsize(EDGES_RAW_PQ)/1024**3:.2f} GB in {time.time()-t0:.0f}s")


def phase2_partition_into_buckets() -> None:
    # Reset stale partition staging dir before each invocation.
    if os.path.isdir(PART_DIR) and os.listdir(PART_DIR):
        log("clearing leftover partition staging dir")
        shutil.rmtree(PART_DIR)
    os.makedirs(PART_DIR, exist_ok=True)

    t_total = time.time()
    for pass_start in range(0, 1000, PASS_SIZE):
        pass_end = pass_start + PASS_SIZE
        needed = [b for b in range(pass_start, pass_end)
                  if not os.path.exists(f"{EDGES_DIR}/bucket={b}.parquet")]
        if not needed:
            log(f"buckets {pass_start}-{pass_end-1} already done, skipping")
            continue
        log(f"phase 2 — partitioning buckets {pass_start}-{pass_end-1} ({len(needed)} needed)...")
        t_pass = time.time()
        con = duck()
        try:
            con.execute(f"""
                COPY (
                    SELECT from_id, to_id, (to_id % 1000)::INT AS bucket
                    FROM read_parquet('{EDGES_RAW_PQ}')
                    WHERE (to_id % 1000) >= {pass_start} AND (to_id % 1000) < {pass_end}
                ) TO '{PART_DIR}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000, PARTITION_BY (bucket))
            """)
        finally:
            con.close()
        log(f"  partitioned in {time.time()-t_pass:.0f}s; compacting...")

        # Compact each pass's buckets into final dir immediately so progress
        # survives container restarts.
        for b in range(pass_start, pass_end):
            src = f"{PART_DIR}/bucket={b}"
            dst = f"{EDGES_DIR}/bucket={b}.parquet"
            if os.path.exists(dst) or not os.path.exists(src):
                shutil.rmtree(src, ignore_errors=True)
                continue
            con = duck(memory=os.getenv("CC_COMPACT_MEMORY", "1GB"))
            try:
                con.execute(f"""
                    COPY (SELECT from_id, to_id FROM read_parquet('{src}/*.parquet'))
                    TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)
                """)
            finally:
                con.close()
            shutil.rmtree(src, ignore_errors=True)
        log(f"  buckets {pass_start}-{pass_end-1} done")

    log(f"phase 2 done in {time.time()-t_total:.0f}s")


def cleanup_intermediates() -> None:
    shutil.rmtree(PART_DIR, ignore_errors=True)
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    if os.path.exists(EDGES_RAW_PQ):
        os.remove(EDGES_RAW_PQ)
        log("cleaned up edges_raw.parquet")


def main() -> int:
    if "--check" in sys.argv:
        return 0 if print_status() else 1

    log(f"release={CC_RELEASE}")
    log(f"cache_dir={CACHE_DIR}  parquet_dir={PARQUET_DIR}")
    log(f"threads={THREADS}  memory_limit={MEMORY_LIMIT}  pass_size={PASS_SIZE}")

    os.makedirs(EDGES_DIR, exist_ok=True)
    os.makedirs(TMP_DIR, exist_ok=True)
    os.makedirs(CC_DIR, exist_ok=True)

    if not ensure_source_data():
        return 0  # silent exit so entrypoint.sh keeps the app booting

    convert_vertices()

    if edge_bucket_count() >= 1000:
        log(f"edges already done ({edge_bucket_count()} buckets)")
        if os.path.exists(EDGES_RAW_PQ):
            os.remove(EDGES_RAW_PQ)
            log("cleaned up edges_raw.parquet")
        return 0

    phase1_gzip_to_raw_parquet()
    phase2_partition_into_buckets()
    cleanup_intermediates()

    total = sum(
        os.path.getsize(os.path.join(EDGES_DIR, f))
        for f in os.listdir(EDGES_DIR)
        if f.endswith(".parquet")
    )
    log(f"all done — {edge_bucket_count()} buckets, {total/1024**3:.2f} GB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
