#!/usr/bin/env python3
"""Convert CC data to parquet — two-phase approach for 4GB containers.

Phase 1: Stream gzip → single raw parquet file (one gzip read, low memory)
Phase 2: Partition from parquet into 1000 buckets (fast parquet reads)
"""
import duckdb, os, shutil, time, sys

CC_DIR = "/data/cc-cache/cc-main-2026-jan-feb-mar"
OUT = "/data/cc-parquet"
EDGES_DIR = f"{OUT}/edges"
PART_DIR = "/data/tmp_edges_part"
TEMP = "/data/tmp_duckdb"
EDGES_GZ = f"{CC_DIR}/cc-main-2026-jan-feb-mar-domain-edges.txt.gz"
VERTS_GZ = f"{CC_DIR}/cc-main-2026-jan-feb-mar-domain-vertices.txt.gz"
EDGES_RAW_PQ = f"{OUT}/edges_raw.parquet"

if not os.path.exists(EDGES_GZ):
    print("PARQUET: No CC data found, skipping conversion.", flush=True)
    sys.exit(0)

os.makedirs(EDGES_DIR, exist_ok=True)
os.makedirs(TEMP, exist_ok=True)

# ── Vertices ────────────────────────────────────────────────────────
verts_out = f"{OUT}/vertices.parquet"
if not os.path.exists(verts_out) or os.path.getsize(verts_out) < 800_000_000:
    print("PARQUET: Converting vertices...", flush=True)
    t0 = time.time()
    con = duckdb.connect(":memory:")
    con.execute("SET threads TO 2")
    con.execute("SET memory_limit = '2GB'")
    con.execute(f"SET temp_directory = '{TEMP}'")
    con.execute(f"""
        COPY (
            SELECT * FROM read_csv('{VERTS_GZ}',
                delim='\t', header=false,
                columns={{'id':'BIGINT','rev_domain':'VARCHAR','num_hosts':'BIGINT'}},
                ignore_errors=true)
        ) TO '{verts_out}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)
    """)
    con.close()
    print(f"PARQUET: Vertices done: {os.path.getsize(verts_out)/1024**2:.0f} MB in {time.time()-t0:.0f}s", flush=True)
else:
    print(f"PARQUET: Vertices OK ({os.path.getsize(verts_out)/1024**2:.0f} MB)", flush=True)

# ── Check if edges already done ─────────────────────────────────────
existing = len([f for f in os.listdir(EDGES_DIR) if f.endswith(".parquet")]) if os.path.isdir(EDGES_DIR) else 0
if existing >= 1000:
    print(f"PARQUET: Edges already done ({existing} buckets)", flush=True)
    # Clean up intermediate file if it exists
    if os.path.exists(EDGES_RAW_PQ):
        os.remove(EDGES_RAW_PQ)
        print("PARQUET: Cleaned up intermediate edges_raw.parquet", flush=True)
    sys.exit(0)

# ── Phase 1: Gzip → single parquet (streaming, one gzip read) ──────
if not os.path.exists(EDGES_RAW_PQ) or os.path.getsize(EDGES_RAW_PQ) < 1_000_000_000:
    print("PARQUET: Phase 1 — Converting edges gzip → raw parquet (streaming)...", flush=True)
    t0 = time.time()
    con = duckdb.connect(":memory:")
    con.execute("SET threads TO 1")
    con.execute("SET memory_limit = '1GB'")
    con.execute(f"SET temp_directory = '{TEMP}'")
    con.execute(f"""
        COPY (
            SELECT from_id, to_id
            FROM read_csv('{EDGES_GZ}',
                delim='\t', header=false,
                columns={{'from_id':'BIGINT','to_id':'BIGINT'}},
                ignore_errors=true)
        ) TO '{EDGES_RAW_PQ}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)
    """)
    con.close()
    sz = os.path.getsize(EDGES_RAW_PQ) / 1024**3
    print(f"PARQUET: Phase 1 done: {sz:.2f} GB in {time.time()-t0:.0f}s", flush=True)
else:
    sz = os.path.getsize(EDGES_RAW_PQ) / 1024**3
    print(f"PARQUET: Phase 1 already done ({sz:.2f} GB)", flush=True)

# ── Phase 2: Partition from parquet (fast reads) ────────────────────
# Clean up leftover temp from previous attempts
if os.path.isdir(PART_DIR) and os.listdir(PART_DIR):
    print("PARQUET: Cleaning leftover temp partition dir...", flush=True)
    shutil.rmtree(PART_DIR)
os.makedirs(PART_DIR, exist_ok=True)

PASS_SIZE = 200
t0 = time.time()
for pass_start in range(0, 1000, PASS_SIZE):
    pass_end = pass_start + PASS_SIZE
    # Skip pass if all its buckets already exist in final dir
    needed = [b for b in range(pass_start, pass_end)
              if not os.path.exists(f"{EDGES_DIR}/bucket={b}.parquet")]
    if not needed:
        print(f"PARQUET: Buckets {pass_start}-{pass_end-1} already done, skipping.", flush=True)
        continue
    print(f"PARQUET: Phase 2 — Partitioning buckets {pass_start}-{pass_end-1} from parquet...", flush=True)
    pt0 = time.time()
    con = duckdb.connect(":memory:")
    con.execute("SET threads TO 2")
    con.execute("SET memory_limit = '2GB'")
    con.execute(f"SET temp_directory = '{TEMP}'")
    con.execute(f"""
        COPY (
            SELECT from_id, to_id, (to_id % 1000)::INT AS bucket
            FROM read_parquet('{EDGES_RAW_PQ}')
            WHERE (to_id % 1000) >= {pass_start} AND (to_id % 1000) < {pass_end}
        ) TO '{PART_DIR}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000, PARTITION_BY (bucket))
    """)
    con.close()
    print(f"PARQUET: Pass {pass_start}-{pass_end-1} partitioned in {time.time()-pt0:.0f}s. Compacting...", flush=True)
    # Compact this pass's buckets immediately so progress survives restarts
    for b in range(pass_start, pass_end):
        src = f"{PART_DIR}/bucket={b}"
        dst = f"{EDGES_DIR}/bucket={b}.parquet"
        if os.path.exists(dst) or not os.path.exists(src):
            shutil.rmtree(src, ignore_errors=True)
            continue
        con = duckdb.connect(":memory:")
        con.execute("SET threads TO 2")
        con.execute("SET memory_limit = '1GB'")
        con.execute(f"SET temp_directory = '{TEMP}'")
        con.execute(f"""
            COPY (SELECT from_id, to_id FROM read_parquet('{src}/*.parquet'))
            TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)
        """)
        con.close()
        shutil.rmtree(src, ignore_errors=True)
    print(f"PARQUET: Buckets {pass_start}-{pass_end-1} done.", flush=True)
print(f"PARQUET: Phase 2 done in {time.time()-t0:.0f}s", flush=True)

# ── Cleanup ─────────────────────────────────────────────────────────
shutil.rmtree(PART_DIR, ignore_errors=True)
shutil.rmtree(TEMP, ignore_errors=True)
# Remove intermediate raw parquet to free disk space
if os.path.exists(EDGES_RAW_PQ):
    os.remove(EDGES_RAW_PQ)
    print("PARQUET: Cleaned up intermediate edges_raw.parquet", flush=True)

total = sum(os.path.getsize(os.path.join(EDGES_DIR, f)) for f in os.listdir(EDGES_DIR) if f.endswith(".parquet"))
print(f"PARQUET: All done! 1000 buckets, {total/1024**3:.2f} GB", flush=True)
