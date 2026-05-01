#!/usr/bin/env python3
"""Convert CC hyperlink graph to Parquet for remote querying via DuckDB httpfs."""
import duckdb
import os
import time

CC_DIR = os.path.expanduser("~/.cache/cc-backlinks/cc-main-2026-jan-feb-mar")
RELEASE = "cc-main-2026-jan-feb-mar"
VERTS = os.path.join(CC_DIR, f"{RELEASE}-domain-vertices.txt.gz")
EDGES = os.path.join(CC_DIR, f"{RELEASE}-domain-edges.txt.gz")
OUT_DIR = "/tmp/backlink-parquet"

os.makedirs(OUT_DIR, exist_ok=True)

con = duckdb.connect(":memory:")
# Use all available threads and generous memory
con.execute("SET threads TO 8")
con.execute("SET memory_limit = '8GB'")

# Step 1: Convert vertices to Parquet (small, fast)
print("Converting vertices...")
t0 = time.time()
con.execute(f"""
    COPY (
        SELECT id, rev_domain, num_hosts
        FROM read_csv('{VERTS}', delim='\t', header=false,
            columns={{'id':'BIGINT','rev_domain':'VARCHAR','num_hosts':'BIGINT'}})
        ORDER BY id
    ) TO '{OUT_DIR}/vertices.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
""")
vsize = os.path.getsize(f"{OUT_DIR}/vertices.parquet")
print(f"  vertices.parquet: {vsize / 1024 / 1024:.0f} MB ({time.time() - t0:.0f}s)")

# Step 2: Convert edges to Parquet sorted by to_id (enables efficient filtering)
print("Converting edges (this will take ~10-15 minutes)...")
t0 = time.time()
con.execute(f"""
    COPY (
        SELECT from_id, to_id
        FROM read_csv('{EDGES}', delim='\t', header=false,
            columns={{'from_id':'BIGINT','to_id':'BIGINT'}})
        ORDER BY to_id, from_id
    ) TO '{OUT_DIR}/edges.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)
""")
esize = os.path.getsize(f"{OUT_DIR}/edges.parquet")
print(f"  edges.parquet: {esize / 1024 / 1024:.0f} MB ({time.time() - t0:.0f}s)")

print(f"\nTotal: {(vsize + esize) / 1024 / 1024 / 1024:.2f} GB")
print(f"Output dir: {OUT_DIR}")
con.close()
