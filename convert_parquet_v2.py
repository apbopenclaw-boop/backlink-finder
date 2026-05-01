#!/usr/bin/env python3
"""Convert CC hyperlink graph to partitioned Parquet for fast lookups.

Strategy: partition edges by (to_id % 1000) so each query reads only 1 partition.
No full sort needed — streaming conversion.
"""
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
con.execute("SET threads TO 8")
con.execute("SET memory_limit = '8GB'")

# Step 1: Vertices — simple streaming conversion (no sort needed)
verts_out = f"{OUT_DIR}/vertices.parquet"
if os.path.exists(verts_out) and os.path.getsize(verts_out) > 0:
    print(f"vertices.parquet already exists ({os.path.getsize(verts_out) / 1024 / 1024:.0f} MB), skipping")
else:
    print("Converting vertices...", flush=True)
    t0 = time.time()
    con.execute(f"""
        COPY (
            SELECT id, rev_domain, num_hosts
            FROM read_csv('{VERTS}', delim='\t', header=false,
                columns={{'id':'BIGINT','rev_domain':'VARCHAR','num_hosts':'BIGINT'}})
        ) TO '{verts_out}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
    """)
    print(f"  vertices.parquet: {os.path.getsize(verts_out) / 1024 / 1024:.0f} MB ({time.time() - t0:.0f}s)", flush=True)

# Step 2: Edges — partitioned by to_id % 1000 (streaming, no sort)
edges_dir = f"{OUT_DIR}/edges"
if os.path.exists(edges_dir) and len(os.listdir(edges_dir)) > 100:
    print(f"edges/ already has {len(os.listdir(edges_dir))} partitions, skipping")
else:
    os.makedirs(edges_dir, exist_ok=True)
    print("Converting edges with partitioning (streaming, ~10 min)...", flush=True)
    t0 = time.time()
    con.execute(f"""
        COPY (
            SELECT from_id, to_id, (to_id % 1000)::INT AS bucket
            FROM read_csv('{EDGES}', delim='\t', header=false,
                columns={{'from_id':'BIGINT','to_id':'BIGINT'}})
        ) TO '{edges_dir}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000, PARTITION_BY (bucket))
    """)
    elapsed = time.time() - t0
    # Count total size
    total = sum(
        os.path.getsize(os.path.join(dp, f))
        for dp, _, fns in os.walk(edges_dir)
        for f in fns
    )
    nparts = len([d for d in os.listdir(edges_dir) if os.path.isdir(os.path.join(edges_dir, d))])
    print(f"  edges/: {nparts} partitions, {total / 1024 / 1024 / 1024:.2f} GB ({elapsed:.0f}s)", flush=True)

# Summary
print("\nDone! Files:", flush=True)
for item in os.listdir(OUT_DIR):
    path = os.path.join(OUT_DIR, item)
    if os.path.isfile(path):
        print(f"  {item}: {os.path.getsize(path) / 1024 / 1024:.0f} MB")
    elif os.path.isdir(path):
        total = sum(
            os.path.getsize(os.path.join(dp, f))
            for dp, _, fns in os.walk(path)
            for f in fns
        )
        nparts = len(os.listdir(path))
        print(f"  {item}/: {nparts} partitions, {total / 1024 / 1024 / 1024:.2f} GB")

con.close()
