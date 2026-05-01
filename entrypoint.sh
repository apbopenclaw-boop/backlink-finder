#!/bin/bash
# Copy seed DB to volume if not already there
if [ ! -f /data/backlinks.db ]; then
    echo "Seeding database from image..."
    cp /app/seed/backlinks.db /data/backlinks.db
else
    echo "Merging new seed data into existing DB..."
    python3 -c "
import sqlite3

seed = sqlite3.connect('/app/seed/backlinks.db')
live = sqlite3.connect('/data/backlinks.db')

existing = set(r[0] for r in live.execute('SELECT target FROM crawls').fetchall())
seed_domains = seed.execute('SELECT id, target, release, crawled_at, result_count FROM crawls').fetchall()

added = 0
for sid, target, release, crawled_at, result_count in seed_domains:
    if target in existing:
        continue
    cur = live.execute('INSERT INTO crawls (target, release, crawled_at, result_count) VALUES (?,?,?,?)',
        (target, release, crawled_at, result_count))
    new_id = cur.lastrowid
    bls = seed.execute('SELECT linking_domain, num_hosts FROM backlinks WHERE crawl_id = ?', (sid,)).fetchall()
    live.executemany('INSERT INTO backlinks (crawl_id, linking_domain, num_hosts) VALUES (?,?,?)',
        [(new_id, b[0], b[1]) for b in bls])
    added += 1
    print(f'  Merged {target}: {len(bls)} backlinks')

live.commit()
live.close()
seed.close()
print(f'Seed merge done: {added} new domains added')
"
fi

# Ensure cc-cache dir exists
mkdir -p /data/cc-cache

# Convert CC data to parquet if not already done
if [ ! -d /data/cc-parquet/edges ] || [ $(ls /data/cc-parquet/edges/*.parquet 2>/dev/null | wc -l) -lt 1000 ]; then
    echo "Parquet conversion needed. Starting in background..."
    python3 /app/convert_parquet_prod.py &
else
    echo "Parquet data ready: $(ls /data/cc-parquet/edges/*.parquet | wc -l) buckets"
fi

exec uvicorn main:app --host 0.0.0.0 --port 8080 --timeout-keep-alive 600
