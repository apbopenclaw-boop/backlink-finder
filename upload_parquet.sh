#!/bin/bash
# Upload Parquet files to Fly volume
# Run after conversion completes
set -e

FLY_TOKEN=$(cat /tmp/backlink-api/.fly-token)
export FLY_API_TOKEN="$FLY_TOKEN"
FLY=~/.fly/bin/fly

echo "=== Step 1: Tar parquet files ==="
cd /tmp/backlink-parquet
tar czf /tmp/backlink-parquet.tar.gz vertices.parquet edges/
ls -lh /tmp/backlink-parquet.tar.gz

echo "=== Step 2: Upload to Fly volume ==="
# Use fly ssh sftp to upload
echo "Uploading tarball to Fly machine..."
$FLY ssh sftp shell -a backlink-finder << SFTP
put /tmp/backlink-parquet.tar.gz /data/backlink-parquet.tar.gz
SFTP

echo "=== Step 3: Extract on Fly machine ==="
$FLY ssh console -a backlink-finder -C "cd /data && tar xzf backlink-parquet.tar.gz && rm backlink-parquet.tar.gz && echo 'Extracted OK' && ls -lh /data/vertices.parquet && ls /data/edges/ | wc -l && echo 'edge partitions'"

echo "=== Done ==="
echo "Parquet data uploaded to Fly volume at /data/"
echo "vertices.parquet + edges/bucket=0..999/"
