# Backlink Finder API — Cost Overview

## How It Works

Each new domain query downloads and scans Common Crawl's full hyperlink graph:
- **Vertices file**: ~2GB gzipped (all domains + metadata)
- **Edges file**: ~16GB gzipped (all links between domains)
- Total CC data: **~18GB** (downloaded once, cached on disk)
- DuckDB scans the full edges file per query (~2-5 min on shared CPU)
- Results stored in SQLite for instant re-query

## Infrastructure Costs (Fly.io, Amsterdam)

### Fixed Monthly
| Item | Spec | Cost/month |
|------|------|-----------|
| Volume (CC data + DB) | 25GB | $3.75 |
| Machine base (auto-stop) | shared-cpu-2x, 2GB RAM | ~$2-6* |
| Bandwidth out (responses) | first 100GB free | $0 |
| **Total fixed** | | **~$6-10/month** |

*With auto-stop: machine only runs when handling requests. $0 when idle.

### Per-Query Variable Costs
| Query Type | What Happens | CPU Time | Cost |
|-----------|-------------|----------|------|
| **New domain crawl** | DuckDB scans 16GB edges | 2-5 min | ~$0.001 |
| **Cached domain** | SQLite read | <1 sec | ~$0.00001 |
| **Gap analysis** | 2x SQLite reads + compare | <1 sec | ~$0.00002 |
| **Enrichment** (PageRank) | API call, 100 domains/batch | 5-10 sec | ~$0.0001 |
| **Enrichment** (Majestic) | 80MB download + match | 30-60 sec | ~$0.0005 |
| **Enrichment** (Tranco) | 5MB download + match | 5-10 sec | ~$0.0001 |

### External API Costs
| Service | Cost | Limit |
|---------|------|-------|
| Common Crawl data | Free | Public dataset |
| Open PageRank API | Free tier | 100 domains/request, unknown rate limit |
| Majestic Million | Free | Public CSV, 1M domains |
| Tranco top-1M | Free | Public dataset |

## Pricing Strategy

### Option A: Simple per-query
| Endpoint | Price | Margin |
|----------|-------|--------|
| `/backlinks/{domain}` (cached) | $0.01 | 99.9% |
| `/backlinks/{domain}` (new crawl) | $0.10 | 99% |
| `/gap` analysis | $0.15 | 99.9% |
| `/enrich` (all sources) | $0.05 | 99% |

### Option B: Higher value positioning
| Endpoint | Price | Margin |
|----------|-------|--------|
| `/backlinks/{domain}` (cached) | $0.05 | 99.99% |
| `/backlinks/{domain}` (new crawl) | $0.50 | 99.5% |
| `/gap` analysis | $0.75 | 99.99% |
| Full report (crawl + enrich + gap) | $1.00 | 99% |

## Break-Even Analysis

At **$6/month fixed cost** with Option A pricing:
- Need 60 new crawls OR 600 cached queries OR 40 gap analyses per month
- Mix: ~50-80 total queries/month to break even

At Option B pricing:
- Need 12 new crawls OR 120 cached queries OR 8 gap analyses
- Mix: ~10-20 queries/month to break even

## Scaling Notes

- CC data updates quarterly (new release every 3 months)
- Each release is a separate ~18GB download
- Can keep only latest release to save disk
- DuckDB query time scales with RAM: 2GB → 3-5 min, 4GB → 1-3 min
- At scale (100+ queries/day): upgrade to dedicated CPU, ~$30/month
- Revenue at 100 queries/day × $0.10 avg = $300/month (30x infra cost)

## Comparison: What Competitors Charge

| Service | Backlink Check Price | Notes |
|---------|---------------------|-------|
| Ahrefs API | $0.004/row (min $449/month) | Enterprise only |
| Moz API | $0.01/link (min $99/month) | Monthly subscription |
| SEMrush API | Not available standalone | Min $499/month plan |
| Majestic API | £50/month + credits | Per-domain pricing |
| **Us (x402)** | $0.10/domain (all links) | No subscription, pay-per-use |

Our positioning: **No subscription, no account, no API key — just pay and get data.**
An AI agent can discover us, pay $0.10, get 1,270 backlinks for pepperl-fuchs.com. Done.
