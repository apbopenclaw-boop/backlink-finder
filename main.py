"""
Backlink Finder API — x402 micropayment service.

Endpoints:
  GET /health          — free health check
  GET /domains         — free: list available domains
  GET /backlinks/{domain} — $0.05: get all backlinks for a domain
  GET /gap?yours=X&competitor=Y — $0.10: gap analysis
"""

import os
import sqlite3

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from x402.http import FacilitatorConfig, HTTPFacilitatorClient, PaymentOption
from x402.http.middleware.fastapi import PaymentMiddlewareASGI
from x402.http.types import RouteConfig
from x402.mechanisms.evm.exact import ExactEvmServerScheme
from x402.schemas import Network
from x402.server import x402ResourceServer

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────

EVM_ADDRESS = os.getenv("EVM_ADDRESS")
EVM_NETWORK: Network = "eip155:84532"  # Base Sepolia (testnet)
FACILITATOR_URL = os.getenv("FACILITATOR_URL", "https://x402.org/facilitator")
DB_PATH = os.getenv("DB_PATH", "data/backlinks.db")

if not EVM_ADDRESS:
    raise ValueError("Set EVM_ADDRESS in .env")

# ── FastAPI app ─────────────────────────────────────────────────────

app = FastAPI(
    title="Backlink Finder API",
    description="Find backlinks to any domain using Common Crawl data. Pay per query with USDC.",
    version="0.1.0",
)

# ── x402 payment middleware ─────────────────────────────────────────

facilitator = HTTPFacilitatorClient(FacilitatorConfig(url=FACILITATOR_URL))
server = x402ResourceServer(facilitator)
server.register(EVM_NETWORK, ExactEvmServerScheme())

routes = {
    "GET /backlinks/*": RouteConfig(
        accepts=[
            PaymentOption(
                scheme="exact",
                pay_to=EVM_ADDRESS,
                price="$0.05",
                network=EVM_NETWORK,
            ),
        ],
        mime_type="application/json",
        description="Get all domains linking to a target domain (Common Crawl data)",
    ),
    "GET /gap": RouteConfig(
        accepts=[
            PaymentOption(
                scheme="exact",
                pay_to=EVM_ADDRESS,
                price="$0.10",
                network=EVM_NETWORK,
            ),
        ],
        mime_type="application/json",
        description="Gap analysis: find domains linking to competitor but not to you",
    ),
}
app.add_middleware(PaymentMiddlewareASGI, routes=routes, server=server)

# Serve .well-known for agent discovery
app.mount("/.well-known", StaticFiles(directory=".well-known"), name="well-known")

# ── DB helpers ──────────────────────────────────────────────────────


def get_db() -> sqlite3.Connection:
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=503, detail="Backlinks database not available")
    return sqlite3.connect(DB_PATH)


# ── Free endpoints ──────────────────────────────────────────────────


@app.get("/health")
async def health():
    return {"status": "ok", "service": "backlink-finder", "version": "0.1.0"}


@app.get("/domains")
async def list_domains():
    """List all domains with stored backlink data (free)."""
    con = get_db()
    rows = con.execute(
        "SELECT target, release, crawled_at, result_count "
        "FROM crawls ORDER BY crawled_at DESC"
    ).fetchall()
    con.close()
    return {
        "domains": [
            {
                "domain": r[0],
                "release": r[1],
                "crawled_at": r[2],
                "backlink_count": r[3],
            }
            for r in rows
        ],
        "total": len(rows),
    }


# ── Paid endpoints ──────────────────────────────────────────────────


@app.get("/backlinks/{domain}")
async def get_backlinks(domain: str):
    """Get all backlinks for a domain. Costs $0.05 USDC."""
    domain = domain.strip().lower().rstrip(".")
    con = get_db()

    row = con.execute(
        "SELECT id, release, crawled_at FROM crawls WHERE target = ? "
        "ORDER BY crawled_at DESC LIMIT 1",
        (domain,),
    ).fetchone()

    if not row:
        con.close()
        raise HTTPException(status_code=404, detail=f"No data for {domain}")

    crawl_id, release, crawled_at = row
    results = con.execute(
        """
        SELECT b.linking_domain, b.num_hosts, b.page_rank,
               m.global_rank, m.ref_subnets, m.ref_ips,
               t.tranco_rank
        FROM backlinks b
        LEFT JOIN majestic_cache m ON m.domain = b.linking_domain
        LEFT JOIN tranco_cache t ON t.domain = b.linking_domain
        WHERE b.crawl_id = ?
        ORDER BY b.num_hosts DESC, b.linking_domain
        """,
        (crawl_id,),
    ).fetchall()
    con.close()

    return {
        "domain": domain,
        "release": release,
        "crawled_at": crawled_at,
        "backlink_count": len(results),
        "backlinks": [
            {
                "linking_domain": r[0],
                "num_hosts": r[1],
                "page_rank": r[2],
                "majestic_rank": r[3],
                "ref_subnets": r[4],
                "ref_ips": r[5],
                "tranco_rank": r[6],
            }
            for r in results
        ],
    }


@app.get("/gap")
async def gap_analysis(
    yours: str = Query(..., description="Your domain"),
    competitor: str = Query(..., description="Competitor domain"),
):
    """Find domains linking to competitor but not to you. Costs $0.10 USDC."""
    yours = yours.strip().lower().rstrip(".")
    competitor = competitor.strip().lower().rstrip(".")
    con = get_db()

    def _get_backlink_domains(target: str) -> set[str] | None:
        row = con.execute(
            "SELECT id FROM crawls WHERE target = ? ORDER BY crawled_at DESC LIMIT 1",
            (target,),
        ).fetchone()
        if not row:
            return None
        rows = con.execute(
            "SELECT linking_domain FROM backlinks WHERE crawl_id = ?",
            (row[0],),
        ).fetchall()
        return {r[0] for r in rows}

    own = _get_backlink_domains(yours)
    comp = _get_backlink_domains(competitor)
    con.close()

    if own is None:
        raise HTTPException(status_code=404, detail=f"No data for {yours}")
    if comp is None:
        raise HTTPException(status_code=404, detail=f"No data for {competitor}")

    gap_domains = comp - own

    # Get enrichment data for gap domains
    con = get_db()
    gap_results = []
    for d in sorted(gap_domains):
        row = con.execute(
            """
            SELECT b.num_hosts, b.page_rank, m.global_rank, t.tranco_rank
            FROM backlinks b
            JOIN crawls c ON c.id = b.crawl_id AND c.target = ?
            LEFT JOIN majestic_cache m ON m.domain = b.linking_domain
            LEFT JOIN tranco_cache t ON t.domain = b.linking_domain
            WHERE b.linking_domain = ?
            ORDER BY c.crawled_at DESC LIMIT 1
            """,
            (competitor, d),
        ).fetchone()
        gap_results.append({
            "domain": d,
            "num_hosts": row[0] if row else None,
            "page_rank": row[1] if row else None,
            "majestic_rank": row[2] if row else None,
            "tranco_rank": row[3] if row else None,
        })
    con.close()

    gap_results.sort(key=lambda r: r["num_hosts"] or 0, reverse=True)

    return {
        "yours": yours,
        "competitor": competitor,
        "your_backlinks": len(own),
        "competitor_backlinks": len(comp),
        "gap_count": len(gap_results),
        "opportunities": gap_results,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=4021)
