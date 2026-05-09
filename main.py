"""
bhrefs — SEO Tools for AI Agents. x402 micropayment service.

Endpoints:
  GET /health          — free health check
  GET /domains         — free: list available domains
  GET /backlinks/{domain} — $0.05: get all backlinks for a domain
  GET /gap?yours=X&competitor=Y — $0.10: gap analysis
"""

import math
import os
import sqlite3
import threading
import time

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from crawler import crawl_and_store, validate_domain, init_db
from cdp_auth import create_cdp_auth_provider

from x402.http import FacilitatorConfig, HTTPFacilitatorClient, PaymentOption
from x402.http.middleware.fastapi import PaymentMiddlewareASGI
from x402.http.types import RouteConfig
from x402.mechanisms.evm.exact import ExactEvmServerScheme
from x402.schemas import Network
from x402.server import x402ResourceServer

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────

EVM_ADDRESS = os.getenv("EVM_ADDRESS")
EVM_NETWORK: Network = "eip155:8453"  # Base mainnet
FACILITATOR_URL = os.getenv("FACILITATOR_URL", "https://x402.org/facilitator")
DB_PATH = os.getenv("DB_PATH", "/data/backlinks.db")
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY", "")
EMAIL_DB_PATH = os.getenv("EMAIL_DB_PATH", "/data/email_finder.db")
MONTHLY_LIMIT = int(os.getenv("MONTHLY_LIMIT", "500"))
PER_PAYER_MONTHLY_LIMIT = int(os.getenv("PER_PAYER_MONTHLY_LIMIT", "100"))

if not EVM_ADDRESS:
    raise ValueError("Set EVM_ADDRESS in .env")

# ── FastAPI app ─────────────────────────────────────────────────────

app = FastAPI(
    title="bhrefs API",
    description="Find backlinks to any domain using Common Crawl data. Pay per query with USDC.",
    version="0.1.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

# ── x402 payment middleware ─────────────────────────────────────────

# Use CDP facilitator if API keys are configured AND using CDP URL (enables Agentic.Market indexing)
cdp_auth = None
if "cdp.coinbase.com" in FACILITATOR_URL:
    cdp_auth = create_cdp_auth_provider()
facilitator_config = FacilitatorConfig(url=FACILITATOR_URL, auth_provider=cdp_auth)
facilitator = HTTPFacilitatorClient(facilitator_config)

# ── V2→V1 conversion for CDP facilitator (only supports V1) ────────
import json as _json

_CAIP2_TO_V1 = {"eip155:8453": "base", "eip155:84532": "base-sepolia"}

def _v2_payload_to_v1(payload_dict: dict) -> dict:
    v1 = {"x402Version": 1}
    v1["scheme"] = payload_dict.get("scheme", "exact")
    raw_net = payload_dict.get("network", EVM_NETWORK)
    v1["network"] = _CAIP2_TO_V1.get(raw_net, raw_net)
    v1["payload"] = payload_dict.get("payload", payload_dict)
    return v1

def _v2_requirements_to_v1(req_dict: dict) -> dict:
    raw_net = req_dict.get("network", EVM_NETWORK)
    extra = req_dict.get("extra", {})
    if isinstance(extra, str):
        try:
            extra = _json.loads(extra)
        except Exception:
            extra = {}
    v1 = {
        "scheme": req_dict.get("scheme", "exact"),
        "network": _CAIP2_TO_V1.get(raw_net, raw_net),
        "maxAmountRequired": req_dict.get("amount", req_dict.get("maxAmountRequired", "0")),
        "resource": req_dict.get("resource", ""),
        "description": req_dict.get("description", ""),
        "mimeType": req_dict.get("mimeType", req_dict.get("mime_type", "application/json")),
        "asset": req_dict.get("asset", ""),
        "payTo": req_dict.get("payTo", req_dict.get("pay_to", "")),
        "maxTimeoutSeconds": req_dict.get("maxTimeoutSeconds", req_dict.get("max_timeout_seconds", 300)),
        "extra": extra,
    }
    # Pass bazaar extension info as outputSchema for CDP Bazaar indexing
    extensions = req_dict.get("extensions", {})
    bazaar = extensions.get("bazaar", {})
    if bazaar.get("info"):
        v1["outputSchema"] = bazaar["info"]
    return v1

_last_debug = {"request": None, "error": None, "result": None}
_orig_verify = facilitator._verify_http
_orig_settle = facilitator._settle_http

async def _v1_verify(version, payload_dict, requirements_dict):
    v1_payload = _v2_payload_to_v1(payload_dict)
    v1_reqs = _v2_requirements_to_v1(requirements_dict)
    body = facilitator._build_request_body(1, v1_payload, v1_reqs)
    _last_debug["request"] = body
    try:
        result = await _orig_verify(1, v1_payload, v1_reqs)
        _last_debug["result"] = "OK"
        _last_debug["error"] = None
        return result
    except Exception as e:
        _last_debug["error"] = str(e)
        raise

async def _v1_settle(version, payload_dict, requirements_dict):
    v1_payload = _v2_payload_to_v1(payload_dict)
    v1_reqs = _v2_requirements_to_v1(requirements_dict)
    return await _orig_settle(1, v1_payload, v1_reqs)

facilitator._verify_http = _v1_verify
facilitator._settle_http = _v1_settle

server = x402ResourceServer(facilitator)
server.register(EVM_NETWORK, ExactEvmServerScheme())

# Single source of truth for endpoint metadata.
# Used to build the x402 middleware routes, /services.json, /.well-known/x402.json,
# and /llms.txt — keeping prices, paths, and descriptions in lockstep.
def _input_schema(query_params: dict[str, str], required: list[str] | None = None) -> dict:
    """Build a bazaar-extension JSON schema for an http GET endpoint."""
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "input": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "const": "http"},
                    "method": {"type": "string", "enum": ["GET"]},
                    "queryParams": {
                        "type": "object",
                        "properties": {k: {"type": "string"} for k in query_params},
                        "required": required if required is not None else list(query_params),
                    },
                },
                "required": ["type", "method"],
                "additionalProperties": False,
            },
            "output": {
                "type": "object",
                "properties": {
                    "type": {"type": "string"},
                    "example": {"type": "object"},
                },
                "required": ["type"],
            },
        },
        "required": ["input"],
    }


ENDPOINT_CATALOG: list[dict] = [
    {
        "method": "GET",
        "path": "/backlinks/{domain}",
        "route_pattern": "GET /backlinks/*",
        "description": "Get all backlinks for any domain. Crawls on-demand from Common Crawl if not cached (may take 2-5 min for new domains).",
        "price_usd": "$0.10",
        "amount_atomic": "100000",
        "query_params": {"domain": "example.com"},
        "output_example": {
            "domain": "example.com",
            "backlink_count": 142,
            "backlinks": [{"linking_domain": "github.com", "num_hosts": 6038, "authority_score": 57}],
        },
    },
    {
        "method": "GET",
        "path": "/find",
        "route_pattern": "GET /find",
        "description": "Find a verified business email for a person given their name and company domain.",
        "price_usd": "$0.05",
        "amount_atomic": "50000",
        "query_params": {"first_name": "John", "last_name": "Smith", "domain": "acme.com"},
        "output_example": {
            "email": "john.smith@acme.com",
            "confidence": 0.92,
            "first_name": "John",
            "last_name": "Smith",
            "title": "VP Engineering",
            "company": "Acme Corp",
            "domain": "acme.com",
        },
    },
    {
        "method": "GET",
        "path": "/gap",
        "route_pattern": "GET /gap",
        "description": "Gap analysis: find domains linking to competitor but not to you.",
        "price_usd": "$0.15",
        "amount_atomic": "150000",
        "query_params": {"yours": "mysite.com", "competitor": "competitor.com"},
        "output_example": {
            "gap_count": 847,
            "opportunities": [{"domain": "techcrunch.com", "authority_score": 89}],
        },
    },
    {
        "method": "GET",
        "path": "/preview/{domain}",
        "route_pattern": None,
        "description": "Free preview of the top 5 backlinks for any cached domain.",
        "price_usd": None,
        "amount_atomic": None,
        "query_params": {},
        "output_example": None,
    },
    {
        "method": "GET",
        "path": "/domains",
        "route_pattern": None,
        "description": "List all cached domains with their backlink counts.",
        "price_usd": None,
        "amount_atomic": None,
        "query_params": {},
        "output_example": None,
    },
    {
        "method": "GET",
        "path": "/health",
        "route_pattern": None,
        "description": "Service health check.",
        "price_usd": None,
        "amount_atomic": None,
        "query_params": {},
        "output_example": {"status": "ok", "service": "backlink-finder", "version": "0.1.0"},
    },
    {
        "method": "GET",
        "path": "/parquet-status",
        "route_pattern": None,
        "description": "Parquet conversion / data-freshness status.",
        "price_usd": None,
        "amount_atomic": None,
        "query_params": {},
        "output_example": None,
    },
]


def _build_paid_routes(catalog: list[dict]) -> dict[str, RouteConfig]:
    """Build the x402 PaymentMiddlewareASGI routes dict from the catalog."""
    return {
        e["route_pattern"]: RouteConfig(
            accepts=[
                PaymentOption(
                    scheme="exact",
                    pay_to=EVM_ADDRESS,
                    price=e["price_usd"],
                    network=EVM_NETWORK,
                ),
            ],
            mime_type="application/json",
            description=e["description"],
            extensions={
                "bazaar": {
                    "info": {
                        "input": {
                            "type": "http",
                            "method": "GET",
                            "queryParams": e["query_params"],
                        },
                        "output": {
                            "type": "json",
                            "example": e["output_example"],
                        },
                    },
                    "schema": _input_schema(e["query_params"]),
                }
            },
        )
        for e in catalog
        if e["route_pattern"] is not None
    }


routes = _build_paid_routes(ENDPOINT_CATALOG)

# Track in-progress crawls to prevent duplicate work
_crawl_locks: dict[str, threading.Lock] = {}
_MAX_CONCURRENT_CRAWLS = 3
_active_crawls = [0]
_crawl_count_lock = threading.Lock()
app.add_middleware(PaymentMiddlewareASGI, routes=routes, server=server)
# Outer middleware that polishes the upstream 402 responses to match the
# x402 spec: JSON payload in body, https:// in resource.url (Fly TLS proxy
# fix), CORS headers, and an x-payment-required v1 fallback. Must be
# registered AFTER PaymentMiddlewareASGI so it wraps it.
from x402_polish import X402ResponsePolish  # noqa: E402
app.add_middleware(X402ResponsePolish)

# Serve static assets (OG image, etc.)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ── DB helpers ──────────────────────────────────────────────────────


def get_db() -> sqlite3.Connection:
    """Get SQLite connection, creating DB if needed."""
    con = init_db(DB_PATH)
    return con


# ── Free endpoints ──────────────────────────────────────────────────


@app.get("/services.json")
async def services_manifest():
    """Agentic Market / Bazaar service manifest for auto-discovery."""
    return {
        "id": "bhrefs",
        "name": "bhrefs — SEO Tools for AI Agents",
        "description": "A suite of SEO tools built for AI agents. Backlink discovery, competitive gap analysis, and email enrichment — all pay-per-query with USDC via x402.",
        "category": "data",
        "x402Version": 2,
        "networks": [EVM_NETWORK],
        "website": "https://bhrefs.com",
        "endpoints": [
            {
                "method": e["method"],
                "path": e["path"],
                "description": e["description"],
                "price": e["price_usd"] or "$0.00",
                "currency": "USDC",
            }
            for e in ENDPOINT_CATALOG
        ],
    }


@app.get("/.well-known/x402.json")
async def x402_manifest():
    """x402 agent discovery manifest, generated from the endpoint catalog."""
    return {
        "x402Version": 2,
        "service": {
            "id": "bhrefs",
            "name": "bhrefs — SEO Tools for AI Agents",
            "description": "A suite of SEO tools built for AI agents. Backlink discovery, competitive gap analysis, and email enrichment — all pay-per-query with USDC via x402.",
            "category": "data",
            "website": "https://bhrefs.com",
            "documentation": "https://bhrefs.com/llms.txt",
            "servicesManifest": "https://bhrefs.com/services.json",
        },
        "payment": {
            "schemes": ["exact"],
            "networks": [EVM_NETWORK],
            "asset": {
                "symbol": "USDC",
                "decimals": 6,
                "address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                "chain": "Base",
            },
            "payTo": EVM_ADDRESS,
            "facilitator": FACILITATOR_URL,
        },
        "endpoints": [
            {
                "method": e["method"],
                "path": e["path"],
                "description": e["description"],
                "accepts": [
                    {
                        "scheme": "exact",
                        "network": EVM_NETWORK,
                        "asset": "USDC",
                        "amount": e["amount_atomic"],
                        "amountDisplay": e["price_usd"],
                        "payTo": EVM_ADDRESS,
                    }
                ] if e["amount_atomic"] else [],
                "input": {
                    "type": "http",
                    "method": e["method"],
                    **({"queryParams": e["query_params"]} if e["query_params"] else {}),
                },
                "output": (
                    {"type": "json", "example": e["output_example"]}
                    if e["output_example"] is not None
                    else {"type": "json"}
                ),
            }
            for e in ENDPOINT_CATALOG
        ],
    }


@app.get("/llms.txt")
async def llms_txt():
    """LLMs.txt convention for AI crawler discovery."""
    from fastapi.responses import PlainTextResponse
    lines = [
        "# bhrefs — SEO Tools for AI Agents",
        "> A suite of SEO tools built for AI agents. Pay per query with USDC via x402.",
        "",
        "## Endpoints",
    ]
    for e in ENDPOINT_CATALOG:
        price = f"{e['price_usd']} USDC" if e["price_usd"] else "Free"
        lines.append(f"- {e['method']} {e['path']} — {price} — {e['description']}")
    lines += [
        "",
        "## Payment",
        "- Protocol: x402 (HTTP 402 micropayments)",
        "- Currency: USDC on Base",
        "- No API keys or accounts needed",
        "- Agent discovery: GET /.well-known/x402.json",
        "",
        "## Links",
        "- Website: https://bhrefs.com",
        "- Services manifest: https://bhrefs.com/services.json",
        "",
    ]
    return PlainTextResponse("\n".join(lines), media_type="text/plain")


@app.get("/robots.txt")
async def robots_txt():
    """Robots.txt with explicit AI crawler access."""
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse(
        "User-agent: *\n"
        "Allow: /\n"
        "\n"
        "# AI crawlers\n"
        "User-agent: GPTBot\n"
        "Allow: /\n"
        "\n"
        "User-agent: ClaudeBot\n"
        "Allow: /\n"
        "\n"
        "User-agent: PerplexityBot\n"
        "Allow: /\n"
        "\n"
        "User-agent: Google-Extended\n"
        "Allow: /\n"
        "\n"
        "Sitemap: https://bhrefs.com/sitemap.xml\n",
        media_type="text/plain",
    )


@app.get("/enrichment")
async def enrichment_page():
    return FileResponse("static/enrichment.html")


@app.get("/")
async def root(request: Request):
    # Serve landing page for browsers, JSON for API clients
    accept = request.headers.get("accept", "")
    if "text/html" in accept:
        return FileResponse("static/index.html")
    endpoints = {
        e["path"]: f"{e['description']} ({e['price_usd']} USDC)" if e["price_usd"]
        else f"{e['description']} (free)"
        for e in ENDPOINT_CATALOG
    }
    endpoints["/.well-known/x402.json"] = "Agent discovery"
    return {
        "service": "bhrefs — SEO Tools for AI Agents",
        "version": "0.1.0",
        "description": "SEO tools built for AI agents. Pay per query with USDC via x402.",
        "endpoints": endpoints,
        "payment": "x402 protocol — USDC on Base network",
    }



@app.get("/sitemap.xml")
async def sitemap_xml():
    """Sitemap for search engines."""
    from fastapi.responses import Response
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://bhrefs.com/</loc>
    <changefreq>weekly</changefreq>
    <priority>1.0</priority>
  </url>
  <url>
    <loc>https://bhrefs.com/enrichment</loc>
    <changefreq>monthly</changefreq>
    <priority>0.6</priority>
  </url>
</urlset>"""
    return Response(content=xml, media_type="application/xml")

@app.get("/health")
async def health():
    return {"status": "ok", "service": "backlink-finder", "version": "0.1.0"}


@app.get("/parquet-status")
async def parquet_status():
    """Conversion / data-freshness status for the Common Crawl parquet store.

    Exposes counts and sizes — no internal paths — so monitoring can detect
    when the fast path is unavailable or when a new release needs conversion.
    """
    from crawler import PARQUET_DIR, _parquet_available
    edges_dir = os.path.join(PARQUET_DIR, "edges")
    verts_path = os.path.join(PARQUET_DIR, "vertices.parquet")

    vertices_mb = (
        round(os.path.getsize(verts_path) / 1024**2, 1)
        if os.path.isfile(verts_path) else None
    )
    bucket_files = (
        [f for f in os.listdir(edges_dir) if f.endswith(".parquet")]
        if os.path.isdir(edges_dir) else []
    )
    edges_total_gb = (
        round(sum(os.path.getsize(os.path.join(edges_dir, f)) for f in bucket_files) / 1024**3, 2)
        if bucket_files else None
    )
    last_modified = None
    if bucket_files:
        last_modified = max(
            os.path.getmtime(os.path.join(edges_dir, f)) for f in bucket_files
        )

    return {
        "available": _parquet_available(),
        "vertices_parquet_mb": vertices_mb,
        "edge_bucket_count": len(bucket_files),
        "edges_total_gb": edges_total_gb,
        "edges_last_modified_unix": last_modified,
    }


def authority_score(num_hosts: int) -> int:
    """Derive a 0–100 authority score from Common Crawl host count."""
    if num_hosts <= 0:
        return 0
    return min(100, round(math.log10(num_hosts) * 15))


# UGC/hosting platforms filtered from preview to show more interesting backlinks
_PREVIEW_EXCLUDE = {
    "blogspot.com", "wordpress.com", "weebly.com", "wixsite.com", "wix.com",
    "webflow.io", "livejournal.com", "github.io", "substack.com", "amazonaws.com",
    "neocities.org", "wpengine.com", "fc2.com", "mystrikingly.com", "gitbook.io",
    "vercel.app", "netlify.app", "herokuapp.com", "medium.com", "tumblr.com",
    "squarespace.com", "godaddysites.com", "hatenablog.com", "over-blog.com",
    "typepad.com", "jimdofree.com", "sites.google.com", "wikidot.com",
    "uptodown.com", "informer.com", "softonic.com", "myshopify.com",
    "free.fr", "cloudfront.net", "seesaa.net", "aptoide.com",
}


@app.get("/preview/{domain}")
async def preview_backlinks(domain: str):
    """Free preview: top 5 notable backlinks for cached domains."""
    try:
        domain = validate_domain(domain)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    con = get_db()
    row = con.execute(
        "SELECT id, release, crawled_at, result_count FROM crawls WHERE target = ? "
        "ORDER BY crawled_at DESC LIMIT 1",
        (domain,),
    ).fetchone()

    if not row:
        con.close()
        raise HTTPException(status_code=404, detail=f"No cached data for {domain}")

    crawl_id, release, crawled_at, total = row
    # Fetch more than 5 so we can filter out generic platforms
    results = con.execute(
        "SELECT linking_domain, num_hosts FROM backlinks WHERE crawl_id = ? "
        "ORDER BY num_hosts DESC LIMIT 50",
        (crawl_id,),
    ).fetchall()
    con.close()

    filtered = [
        r for r in results if r[0] not in _PREVIEW_EXCLUDE
    ][:5]

    return {
        "domain": domain,
        "total_backlinks": total,
        "preview": True,
        "showing": len(filtered),
        "backlinks": [
            {"linking_domain": r[0], "num_hosts": r[1], "authority_score": authority_score(r[1])}
            for r in filtered
        ],
        "upgrade": f"Pay $0.10 USDC via /backlinks/{domain} for all {total} results",
    }


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


# ── Email Finder DB + helpers ───────────────────────────────────────


def _init_email_db():
    os.makedirs(os.path.dirname(EMAIL_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(EMAIL_DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lookups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT, last_name TEXT, domain TEXT,
            email TEXT, confidence TEXT, title TEXT, company TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS monthly_usage (
            month TEXT PRIMARY KEY, count INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS monthly_usage_by_payer (
            month TEXT, payer TEXT, count INTEGER DEFAULT 0,
            PRIMARY KEY (month, payer)
        )
    """)
    conn.commit()
    conn.close()


if APOLLO_API_KEY:
    _init_email_db()


def _email_db() -> sqlite3.Connection:
    conn = sqlite3.connect(EMAIL_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _current_month() -> str:
    return time.strftime("%Y-%m")


def _get_monthly_count() -> int:
    db = _email_db()
    row = db.execute("SELECT count FROM monthly_usage WHERE month = ?", (_current_month(),)).fetchone()
    db.close()
    return row["count"] if row else 0


def _increment_monthly_count():
    db = _email_db()
    db.execute(
        "INSERT INTO monthly_usage (month, count) VALUES (?, 1) ON CONFLICT(month) DO UPDATE SET count = count + 1",
        (_current_month(),),
    )
    db.commit()
    db.close()


def _get_payer_address(request: Request) -> str | None:
    """Extract the verified payer's wallet address from the x402 middleware.

    The middleware sets request.state.payment_payload after a successful
    verification; for the EVM exact scheme the payer is at
    payload["authorization"]["from"].
    """
    pp = getattr(request.state, "payment_payload", None)
    if pp is None:
        return None
    payload = getattr(pp, "payload", None) or (pp.get("payload") if isinstance(pp, dict) else None)
    if not isinstance(payload, dict):
        return None
    auth = payload.get("authorization") or {}
    addr = auth.get("from") or ""
    return addr.lower() or None


def _get_payer_monthly_count(payer: str) -> int:
    db = _email_db()
    row = db.execute(
        "SELECT count FROM monthly_usage_by_payer WHERE month = ? AND payer = ?",
        (_current_month(), payer),
    ).fetchone()
    db.close()
    return row["count"] if row else 0


def _increment_payer_monthly_count(payer: str):
    db = _email_db()
    db.execute(
        "INSERT INTO monthly_usage_by_payer (month, payer, count) VALUES (?, ?, 1) "
        "ON CONFLICT(month, payer) DO UPDATE SET count = count + 1",
        (_current_month(), payer),
    )
    db.commit()
    db.close()


def _check_email_cache(first_name: str, last_name: str, domain: str) -> dict | None:
    """Return the cached row if we've ever queried this person+domain, else None.

    The row may represent a positive (email present), a negative-with-person
    (Apollo found a person but no email — company/title populated), or a
    negative-no-person (all fields empty). The caller decides how to respond.
    """
    db = _email_db()
    row = db.execute(
        "SELECT email, confidence, title, company FROM lookups WHERE lower(first_name)=? AND lower(last_name)=? AND lower(domain)=?",
        (first_name.lower(), last_name.lower(), domain.lower()),
    ).fetchone()
    db.close()
    return dict(row) if row else None


def _save_email_lookup(first_name, last_name, domain, email, confidence, title, company):
    db = _email_db()
    db.execute(
        "INSERT INTO lookups (first_name, last_name, domain, email, confidence, title, company) VALUES (?,?,?,?,?,?,?)",
        (first_name, last_name, domain, email, confidence, title, company),
    )
    db.commit()
    db.close()


async def _apollo_people_match(first_name: str, last_name: str, domain: str) -> dict:
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            "https://api.apollo.io/api/v1/people/match",
            headers={"x-api-key": APOLLO_API_KEY, "Content-Type": "application/json"},
            json={"first_name": first_name, "last_name": last_name, "organization_name": domain, "domain": domain},
        )
        if resp.status_code != 200:
            return {"error": f"Apollo returned {resp.status_code}"}
        person = resp.json().get("person")
        if not person:
            return {"error": "No match found", "definitive": True}
        return {
            "email": person.get("email", ""),
            "confidence": "high" if person.get("email_status") == "verified" else person.get("email_status", "unknown"),
            "first_name": person.get("first_name", first_name),
            "last_name": person.get("last_name", last_name),
            "title": person.get("title", ""),
            "company": person.get("organization", {}).get("name", ""),
        }


@app.get("/find")
async def find_email(
    request: Request,
    first_name: str = Query(..., description="Person's first name"),
    last_name: str = Query(..., description="Person's last name"),
    domain: str = Query(..., description="Company domain (e.g. acme.com)"),
):
    """Find a verified business email. Costs $0.05 USDC."""
    if not APOLLO_API_KEY:
        raise HTTPException(status_code=503, detail="Email finder not configured")

    if _get_monthly_count() >= MONTHLY_LIMIT:
        raise HTTPException(status_code=429, detail=f"Monthly limit of {MONTHLY_LIMIT} lookups reached. Resets on the 1st.")

    payer = _get_payer_address(request)
    if payer and _get_payer_monthly_count(payer) >= PER_PAYER_MONTHLY_LIMIT:
        raise HTTPException(
            status_code=429,
            detail=f"Per-wallet monthly limit of {PER_PAYER_MONTHLY_LIMIT} lookups reached. Resets on the 1st.",
        )

    cached = _check_email_cache(first_name, last_name, domain)
    if cached is not None:
        if cached["email"]:
            return {
                "email": cached["email"], "email_found": True,
                "confidence": cached["confidence"],
                "first_name": first_name, "last_name": last_name,
                "title": cached["title"], "company": cached["company"],
                "domain": domain, "cached": True,
            }
        if cached["company"] or cached["title"]:
            return {
                "email": None, "email_found": False,
                "confidence": "unavailable",
                "first_name": first_name, "last_name": last_name,
                "title": cached["title"] or "", "company": cached["company"] or "",
                "domain": domain, "cached": True,
            }
        raise HTTPException(status_code=404, detail="No match found")

    result = await _apollo_people_match(first_name, last_name, domain)
    if "error" in result:
        if result.get("definitive"):
            _save_email_lookup(first_name, last_name, domain, "", "", "", "")
        raise HTTPException(status_code=404, detail=result["error"])

    email = result["email"] or None
    _save_email_lookup(
        first_name, last_name, domain,
        email or "", result["confidence"] if email else "",
        result["title"], result["company"],
    )
    _increment_monthly_count()
    if payer:
        _increment_payer_monthly_count(payer)

    return {
        "email": email, "email_found": bool(email),
        "confidence": result["confidence"] if email else "unavailable",
        "first_name": result["first_name"], "last_name": result["last_name"],
        "title": result["title"], "company": result["company"],
        "domain": domain, "cached": False,
    }


# ── Paid endpoints ──────────────────────────────────────────────────


@app.get("/backlinks/{domain}")
async def get_backlinks(domain: str):
    """Get all backlinks for a domain. Crawls on-demand if not cached. Costs $0.10 USDC."""
    try:
        domain = validate_domain(domain)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    con = get_db()

    row = con.execute(
        "SELECT id, release, crawled_at FROM crawls WHERE target = ? "
        "ORDER BY crawled_at DESC LIMIT 1",
        (domain,),
    ).fetchone()

    if not row:
        con.close()
        # On-demand crawl — enforce global concurrency limit
        with _crawl_count_lock:
            if _active_crawls[0] >= _MAX_CONCURRENT_CRAWLS:
                raise HTTPException(
                    status_code=429,
                    detail=f"Too many crawls in progress ({_MAX_CONCURRENT_CRAWLS}). Try again later.",
                )
        lock = _crawl_locks.setdefault(domain, threading.Lock())
        if not lock.acquire(blocking=False):
            raise HTTPException(
                status_code=409,
                detail=f"Crawl already in progress for {domain}. Try again in a few minutes.",
            )
        with _crawl_count_lock:
            _active_crawls[0] += 1
        try:
            results, crawl_id = crawl_and_store(domain, DB_PATH)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Crawl failed: {e}")
        finally:
            with _crawl_count_lock:
                _active_crawls[0] -= 1
            lock.release()
            _crawl_locks.pop(domain, None)

        return {
            "domain": domain,
            "release": "cc-main-2026-jan-feb-mar",
            "crawled_at": "just now",
            "backlink_count": len(results),
            "source": "live_crawl",
            "backlinks": [
                {
                    "linking_domain": r["domain"],
                    "num_hosts": r["num_hosts"],
                    "authority_score": authority_score(r["num_hosts"]),
                    "page_rank": None,
                    "majestic_rank": None,
                    "ref_subnets": None,
                    "ref_ips": None,
                    "tranco_rank": None,
                }
                for r in results
            ],
        }

    # Cached path
    con = get_db()  # re-open in case crawl path closed it
    row = con.execute(
        "SELECT id, release, crawled_at FROM crawls WHERE target = ? "
        "ORDER BY crawled_at DESC LIMIT 1",
        (domain,),
    ).fetchone()

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
                "authority_score": authority_score(r[1]),
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
        nh = row[0] if row else None
        gap_results.append({
            "domain": d,
            "num_hosts": nh,
            "authority_score": authority_score(nh) if nh else None,
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
