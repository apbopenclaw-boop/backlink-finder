"""
Backlink Finder API — x402 micropayment service.

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

if not EVM_ADDRESS:
    raise ValueError("Set EVM_ADDRESS in .env")

# ── FastAPI app ─────────────────────────────────────────────────────

app = FastAPI(
    title="Backlink Finder API",
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

routes = {
    "GET /backlinks/*": RouteConfig(
        accepts=[
            PaymentOption(
                scheme="exact",
                pay_to=EVM_ADDRESS,
                price="$0.10",
                network=EVM_NETWORK,
            ),
        ],
        mime_type="application/json",
        description="Get all backlinks for any domain. Crawls on-demand from Common Crawl if not cached (may take 2-5 min for new domains).",
        extensions={
            "bazaar": {
                "info": {
                    "input": {
                        "type": "http",
                        "method": "GET",
                        "queryParams": {
                            "domain": "example.com"
                        },
                    },
                    "output": {
                        "type": "json",
                        "example": {
                            "domain": "example.com",
                            "backlink_count": 142,
                            "backlinks": [{"linking_domain": "github.com", "num_hosts": 6038, "authority_score": 57}],
                        },
                    },
                },
                "schema": {
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
                                    "properties": {
                                        "domain": {"type": "string", "description": "Target domain"}
                                    },
                                    "required": ["domain"],
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
                },
            }
        },
    ),
    "GET /find": RouteConfig(
        accepts=[
            PaymentOption(
                scheme="exact",
                pay_to=EVM_ADDRESS,
                price="$0.05",
                network=EVM_NETWORK,
            ),
        ],
        mime_type="application/json",
        description="Find a verified business email for a person given their name and company domain.",
        extensions={
            "bazaar": {
                "info": {
                    "input": {
                        "type": "http",
                        "method": "GET",
                        "queryParams": {
                            "first_name": "John",
                            "last_name": "Smith",
                            "domain": "acme.com",
                        },
                    },
                    "output": {
                        "type": "json",
                        "example": {
                            "email": "john.smith@acme.com",
                            "confidence": 0.92,
                            "first_name": "John",
                            "last_name": "Smith",
                            "title": "VP Engineering",
                            "company": "Acme Corp",
                            "domain": "acme.com",
                        },
                    },
                },
                "schema": {
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
                                    "properties": {
                                        "first_name": {"type": "string"},
                                        "last_name": {"type": "string"},
                                        "domain": {"type": "string"},
                                    },
                                    "required": ["first_name", "last_name", "domain"],
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
                },
            }
        },
    ),
    "GET /gap": RouteConfig(
        accepts=[
            PaymentOption(
                scheme="exact",
                pay_to=EVM_ADDRESS,
                price="$0.15",
                network=EVM_NETWORK,
            ),
        ],
        mime_type="application/json",
        description="Gap analysis: find domains linking to competitor but not to you",
        extensions={
            "bazaar": {
                "info": {
                    "input": {
                        "type": "http",
                        "method": "GET",
                        "queryParams": {
                            "yours": "mysite.com",
                            "competitor": "competitor.com",
                        },
                    },
                    "output": {
                        "type": "json",
                        "example": {
                            "gap_count": 847,
                            "opportunities": [{"domain": "techcrunch.com", "authority_score": 89}],
                        },
                    },
                },
                "schema": {
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
                                    "properties": {
                                        "yours": {"type": "string"},
                                        "competitor": {"type": "string"},
                                    },
                                    "required": ["yours", "competitor"],
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
                },
            }
        },
    ),
}

# Track in-progress crawls to prevent duplicate work
_crawl_locks: dict[str, threading.Lock] = {}
_MAX_CONCURRENT_CRAWLS = 3
_active_crawls = [0]
_crawl_count_lock = threading.Lock()
app.add_middleware(PaymentMiddlewareASGI, routes=routes, server=server)

# Serve .well-known for agent discovery
app.mount("/.well-known", StaticFiles(directory=".well-known"), name="well-known")

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
        "id": "backlink-finder",
        "name": "Backlink Finder",
        "description": "Backlink discovery API for AI agents. Find every domain linking to any target using web-scale crawl data.",
        "category": "data",
        "x402Version": 2,
        "networks": [EVM_NETWORK.split(":")[0] + ":" + EVM_NETWORK.split(":")[1]],
        "website": "https://backlink-finder.fly.dev",
        "endpoints": [
            {
                "method": "GET",
                "path": "/backlinks/{domain}",
                "description": "Get all backlinks for any domain. Crawls on-demand if not cached.",
                "price": "$0.10",
                "currency": "USDC",
            },
            {
                "method": "GET",
                "path": "/gap",
                "description": "Gap analysis: find domains linking to competitor but not to you.",
                "price": "$0.15",
                "currency": "USDC",
            },
            {
                "method": "GET",
                "path": "/preview/{domain}",
                "description": "Free preview: top 5 backlinks for any cached domain.",
                "price": "$0.00",
                "currency": "USDC",
            },
            {
                "method": "GET",
                "path": "/domains",
                "description": "Free: list all cached domains with backlink counts.",
                "price": "$0.00",
                "currency": "USDC",
            },
            {
                "method": "GET",
                "path": "/find",
                "description": "Find verified business email given first name, last name, and company domain.",
                "price": "$0.05",
                "currency": "USDC",
            },
        ],
    }


@app.get("/llms.txt")
async def llms_txt():
    """LLMs.txt convention for AI crawler discovery."""
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse(
        "# Backlink Finder\n"
        "> Backlink discovery API for AI agents. Pay per query with USDC via x402.\n"
        "\n"
        "## Endpoints\n"
        "- GET /backlinks/{domain} — $0.10 USDC — All backlinks for any domain\n"
        "- GET /gap?yours=X&competitor=Y — $0.15 USDC — Gap analysis between two domains\n"
        "- GET /preview/{domain} — Free — Top 5 backlinks for cached domains\n"
        "- GET /domains — Free — List all cached domains\n"
        "- GET /find?first_name=John&last_name=Doe&domain=acme.com — $0.05 USDC — Find verified business email\n"
        "- GET /health — Free — Health check\n"
        "\n"
        "## Payment\n"
        "- Protocol: x402 (HTTP 402 micropayments)\n"
        "- Currency: USDC on Base\n"
        "- No API keys or accounts needed\n"
        "- Agent discovery: GET /.well-known/x402.json\n"
        "\n"
        "## Links\n"
        "- Website: https://backlink-finder.fly.dev\n"
        "- Services manifest: https://backlink-finder.fly.dev/services.json\n",
        media_type="text/plain",
    )


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
        "Sitemap: https://backlink-finder.fly.dev/sitemap.xml\n",
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
    return {
        "service": "Backlink Finder API",
        "version": "0.1.0",
        "description": "Find backlinks to any domain using Common Crawl data. Pay per query with USDC.",
        "endpoints": {
            "/domains": "List available domains (free)",
            "/backlinks/{domain}": "Get all backlinks ($0.10 USDC)",
            "/gap?yours=X&competitor=Y": "Gap analysis ($0.15 USDC)",
            "/health": "Health check (free)",
            "/.well-known/x402.json": "Agent discovery",
        },
        "payment": "x402 protocol — USDC on Base network",
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "backlink-finder", "version": "0.1.0"}


# /debug/x402 removed — exposed facilitator URL and request state


# /debug/storage removed — exposed filesystem layout and data sizes
async def _debug_storage_disabled():
    """Removed: exposed filesystem layout."""
    import glob
    from crawler import PARQUET_DIR, CACHE_DIR, _parquet_available
    parquet_dir = PARQUET_DIR
    info = {
        "parquet_dir": parquet_dir,
        "parquet_available": _parquet_available(),
        "vertices_exists": os.path.isfile(os.path.join(parquet_dir, "vertices.parquet")),
        "edges_dir_exists": os.path.isdir(os.path.join(parquet_dir, "edges")),
    }
    vp = os.path.join(parquet_dir, "vertices.parquet")
    if os.path.isfile(vp):
        info["vertices_size_mb"] = round(os.path.getsize(vp) / 1024 / 1024, 1)
    edges_dir = os.path.join(parquet_dir, "edges")
    if os.path.isdir(edges_dir):
        files = [f for f in os.listdir(edges_dir) if f.endswith(".parquet")]
        info["edge_bucket_count"] = len(files)
        info["edge_buckets_sample"] = sorted(files)[:5]
        if files:
            total = sum(os.path.getsize(os.path.join(edges_dir, f)) for f in files)
            info["edges_total_size_gb"] = round(total / 1024**3, 2)
    # Show intermediate raw parquet (phase 1 output)
    raw_pq = os.path.join(parquet_dir, "edges_raw.parquet")
    if os.path.isfile(raw_pq):
        info["edges_raw_parquet_gb"] = round(os.path.getsize(raw_pq) / 1024**3, 2)
    # Also show what's at top level of parquet dir
    if os.path.isdir(parquet_dir):
        info["parquet_dir_contents"] = os.listdir(parquet_dir)
    else:
        info["parquet_dir_contents"] = "DIRECTORY DOES NOT EXIST"
    # Show temp dirs to monitor conversion progress
    for tmp_name, tmp_path in [("tmp_edges_part", "/data/tmp_edges_part"), ("tmp_duckdb", "/data/tmp_duckdb")]:
        if os.path.isdir(tmp_path):
            contents = os.listdir(tmp_path)
            total_size = 0
            for item in contents:
                item_path = os.path.join(tmp_path, item)
                if os.path.isfile(item_path):
                    total_size += os.path.getsize(item_path)
                elif os.path.isdir(item_path):
                    for f in os.listdir(item_path):
                        fp = os.path.join(item_path, f)
                        if os.path.isfile(fp):
                            total_size += os.path.getsize(fp)
            info[tmp_name] = {"exists": True, "items": len(contents), "size_mb": round(total_size/1024/1024, 1)}
        else:
            info[tmp_name] = {"exists": False}
    # Show cache dir
    cache_dir = CACHE_DIR
    info["cache_dir"] = cache_dir
    if os.path.isdir(cache_dir):
        info["cache_dir_contents"] = []
        for root, dirs, fnames in os.walk(cache_dir):
            for fn in fnames:
                fp = os.path.join(root, fn)
                info["cache_dir_contents"].append({"path": fp, "size_mb": round(os.path.getsize(fp)/1024/1024, 1)})
    return info


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


def _check_email_cache(first_name: str, last_name: str, domain: str) -> dict | None:
    db = _email_db()
    row = db.execute(
        "SELECT email, confidence, title, company FROM lookups WHERE lower(first_name)=? AND lower(last_name)=? AND lower(domain)=?",
        (first_name.lower(), last_name.lower(), domain.lower()),
    ).fetchone()
    db.close()
    return dict(row) if row and row["email"] else None


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
            return {"error": "No match found"}
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
    first_name: str = Query(..., description="Person's first name"),
    last_name: str = Query(..., description="Person's last name"),
    domain: str = Query(..., description="Company domain (e.g. acme.com)"),
):
    """Find a verified business email. Costs $0.01 USDC."""
    if not APOLLO_API_KEY:
        raise HTTPException(status_code=503, detail="Email finder not configured")

    usage = _get_monthly_count()
    if usage >= MONTHLY_LIMIT:
        raise HTTPException(status_code=429, detail=f"Monthly limit of {MONTHLY_LIMIT} lookups reached. Resets on the 1st.")

    cached = _check_email_cache(first_name, last_name, domain)
    if cached:
        return {
            "email": cached["email"], "confidence": cached["confidence"],
            "first_name": first_name, "last_name": last_name,
            "title": cached["title"], "company": cached["company"],
            "domain": domain, "cached": True,
        }

    result = await _apollo_people_match(first_name, last_name, domain)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    _save_email_lookup(first_name, last_name, domain, result["email"], result["confidence"], result["title"], result["company"])
    _increment_monthly_count()

    return {
        "email": result["email"], "confidence": result["confidence"],
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
