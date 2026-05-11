# Marketing site update prompt — bhrefs.com

Copy-paste-ready brief for the marketing-site bot.

---

## TASK
Update the bhrefs marketing site (bhrefs.com — backlinks/SEO product) to reflect three changes shipped between 2026-05-10 and 2026-05-11. Do **not** touch the `/enrichment` page (that's the email-finder product).

## WHAT CHANGED

### 1. Price cut: `/backlinks` is now $0.01 (was $0.10)
A 10× reduction. Anywhere the old price appears on the site, replace it.

### 2. New feature: Domain Rating (DR), per linker and per target
Every `/backlinks` response now includes a `dr_score` (0-100) for each linking domain. It's the Ahrefs DR equivalent, derived from the Common Crawl Web Graph harmonic centrality on a log-rank scale.

| harmonic_rank | dr_score |
|---|---|
| 1 (googleapis.com) | 100 |
| 24 (github.com) | 92 |
| 63 (nytimes.com) | 78 |
| 1 000 | 63 |
| 1 000 000 | 25 |
| 5 000 000 | 17 |

Top 5 M domains covered. Anything outside top 5 M returns `dr_score: null`.

### 3. New endpoint: `/bundle` ($0.015 combo)
Target's own DR + full backlinks list with per-linker DR, in one paid call.

### 4. Bonus: free DR preview
`/preview/{domain}` (already free, already on the site) now also returns:
- `target_dr_score` — the queried domain's own DR
- `dr_score` per backlink in the preview
- An updated `upgrade` message that mentions both `/backlinks` and `/bundle`

Agents can sanity-check the DR distribution before paying.

## ENDPOINTS — CANONICAL LIST FOR THE PRICING TABLE

| Endpoint | Price | What you get |
|---|---|---|
| `GET /backlinks?domain=X` | **$0.01** | Full backlinks list, each with `dr_score`, `harmonic_rank`, `pagerank`, plus existing signals (Majestic, Tranco, authority_score, ref_subnets, ref_ips). Sorted by DR DESC. |
| `GET /bundle?domain=X` | **$0.015** | Target's own DR profile + the full backlinks list. The combo deal — saves vs buying separately. |
| `GET /gap?yours=X&competitor=Y` | $0.10 | Domains linking to competitor but not to you. Each gap entry includes DR. |
| `GET /find?first_name=…&last_name=…&domain=…` | $0.05 | Email finder (separate product, on `/enrichment`). |
| `GET /preview/{domain}` | free | Top-5 backlinks preview with DR. Decide whether to pay before paying. |
| `GET /domains` | free | List of cached domains. |
| `GET /health`, `/.well-known/x402.json`, `/services.json`, `/llms.txt` | free | Agent-discovery + diagnostics. |

## RESPONSE SHAPE — `/backlinks` and `/bundle`

```json
{
  "domain": "nytimes.com",
  "target_dr_score": 78,                  // /bundle only
  "target_harmonic_rank": 63,             // /bundle only
  "target_pagerank": 0.00012,             // /bundle only
  "target_n_hosts": 56,                   // /bundle only
  "backlink_count": 182104,
  "release": "cc-main-2026-jan-feb-mar",
  "crawled_at": "2026-05-09T…",
  "backlinks": [
    {
      "linking_domain": "googleapis.com",
      "num_hosts": 2899,
      "dr_score": 100,
      "harmonic_rank": 1,
      "pagerank": 0.01435,
      "authority_score": 52,
      "majestic_rank": null,
      "ref_subnets": null,
      "ref_ips": null,
      "tranco_rank": null
    },
    ...
  ]
}
```

## SUGGESTED HERO COPY

> **Backlinks with Domain Rating, $0.01 per query.**
>
> Every link gets a DR score (0–100), same shape as Ahrefs DR — derived from the open Common Crawl Web Graph. Sorted by authority, paid in USDC, no API keys, no subscriptions. Free preview lets your agent check the DR distribution before paying.

## SUGGESTED BULLETS

- **DR per linker.** 0–100 Domain Rating for every linking domain, same log-rank shape as Ahrefs DR. Sorted strongest first.
- **DR per target.** `/bundle` returns the queried domain's own DR alongside its backlinks.
- **Free preview with DR.** `/preview/{domain}` is free and shows the top 5 backlinks **with DR** plus the target's DR — sanity-check before paying.
- **Open data, no API keys.** DR comes from the Common Crawl Web Graph (`cc-main-2025-feb-mar-apr`, 109 M domains, refreshed quarterly). Verifiable, transparent, not a black box.
- **$0.01 per query.** 10× cheaper than two weeks ago. Combo deal at $0.015.

## FAQ ENTRY — "How does this DR compare to Ahrefs?"

> Both Ahrefs DR and our `dr_score` rank domains 0–100 on a logarithmic scale of backlink authority. The headline difference is the source crawl: Ahrefs uses their private crawler; we use the public Common Crawl Web Graph. For most well-known domains the scores agree within 10–15 points; the very long tail (rank > 5 M) is `null` rather than scored.

## CODE EXAMPLE FOR THE LANDING PAGE

```bash
# Free preview — includes target DR + per-linker DR
curl https://backlink-finder.fly.dev/preview/nytimes.com

# Paid: full backlinks list — $0.01 USDC
curl https://backlink-finder.fly.dev/backlinks?domain=nytimes.com \
     -H "X-Payment: <eip3009-payload>"

# Paid combo: target DR + backlinks — $0.015 USDC
curl https://backlink-finder.fly.dev/bundle?domain=nytimes.com \
     -H "X-Payment: <eip3009-payload>"
```

## METADATA / SEO

- Title: "bhrefs — DR-Scored Backlinks API for AI Agents, $0.01/query"
- Description: "Pay-per-query backlink data with Domain Rating (0–100) for every linking site. Free preview, USDC via x402, no API keys."
- OG description: "Backlinks + DR for $0.01. Same shape as Ahrefs DR, from open Common Crawl data. Built for AI agents."

## STRUCTURED DATA (schema.org Offer)

```json
{
  "@type": "Offer",
  "price": "0.01",
  "priceCurrency": "USD",
  "description": "Per-query backlinks with DR via USDC micropayments"
}
```

## DO NOT TOUCH
- `/enrichment` — that's the email-finder product, separate.
- The `/find` and `/gap` prices — those are unchanged.

## VERIFICATION CHECKLIST AFTER UPDATE
- [ ] No `$0.10` remains on the backlinks pages
- [ ] DR score is mentioned in the hero, the feature list, and at least one code example
- [ ] `/bundle` listed in the pricing table with the $0.015 price
- [ ] Free preview is positioned as a pre-purchase check, not just a marketing teaser
- [ ] OG image/title/description reflect the DR feature
