# Backlink Finder MCP Server

MCP server for [Backlink Finder](https://backlink-finder.fly.dev) — discover backlinks to any domain. AI agents pay per query with USDC via x402.

## Tools

| Tool | Description | Cost |
|------|-------------|------|
| `get_backlinks` | All backlinks for any domain | $0.10 USDC |
| `gap_analysis` | Find link-building opportunities vs a competitor | $0.15 USDC |
| `preview_backlinks` | Top 5 backlinks for cached domains | Free |
| `list_domains` | List all cached domains | Free |

## Setup

### Claude Desktop / Claude Code

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "backlink-finder": {
      "command": "node",
      "args": ["/path/to/backlink-finder/mcp/index.mjs"],
      "env": {
        "X402_WALLET_PRIVATE_KEY": "0x..."
      }
    }
  }
}
```

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `X402_WALLET_PRIVATE_KEY` | For paid tools | Base mainnet wallet private key (hex). Free tools work without it. |
| `BACKLINK_API_URL` | No | API URL (default: `https://backlink-finder.fly.dev`) |

## How Payment Works

1. You call `get_backlinks("tesla.com")`
2. API returns HTTP 402 with USDC payment details
3. MCP server signs a USDC transfer from your wallet on Base
4. Retries the request with payment proof
5. Returns full backlink data

No API keys, no subscriptions — just USDC on Base mainnet.

## Wallet Setup

You need a wallet with USDC on Base mainnet:

1. Create a wallet (e.g., via MetaMask or any EVM wallet)
2. Bridge USDC to Base mainnet
3. Set the private key as `X402_WALLET_PRIVATE_KEY`

Even $1 is enough for 10 full backlink lookups.

## Try Free First

The `preview_backlinks` and `list_domains` tools work without any wallet. Try them to see the data before paying.
