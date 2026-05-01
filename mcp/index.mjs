#!/usr/bin/env node
/**
 * Backlink Finder MCP Server
 *
 * Tools:
 *   - get_backlinks(domain)    — $0.10 USDC — all backlinks for a domain
 *   - gap_analysis(yours, competitor) — $0.15 USDC — gap analysis
 *   - preview_backlinks(domain) — free — top 5 backlinks
 *   - list_domains()           — free — list cached domains
 *
 * Env:
 *   X402_WALLET_PRIVATE_KEY — Base mainnet wallet private key (hex, with or without 0x)
 *   BACKLINK_API_URL — optional, defaults to https://backlink-finder.fly.dev
 */

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { ethers } from "ethers";

const API_URL = process.env.BACKLINK_API_URL || "https://backlink-finder.fly.dev";
const PRIVATE_KEY = process.env.X402_WALLET_PRIVATE_KEY;

// Base mainnet chain ID
const BASE_CHAIN_ID = 8453;

/**
 * Parse the x402 payment-required header from a 402 response.
 */
function parsePaymentRequired(headerValue) {
  const decoded = Buffer.from(headerValue, "base64").toString("utf-8");
  return JSON.parse(decoded);
}

/**
 * Sign an EIP-3009 transferWithAuthorization for x402 payment.
 * The facilitator pays gas — client just signs.
 */
async function signEIP3009Payment(wallet, accept) {
  const now = Math.floor(Date.now() / 1000);
  const validAfter = now.toString();
  const validBefore = (now + (accept.maxTimeoutSeconds || 300)).toString();
  const nonce = ethers.hexlify(ethers.randomBytes(32));

  const authorization = {
    from: wallet.address,
    to: accept.payTo,
    value: accept.amount,
    validAfter,
    validBefore,
    nonce,
  };

  // Get token name and version from the 402 response extra field
  const tokenName = accept.extra?.name || "USD Coin";
  const tokenVersion = accept.extra?.version || "2";

  // EIP-712 domain for USDC on Base
  const domain = {
    name: tokenName,
    version: tokenVersion,
    chainId: BASE_CHAIN_ID,
    verifyingContract: accept.asset,
  };

  // EIP-712 types for TransferWithAuthorization
  const types = {
    TransferWithAuthorization: [
      { name: "from", type: "address" },
      { name: "to", type: "address" },
      { name: "value", type: "uint256" },
      { name: "validAfter", type: "uint256" },
      { name: "validBefore", type: "uint256" },
      { name: "nonce", type: "bytes32" },
    ],
  };

  const signature = await wallet.signTypedData(domain, types, authorization);

  return { signature, authorization };
}

/**
 * Make an x402 payment and retry the request.
 */
async function payAndRetry(url, paymentInfo) {
  if (!PRIVATE_KEY) {
    return {
      error: "No wallet configured. Set X402_WALLET_PRIVATE_KEY env var to enable paid queries.",
      payment_required: paymentInfo,
    };
  }

  const accept = paymentInfo.accepts?.[0];
  if (!accept) {
    return { error: "No accepted payment methods in 402 response" };
  }

  const wallet = new ethers.Wallet(PRIVATE_KEY.startsWith("0x") ? PRIVATE_KEY : `0x${PRIVATE_KEY}`);

  // Sign EIP-3009 authorization (no gas needed — facilitator settles)
  const { signature, authorization } = await signEIP3009Payment(wallet, accept);

  // Build the x402 payment payload (must include full `accepted` requirements)
  const paymentPayload = {
    x402Version: paymentInfo.x402Version || 2,
    payload: {
      signature,
      authorization,
    },
    accepted: accept,
    resource: paymentInfo.resource || undefined,
  };
  const paymentHeader = Buffer.from(JSON.stringify(paymentPayload)).toString("base64");

  // Retry the original request with payment proof (v2 header)
  const retryResp = await fetch(url, {
    headers: { "PAYMENT-SIGNATURE": paymentHeader },
  });

  if (!retryResp.ok) {
    const errText = await retryResp.text();
    return { error: `Payment signed but request failed (${retryResp.status}): ${errText}` };
  }

  return await retryResp.json();
}

/**
 * Make an API request, handling 402 payments automatically.
 */
async function apiRequest(path) {
  const url = `${API_URL}${path}`;
  const resp = await fetch(url);

  if (resp.status === 402) {
    const paymentHeader = resp.headers.get("payment-required");
    if (!paymentHeader) {
      return { error: "Got 402 but no payment-required header" };
    }
    const paymentInfo = parsePaymentRequired(paymentHeader);
    return await payAndRetry(url, paymentInfo);
  }

  if (!resp.ok) {
    const errText = await resp.text();
    throw new Error(`API error ${resp.status}: ${errText}`);
  }

  return await resp.json();
}

// ── MCP Server ─────────────────────────────────────────────────────

const server = new McpServer({
  name: "backlink-finder",
  version: "1.0.0",
});

server.tool(
  "get_backlinks",
  "Get all backlinks for any domain. Costs $0.10 USDC via x402. Returns every domain linking to the target with host counts and authority scores.",
  { domain: z.string().describe("Target domain (e.g. tesla.com)") },
  async ({ domain }) => {
    try {
      const data = await apiRequest(`/backlinks/${encodeURIComponent(domain)}`);
      return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
    } catch (e) {
      return { content: [{ type: "text", text: `Error: ${e.message}` }], isError: true };
    }
  }
);

server.tool(
  "gap_analysis",
  "Find domains linking to a competitor but not to you. Costs $0.15 USDC via x402. Great for discovering link-building opportunities.",
  {
    yours: z.string().describe("Your domain (e.g. mysite.com)"),
    competitor: z.string().describe("Competitor domain (e.g. competitor.com)"),
  },
  async ({ yours, competitor }) => {
    try {
      const data = await apiRequest(`/gap?yours=${encodeURIComponent(yours)}&competitor=${encodeURIComponent(competitor)}`);
      return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
    } catch (e) {
      return { content: [{ type: "text", text: `Error: ${e.message}` }], isError: true };
    }
  }
);

server.tool(
  "preview_backlinks",
  "Free preview: get top 5 notable backlinks for any cached domain. No payment required.",
  { domain: z.string().describe("Target domain (e.g. tesla.com)") },
  async ({ domain }) => {
    try {
      const data = await apiRequest(`/preview/${encodeURIComponent(domain)}`);
      return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
    } catch (e) {
      return { content: [{ type: "text", text: `Error: ${e.message}` }], isError: true };
    }
  }
);

server.tool(
  "list_domains",
  "List all domains with cached backlink data. Free, no payment required.",
  {},
  async () => {
    try {
      const data = await apiRequest("/domains");
      return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
    } catch (e) {
      return { content: [{ type: "text", text: `Error: ${e.message}` }], isError: true };
    }
  }
);

// ── Start ──────────────────────────────────────────────────────────

const transport = new StdioServerTransport();
await server.connect(transport);
