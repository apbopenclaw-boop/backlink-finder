/**
 * Quick test: call get_backlinks via the MCP server with x402 payment.
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";

const transport = new StdioClientTransport({
  command: "node",
  args: ["index.mjs"],
  env: {
    ...process.env,
    X402_WALLET_PRIVATE_KEY: process.env.X402_WALLET_PRIVATE_KEY,
  },
});

const client = new Client({ name: "test-client", version: "1.0.0" });
await client.connect(transport);

console.log("Connected. Tools:", (await client.listTools()).tools.map(t => t.name));

// Test free endpoint first
console.log("\n--- Preview (free) ---");
const preview = await client.callTool({ name: "preview_backlinks", arguments: { domain: "tesla.com" } });
const previewData = JSON.parse(preview.content[0].text);
console.log(`${previewData.domain}: ${previewData.total_backlinks} total, showing ${previewData.showing}`);
for (const b of previewData.backlinks) {
  console.log(`  ${b.linking_domain.padEnd(30)} auth=${b.authority_score}`);
}

// Test paid endpoint
console.log("\n--- Full backlinks (paid $0.10) ---");
try {
  const full = await client.callTool({ name: "get_backlinks", arguments: { domain: "tesla.com" } });
  const fullData = JSON.parse(full.content[0].text);
  if (fullData.error) {
    console.log("Error:", fullData.error);
    if (fullData.txHash) console.log("TX:", fullData.txHash);
  } else {
    console.log(`${fullData.domain}: ${fullData.backlink_count} backlinks returned`);
    console.log("First 3:", fullData.backlinks?.slice(0, 3).map(b => b.linking_domain));
  }
} catch (e) {
  console.log("Error:", e.message);
}

await client.close();
process.exit(0);
