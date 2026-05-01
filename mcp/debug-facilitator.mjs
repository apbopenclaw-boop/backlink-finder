#!/usr/bin/env node
/**
 * Test payment verification directly against x402.org facilitator.
 */
import { ethers } from "ethers";
import { readFileSync } from "fs";

const PRIVATE_KEY = readFileSync("/tmp/backlink-api/.test-wallet-key", "utf-8").trim();
const wallet = new ethers.Wallet(PRIVATE_KEY.startsWith("0x") ? PRIVATE_KEY : `0x${PRIVATE_KEY}`);

console.log("Wallet:", wallet.address);

// Build the same payment as our debug script
const accept = {
  scheme: "exact",
  network: "eip155:8453",
  asset: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
  amount: "100000",
  payTo: "0x02cdA6109aDc825E29287B4f7c1C72ae2f14858E",
  maxTimeoutSeconds: 300,
  extra: { name: "USD Coin", version: "2" },
};

const now = Math.floor(Date.now() / 1000);
const authorization = {
  from: wallet.address,
  to: accept.payTo,
  value: accept.amount,
  validAfter: now.toString(),
  validBefore: (now + 300).toString(),
  nonce: ethers.hexlify(ethers.randomBytes(32)),
};

const domain = {
  name: accept.extra.name,
  version: accept.extra.version,
  chainId: 8453,
  verifyingContract: accept.asset,
};

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
console.log("Signature:", signature);

const paymentPayload = {
  signature,
  authorization,
};

const requirements = {
  scheme: accept.scheme,
  network: accept.network,
  asset: accept.asset,
  amount: accept.amount,
  payTo: accept.payTo,
  maxTimeoutSeconds: accept.maxTimeoutSeconds,
  extra: accept.extra,
};

// Test against x402.org facilitator
const verifyBody = {
  payload: paymentPayload,
  requirements,
};

console.log("\n=== Verify against x402.org ===");
console.log("Request:", JSON.stringify(verifyBody, null, 2));

const verifyResp = await fetch("https://x402.org/facilitator/verify", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(verifyBody),
});
console.log("Status:", verifyResp.status);
const verifyText = await verifyResp.text();
console.log("Response:", verifyText);

// Also test against CDP facilitator
console.log("\n=== Verify against CDP ===");
const cdpResp = await fetch("https://api.cdp.coinbase.com/platform/v2/x402/verify", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(verifyBody),
});
console.log("Status:", cdpResp.status);
const cdpText = await cdpResp.text();
console.log("Response:", cdpText);
