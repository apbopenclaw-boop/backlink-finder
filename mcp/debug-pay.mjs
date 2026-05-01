#!/usr/bin/env node
/**
 * Debug x402 payment flow step by step.
 */
import { ethers } from "ethers";
import { readFileSync } from "fs";

const API_URL = "https://backlink-finder.fly.dev";
const PRIVATE_KEY = readFileSync("/tmp/backlink-api/.test-wallet-key", "utf-8").trim();

// Step 1: Hit paid endpoint, get 402
console.log("=== Step 1: GET /backlinks/tesla.com ===");
const resp = await fetch(`${API_URL}/backlinks/tesla.com`);
console.log("Status:", resp.status);
console.log("Headers:", Object.fromEntries(resp.headers));

const paymentHeader = resp.headers.get("payment-required");
if (!paymentHeader) {
  console.log("No payment-required header!");
  process.exit(1);
}

const paymentInfo = JSON.parse(Buffer.from(paymentHeader, "base64").toString("utf-8"));
console.log("\nPayment info:", JSON.stringify(paymentInfo, null, 2));

const accept = paymentInfo.accepts[0];
console.log("\nAccept:", JSON.stringify(accept, null, 2));

// Step 2: Sign EIP-3009 transferWithAuthorization
console.log("\n=== Step 2: Sign EIP-3009 ===");
const wallet = new ethers.Wallet(PRIVATE_KEY.startsWith("0x") ? PRIVATE_KEY : `0x${PRIVATE_KEY}`);
console.log("Wallet:", wallet.address);

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
console.log("Authorization:", JSON.stringify(authorization, null, 2));

const domain = {
  name: accept.extra?.name || "USD Coin",
  version: accept.extra?.version || "2",
  chainId: 8453,
  verifyingContract: accept.asset,
};
console.log("EIP-712 domain:", JSON.stringify(domain, null, 2));

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

// Step 3: Build payment payload exactly matching spec
console.log("\n=== Step 3: Build X-PAYMENT ===");
const paymentPayload = {
  x402Version: paymentInfo.x402Version || 2,
  resource: paymentInfo.resource,
  accepted: accept,
  payload: {
    signature,
    authorization,
  },
};
console.log("Payment payload:", JSON.stringify(paymentPayload, null, 2));

const paymentB64 = Buffer.from(JSON.stringify(paymentPayload)).toString("base64");
console.log("Base64 length:", paymentB64.length);

// Step 4: Retry with payment
console.log("\n=== Step 4: Retry with X-PAYMENT ===");
const retryResp = await fetch(`${API_URL}/backlinks/tesla.com`, {
  headers: { "X-PAYMENT": paymentB64 },
});
console.log("Status:", retryResp.status);
console.log("Headers:", Object.fromEntries(retryResp.headers));
const body = await retryResp.text();
console.log("Body:", body.slice(0, 2000));
