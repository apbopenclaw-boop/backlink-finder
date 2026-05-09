"""ASGI middleware that conforms x402 PaymentMiddlewareASGI's 402 responses
to the published x402 spec / what wallet implementations expect.

Bugs in the upstream `x402` 2.8 middleware:
  1. The 402 response body is `{}` — the JSON payload only lives in the
     base64 `payment-required` header. The spec says the same JSON should
     be in the body too.
  2. `resource.url` inside the payload is built from the request scheme.
     Behind Fly.io's TLS-terminating proxy that scheme is `http://`, but
     the public URL is `https://`. Wallets that try to fetch the URL get
     redirected.
  3. The response lacks the CORS headers a browser-based wallet needs
     to read the payment-required headers.
  4. v1 wallets look for `x-payment-required`; v2 puts it under
     `payment-required`. Emitting both makes us compatible with either.

This middleware sits OUTSIDE the PaymentMiddlewareASGI in the stack
(register it AFTER PaymentMiddlewareASGI so it runs LATER on the way
out of the app), buffers any response with status 402, and rewrites it.

Add to your FastAPI app like this:

    app.add_middleware(PaymentMiddlewareASGI, routes=routes, server=server)
    app.add_middleware(X402ResponsePolish)   # OUTERMOST — must come LAST.
"""
import base64
import json as _json


CORS_HEADERS: list[tuple[bytes, bytes]] = [
    (b"access-control-allow-headers",
     b"Content-Type, X-Payment, PAYMENT-SIGNATURE"),
    (b"access-control-expose-headers",
     b"X-Payment-Required, X-Payment-Response, PAYMENT-REQUIRED, PAYMENT-RESPONSE"),
    (b"access-control-allow-origin", b"*"),
    (b"access-control-allow-methods", b"GET, OPTIONS"),
]


def _fix_resource_scheme(payload: dict) -> None:
    """Rewrite resource.url's http:// to https:// in-place."""
    resource = payload.get("resource")
    if isinstance(resource, dict):
        url = resource.get("url", "")
        if isinstance(url, str) and url.startswith("http://"):
            resource["url"] = "https://" + url[len("http://"):]
    elif isinstance(resource, str) and resource.startswith("http://"):
        payload["resource"] = "https://" + resource[len("http://"):]


def _decode_pr_header(value: str) -> dict | None:
    if not value:
        return None
    # Strip optional padding tolerantly (some encoders drop `=`).
    pad = "=" * (-len(value) % 4)
    try:
        raw = base64.b64decode(value + pad)
        return _json.loads(raw.decode("utf-8"))
    except Exception:
        return None


def _rewrite_402(headers_list, original_body: bytes):
    """Given the original 402 response, return (new_headers_list, new_body_bytes)."""
    # Build a case-insensitive dict for lookups
    lc_headers: dict[str, str] = {}
    for k, v in headers_list:
        lc_headers[k.decode("latin-1").lower()] = v.decode("latin-1")

    # Source the payload: prefer the body if it already has content, fall back
    # to decoding the payment-required header.
    payload: dict | None = None
    if original_body:
        try:
            decoded = _json.loads(original_body.decode("utf-8"))
            if isinstance(decoded, dict) and decoded:
                payload = decoded
        except Exception:
            payload = None
    if payload is None:
        payload = _decode_pr_header(lc_headers.get("payment-required", ""))
    if payload is None:
        payload = _decode_pr_header(lc_headers.get("x-payment-required", ""))

    if not isinstance(payload, dict):
        # Couldn't recover a payload — leave the body alone, just add CORS.
        new_headers = list(headers_list) + CORS_HEADERS
        return new_headers, original_body

    # Fix scheme in payload, then re-encode.
    _fix_resource_scheme(payload)
    new_body_bytes = _json.dumps(payload).encode("utf-8")
    new_pr_b64 = base64.b64encode(new_body_bytes).decode("ascii")

    # Build new header list. Drop content-length/type/payment-required headers
    # we'll resend, plus any pre-existing access-control-* (replaced with ours).
    new_headers: list[tuple[bytes, bytes]] = []
    for k, v in headers_list:
        kl = k.decode("latin-1").lower()
        if kl in ("content-length", "content-type",
                  "payment-required", "x-payment-required"):
            continue
        if kl.startswith("access-control-"):
            continue
        new_headers.append((k, v))

    new_headers.append((b"content-type", b"application/json"))
    new_headers.append((b"content-length",
                        str(len(new_body_bytes)).encode("ascii")))
    new_headers.append((b"payment-required", new_pr_b64.encode("ascii")))
    new_headers.append((b"x-payment-required", new_pr_b64.encode("ascii")))
    new_headers.extend(CORS_HEADERS)
    return new_headers, new_body_bytes


class X402ResponsePolish:
    """Outer ASGI middleware that polishes 402 responses from the x402
    PaymentMiddlewareASGI."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http":
            return await self.app(scope, receive, send)

        # Buffer state — populated only when status == 402.
        buffering = {"flag": False}
        captured_headers: list = []
        body_chunks: list[bytes] = []
        sent_start = {"flag": False}

        async def wrapped_send(message):
            mtype = message.get("type")

            if mtype == "http.response.start":
                if message.get("status") == 402:
                    buffering["flag"] = True
                    captured_headers.extend(message.get("headers", []))
                    return  # Hold off sending until we have the body.
                # Non-402 — pass through.
                await send(message)
                sent_start["flag"] = True
                return

            if mtype == "http.response.body":
                if buffering["flag"]:
                    body_chunks.append(message.get("body", b""))
                    if message.get("more_body"):
                        return
                    # Stream complete: rewrite and emit.
                    new_headers, new_body = _rewrite_402(
                        captured_headers, b"".join(body_chunks)
                    )
                    await send({
                        "type": "http.response.start",
                        "status": 402,
                        "headers": new_headers,
                    })
                    await send({
                        "type": "http.response.body",
                        "body": new_body,
                    })
                    return
                # Non-402 path
                await send(message)
                return

            # Other message types (e.g. http.response.trailer) pass through
            await send(message)

        await self.app(scope, receive, wrapped_send)
