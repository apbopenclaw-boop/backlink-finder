FROM python:3.12-slim

WORKDIR /app

# Install uv for fast dependency resolution
RUN pip install --no-cache-dir uv

# Copy and install dependencies
COPY pyproject.toml .
RUN uv pip install --system "fastapi>=0.115" "uvicorn>=0.34" "x402[fastapi,evm]>=2.9" "python-dotenv>=1.0" "duckdb>=1.2" "PyJWT[crypto]>=2.8" "httpx>=0.27"

# Copy application
COPY main.py crawler.py cdp_auth.py convert_parquet_prod.py entrypoint.sh ./
COPY .well-known .well-known
COPY static static

# Seed DB baked into image (copied to volume on first run)
RUN mkdir -p /app/seed
COPY backlinks.db /app/seed/backlinks.db

RUN chmod +x /app/entrypoint.sh

# Data lives on persistent volume at /data
ENV DB_PATH=/data/backlinks.db
ENV CC_CACHE_DIR=/data/cc-cache
ENV CC_PARQUET_DIR=/data/cc-parquet
ENV EVM_ADDRESS=0x2D8cFC122D13971EEf8cfB4CBC047F527eB76FAd
ENV FACILITATOR_URL=https://x402.org/facilitator

EXPOSE 8080

CMD ["/app/entrypoint.sh"]
