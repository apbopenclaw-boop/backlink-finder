FROM python:3.12-slim

WORKDIR /app

# Install uv for fast dependency resolution
RUN pip install --no-cache-dir uv

# Copy and install dependencies
COPY pyproject.toml .
RUN uv pip install --system "fastapi>=0.115" "uvicorn>=0.34" "x402[fastapi,evm]>=2.8" "python-dotenv>=1.0"

# Copy application
COPY main.py .
COPY .well-known .well-known
COPY backlinks.db .

# Set DB path to baked-in copy
ENV DB_PATH=/app/backlinks.db
ENV EVM_ADDRESS=0x0000000000000000000000000000000000000000
ENV FACILITATOR_URL=https://x402.org/facilitator

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
