# ── Stage 1: Builder ─────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc && \
    rm -rf /var/lib/apt/lists/*

COPY deploy/requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: Runtime ─────────────────────────────────────────
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends tini && \
    rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy source code
COPY firewall/           firewall/
COPY Executor_Client/    Executor_Client/
COPY Executor_Error/     Executor_Error/
COPY Executor_Pending/   Executor_Pending/
COPY executor_throttler/ executor_throttler/
COPY deploy/healthcheck.py healthcheck.py

# 🔥 Create non-root user FIRST
RUN groupadd -r oms && useradd -r -g oms -d /app oms

# 🔥 Create log directories INSIDE runtime
RUN mkdir -p /app/logs executor_throttler/logs

# 🔥 Give permission to oms user
RUN chown -R oms:oms /app

# Switch to non-root user
USER oms

ENV PYTHONUNBUFFERED=1

ENV PYTHONDONTWRITEBYTECODE=1

# ✅ Correct tini usage (fix warning)
ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["python", "--version"]