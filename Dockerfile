FROM python:3.12-slim
WORKDIR /app

# Node.js 22 + Claude Code CLI
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates xz-utils && rm -rf /var/lib/apt/lists/*
RUN ARCH=$(case "${TARGETARCH}" in arm64) echo "arm64";; *) echo "x64";; esac) && \
    curl -fsSL https://nodejs.org/dist/v22.14.0/node-v22.14.0-linux-${ARCH}.tar.xz | tar -xJ --strip-components=1 -C /usr/local
RUN for i in 1 2 3 4 5; do npm install -g @anthropic-ai/claude-code@latest && break || sleep 10; done

COPY pyproject.toml .
COPY packages/tg_shared ./packages/tg_shared
RUN pip install --no-cache-dir . ./packages/tg_shared
COPY . .
# The only monorepo package the image needs (#1421): tg-shared for the runtime.
RUN mkdir -p /app/data
# Worker image (automation-first): the Telegram worker runtime with the managed
# PID file; there is no web surface (removed in #1868) — hence no EXPOSE and no
# HTTP healthcheck.
CMD ["python", "-m", "tg_agent.main", "worker"]
