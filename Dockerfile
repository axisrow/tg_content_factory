FROM python:3.12-slim
WORKDIR /app

# Node.js 22 + Claude Code CLI
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates xz-utils && rm -rf /var/lib/apt/lists/*
RUN ARCH=$(case "${TARGETARCH}" in arm64) echo "arm64";; *) echo "x64";; esac) && \
    curl -fsSL https://nodejs.org/dist/v22.14.0/node-v22.14.0-linux-${ARCH}.tar.xz | tar -xJ --strip-components=1 -C /usr/local
RUN for i in 1 2 3 4 5; do npm install -g @anthropic-ai/claude-code@latest && break || sleep 10; done

COPY pyproject.toml .
RUN pip install --no-cache-dir .
COPY . .
# The only monorepo package the image needs (#1421): tg-shared for the runtime.
# The legacy web panel lives in its own repo/PyPI (tg-webui) and is not part of
# the image — `pip install tg-webui` cannot resolve yet anyway (tg-shared is
# not on PyPI), so a panel image would need a hand-maintained dep list.
RUN pip install --no-cache-dir ./packages/tg_shared
RUN mkdir -p /app/data
# Worker-only image (automation-first): the Telegram worker runtime with the
# managed PID file and no web surface — hence no EXPOSE and no HTTP healthcheck.
CMD ["python", "-m", "tg_agent.main", "worker"]
