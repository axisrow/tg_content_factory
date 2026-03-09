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
RUN mkdir -p /app/data
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"
CMD ["python", "-m", "src.main", "serve"]
