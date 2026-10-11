# velox-grid image: node + chromium (cdp jobs run inside the container)
FROM node:22-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends chromium ca-certificates \
 && rm -rf /var/lib/apt/lists/*
ENV VELOX_BROWSER=/usr/bin/chromium

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund

COPY src ./src
COPY bin ./bin
COPY migrations ./migrations

ENV ARTIFACT_DIR=/data/artifacts
VOLUME /data

# MVP runs as root inside the container (see README known limitations)
CMD ["node", "bin/grid.js", "serve"]
