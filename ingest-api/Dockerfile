# =============================================================================
# Ingest API — container image
# =============================================================================
# LAYER: Ingest API packaging. Builds the single image used, unmodified, by
#   all three prototypes' docker-compose files — PROTOTYPE (p1/p2/p3) is
#   supplied at `docker compose up` time via environment variables, not at
#   build time (see docker-compose.p1/p2/p3.yml and src/index.js).
# =============================================================================

FROM node:20-alpine

WORKDIR /app

# Install dependencies first (layer caching — only re-runs if package files change)
COPY package.json package-lock.json* ./
RUN npm ci --production 2>/dev/null || npm install --production

COPY src/ ./src/

CMD ["node", "src/index.js"]
