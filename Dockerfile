# ── Build frontend ─────────────────────────────────────────────────────────────
FROM node:20-alpine AS frontend-build
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# ── Image Python finale ────────────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Dépendances Python
COPY backend/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Code backend
COPY backend/ ./backend/

# Frontend compilé
COPY --from=frontend-build /app/frontend/dist ./frontend/dist

# Base de données SQLite — telechargee depuis GitHub Releases au moment du build
# Mise a jour via le workflow GitHub Actions "Update Data" + Render redeploy
ARG GITHUB_REPO=applenostalgeek-sketch/vivreici
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fL "https://github.com/${GITHUB_REPO}/releases/download/db-current/vivreici.db" \
       -o vivreici.db --retry 3 --retry-delay 5

EXPOSE 8080

# Variable d'environnement pour la DB
ENV DATABASE_URL="sqlite+aiosqlite:///./vivreici.db"
# CORS : autoriser le domaine en prod (à surcharger via variable d'env Render/Fly)
ENV CORS_ORIGINS="https://vivreici.fr,https://www.vivreici.fr"

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8080"]
