#!/usr/bin/env bash
set -e

echo "=== VivreIci static build ==="

# 1. Download DB from GitHub Releases (public repo, no auth needed)
REPO="${GITHUB_REPO:-applenostalgeek-sketch/vivreici}"
DB_URL="https://github.com/${REPO}/releases/download/db-current/vivreici.db"
echo "Downloading database from ${DB_URL}..."
curl -fL "${DB_URL}" -o vivreici.db --retry 3 --retry-delay 5

echo "DB size: $(du -sh vivreici.db | cut -f1)"

# 2. Install Python deps for export (shapely pour polygones départements)
echo "Installing Python dependencies..."
pip install shapely numpy --quiet

# 3. Export all static JSON
echo "Exporting static data..."
python3 scripts/export_all_static.py

# 4. Build frontend
echo "Building frontend..."
cd frontend
npm ci
npm run build
cd ..

echo "=== Build complete ==="
