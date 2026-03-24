#!/bin/bash
# Upload initial DB to GitHub Releases
# Usage: ./scripts/upload-db.sh
# Prerequisite: gh CLI installed and authenticated (gh auth login)

set -e

REPO="applenostalgeek-sketch/vivreici"
DB_PATH="vivreici.db"

if [ ! -f "$DB_PATH" ]; then
  echo "Error: $DB_PATH not found. Run from /Users/admin/vivreici/"
  exit 1
fi

echo "Creating release 'db-current' if it doesn't exist..."
gh release create db-current \
  --title "Database - current" \
  --notes "SQLite database — auto-updated by GitHub Actions. Do not edit manually." \
  --repo "$REPO" 2>/dev/null || echo "(Release already exists, continuing)"

echo "Uploading vivreici.db ($(du -sh $DB_PATH | cut -f1))..."
gh release upload db-current "$DB_PATH" --clobber --repo "$REPO"

echo "Done. DB available at:"
echo "https://github.com/$REPO/releases/download/db-current/vivreici.db"
