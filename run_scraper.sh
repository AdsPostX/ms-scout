#!/bin/bash
# run_scraper.sh — daily offer sync
# Scheduled via cron. Loads .env automatically via python-dotenv.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/scraper.log"

cd "$SCRIPT_DIR"

echo "=== Offer scraper run: $(date) ===" >> "$LOG_FILE"
"$SCRIPT_DIR/.venv/bin/python3" "$SCRIPT_DIR/offer_scraper.py" >> "$LOG_FILE" 2>&1
echo "=== Done: $(date) ===" >> "$LOG_FILE"
