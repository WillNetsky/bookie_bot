#!/bin/bash
set -e

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVICE="ai.bookie_bot"

echo "==> Pulling latest code..."
cd "$REPO_DIR"
git pull

echo "==> Installing dependencies..."
pip install -r requirements.txt -q

echo "==> Restarting $SERVICE..."
launchctl stop "$SERVICE"
sleep 1
launchctl start "$SERVICE"

echo "==> Done."
