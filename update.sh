#!/bin/bash
set -e

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVICE="bookie-bot.service"

echo "==> Pulling latest code..."
cd "$REPO_DIR"
git pull

echo "==> Installing dependencies..."
venv/bin/pip install -r requirements.txt -q

echo "==> Restarting $SERVICE..."
systemctl --user restart "$SERVICE"
systemctl --user is-active --quiet "$SERVICE"

echo "==> Done."
