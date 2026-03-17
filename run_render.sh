#!/usr/bin/env bash
set -e

cd /opt/render/project/src

python3 bot.py &
BOT_PID=$!

cleanup() {
  kill "$BOT_PID" 2>/dev/null || true
}

trap cleanup EXIT INT TERM

exec python3 backend/server.py
