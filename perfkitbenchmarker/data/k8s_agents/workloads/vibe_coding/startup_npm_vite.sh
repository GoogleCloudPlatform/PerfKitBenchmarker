#!/bin/bash
# Vibe Coding Startup Script — npm + Vite dev server
#
# Simulates a typical agentic sandbox "vibe coding" cold-start:
#   1. Install Node.js dependencies (bun/npm)
#   2. Start a Vite dev server
#   3. Wait for the server to be ready (first page served)
#
# This script is designed to run inside the sandbox container (python:3.11-slim).
# It installs Node.js + npm + dependencies from scratch to measure realistic
# cold-start latency including package installation.
#
# Usage (cold-start only):
#   python sweeps/snapshot_saturation_search.py \
#     --skip_snapshot \
#     --preload_mode=script:workloads/vibe_coding/startup_npm_vite.sh \
#     --burst_size=3 \
#     --search_mode=binary --search_min=10 --search_max=30 \
#     --ttfe_threshold_s=120
#
# Usage (with snapshot/restore):
#   python sweeps/snapshot_saturation_search.py \
#     --preload_mode=script:workloads/vibe_coding/startup_npm_vite.sh \
#     --burst_size=3 \
#     --search_mode=binary --search_min=10 --search_max=30 \
#     --ttfe_threshold_s=120 --restore_threshold_s=10
#
# NOTE: --search_min/--search_max control the PRELOAD_MB env var passed to
# the container; in script mode this is unused by the script itself but
# varies memory requests to test different resource pressure levels.

set -e

echo "[vibe-coding] Installing Node.js..."
apt-get update -qq && apt-get install -y -qq nodejs npm > /dev/null 2>&1

echo "[vibe-coding] Creating project scaffold..."
mkdir -p /tmp/vibe-project && cd /tmp/vibe-project

# Create a minimal package.json with Vite
cat > package.json << 'EOF'
{
  "name": "vibe-sandbox",
  "private": true,
  "scripts": {
    "dev": "vite --host 0.0.0.0 --port 5173"
  },
  "dependencies": {
    "vite": "^5.0.0"
  }
}
EOF

# Create minimal index.html for Vite to serve
cat > index.html << 'EOF'
<!DOCTYPE html>
<html><head><title>Vibe</title></head>
<body><h1>Ready</h1></body>
</html>
EOF

echo "[vibe-coding] Installing npm dependencies..."
npm install --prefer-offline 2>&1 | tail -5

echo "[vibe-coding] Starting Vite dev server..."
npx vite --host 0.0.0.0 --port 5173 &
VITE_PID=$!

echo "[vibe-coding] Waiting for server to be ready..."
MAX_WAIT=60
ELAPSED=0
while ! curl -s http://localhost:5173 > /dev/null 2>&1; do
    sleep 1
    ELAPSED=$((ELAPSED + 1))
    if [ $ELAPSED -ge $MAX_WAIT ]; then
        echo "[vibe-coding] ERROR: Server did not start within ${MAX_WAIT}s"
        exit 1
    fi
done

echo "[vibe-coding] First page served successfully (${ELAPSED}s)"

# Kill the vite server — we only needed to measure startup time
kill $VITE_PID 2>/dev/null || true
