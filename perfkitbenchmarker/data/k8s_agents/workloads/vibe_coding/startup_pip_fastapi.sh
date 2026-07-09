#!/bin/bash
# Lightweight Vibe Coding Startup Script — pip install + FastAPI
#
# Simulates a Python-based agentic sandbox cold-start:
#   1. Install Python packages (FastAPI + uvicorn)
#   2. Start a web server
#   3. Wait for the server to respond
#
# This is lighter weight than the npm/Vite variant and runs natively
# in the python:3.11-slim base image without needing to install Node.js.
#
# Usage (cold-start only):
#   python sweeps/snapshot_saturation_search.py \
#     --skip_snapshot \
#     --preload_mode=script:workloads/vibe_coding/startup_pip_fastapi.sh \
#     --burst_size=3 \
#     --search_mode=binary --search_min=10 --search_max=30 \
#     --ttfe_threshold_s=20
#
# Usage (with snapshot/restore):
#   python sweeps/snapshot_saturation_search.py \
#     --preload_mode=script:workloads/vibe_coding/startup_pip_fastapi.sh \
#     --burst_size=3 \
#     --search_mode=binary --search_min=10 --search_max=30 \
#     --ttfe_threshold_s=20 --restore_threshold_s=10
#
# NOTE: --search_min/--search_max control the PRELOAD_MB env var passed to
# the container; in script mode this is unused by the script itself but
# varies memory requests to test different resource pressure levels.

set -e

echo "[vibe-coding] Installing Python packages..."
pip install --quiet fastapi uvicorn 2>&1 | tail -3

echo "[vibe-coding] Creating app..."
cat > /tmp/app.py << 'EOF'
from fastapi import FastAPI
app = FastAPI()

@app.get("/")
def root():
    return {"status": "ready"}
EOF

echo "[vibe-coding] Starting uvicorn server..."
python -m uvicorn app:app --host 0.0.0.0 --port 8000 --app-dir /tmp &
SERVER_PID=$!

echo "[vibe-coding] Waiting for server to be ready..."
MAX_WAIT=30
ELAPSED=0
while ! python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/')" 2>/dev/null; do
    sleep 1
    ELAPSED=$((ELAPSED + 1))
    if [ $ELAPSED -ge $MAX_WAIT ]; then
        echo "[vibe-coding] ERROR: Server did not start within ${MAX_WAIT}s"
        exit 1
    fi
done

echo "[vibe-coding] First request served successfully (${ELAPSED}s)"

# Kill the server — we only needed to measure startup time
kill $SERVER_PID 2>/dev/null || true
