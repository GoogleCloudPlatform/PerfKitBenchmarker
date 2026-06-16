# Vibe Coding Startup Scripts

Pluggable startup scripts for the UC-A snapshot saturation harness (`sweeps/snapshot_saturation_search.py`). Each script simulates a realistic "vibe coding" sandbox cold-start — the kind of environment setup that happens when an AI coding agent provisions a new sandbox for a user.

## How It Works

When `--preload_mode=script:<path>` is passed to the sweep harness:

1. The script is read from disk and embedded into the pod's container entrypoint
2. The pod runs the script to completion (installs packages, starts services, etc.)
3. After the script exits 0, the harness prints `SCRIPT_READY` and starts a counter loop
4. **TTFE** is measured as the total time from SandboxClaim creation to `SCRIPT_READY`

This lets you compare cold-start TTFE (full script execution) against snapshot/restore TTFE (resuming from a pre-snapshotted state where the script already ran).

## Scripts

### startup_pip_fastapi.sh

**Lightweight Python variant.** Runs natively in the `python:3.11-slim` base image.

Steps: `pip install fastapi uvicorn` → create app → start uvicorn → wait for first HTTP response.

Typical cold-start: ~5–8s on GKE with fast network.

```bash
# Cold-start only
python sweeps/snapshot_saturation_search.py \
    --skip_snapshot \
    --preload_mode=script:workloads/vibe_coding/startup_pip_fastapi.sh \
    --burst_size=3 --search_mode=binary --search_min=10 --search_max=30 \
    --ttfe_threshold_s=20

# With snapshot/restore (shows restore speedup vs cold-start)
python sweeps/snapshot_saturation_search.py \
    --preload_mode=script:workloads/vibe_coding/startup_pip_fastapi.sh \
    --burst_size=3 --search_mode=binary --search_min=10 --search_max=30 \
    --ttfe_threshold_s=20 --restore_threshold_s=10
```

### startup_npm_vite.sh

**Heavier Node.js variant.** Installs Node.js + npm from apt, then npm-installs Vite and starts a dev server.

Steps: `apt-get install nodejs npm` → `npm install vite` → start Vite dev server → wait for first page served.

Typical cold-start: ~30–60s (apt + npm on cold cache).

```bash
python sweeps/snapshot_saturation_search.py \
    --preload_mode=script:workloads/vibe_coding/startup_npm_vite.sh \
    --burst_size=3 --search_mode=binary --search_min=10 --search_max=30 \
    --ttfe_threshold_s=120 --restore_threshold_s=10
```

## Writing Your Own Script

Requirements:
- Must be a bash script (runs via `bash -c` in a `python:3.11-slim` container)
- Must exit 0 on success (use `set -e` for fail-fast)
- Should print progress to stdout (visible in pod logs for debugging)
- The harness appends `SCRIPT_READY` + counter loop after your script — don't add your own

The `PRELOAD_MB` env var is available but unused by these scripts. The sweep varies it to test different memory request levels on the pod.
