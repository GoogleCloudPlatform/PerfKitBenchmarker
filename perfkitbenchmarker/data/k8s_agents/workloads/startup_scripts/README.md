# Agent Startup Scripts

Pluggable startup scripts for the snapshot saturation harness. Each script simulates a realistic sandbox cold-start — the kind of environment setup that happens when an AI coding agent provisions a new sandbox for a user.

## How It Works

These scripts are exclusively used by the Pod Snapshot benchmark. They are not run simultaneously; rather, the user selects *one* of these scripts via the `--k8s_snapshot_preload_mode=script:<path>` flag. The benchmark uses the selected script to prepare the environment before taking a snapshot.

1. The script is read from disk and embedded into the pod's container entrypoint.
2. The pod runs the script to completion (installs packages, starts services, etc.).
3. After the script exits 0, the harness prints `SCRIPT_READY` and starts a counter loop.
4. **TTFE** (Time To First Execution) is measured as the total time from SandboxClaim creation to `SCRIPT_READY`.

This allows you to compare cold-start TTFE (full script execution) against snapshot/restore TTFE (resuming from a pre-snapshotted state where the script already ran).

## Scripts

These scripts are provided as distinct options to test different workload characteristics (lightweight vs. heavyweight initialization).

### pip_fastapi.sh

**Lightweight Python variant.** Runs natively in the `python:3.11-slim` base image.
Simulates an agent setting up a Python API. Steps: `pip install fastapi uvicorn` → create app → start uvicorn → wait for first HTTP response.

Typical cold-start: ~5–8s on GKE with fast network.

```bash
# Cold-start only
python sweeps/snapshot_saturation_search.py \
    --skip_snapshot \
    --k8s_snapshot_preload_mode=script:workloads/startup_scripts/pip_fastapi.sh \
    --burst_size=3
```

### npm_vite.sh

**Heavier Node.js variant.**
Simulates an agent setting up a frontend web project. Installs Node.js + npm from apt, then npm-installs Vite and starts a dev server.

Typical cold-start: ~30–60s (apt + npm on cold cache).

```bash
python sweeps/snapshot_saturation_search.py \
    --k8s_snapshot_preload_mode=script:workloads/startup_scripts/npm_vite.sh \
    --burst_size=3
```

## Writing Your Own Script

Requirements:
- Must be a bash script (runs via `bash -c` in a `python:3.11-slim` container)
- Must exit 0 on success (use `set -e` for fail-fast)
- Should print progress to stdout (visible in pod logs for debugging)
- The harness appends `SCRIPT_READY` + counter loop after your script — don't add your own

The `PRELOAD_MB` env var is available but unused by these scripts. The sweep varies it to test different memory request levels on the pod.