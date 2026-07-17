# GKE Agentic Benchmark Scripts

Standalone scripts for managing the GKE Agent Sandbox benchmarking lifecycle.
These scripts run outside of PKB and interact with GCP APIs directly.

## Scripts

### Prerequisites & Teardown

| Script | When | Purpose |
|--------|------|---------|
| `gke_prerequisites.py` | Once, before first PKB run | Enable APIs, create Artifact Registry, build Chrome/Router images |
| `gke_post_teardown.py` | Once, after final PKB teardown | Delete AR repos, GCS snapshot bucket, revoke IAM |

```bash
# One-time setup
python -m perfkitbenchmarker.scripts.agentic.gke_prerequisites \
    --project_id=<project> --region=<region> --target_arch=amd64

# One-time cleanup (after PKB teardown)
python -m perfkitbenchmarker.scripts.agentic.gke_post_teardown \
    --project_id=<project> --region=<region>
```

### Sweep Runner

`sweep.py` orchestrates multi-variant benchmark sweeps. It calls PKB
for each variant and sweep value, draining the warm pool between levels.

```bash
python perfkitbenchmarker/scripts/agentic/sweep.py \
    --benchmark k8s_python_density \
    --variant baseline \
    --stages provision,prepare,run,teardown \
    --sweep-values 1,4,8,16
```

- `--stages` defaults to `provision,prepare,run,teardown`
- Provision and prepare run once; sweep values only affect the run stage
- Pass additional PKB flags after `--`: `... -- --k8s_agentic_portforward_local_port=8085`
- Use `--all` to run all 11 optimization variants sequentially

### Results Analyzer

`analyze.py` parses PKB NDJSON results, computes deltas vs baseline,
and generates markdown reports with CSV exports.

```bash
python perfkitbenchmarker/scripts/agentic/analyze.py \
    --benchmark k8s_python_density \
    --results-dir results/pkb/k8s_python_density \
    --output-dir reports/k8s_python_density
```

### Image Build Utilities

`gke_image_build_utils.py` is a library (not a standalone script) that
builds Chrome Sandbox and Sandbox Router images via Google Cloud Build.
It supports cross-architecture builds (amd64/arm64) and is called by
`gke_prerequisites.py`.

## Lifecycle Overview

```
gke_prerequisites.py    (once)
    |
PKB provision           (creates GKE cluster)
PKB prepare             (deploys Agent Sandbox ecosystem)
PKB run                 (executes benchmark)
PKB cleanup             (drains warm pool)
PKB teardown            (deletes cluster)
    |
gke_post_teardown.py    (once)
```
