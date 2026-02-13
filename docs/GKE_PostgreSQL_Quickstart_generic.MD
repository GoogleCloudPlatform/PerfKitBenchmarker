# PostgreSQL on GKE - Benchmark Quickstart Guide

## Overview

PerfKitBenchmarker has a built-in benchmark called `postgres_sysbench_gke` that:
- Creates a GKE cluster automatically.
- Deploys PostgreSQL (StatefulSet) and Sysbench (Client Pod).
- Runs the Sysbench OLTP Read/Write workload.
- Measures TPS (Transactions Per Second), QPS (Queries Per Second), and Latency.

## Architecture Overview

1.  **GKE Cluster**: Created with 2 nodepools:
    *   `postgres`: For the PostgreSQL server (StatefulSet).
    *   `clients`: For the Sysbench client (Pod).
2.  **Private Networking**:
    *   PostgreSQL runs as a StatefulSet with a Persistent Volume.
    *   Sysbench connects via the **Private Pod IP** of the server.
    *   No public IPs are used for database traffic.
3.  **Storage**:
    *   Uses `pd-ssd` (for N-series) or `hyperdisk-balanced` (for C4-series).

## New Developer Setup (First Time Only)

If you're a new developer cloning this repository for the first time:

```bash
# 1. Clone the repository
git clone <repository-url>
cd PerfKitBenchmarker

# 2. Create Python virtual environment (first time only)
python3 -m venv venv_postgres

# 3. Activate virtual environment
source venv_postgres/bin/activate

# 4. Install Python dependencies
pip install "setuptools<70.0.0"
pip install pytz
pip install -r requirements.txt
# This may take 2-3 minutes

# 5. Authenticate with GCP
gcloud auth login
gcloud auth application-default login

# 6. Set your GCP project
export PROJECT_ID="your-project-id"
gcloud config set project $PROJECT_ID
```

**Note**: The `venv_postgres/` directory is NOT in git. Each developer creates their own.

## Prerequisites (For Each Session)

```bash
# 1. Activate virtual environment
source venv_postgres/bin/activate

# 2. Create temp directory (for logs and results)
mkdir -p pkb_temp

# 3. Set GCP project variable
export PROJECT_ID="your-project-id"
```

**Note**:
- The `pkb_temp/` directory stores benchmark logs and results. It's excluded from git via `.gitignore`.


## Baseline Tests

Runs the benchmark with standard PostgreSQL settings (no special tuning).

### Baseline Run (C4 Standard)

```bash
python3 pkb.py \
    --benchmarks=postgres_sysbench_gke \
    --cloud=GCP \
    --vm_platform=Kubernetes \
    --zone=us-central1-a \
    --project=$PROJECT_ID \
    --postgres_gke_server_machine_type=c4-standard-16 \
    --postgres_gke_client_machine_type=c4-standard-16 \
    --postgres_gke_client_mode=pod \
    --postgres_gke_disk_type=hyperdisk-balanced \
    --postgres_gke_disk_size=500 \
    --postgres_gke_optimization_profile=baseline \
    --sysbench_tables=10 \
    --sysbench_table_size=4000000 \
    --sysbench_run_threads=512 \
    --sysbench_run_seconds=300 \
    --sysbench_testname=oltp_read_write \
    --metadata=cloud:GCP \
    --metadata=geo:us-central1 \
    --metadata=scenario:postgres_baseline \
    --temp_dir=./pkb_temp \
    --run_stage_iterations=1 \
    --owner=$(whoami | tr '.' '-') \
    --log_level=error \
    --accept_licenses
```

## Optimized Tests - example

Runs the benchmark with specific optimization profiles.



### 1. Profile V6 (Postgres Tuning)
Aggressive PostgreSQL configuration tuning (Shared Buffers, Workers, etc.).

```bash
python3 pkb.py \
    --benchmarks=postgres_sysbench_gke \
    --cloud=GCP \
    --vm_platform=Kubernetes \
    --zone=us-central1-a \
    --project=$PROJECT_ID \
    --postgres_gke_server_machine_type=c4-standard-16 \
    --postgres_gke_client_machine_type=c4-standard-16 \
    --postgres_gke_client_mode=pod \
    --postgres_gke_disk_type=hyperdisk-balanced \
    --postgres_gke_disk_size=500 \
    --postgres_gke_optimization_profile=v6 \
    --sysbench_tables=10 \
    --sysbench_table_size=4000000 \
    --sysbench_run_threads=512 \
    --sysbench_run_seconds=300 \
    --sysbench_testname=oltp_read_write \
    --metadata=cloud:GCP \
    --metadata=geo:us-central1 \
    --metadata=scenario:postgres_optimized_v6 \
    --metadata=optimization_profile:v6 \
    --temp_dir=./pkb_temp \
    --run_stage_iterations=1 \
    --owner=$(whoami | tr '.' '-') \
    --log_level=error \
    --accept_licenses
```


## HA (CloudNativePG) Tests

These tests run the High Availability (HA) benchmark using the CloudNativePG (CNPG) operator.

### 1. HA Baseline
Standard HA deployment (3 replicas) with default settings.

```bash
python3 pkb.py \
    --benchmarks=postgres_cnpg_benchmark \
    --cloud=GCP \
    --vm_platform=Kubernetes \
    --zone=us-central1-a \
    --project=$PROJECT_ID \
    --config_override=postgres_cnpg_benchmark.container_cluster.nodepools.postgres.vm_spec.GCP.machine_type=c4-standard-16 \
    --config_override=postgres_cnpg_benchmark.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=c4-standard-16 \
    --postgres_cnpg_instances=3 \
    --postgres_cnpg_storage_class=hyperdisk-balanced \
    --postgres_cnpg_disk_size=500 \
    --postgres_cnpg_optimization_profile=baseline \
    --sysbench_tables=10 \
    --sysbench_table_size=4000000 \
    --sysbench_run_threads=512 \
    --sysbench_run_seconds=300 \
    --sysbench_testname=oltp_read_write \
    --run_uri=c4bsln2 \
    --metadata=cloud:GCP \
    --metadata=geo:us-central1 \
    --metadata=scenario:ha_baseline \
    --temp_dir=./pkb_temp \
    --run_stage_iterations=1 \
    --owner=$(whoami | tr '.' '-') \
    --log_level=info \
    --accept_licenses
```


### 2. HA Profile V6 (Tuning)
HA deployment with PostgreSQL parameter tuning.

```bash
python3 pkb.py \
    --benchmarks=postgres_cnpg_benchmark \
    --cloud=GCP \
    --vm_platform=Kubernetes \
    --zone=us-central1-a \
    --project=$PROJECT_ID \
    --config_override=postgres_cnpg_benchmark.container_cluster.nodepools.postgres.vm_spec.GCP.machine_type=c4-standard-16 \
    --config_override=postgres_cnpg_benchmark.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=c4-standard-16 \
    --postgres_cnpg_instances=3 \
    --postgres_cnpg_storage_class=hyperdisk-balanced \
    --postgres_cnpg_disk_size=500 \
    --postgres_cnpg_optimization_profile=v6 \
    --sysbench_tables=10 \
    --sysbench_table_size=4000000 \
    --sysbench_run_threads=512 \
    --sysbench_run_seconds=300 \
    --sysbench_testname=oltp_read_write \
    --run_uri=c4v61 \
    --metadata=cloud:GCP \
    --metadata=geo:us-central1 \
    --metadata=scenario:ha_v6 \
    --temp_dir=./pkb_temp \
    --run_stage_iterations=1 \
    --owner=$(whoami | tr '.' '-') \
    --log_level=info \
    --accept_licenses
```


## Understanding the Workload

*   **Workload**: Sysbench OLTP Read/Write (`oltp_read_write`).
*   **Tables**: 10 tables.
*   **Table Size**: 4,000,000 rows per table.
*   **Threads**: 512 concurrent threads.
*   **Duration**: 300 seconds (5 minutes) per run.

## Results Location

Results are saved to:
```
./pkb_temp/runs/<run_uri>/perfkitbenchmarker_results.json
```

View results:
```bash
cat ./pkb_temp/runs/<run_uri>/perfkitbenchmarker_results.json | jq
```
