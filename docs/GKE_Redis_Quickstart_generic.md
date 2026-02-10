# Redis on GKE - Baseline Testing Guide

## Overview

PerfKitBenchmarker has a built-in benchmark called `redis_memtier_caching_single_node` that:
- Creates a GKE cluster automatically
- Deploys Redis in Pods
- Runs memtier from client pods
- Same metrics as traditional VM approach (Ops Throughput, Latency, etc.)

## Architecture overview

1. **GKE cluster created with 2 nodepools**:
   - servers nodepool (for Redis pods)
   - clients nodepool (for memtier pods)

2. **Pods treated as VMs**:
   - Each pod runs like a traditional VM
   - Redis installed and started in server pod
   - Memtier installed in client pod

3. **Architecture details**:
   - Redis runs as a single instance in a pod (not StatefulSet/cluster)
   - Same redis_memtier benchmark code runs (VM or pod)
   - Kubernetes provides the compute infrastructure

## New Developer Setup (First Time Only)

If you're a new developer cloning this repository for the first time:

```bash
# 1. Clone the repository
git clone <repository-url>
cd PerfKitBenchmarker

# 2. Create Python virtual environment (first time only)
python3 -m venv venv_redis

# 3. Activate virtual environment
source venv_redis/bin/activate
# You should see (venv) prefix in your terminal
# To exit from a virtual environment, run deactivate

# 4. Install Python dependencies
pip install "setuptools<70.0.0"
pip install pytz
pip install -r requirements.txt
# This may take 2-3 minutes

# 5. Authenticate with GCP
gcloud auth login
gcloud auth application-default login

# 6. Set your GCP project
gcloud config set project $PROJECT_ID
```

**Note**: The `venv/` directory is NOT in git (it's local to your machine). Each developer creates their own virtual environment.

**Every time you open a new terminal**, you need to activate the virtual environment:
```bash
source venv_redis/bin/activate
```

## Prerequisites (For Each Session)

```bash
# 1. Activate virtual environment
source venv_redis/bin/activate

# 2. Create temp directory
mkdir -p pkb_temp

# 3. Set GCP project
gcloud config set project $PROJECT_ID
gcloud services enable compute.googleapis.com
gcloud services enable container.googleapis.com
gcloud services enable iam.googleapis.com
```

## Baseline Tests
This guide provides the **baseline configuration** for Redis benchmarking on GKE, incorporating:
- Team's manual run configurations
- Baseline KPI settings
- Machine types and parameters

### Baseline Configuration

```bash
# 1. Activate virtual environment
source venv/bin/activate

# 2. Create temp directory
mkdir -p pkb_temp

# 3. Set GCP project
gcloud config set project $PROJECT_ID
```

### Using redis_memtier_caching_single_node

**Infrastructure Details:**
- **GKE Cluster**: 1 control plane VM
- **Redis Nodepool**: 1 server node
- **Client Nodepool**: 1 client node
- **Redis Config**: Redis 7.2.6, snapshots enabled, 1 IO thread, reads on main thread

**Pre-configured Test Settings (from baseline):**
- Run duration: 300 seconds
- Clients: 12
- Threads: 32
- Data size: 1024 bytes
- Ratio: 1:4 (80% GETs, 20% SETs)
- Key maximum: 6,400,000
- Pipeline: 1

### Memtier Parameters Explained

```bash
--memtier_run_duration=300        # 5 minutes per run
--memtier_run_count=10            # 10 runs for median calculation
--memtier_clients=12              # 12 client connections per thread
--memtier_threads=32              # 32 threads = 384 total connections
--memtier_data_size=1024          # 1KB value size
--memtier_ratio=1:4               # 1 SET : 4 GETs (80% reads, 20% writes)
--memtier_key_maximum=6400000     # 6.4M key range
--memtier_pipeline=1              # No pipelining (more realistic)
```

**Total Connections**: 12 clients × 32 threads = **384 concurrent connections**

### Redis Server Configuration

```bash
--redis_server_enable_snapshots=True        # Enable RDB snapshots - Snapshots enabled: Production-like persistence
--redis_server_io_threads=1                 # 1 I/O thread - Single I/O thread: Baseline configuration
--redis_server_io_threads_do_reads=False    # Main thread handles reads -  Reads on main thread: Standard Redis behavior
```

## Understanding the Workload

### Key Distribution

```
Key Pattern: Random (R:R)
Key Range: 1 to 6,400,000
Working Set: 6.4M keys × 1KB = ~6.4GB
```

### Traffic Pattern

```
Ratio: 1:4 (SET:GET)
Operations per second: ~XXX,XXX ops/s (measured)
  - SETs: ~20% of traffic
  - GETs: ~80% of traffic
```

### Load Profile

```
Total Connections: 384 (12 clients × 32 threads)
Pipeline Depth: 1 (synchronous)
Test Duration: 300 seconds (5 minutes) per run
```

## Results Location

Results are saved to:
```
./pkb_temp/runs/<run_uri>/perfkitbenchmarker_results.json
```

View results:
```bash
cat ./pkb_temp/runs/<run_uri>/perfkitbenchmarker_results.json | jq
```

---

# Execution Reference

## 1. Optimized Redis 7 - N2 Standard
Runs the optimized benchmark for Redis 7 on N2 nodes.


** PKB Command (Single Iteration):**
```bash
python3 pkb.py \
  --benchmarks=gke_optimized_redis_memtier_v2 \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_v2_machine_type=n2-standard-4 \
  --gke_redis_v2_server_machine_type=n2-standard-4 \
  --gke_redis_v2_client_machine_type=n2-standard-32 \
  --gke_redis_v2_enable_optimization=True \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.servers.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_optimized_redis_memtier_v2.vm_groups.servers.vm_spec.Kubernetes.host_network=True \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=False \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:4 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --redis_aof=False \
  --create_and_boot_post_task_delay=180 \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=error \
  --accept_licenses
```

## 2. Optimized Redis 7 - C4 Standard
Runs the optimized benchmark for Redis 7 on C4 nodes.


** PKB Command (Single Iteration):**
```bash
python3 pkb.py \
  --benchmarks=gke_optimized_redis_memtier_v2 \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_v2_machine_type=c4-standard-4 \
  --gke_redis_v2_server_machine_type=c4-standard-4 \
  --gke_redis_v2_client_machine_type=c4-standard-32 \
  --gke_redis_v2_enable_optimization=True \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.servers.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_optimized_redis_memtier_v2.vm_groups.servers.vm_spec.Kubernetes.host_network=True \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=False \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:4 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --redis_aof=False \
  --create_and_boot_post_task_delay=180 \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=error \
  --accept_licenses
```

## 3. Optimized Redis 8 - N2 Standard
Runs the optimized benchmark for Redis 8 on N2 nodes.


** PKB Command (Single Iteration):**
```bash
python3 pkb.py \
  --benchmarks=gke_optimized_redis_memtier_v2 \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_v2_machine_type=n2-standard-4 \
  --gke_redis_v2_server_machine_type=n2-standard-4 \
  --gke_redis_v2_client_machine_type=n2-standard-32 \
  --gke_redis_v2_enable_optimization=True \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.servers.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_optimized_redis_memtier_v2.vm_groups.servers.vm_spec.Kubernetes.host_network=True \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=False \
  --redis_server_version=8.0.5 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:4 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --redis_aof=False \
  --create_and_boot_post_task_delay=180 \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=error \
  --accept_licenses
```

## 4. Optimized Redis 8 - C4 Standard
Runs the optimized benchmark for Redis 8 on C4 nodes.


** PKB Command (Single Iteration):**
```bash
python3 pkb.py \
  --benchmarks=gke_optimized_redis_memtier_v2 \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_v2_machine_type=c4-standard-4 \
  --gke_redis_v2_server_machine_type=c4-standard-4 \
  --gke_redis_v2_client_machine_type=c4-standard-32 \
  --gke_redis_v2_enable_optimization=True \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.servers.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_optimized_redis_memtier_v2.vm_groups.servers.vm_spec.Kubernetes.host_network=True \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=False \
  --redis_server_version=8.0.5 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:4 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --redis_aof=False \
  --create_and_boot_post_task_delay=180 \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=error \
  --accept_licenses
```

## 5. Valkey 8 Baseline - N2 Standard
Runs the baseline benchmark for Valkey 8 on N2 nodes.


** PKB Command (Single Iteration):**
```bash
python3 pkb.py \
  --benchmarks=redis_memtier_caching_single_node \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=n2-standard-32 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.servers.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=redis_memtier_caching_single_node.container_cluster.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.servers.vm_spec.GCP.boot_disk_type=pd-ssd \
  --memtier_key_maximum=6400000 \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --redis_server_enable_snapshots=True \
  --redis_server_version=8.0.0 \
  --redis_git_repo=https://github.com/valkey-io/valkey.git \
  --redis_type=valkey \
  --redis_server_io_threads=1 \
  --redis_server_io_threads_do_reads=False \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:4 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --redis_aof=False \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=error \
  --accept_licenses
```

## 6. Valkey 8 Baseline - C4 Standard
Runs the baseline benchmark for Valkey 8 on C4 nodes.

** PKB Command (Single Iteration):**
```bash
python3 pkb.py \
  --benchmarks=redis_memtier_caching_single_node \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=c4-standard-32 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.servers.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=redis_memtier_caching_single_node.container_cluster.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.servers.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --memtier_key_maximum=6400000 \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --redis_server_enable_snapshots=True \
  --redis_server_version=8.0.0 \
  --redis_git_repo=https://github.com/valkey-io/valkey.git \
  --redis_type=valkey \
  --redis_server_io_threads=1 \
  --redis_server_io_threads_do_reads=False \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:4 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --redis_aof=False \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=error \
  --accept_licenses
```

## 7. Valkey 8 Optimized - N2 Standard
Runs the optimized benchmark for Valkey 8 on N2 nodes.

** PKB Command (Single Iteration):**
```bash
python3 pkb.py \
  --benchmarks=gke_optimized_redis_memtier_v2 \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_v2_machine_type=n2-standard-4 \
  --gke_redis_v2_server_machine_type=n2-standard-4 \
  --gke_redis_v2_client_machine_type=n2-standard-32 \
  --gke_redis_v2_enable_optimization=True \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.servers.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_optimized_redis_memtier_v2.vm_groups.servers.vm_spec.Kubernetes.host_network=True \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=False \
  --redis_server_version=8.0.0 \
  --redis_git_repo=https://github.com/valkey-io/valkey.git \
  --redis_type=valkey \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:4 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --redis_aof=False \
  --create_and_boot_post_task_delay=180 \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=error \
  --accept_licenses
```

## 8. Valkey 8 Optimized - C4 Standard
Runs the optimized benchmark for Valkey 8 on C4 nodes.


** PKB Command (Single Iteration):**
```bash
python3 pkb.py \
  --benchmarks=gke_optimized_redis_memtier_v2 \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_v2_machine_type=c4-standard-4 \
  --gke_redis_v2_server_machine_type=c4-standard-4 \
  --gke_redis_v2_client_machine_type=c4-standard-32 \
  --gke_redis_v2_enable_optimization=True \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.servers.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_optimized_redis_memtier_v2.vm_groups.servers.vm_spec.Kubernetes.host_network=True \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=False \
  --redis_server_version=8.0.0 \
  --redis_git_repo=https://github.com/valkey-io/valkey.git \
  --redis_type=valkey \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:4 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --redis_aof=False \
  --create_and_boot_post_task_delay=180 \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=error \
  --accept_licenses
```

## 9. Redis 7 Baseline (N2 Standard) - 1:1 with AOF
Runs the baseline benchmark with AOF enabled and 1:1 read/write ratio on N2 nodes.


** PKB Command (Single Iteration):**
```bash
python3 pkb.py \
  --benchmarks=redis_memtier_caching_single_node \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=n2-standard-32 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.servers.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=redis_memtier_caching_single_node.container_cluster.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.servers.vm_spec.GCP.boot_disk_type=pd-ssd \
  --memtier_key_maximum=6400000 \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --redis_server_enable_snapshots=True \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_server_io_threads=1 \
  --redis_server_io_threads_do_reads=False \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:1 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --redis_aof=True \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=error \
  --accept_licenses
```

## 10. Redis 7 Baseline (C4 Standard) - 1:1 with AOF
Runs the baseline benchmark with AOF enabled and 1:1 read/write ratio on C4 nodes.


** PKB Command (Single Iteration):**
```bash
python3 pkb.py \
  --benchmarks=redis_memtier_caching_single_node \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=c4-standard-32 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.servers.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=redis_memtier_caching_single_node.container_cluster.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.servers.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --memtier_key_maximum=6400000 \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --redis_server_enable_snapshots=True \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_server_io_threads=1 \
  --redis_server_io_threads_do_reads=False \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:1 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --redis_aof=True \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=error \
  --accept_licenses
```

## 11. Redis 7 Optimized (C4 Standard) - 1:1 with AOF
Runs the optimized benchmark with AOF enabled and 1:1 read/write ratio on C4 nodes.


** PKB Command (Single Iteration):**
```bash
python3 pkb.py \
  --benchmarks=gke_optimized_redis_memtier_v2 \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_v2_machine_type=c4-standard-4 \
  --gke_redis_v2_server_machine_type=c4-standard-4 \
  --gke_redis_v2_client_machine_type=c4-standard-32 \
  --gke_redis_v2_enable_optimization=True \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.servers.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_optimized_redis_memtier_v2.vm_groups.servers.vm_spec.Kubernetes.host_network=True \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=True \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:1 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --redis_aof=True \
  --create_and_boot_post_task_delay=180 \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=error \
  --accept_licenses
```

## 12. Redis 7 Optimized (N2 Standard) - 1:1 with AOF
Runs the optimized benchmark with AOF enabled and 1:1 read/write ratio on N2 nodes.

** PKB Command (Single Iteration):**
```bash
python3 pkb.py \
  --benchmarks=gke_optimized_redis_memtier_v2 \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_v2_machine_type=n2-standard-4 \
  --gke_redis_v2_server_machine_type=n2-standard-4 \
  --gke_redis_v2_client_machine_type=n2-standard-32 \
  --gke_redis_v2_enable_optimization=True \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.servers.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_optimized_redis_memtier_v2.vm_groups.servers.vm_spec.Kubernetes.host_network=True \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=True \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:1 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --redis_aof=True \
  --create_and_boot_post_task_delay=180 \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=error \
  --accept_licenses
```

## 13. Redis 6 Baseline - C4 Standard
Runs the baseline benchmark for Redis 6 on C4 nodes.


** PKB Command (Single Iteration):**
```bash
python3 pkb.py \
  --benchmarks=redis_memtier_caching_single_node \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=c4-standard-32 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.servers.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=redis_memtier_caching_single_node.container_cluster.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.servers.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --memtier_key_maximum=6400000 \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --redis_server_enable_snapshots=True \
  --redis_server_version=6.2.14 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_server_io_threads=1 \
  --redis_server_io_threads_do_reads=False \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:4 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --redis_aof=False \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=error \
  --accept_licenses
```

## 14. Redis 6 Baseline - N2 Standard
Runs the baseline benchmark for Redis 6 on N2 nodes.

** PKB Command (Single Iteration):**
```bash
python3 pkb.py \
  --benchmarks=redis_memtier_caching_single_node \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=n2-standard-32 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.servers.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=redis_memtier_caching_single_node.container_cluster.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=redis_memtier_caching_single_node.container_cluster.nodepools.servers.vm_spec.GCP.boot_disk_type=pd-ssd \
  --memtier_key_maximum=6400000 \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --redis_server_enable_snapshots=True \
  --redis_server_version=6.2.14 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_server_io_threads=1 \
  --redis_server_io_threads_do_reads=False \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:4 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --redis_aof=False \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=error \
  --accept_licenses
```

## 15. Redis 6 Optimized - N2 Standard
Runs the optimized benchmark for Redis 6 on N2 nodes.


** PKB Command (Single Iteration):**
```bash
python3 pkb.py \
  --benchmarks=gke_optimized_redis_memtier_v2 \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_v2_machine_type=n2-standard-4 \
  --gke_redis_v2_server_machine_type=n2-standard-4 \
  --gke_redis_v2_client_machine_type=n2-standard-32 \
  --gke_redis_v2_enable_optimization=True \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.servers.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_optimized_redis_memtier_v2.vm_groups.servers.vm_spec.Kubernetes.host_network=True \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=False \
  --redis_server_version=6.2.14 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:4 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --redis_aof=False \
  --create_and_boot_post_task_delay=180 \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=error \
  --accept_licenses
```

## 16. Redis 6 Optimized - C4 Standard
Runs the optimized benchmark for Redis 6 on C4 nodes.


** PKB Command (Single Iteration):**
```bash
python3 pkb.py \
  --benchmarks=gke_optimized_redis_memtier_v2 \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_v2_machine_type=c4-standard-4 \
  --gke_redis_v2_server_machine_type=c4-standard-4 \
  --gke_redis_v2_client_machine_type=c4-standard-32 \
  --gke_redis_v2_enable_optimization=True \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.servers.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_optimized_redis_memtier_v2.vm_groups.servers.vm_spec.Kubernetes.host_network=True \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=False \
  --redis_server_version=6.2.14 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:4 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --redis_aof=False \
  --create_and_boot_post_task_delay=180 \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=error \
  --accept_licenses
```

# Redis HA (Regional) Execution Reference

## 17. Redis 7 HA Optimized - C4 Standard (0:1 - 100% Reads)
Runs optimized regional HA benchmark (Primary+Replica+HAProxy) on C4 Standard nodes with 100% reads.


** PKB Command:**
```bash
python3 pkb.py \
  --benchmarks=gke_redis_ha_haproxy \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_ha_machine_type=c4-standard-4 \
  --gke_redis_ha_enable_optimization=True \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=c4-standard-32 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=True \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=0:1 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --memtier_protocol=redis \
  --redis_aof=True \
  --create_and_boot_post_task_delay=180 \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=info \
  --accept_licenses
```

## 18. Redis 7 HA Optimized - C4 Standard (1:0 - 100% Writes)
Runs optimized regional HA benchmark (Primary+Replica+HAProxy) on C4 Standard nodes with 100% writes.


** PKB Command:**
```bash
python3 pkb.py \
  --benchmarks=gke_redis_ha_haproxy \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_ha_machine_type=c4-standard-4 \
  --gke_redis_ha_enable_optimization=True \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=c4-standard-32 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=True \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:0 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --memtier_protocol=redis \
  --redis_aof=True \
  --create_and_boot_post_task_delay=180 \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=info \
  --accept_licenses
```

## 19. Redis 7 HA Optimized - C4 Standard (1:1 - Mixed)
Runs optimized regional HA benchmark (Primary+Replica+HAProxy) on C4 Standard nodes with 1:1 read/write ratio.



** PKB Command:**
```bash
python3 pkb.py \
  --benchmarks=gke_redis_ha_haproxy \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_ha_machine_type=c4-standard-4 \
  --gke_redis_ha_enable_optimization=True \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=c4-standard-32 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=True \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:1 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --memtier_protocol=redis \
  --redis_aof=True \
  --create_and_boot_post_task_delay=180 \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=info \
  --accept_licenses
```

## 20. Redis 7 HA Optimized - N2 Standard (0:1 - 100% Reads)
Runs optimized regional HA benchmark (Primary+Replica+HAProxy) on N2 Standard nodes with 100% reads.



** PKB Command:**
```bash
python3 pkb.py \
  --benchmarks=gke_redis_ha_haproxy \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_ha_machine_type=n2-standard-4 \
  --gke_redis_ha_enable_optimization=True \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=n2-standard-32 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.boot_disk_type=pd-ssd \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=True \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=0:1 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --memtier_protocol=redis \
  --redis_aof=True \
  --create_and_boot_post_task_delay=180 \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=info \
  --accept_licenses
```

## 21. Redis 7 HA Optimized - N2 Standard (1:0 - 100% Writes)
Runs optimized regional HA benchmark (Primary+Replica+HAProxy) on N2 Standard nodes with 100% writes.



** PKB Command:**
```bash
python3 pkb.py \
  --benchmarks=gke_redis_ha_haproxy \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_ha_machine_type=n2-standard-4 \
  --gke_redis_ha_enable_optimization=True \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=n2-standard-32 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.boot_disk_type=pd-ssd \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=True \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:0 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --memtier_protocol=redis \
  --redis_aof=True \
  --create_and_boot_post_task_delay=180 \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=info \
  --accept_licenses
```

## 22. Redis 7 HA Optimized - N2 Standard (1:1 - Mixed)
Runs optimized regional HA benchmark (Primary+Replica+HAProxy) on N2 Standard nodes with 1:1 read/write ratio.



** PKB Command:**
```bash
python3 pkb.py \
  --benchmarks=gke_redis_ha_haproxy \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_ha_machine_type=n2-standard-4 \
  --gke_redis_ha_enable_optimization=True \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=n2-standard-32 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.boot_disk_type=pd-ssd \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=True \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:1 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --memtier_protocol=redis \
  --redis_aof=True \
  --create_and_boot_post_task_delay=180 \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=info \
  --accept_licenses
```

## 23. Redis 7 HA Baseline - C4 Standard (0:1 - 100% Reads)
Runs baseline regional HA benchmark (Primary+Replica+HAProxy) on C4 Standard nodes with 100% reads.


** PKB Command:**
```bash
python3 pkb.py \
  --benchmarks=gke_redis_ha_haproxy \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_ha_enable_optimization=False \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=c4-standard-32 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=True \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=0:1 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --memtier_protocol=redis \
  --redis_aof=True \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=info \
  --accept_licenses
```

## 24. Redis 7 HA Baseline - C4 Standard (1:0 - 100% Writes)
Runs baseline regional HA benchmark (Primary+Replica+HAProxy) on C4 Standard nodes with 100% writes.


** PKB Command:**
```bash
python3 pkb.py \
  --benchmarks=gke_redis_ha_haproxy \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_ha_enable_optimization=False \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=c4-standard-32 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=True \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:0 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --memtier_protocol=redis \
  --redis_aof=True \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=info \
  --accept_licenses
```

## 25. Redis 7 HA Baseline - C4 Standard (1:1 - Mixed)
Runs baseline regional HA benchmark (Primary+Replica+HAProxy) on C4 Standard nodes with 1:1 read/write ratio.



** PKB Command:**
```bash
python3 pkb.py \
  --benchmarks=gke_redis_ha_haproxy \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_ha_enable_optimization=False \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=c4-standard-32 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.machine_type=c4-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.boot_disk_type=hyperdisk-balanced \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=True \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:1 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --memtier_protocol=redis \
  --redis_aof=True \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=info \
  --accept_licenses
```

## 26. Redis 7 HA Baseline - N2 Standard (0:1 - 100% Reads)
Runs baseline regional HA benchmark (Primary+Replica+HAProxy) on N2 Standard nodes with 100% reads.



** PKB Command:**
```bash
python3 pkb.py \
  --benchmarks=gke_redis_ha_haproxy \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_ha_enable_optimization=False \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=n2-standard-32 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.boot_disk_type=pd-ssd \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=True \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=0:1 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --memtier_protocol=redis \
  --redis_aof=True \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=info \
  --accept_licenses
```

## 27. Redis 7 HA Baseline - N2 Standard (1:0 - 100% Writes)
Runs baseline regional HA benchmark (Primary+Replica+HAProxy) on N2 Standard nodes with 100% writes.


** PKB Command:**
```bash
python3 pkb.py \
  --benchmarks=gke_redis_ha_haproxy \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_ha_enable_optimization=False \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=n2-standard-32 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.boot_disk_type=pd-ssd \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=True \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:0 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --memtier_protocol=redis \
  --redis_aof=True \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=info \
  --accept_licenses
```

## 28. Redis 7 HA Baseline - N2 Standard (1:1 - Mixed)
Runs baseline regional HA benchmark (Primary+Replica+HAProxy) on N2 Standard nodes with 1:1 read/write ratio.


** PKB Command:**
```bash
python3 pkb.py \
  --benchmarks=gke_redis_ha_haproxy \
  --cloud=GCP \
  --vm_platform=Kubernetes \
  --zone=us-east1-b \
  --project=$PROJECT_ID \
  --os_type=ubuntu2404 \
  --gke_release_channel=rapid \
  --gke_max_cpu=1000 \
  --gke_max_memory=4000 \
  --gke_redis_ha_enable_optimization=False \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.machine_type=n2-standard-32 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.machine_type=n2-standard-4 \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.primary.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.replica.vm_spec.GCP.boot_disk_type=pd-ssd \
  --config_override=gke_redis_ha_haproxy.container_cluster.nodepools.haproxy.vm_spec.GCP.boot_disk_type=pd-ssd \
  --memtier_key_pattern=R:R \
  --memtier_distinct_client_seed=True \
  --memtier_key_maximum=6400000 \
  --redis_server_enable_snapshots=True \
  --redis_server_version=7.2.6 \
  --redis_git_repo=https://github.com/redis/redis.git \
  --redis_type=redis \
  --redis_eviction_policy=allkeys-lru \
  --iostat=True \
  --sar=False \
  --memtier_data_size=1024 \
  --memtier_ratio=1:1 \
  --memtier_threads=32 \
  --memtier_clients=12 \
  --memtier_run_duration=300 \
  --memtier_run_count=1 \
  --memtier_pipeline=1 \
  --memtier_protocol=redis \
  --redis_aof=True \
  --temp_dir=./pkb_temp \
  --owner=$(whoami | tr '.' '-') \
  --log_level=info \
  --accept_licenses
```
