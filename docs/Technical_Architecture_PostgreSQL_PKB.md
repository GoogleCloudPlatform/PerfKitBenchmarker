# Technical Architecture: PostgreSQL Benchmarking on GKE with PerfKitBenchmarker

This document provides a technical deep dive into the architecture and implementation of the PostgreSQL benchmarking suite used for evaluating performance on Google Kubernetes Engine (GKE). It covers the implementation details of both the Baseline and Optimized benchmarks, explaining how PerfKitBenchmarker (PKB) is leveraged to simulate real-world workloads using Sysbench.

## Overview

The benchmarking suite is designed to compare the performance of standard PostgreSQL deployments ("Baseline") against GKE-optimized PostgreSQL configurations ("Optimized"). The benchmarks use `sysbench` (OLTP Read/Write) as the load generator and are orchestrated by PKB.

## Baseline Benchmark Implementation

The baseline benchmark is executed using the `postgres_sysbench_gke` benchmark configuration. This configuration represents a standard, unoptimized PostgreSQL deployment on Kubernetes.

### Execution Command


```bash
python3 pkb.py \
    --benchmarks=postgres_sysbench_gke \
    --postgres_gke_optimization_profile=baseline \
    ...
```

### Architecture & Logic
1.  **Kubernetes-Native Architecture**: PKB provisions a native Kubernetes architecture:
    *   **Server**: A StatefulSet with 1 replica (`postgres-standalone-0`) running PostgreSQL 16.
    *   **Client**: A separate Pod (`postgres-client`) running `sysbench`.
2.  **StatefulSet & Storage**: The PostgreSQL server uses a StatefulSet to ensure stable identity and persistent storage. It claims a Persistent Volume (PVC) using either `pd-ssd` (for N-series) or `hyperdisk-balanced` (for C4-series).
3.  **Private Connectivity**: To ensure secure and low-latency communication, the client pod connects to the server using the **Pod IP** (`.status.podIP`) of the server pod. This avoids any potential public load balancer paths and keeps traffic internal to the cluster.
4.  **Secure Authentication**: The benchmark generates a password (or uses `POSTGRES_PASSWORD` env var) and passes it securely to the server (via Secret) and the client (via `PGPASSWORD` env var).

## Optimized Benchmark Implementation

The optimized benchmark uses the same `postgres_sysbench_gke` benchmark class but applies specific "Optimization Profiles" to tune the infrastructure and database configuration.

### Execution Command

```bash
python3 pkb.py \
    --benchmarks=postgres_sysbench_gke \
    --postgres_gke_optimization_profile=infra+postgres+hugepages \
    ...
```

### Optimization Profiles
The benchmark supports granular optimization profiles that can be combined:

*   **infra-tuned**: Uses Container-Optimized OS (COS) for nodes and Ubuntu 24.04 for the client.
*   **fast-startup**: Uses Ubuntu node image and removes the init container for faster startup (at the cost of less robust permission handling).
*   **kernel-tuned**: Applies sysctl tuning (`vm.swappiness=1`, `vm.dirty_ratio=10`, etc.) to the node.
*   **hugepages**: Enables HugePages (2MB) on the node and configures PostgreSQL (`huge_pages=on`) to use them. This reduces TLB misses and improves memory management efficiency.
*   **postgres-tuned**: Applies aggressive PostgreSQL configuration tuning.
*   **infra+postgres**: Combines Infrastructure and Postgres Tuning profiles.
*   **infra+postgres+hugepages**: Combines Infrastructure, Postgres Tuning, and HugePages for maximum performance.
*   **infra+postgres+hugepages+hostnetwork**: Extends the "All-in-One" profile by enabling Host Networking (`hostNetwork: true`) for the PostgreSQL pods. This bypasses the Kubernetes CNI/Overlay network stack, allowing the database to use the node's native network interface for maximum throughput and reduced latency.

## Control Parameters Comparison

The following table summarizes the key control parameters used in both the Baseline and Optimized runs.

### Sysbench Parameters (Load Generator)

| Parameter | Baseline | Optimized |
| :--- | :--- | :--- |
| `tables` | 10 | 10 |
| `table_size` | 4,000,000 | 4,000,000 |
| `threads` | 512 | 512 |
| `testname` | oltp_read_write | oltp_read_write |
| `duration` | 300s | 300s |
| `report_interval` | 10s | 10s |

### PostgreSQL Server Parameters

Memory configurations like `shared_buffers` and `effective_cache_size` are determined dynamically by a rule-based sizing engine that detects the Server Machine Type (`--postgres_gke_server_machine_type`) and aggressively scales K8s pod resources to ~85% of total node RAM, assigning proportionate limits to PostgreSQL to prevent Out-Of-Memory. 

| Parameter | Baseline | Optimized (postgres-tuned / infra+postgres+hugepages) |
| :--- | :--- | :--- |
| **Shared Buffers** | 25% of Pod RAM | 40% of Pod RAM |
| **Effective Cache Size** | 50% of Pod RAM | 75% of Pod RAM |
| **Work Mem** | 64MB | 256MB |
| **Effective IO Concurrency** | 100 | 200 |
| **Huge Pages** | Off | On (hugepages) |
| **WAL Buffers** | 64MB | 512MB |
| **Max Worker Processes** | 20 | 32 |
| **Host Network** | False | Optional (infra+postgres+hugepages+hostnetwork) |

## Implementation Details

### 1. Private IP Implementation
To enforce private networking:
*   The benchmark explicitly retrieves the Pod IP: `kubectl get pod postgres-standalone-0 -o jsonpath={.status.podIP}`.
*   This IP is passed to `sysbench` via the `--pgsql-host` flag.
*   The client architecture operates exclusively via native K8s pods initialized in the exact namespace as the Server, maintaining an exact replication of enterprise internal-cluster layouts.

### 2. Disk Type Selection
The benchmark automatically maps machine types to optimal disk types:
*   **C4 / C4A / C4D / N4 / N4A / N4D**: `hyperdisk-balanced`

### 3. Sysbench Execution
*   The benchmark installs `sysbench` in the client pod via `apt-get`.
*   It executes the `oltp_read_write.lua` script located at `/usr/share/sysbench/`.
*   The execution command includes a timeout buffer (`duration + 120s`) to prevent premature termination.

### 4. Password Handling & Security
*   **Dynamic Password Generation**: A unique password is generated per benchmark run based on the Run URI, ensuring isolation between runs. The plaintext password is never hardcoded or stored in source control. PostgreSQL handles password hashing internally on the server side.
*   **Secret Management**:
    *   **Standalone**: Password is injected into the PostgreSQL pod via the StatefulSet manifest and passed to the Sysbench client via the `PGPASSWORD` environment variable, preventing it from appearing in process listings or command-line logs.



*   **Disk Automation**: Selects `hyperdisk-balanced` (C4) or `pd-ssd` (N2) automatically.
