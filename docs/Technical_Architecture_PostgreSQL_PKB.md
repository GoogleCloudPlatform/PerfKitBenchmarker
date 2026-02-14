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
    --postgres_gke_client_mode=pod \
    ...
```

### Architecture & Logic
1.  **Pod as VM Abstraction**: PKB's Kubernetes provider treats Kubernetes pods as Virtual Machines. When the benchmark runs, PKB provisions:
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
    --postgres_gke_optimization_profile=v1+v6+v4 \
    ...
```

### Optimization Profiles
The benchmark supports granular optimization profiles that can be combined:

*   **v1 (Infrastructure)**: Uses Container-Optimized OS (COS) for nodes and Ubuntu 24.04 for the client.
*   **v2 (Startup)**: Removes the init container for faster startup (at the cost of less robust permission handling).
*   **v3 (Kernel)**: Applies sysctl tuning (`vm.swappiness=1`, `vm.dirty_ratio=10`, etc.) to the node.
*   **v4 (HugePages)**: Enables HugePages (2MB) on the node and configures PostgreSQL (`huge_pages=on`) to use them. This reduces TLB misses and improves memory management efficiency.
*   **v6 (Postgres Tuning)**: Applies aggressive PostgreSQL configuration tuning (e.g., `shared_buffers=35GB`, `effective_io_concurrency=200`, `max_worker_processes=32`).
*   **v1+v6+v4 (All-in-One)**: Combines Infrastructure, Postgres Tuning, and HugePages for maximum performance.
*   **v1+v6+v4+hostnetwork (HostNetwork Optimized)**: Extends the "All-in-One" profile by enabling Host Networking (`hostNetwork: true`) for the PostgreSQL pods. This bypasses the Kubernetes CNI/Overlay network stack, allowing the database to use the node's native network interface for maximum throughput and reduced latency.

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

| Parameter | Baseline | Optimized (v6/v1+v6+v4) |
| :--- | :--- | :--- |
| **Shared Buffers** | 15GB | 35GB |
| **Effective Cache Size** | 30GB | 50GB |
| **Work Mem** | 64MB | 256MB |
| **Effective IO Concurrency** | 100 | 200 |
| **Huge Pages** | Off | On (v4) |
| **WAL Buffers** | 64MB | 512MB |
| **Max Worker Processes** | 20 | 32 |
| **Host Network** | False | Optional (v1+v6+v4+hostnetwork) |

## Implementation Details

### 1. Private IP Implementation
To enforce private networking:
*   The benchmark explicitly retrieves the Pod IP: `kubectl get pod postgres-standalone-0 -o jsonpath={.status.podIP}`.
*   This IP is passed to `sysbench` via the `--pgsql-host` flag.
*   The client mode is forced to `pod` (`--postgres_gke_client_mode=pod`) to ensure the client runs within the cluster network.

### 2. Disk Type Selection
The benchmark automatically maps machine types to optimal disk types:
*   **C4 / C4A / C4D**: `hyperdisk-balanced`
*   **N2 / N2D / E2**: `pd-ssd`

### 3. Sysbench Execution
*   The benchmark installs `sysbench` in the client pod via `apt-get`.
*   It executes the `oltp_read_write.lua` script located at `/usr/share/sysbench/`.
*   The execution command includes a timeout buffer (`duration + 120s`) to prevent premature termination.

### 4. Password Handling & Security
*   **Dynamic Password Generation**: To improve security and isolation, passwords are not hardcoded. A unique password is dynamically generated for each benchmark run based on the `Run URI` (e.g., `c4v6...`).
*   **Secure Hashing**: The benchmark computes an MD5 hash of the password (concatenated with the username) before executing the `ALTER USER` command. This ensures that the plaintext password is never stored directly in the database's user table, aligning with PostgreSQL `md5` authentication best practices.
*   **Secret Management**:
    *   **Server Side**: A Kubernetes Secret (`gke-pg-cluster-superuser`) stores the initial superuser password.
    *   **Client Side**: The benchmark retrieves this secret and passes it to the `sysbench` client pod securely via the `PGPASSWORD` environment variable, preventing it from appearing in process listings or command-line logs.

## High Availability (HA) Benchmark Implementation

This section details the implementation of the High Availability (HA) benchmark using the **CloudNativePG (CNPG)** operator.

### 1. Objective
Assess the performance of a robust HA cluster (Primary + 2 Replicas) under various optimization profiles.

### 2. Architecture Design
The benchmark deploys a CloudNativePG `Cluster` resource and executes `sysbench` against it.

#### 2.1 Components
*   **Orchestrator**: GKE (Standard Cluster).
*   **Operator**: [CloudNativePG (CNPG)](https://cloudnative-pg.io/) (v1.23+).
*   **Topology**:
    *   **Instances**: 3 PostgreSQL pods (1 Primary, 2 Replicas).
    *   **Distribution**: Zonal (Anti-Affinity) or Regional (Multi-Zone) based on configuration.
*   **Storage**: Persistent Disks (Hyperdisk Balanced / PD-SSD).
*   **Connection**: Direct connection to the Primary Service (`gke-pg-cluster-rw`).
*   **Workload Generator**: Sysbench (Client Pod).

#### 2.2 Traffic Flow
```
[Sysbench Client]
      ⬇ (TCP/5432)
[K8s Service: gke-pg-cluster-rw]
      ⬇
[Postgres Primary Pod] ➡ [Replica 1] (Async/Sync Replication)
                       ➡ [Replica 2]
```

### 3. Implementation Details

#### 3.1 Benchmark: `postgres_cnpg_benchmark.py`
*   **Install Operator**: Applies upstream CNPG manifest.
*   **Deploy Cluster**: Renders `postgres_cluster.yaml.j2`.
*   **Wait for Healthy**: Checks for `Ready` instances.
*   **Secure Auth**: Generates dynamic MD5-hashed passwords per run.
*   **Run Sysbench**: Targets the RW Service IP.

#### 3.2 Key Features
*   **Optimization Profiles**: `baseline`, `v4` (HugePages), `v6` (Tuning), `v1+v6+v4` (Combined).
*   **Host Network Support**: `v1+v6+v4+hostnetwork` profile bypasses K8s Overlay for max throughput.
*   **Dynamic Security**: Run-specific passwords with MD5 hashing.
*   **Disk Automation**: Selects `hyperdisk-balanced` (C4) or `pd-ssd` (N2) automatically.
