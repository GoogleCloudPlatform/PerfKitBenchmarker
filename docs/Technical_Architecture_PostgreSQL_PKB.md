# Technical Architecture: PostgreSQL Benchmarking on GKE with PerfKitBenchmarker

This document provides a technical deep dive into the architecture and
implementation of the PostgreSQL benchmarking suite used for evaluating
performance on Google Kubernetes Engine (GKE). It covers the implementation
details of both the Baseline and Optimized benchmarks, explaining how
PerfKitBenchmarker (PKB) is leveraged to simulate real-world workloads using
Sysbench.

## Overview

The benchmarking suite is designed to compare the performance of standard
PostgreSQL deployments ("Baseline") against GKE-optimized PostgreSQL
configurations ("Optimized"). The benchmarks use `sysbench` (OLTP Read/Write) as
the load generator and are orchestrated by PKB.

## Baseline Benchmark Implementation

The baseline benchmark is executed using the `kubernetes_postgres_sysbench`
benchmark configuration. This configuration represents a standard, unoptimized
PostgreSQL deployment on Kubernetes.

### Execution Command

```bash
python3 pkb.py \
    --benchmarks=kubernetes_postgres_sysbench \
    ...
```

### Architecture & Logic

1.  **Kubernetes-Native Architecture**: PKB provisions a native Kubernetes
    architecture:
    *   **Server**: A StatefulSet with 1 replica (`postgres-standalone-0`)
        running PostgreSQL 16.
    *   **Client**: A separate Pod (`postgres-client`) running `sysbench`.
2.  **StatefulSet & Storage**: The PostgreSQL server uses a StatefulSet to
    ensure stable identity and persistent storage. It claims a Persistent Volume
    (PVC) using either `pd-ssd` (for N-series) or `hyperdisk-balanced` (for
    C4-series).
3.  **Private Connectivity**: To ensure secure and low-latency communication,
    the client pod connects to the server using the **Pod IP** (`.status.podIP`)
    of the server pod. This avoids any potential public load balancer paths and
    keeps traffic internal to the cluster.
4.  **Secure Authentication**: The benchmark generates a password (or uses
    `POSTGRES_PASSWORD` env var) and passes it securely to the server (via
    Secret) and the client (via `PGPASSWORD` env var).

## Optimized Benchmark Implementation

The optimized benchmark uses the same `kubernetes_postgres_sysbench` benchmark
class but allows users to apply specific "Custom Manifest Templates" to tune the
infrastructure and database configuration.

### Execution Command

```bash
python3 pkb.py \
    --benchmarks=kubernetes_postgres_sysbench \
    --kubernetes_postgres_sysbench_template_path=/path/to/optimized_postgres.yaml.j2 \
    ...
```

### Manifest Override Architecture

Unlike the baseline benchmark which uses a standard default YAML manifest,
optimized runs bypass internal parameter sizing engines entirely. Users provide
a fully customized standalone Jinja2 manifest (`.yaml.j2`) that explicitly
defines:

*   **Infrastructure Sizing**: Kubernetes Pod CPU and Memory Requests/Limits.
*   **Database Parameters**: PostgreSQL `postgresql.conf` parameters (e.g.,
    `shared_buffers`, `effective_cache_size`, `max_worker_processes`) set via
    environment variables or ConfigMaps.
*   **Kernel & Hardware Tuning**: Linux HugePages allocations (`huge_pages=on`),
    custom Pod `securityContext.sysctls`, and Host Networking (`hostNetwork:
    true`).

## Control Parameters Comparison

The following table summarizes the key control parameters used in both the
Baseline and Optimized runs.

### Sysbench Parameters (Load Generator)

Parameter         | Baseline        | Optimized
:---------------- | :-------------- | :--------------
`tables`          | 10              | 10
`table_size`      | 4,000,000       | 4,000,000
`threads`         | 512             | 512
`testname`        | oltp_read_write | oltp_read_write
`duration`        | 300s            | 300s
`report_interval` | 10s             | 10s

### PostgreSQL Server Parameters

In the Baseline run, PKB applies sensible, middle-of-the-road database defaults
to the upstream `postgres_all.yaml.j2` manifest. In Optimized runs, all values
are explicitly configured by the user's custom manifest template.

| Parameter          | Baseline Run             | Custom Optimized Manifest  |
:                    : (`postgres_all.yaml.j2`) : (Example)                  :
| :----------------- | :----------------------- | :------------------------- |
| **Shared Buffers** | 15GB                     | 35GB                       |
| **Effective Cache  | 30GB                     | 50GB                       |
: Size**             :                          :                            :
| **Work Mem**       | 64MB                     | 256MB                      |
| **Effective IO     | 100                      | 200                        |
: Concurrency**      :                          :                            :
| **Huge Pages**     | Off                      | On (`huge_pages=on`)       |
| **WAL Buffers**    | 64MB                     | 512MB                      |
| **Max Worker       | 20                       | 32                         |
: Processes**        :                          :                            :
| **Host Network**   | False                    | True (`hostNetwork: true`) |

## Implementation Details

### 1. Private IP Implementation

To enforce private networking:

*   The benchmark explicitly retrieves the Pod IP: `kubectl get pod
    postgres-standalone-0 -o jsonpath={.status.podIP}`.
*   This IP is passed to `sysbench` via the `--pgsql-host` flag.
*   The client architecture operates exclusively via native K8s pods initialized
    in the exact namespace as the Server, maintaining an exact replication of
    enterprise internal-cluster layouts.

### 2. Disk Type Selection

The benchmark automatically maps machine types to optimal disk types:

*   **C4 / C4A / C4D / N4 / N4A / N4D**: `hyperdisk-balanced`

### 3. Sysbench Execution

*   The benchmark installs `sysbench` in the client pod via `apt-get`.
*   It executes the `oltp_read_write.lua` script located at
    `/usr/share/sysbench/`.
*   The execution command includes a timeout buffer (`duration + 120s`) to
    prevent premature termination.

### 4. Password Handling & Security

*   **Dynamic Password Generation**: A unique password is generated per
    benchmark run based on the Run URI, ensuring isolation between runs. The
    plaintext password is never hardcoded or stored in source control.
    PostgreSQL handles password hashing internally on the server side.
*   **Secret Management**:

    *   **Standalone**: Password is injected into the PostgreSQL pod via the
        StatefulSet manifest and passed to the Sysbench client via the
        `PGPASSWORD` environment variable, preventing it from appearing in
        process listings or command-line logs.

*   **Disk Automation**: Selects `hyperdisk-balanced` (C4) or `pd-ssd` (N2)
    automatically.
