# Technical Architecture: Redis Benchmarking on GKE with PerfKitBenchmarker

This document provides a technical deep dive into the architecture and implementation of the Redis benchmarking suite used for evaluating performance on Google Kubernetes Engine (GKE). It covers the implementation details of both the Baseline and Optimized benchmarks, explaining how PerfKitBenchmarker (PKB) is leveraged to simulate real-world workloads.

## Overview

The benchmarking suite is designed to compare the performance of standard Redis deployments ("Baseline") against GKE-optimized Redis configurations ("Optimized"). The benchmarks use `memtier_benchmark` as the load generator and are orchestrated by PKB.

## Baseline Benchmark Implementation

The baseline benchmark is executed using the `redis_memtier_caching_single_node` benchmark configuration. This configuration is provided by Google and represents a standard, unoptimized Redis deployment on Kubernetes.

### Execution Command
The baseline scripts (e.g., `redis-baseline-v3-with-versions.sh`) invoke PKB with the following key arguments:

```bash
python3 pkb.py \
    --benchmarks=redis_memtier_caching_single_node \
    --cloud=GCP \
    --vm_platform=Kubernetes \
    ...
```

### Architecture & Logic
1.  **Pod as VM Abstraction**: PKB's Kubernetes provider treats Kubernetes pods as Virtual Machines. When the benchmark runs, PKB provisions a "server" pod running Redis and one or more "client" pods running `memtier_benchmark`.
2.  **Configuration Inheritance**: The `redis_memtier_caching_single_node` config inherits from `redis_memtier_single_base` in `default_benchmark_config.yaml`.
3.  **Eviction Policy**: Crucially, the baseline configuration explicitly sets `redis_eviction_policy: allkeys-lru`. This ensures that when memory pressure increases (e.g., during 1:1 read/write tests), Redis evicts old keys rather than crashing with an OOM error.
4.  **Resource Allocation**: Resources (CPU/Memory) are defined via the `--machine_type` flag, which PKB translates into Kubernetes resource requests and limits for the pods.

## Optimized Benchmark Implementation

The optimized benchmark is executed using the `gke_optimized_redis_memtier_v2` benchmark configuration. This is a custom benchmark developed to leverage GKE-specific optimizations and advanced Redis configurations.

### Execution Command
The optimized scripts (e.g., `redis-optimized-v5-with-versions.sh`) invoke PKB as follows:

```bash
python3 pkb.py \
    --benchmarks=gke_optimized_redis_memtier_v2 \
    --cloud=GCP \
    --vm_platform=Kubernetes \
    ...
```

### Architecture & Customizations
1.  **Custom Benchmark Class**: This benchmark is defined in `gke_optimized_redis_memtier_v2_benchmark.py`. It extends the standard Redis benchmark but adds logic to apply specific GKE optimizations.
2.  **Host Networking**: The optimized benchmark enables `Host Network: TRUE`. This bypasses the Kubernetes network overlay (CNI), allowing Redis to bind directly to the node's network interface, significantly reducing network latency and CPU overhead.
3.  **IO Threading**: Unlike the baseline which uses a fixed single IO thread, the optimized configuration dynamically manages IO threads (often setting them to 2 or 4) to maximize throughput on multi-core nodes.
4.  **Snapshotting Disabled**: RDB snapshots are disabled (`--redis_server_enable_snapshots=False`) to prevent disk I/O from interfering with the benchmark performance, isolating the memory and network performance.

## Control Parameters Comparison

The following table summarizes the key control parameters used in both the Baseline and Optimized runs. This ensures a fair comparison while highlighting the specific tuning applied in the optimized scenario.

### Memtier Parameters (Load Generator)

| Parameter | Baseline | Optimized |
| :--- | :--- | :--- |
| `--memtier_key_pattern` | R:R | R:R |
| `--memtier_distinct_client_seed` | TRUE | TRUE |
| `--memtier_randomize` | TRUE | TRUE |
| `--memtier_data_size` | 1024 | 1024 |
| `--memtier_ratio` | 1:4 | 1:4 |
| `--memtier_threads` | 32 | 32 |
| `--memtier_clients` | 12 | 12 |
| `--memtier_run_duration` | 300 | 300 |
| `--memtier_run_count` | 1 | 1 |
| `--memtier_pipeline` | 1 | 1 |
| `--memtier_key_maximum` | 6400000 | 6400000 |

### Redis Server Parameters

| Parameter | Baseline | Optimized |
| :--- | :--- | :--- |
| **Snapshots** | TRUE | FALSE (optimization) |
| **IO Threads** | 1 (explicit) | Managed via NAP/webhook (2/4) |
| **IO Threads Do Reads** | False (explicit) | Managed via NAP/webhook |
| **Eviction Policy** | allkeys-lru (Inherited from config) | allkeys-lru |
| **Maxmemory** | 70% of Node RAM (or Node RAM - 10.5GB for AOF) | Implemented same logic as baseline |
| **Host Network** | Disabled | TRUE |

## Summary of Customizations

To make the `gke_optimized_redis_memtier_v2` benchmark work effectively, several customizations were added:
*   **Dynamic Eviction**: We explicitly implemented `allkeys-lru` in the optimized benchmark to match the baseline's behavior and prevent OOMs during high-write scenarios.
*   **Memory Calculation**: We ported the memory calculation logic (`vm.total_memory_kb * 0.7` or `vm.total_memory_kb - 10.5GB`) from the standard `redis_server` package to the optimized benchmark to ensure consistent memory limits across different machine types (N2 vs C4).
*   **AOF Handling**: For AOF-enabled runs (Session Storage scenario), we added specific logic to handle the increased memory overhead of AOF rewriting, ensuring the benchmark remains stable even under heavy write loads.
