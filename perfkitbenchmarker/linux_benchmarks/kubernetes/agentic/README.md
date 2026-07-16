# GKE Agent Sandbox Benchmarks

PKB benchmark modules for measuring GKE Agent Sandbox performance under
gVisor isolation. Each benchmark is an atomic single-point measurement
designed to be invoked repeatedly by the sweep runner with varying parameters.

## Benchmarks

| Benchmark | Module | Agent API | Measures |
|-----------|--------|:---------:|----------|
| Python Density | `k8s_python_density_benchmark.py` | Yes | CEL, TTFE, RSS, per-type latency (compute/syscall/import) |
| Payload Transfer | `k8s_payload_benchmark.py` | Yes | Generation, serialization, stdout write, throughput |
| Chromium Density | `k8s_chromium_density_benchmark.py` | Yes | Navigate, evaluate, click, fill, screenshot latency |
| QPS Saturation | `k8s_qps_benchmark.py` | Yes/No | TTFE at controlled request rates, warm pool drain detection |
| Warmpool Scale-Up | `k8s_warmpool_benchmark.py` | No | Provisioning speed, pod lifecycle timestamps |
| Deletion & Cleanup | `k8s_deletion_benchmark.py` | No | Per-pod deletion latency, IP reclamation timing |
| Pod Snapshot | `k8s_snapshot_benchmark.py` | No | Snapshot/restore latency, TTFE, correctness verification |

## Shared Utilities

| Module | Purpose |
|--------|---------|
| `k8s_benchmark_utils.py` | Agent API helpers, kubectl wrappers, warm pool management, port-forward, sample construction |
| `gke_deploy_utils.py` | Idempotent deployment of Agent Sandbox ecosystem (CRDs, templates, warm pools, router, agent) |

## Benchmark Lifecycle

Each benchmark implements the standard PKB interface:

```python
def GetConfig(user_config) -> dict       # Load benchmark config
def Prepare(benchmark_spec) -> None      # Deploy workloads, start port-forward
def Run(benchmark_spec) -> list[Sample]  # Execute measurement, emit samples
def Cleanup(benchmark_spec) -> None      # Drain warm pool, stop port-forward
```

- **Prepare** calls `DeployWorkloads()` to install the Agent Sandbox ecosystem
- **Run** is designed to be called multiple times with different parameters (sweep)
- **Cleanup** drains the warm pool but preserves the cluster for the next run

## Flag Naming Convention

| Category | Pattern | Example |
|----------|---------|---------|
| Shared | `k8s_agentic_<param>` | `k8s_agentic_namespace` |
| Per-benchmark | `k8s_<benchmark>_<param>` | `k8s_python_density_sample_count` |
| Infrastructure | `agent_sandbox_<param>` | `agent_sandbox_version` |

## Adding a New Benchmark

1. Create `k8s_<name>_benchmark.py` with `GetConfig`, `Prepare`, `Run`, `Cleanup`
2. Use `k8s_benchmark_utils.MakeSample()` for consistent metadata
3. Add the benchmark to the sweep runner registry in `sweep.py`
4. Add the benchmark key to all variant config YAMLs
