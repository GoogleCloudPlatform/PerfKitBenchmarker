"""PKB Benchmark: GKE Agent Python Sandbox Density .

Atomic single-point measurement of Python sandbox density on a
pre-provisioned GKE cluster with gVisor isolation. Measures Code Execution
Latency (CEL), Time To First Execution (TTFE), RSS memory growth, and
per-type latency breakdown (compute, syscall, import) at a given
concurrent session count.

Workflow per session:
  1. Claim a pre-warmed sandbox pod from the SandboxWarmPool
  2. Upload and execute the benchmark script inside the gVisor sandbox
  3. Run `sample_warmup` iterations (results discarded - stabilizes caches)
  4. Run `sample_count` measured iterations (results recorded)
  5. Report TTFE, per-iteration CEL, RSS, and per-task-type breakdown
  6. Release the sandbox claim

This benchmark is designed to be invoked repeatedly by an external sweep
controller that varies the density parameter across iterations to find
the saturation point.

Usage:
  python pkb.py --benchmarks=gke_python_density \\
                --k8s_python_density_concurrent_sandbox_count=16 \\
                --k8s_python_density_sample_count=20 \\
                --k8s_python_density_sample_warmup=0 \\
                --k8s_agentic_namespace=agentic \\
                --k8s_agentic_agent_api_url=http://localhost:8080

Samples emitted (per run):
  - gke_python_density_orchestrator_cel_mean       (ms)
  - gke_python_density_orchestrator_cel_p50        (ms)
  - gke_python_density_orchestrator_cel_p95        (ms)
  - gke_python_density_orchestrator_cel_p99        (ms)
  - gke_python_density_orchestrator_cel_min        (ms)
  - gke_python_density_orchestrator_cel_max        (ms)
  - gke_python_density_sandbox_total_cel_mean      (ms)
  - gke_python_density_sandbox_total_cel_p50       (ms)
  - gke_python_density_sandbox_total_cel_p95       (ms)
  - gke_python_density_sandbox_total_cel_p99       (ms)
  - gke_python_density_sandbox_total_cel_min       (ms)
  - gke_python_density_sandbox_total_cel_max       (ms)
  - gke_python_density_sandbox_ttfe                (ms)
  - gke_python_density_sandbox_rss_start           (MB)
  - gke_python_density_sandbox_rss_end             (MB)
  - gke_python_density_sandbox_rss_growth          (MB)
  - gke_python_density_sandbox_compute_cel_mean    (ms)
  - gke_python_density_sandbox_syscall_cel_mean    (ms)
  - gke_python_density_sandbox_import_cel_mean     (ms)
  - gke_python_density_wall_time                   (seconds)
"""

from __future__ import annotations


import logging
import time

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker.linux_benchmarks.kubernetes.agentic import (
    k8s_benchmark_utils as utils,
)
from perfkitbenchmarker.linux_benchmarks.kubernetes.agentic import (
    gke_deploy_utils as deploy_utils,
)

FLAGS = flags.FLAGS

BENCHMARK_NAME = "k8s_python_density"
BENCHMARK_CONFIG = """
k8s_python_density:
  description: >
    Atomic single-point Python sandbox density measurement on a
    pre-provisioned GKE cluster with gVisor isolation.
"""

_WARMPOOL_NAME = "python-sandbox-warmpool"
_WARMPOOL_LABEL = "sandbox=python-sandbox-example"

# ---------------------------------------------------------------------------
# Benchmark-specific flags
# ---------------------------------------------------------------------------

flags.DEFINE_integer(
    "k8s_python_density_concurrent_sandbox_count",
    1,
    "Number of concurrent sandbox sessions to run.",
)

flags.DEFINE_integer(
    "k8s_python_density_sample_count",
    20,
    "Number of sample iterations per sandbox session.",
)

flags.DEFINE_integer(
    "k8s_python_density_sample_warmup",
    0,
    "Number of warmup iterations per session (excluded from stats). "
    "Warmup iterations execute the same benchmark tasks as measured "
    "iterations but their latency results are discarded. This allows "
    "JIT compilation, caches, and gVisor page faults to stabilize "
    "before measurement begins.",
)

flags.DEFINE_bool(
    "k8s_python_density_patch_warmpool",
    True,
    "Patch SandboxWarmPool replicas to match density before measurement.",
)

flags.DEFINE_integer(
    "k8s_python_density_exec_timeout",
    600,
    "Timeout in seconds for the API call.",
)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def GetConfig(user_config: dict) -> dict:
    """Load and return benchmark config.

    No vm_groups — PKB skips Provision() and Teardown().
    """
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: object) -> None:
    """Deploy workloads and verify agent API."""
    benchmark_spec.always_call_cleanup = True
    logging.info("=== Prepare: deploying workloads ===")
    deploy_utils.DeployWorkloads(benchmark_spec)
    utils.CheckAgentHealthz(required=False)
    utils.EnsurePortForward()
    logging.info("Prepare complete.")


def Run(benchmark_spec: object) -> list[sample.Sample]:
    """Execute a single density measurement and return samples.

    Returns:
      List of sample.Sample objects.
    """
    utils.set_benchmark_spec(benchmark_spec)

    ns = FLAGS.k8s_agentic_namespace
    density = FLAGS.k8s_python_density_concurrent_sandbox_count

    logging.info("=== Run: density=%d ===", density)

    # Ensure port-forward is active (needed when sweeps skip Prepare)
    utils.EnsurePortForward()

    # Patch warm pool to match density (moved from Prepare for sweep compatibility)
    if FLAGS.k8s_python_density_patch_warmpool:
        utils.PatchWarmPool(
            namespace=ns,
            warmpool_name=_WARMPOOL_NAME,
            replicas=density,
            label=_WARMPOOL_LABEL,
        )

    # POST to agent API
    payload = {
        "sample_count": FLAGS.k8s_python_density_sample_count,
        "sample_warmup": FLAGS.k8s_python_density_sample_warmup,
        "concurrent_sessions": density,
        "sandbox_exec_timeout_s": FLAGS.k8s_python_density_exec_timeout,
    }

    t0 = time.time()
    result = utils.CallAgentApi("/benchmark/python/density", payload)
    wall_time = time.time() - t0

    successful = result.get("successful_sessions", 0)
    failed = result.get("failed_sessions", 0)
    agg = result.get("aggregate", {})

    logging.info(
        "API response: %d successful, %d failed sessions (%.1fs)",
        successful,
        failed,
        wall_time,
    )

    # Build samples
    extra = {
        "density": density,
        "successful_sessions": successful,
        "failed_sessions": failed,
        "sample_count": FLAGS.k8s_python_density_sample_count,
        "sample_warmup": FLAGS.k8s_python_density_sample_warmup,
        "wall_time_s": round(wall_time, 2),
    }

    samples = []

    # Orchestrator-side CEL
    _emit(
        samples,
        agg,
        "orchestrator_cel_mean_ms",
        "orchestrator_cel_mean",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples, agg, "orchestrator_cel_p50_ms", "orchestrator_cel_p50", "ms", ns, extra
    )
    _emit(
        samples, agg, "orchestrator_cel_p95_ms", "orchestrator_cel_p95", "ms", ns, extra
    )
    _emit(
        samples, agg, "orchestrator_cel_p99_ms", "orchestrator_cel_p99", "ms", ns, extra
    )
    _emit(
        samples, agg, "orchestrator_cel_min_ms", "orchestrator_cel_min", "ms", ns, extra
    )
    _emit(
        samples, agg, "orchestrator_cel_max_ms", "orchestrator_cel_max", "ms", ns, extra
    )

    # Sandbox-side total CEL
    _emit(
        samples,
        agg,
        "sandbox_total_cel_mean_ms",
        "sandbox_total_cel_mean",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_total_cel_p50_ms",
        "sandbox_total_cel_p50",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_total_cel_p95_ms",
        "sandbox_total_cel_p95",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_total_cel_p99_ms",
        "sandbox_total_cel_p99",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_total_cel_min_ms",
        "sandbox_total_cel_min",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_total_cel_max_ms",
        "sandbox_total_cel_max",
        "ms",
        ns,
        extra,
    )

    # TTFE
    _emit(samples, agg, "sandbox_ttfe_ms", "sandbox_ttfe", "ms", ns, extra)

    # RSS
    _emit(samples, agg, "sandbox_rss_start_mb", "sandbox_rss_start", "MB", ns, extra)
    _emit(samples, agg, "sandbox_rss_end_mb", "sandbox_rss_end", "MB", ns, extra)
    _emit(samples, agg, "sandbox_rss_growth_mb", "sandbox_rss_growth", "MB", ns, extra)

    # Per-type CEL breakdown
    _emit(
        samples,
        agg,
        "sandbox_compute_cel_mean_ms",
        "sandbox_compute_cel_mean",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_syscall_cel_mean_ms",
        "sandbox_syscall_cel_mean",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_import_cel_mean_ms",
        "sandbox_import_cel_mean",
        "ms",
        ns,
        extra,
    )

    # Wall time
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_wall_time",
            round(wall_time, 2),
            "seconds",
            ns,
            extra,
        )
    )

    logging.info("Emitted %d samples for density=%d.", len(samples), density)
    return samples


def Cleanup(benchmark_spec: object) -> None:
    """Clean up after measurement. Scale warm pool to 0."""
    ns = FLAGS.k8s_agentic_namespace
    logging.info("Cleanup: draining warm pool.")

    if FLAGS.k8s_python_density_patch_warmpool:
        utils.DrainWarmPool(
            namespace=ns,
            warmpool_name=_WARMPOOL_NAME,
            label=_WARMPOOL_LABEL,
        )

    utils.StopPortForward()
    logging.info("Cleanup complete (cluster persists).")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _emit(samples: list, agg: dict, agg_key: str, metric_suffix: str, unit: str, namespace: str, extra: dict) -> None:
    """Emit a sample if the key exists in the aggregate dict.

    Args:
        samples: List to append the new sample.Sample to.
        agg: Aggregate metrics dict returned by the agent API response.
        agg_key: Key to look up in `agg` (e.g. "orchestrator_cel_mean_ms").
        metric_suffix: Suffix appended to BENCHMARK_NAME to form the metric
            name (e.g. "orchestrator_cel_mean").
        unit: Unit string for the sample (e.g. "ms", "MB", "seconds").
        namespace: Kubernetes namespace (included in sample metadata).
        extra: Dict of additional metadata key-value pairs attached to
            every sample (density, session counts, wall time, etc.).
    """
    value = agg.get(agg_key)
    if value is not None:
        samples.append(
            utils.MakeSample(
                f"{BENCHMARK_NAME}_{metric_suffix}",
                value,
                unit,
                namespace,
                extra,
            )
        )
