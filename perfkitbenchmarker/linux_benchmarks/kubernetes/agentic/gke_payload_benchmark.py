"""PKB Benchmark: GKE Agent Payload Transfer Saturation .

Atomic single-point measurement of payload transfer latency from a gVisor
sandbox back to the orchestrator on a pre-provisioned GKE cluster.  Measures
generation time, serialization time, stdout write time, total transfer time,
throughput, and RSS at a given payload_size_mb and concurrent_sessions count.

This benchmark is designed to be invoked repeatedly by an external sweep
controller that varies the payload_size_mb parameter across iterations to
find the saturation point.

Usage:
  python pkb.py --benchmarks=gke_payload \
                --gke_payload_size_mb=50 \
                --gke_payload_iterations=20 \
                --gke_payload_concurrent_sessions=5 \
                --k8s_namespace=agentic \
                --k8s_agent_api_url=http://localhost:8080

Samples emitted (per run):
  - gke_payload_orchestrator_transfer_mean       (ms)
  - gke_payload_orchestrator_transfer_p50        (ms)
  - gke_payload_orchestrator_transfer_p95        (ms)
  - gke_payload_orchestrator_transfer_p99        (ms)
  - gke_payload_orchestrator_transfer_min        (ms)
  - gke_payload_orchestrator_transfer_max        (ms)
  - gke_payload_sandbox_payload_size_bytes       (bytes)
  - gke_payload_sandbox_payload_encoded_size_bytes (bytes)
  - gke_payload_sandbox_payload_iterations       (count)
  - gke_payload_sandbox_generation_time_mean     (ms)
  - gke_payload_sandbox_generation_time_p50      (ms)
  - gke_payload_sandbox_generation_time_p95      (ms)
  - gke_payload_sandbox_generation_time_p99      (ms)
  - gke_payload_sandbox_generation_time_min      (ms)
  - gke_payload_sandbox_generation_time_max      (ms)
  - gke_payload_sandbox_serialization_time_mean  (ms)
  - gke_payload_sandbox_serialization_time_p50   (ms)
  - gke_payload_sandbox_serialization_time_p95   (ms)
  - gke_payload_sandbox_serialization_time_p99   (ms)
  - gke_payload_sandbox_serialization_time_min   (ms)
  - gke_payload_sandbox_serialization_time_max   (ms)
  - gke_payload_sandbox_stdout_time_mean         (ms)
  - gke_payload_sandbox_stdout_time_p50          (ms)
  - gke_payload_sandbox_stdout_time_p95          (ms)
  - gke_payload_sandbox_stdout_time_p99          (ms)
  - gke_payload_sandbox_stdout_time_min          (ms)
  - gke_payload_sandbox_stdout_time_max          (ms)
  - gke_payload_sandbox_transfer_time_mean       (ms)
  - gke_payload_sandbox_transfer_time_p50        (ms)
  - gke_payload_sandbox_transfer_time_p95        (ms)
  - gke_payload_sandbox_transfer_time_p99        (ms)
  - gke_payload_sandbox_transfer_time_min        (ms)
  - gke_payload_sandbox_transfer_time_max        (ms)
  - gke_payload_sandbox_throughput_mean           (MB/s)
  - gke_payload_sandbox_throughput_p50            (MB/s)
  - gke_payload_sandbox_throughput_min            (MB/s)
  - gke_payload_sandbox_rss_start                (MB)
  - gke_payload_sandbox_rss_end                  (MB)
  - gke_payload_sandbox_rss_growth               (MB)
  - gke_payload_wall_time                        (seconds)
"""

import logging
import time

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker.linux_benchmarks.kubernetes.agentic import (
    gke_benchmark_utils as utils,
)
from perfkitbenchmarker.linux_benchmarks.kubernetes.agentic import (
    gke_deploy_utils as deploy_utils,
)

FLAGS = flags.FLAGS

BENCHMARK_NAME = "gke_payload"
BENCHMARK_CONFIG = """
gke_payload:
  description: >
    Atomic single-point payload transfer saturation measurement on a
    pre-provisioned GKE cluster with gVisor isolation.
"""

_WARMPOOL_NAME = "python-sandbox-warmpool"
_WARMPOOL_LABEL = "sandbox=python-sandbox-example"

# ---------------------------------------------------------------------------
# Benchmark-specific flags
# ---------------------------------------------------------------------------

flags.DEFINE_float(
    "gke_payload_size_mb",
    1.0,
    "Payload size in megabytes to transfer from the sandbox.",
)

flags.DEFINE_integer(
    "gke_payload_iterations",
    20,
    "Number of transfer iterations per sandbox session.",
)

flags.DEFINE_integer(
    "gke_payload_concurrent_sessions",
    5,
    "Number of parallel sandbox sessions.",
)

flags.DEFINE_integer(
    "gke_payload_exec_timeout",
    300,
    "Sandbox command execution timeout in seconds.",
)

flags.DEFINE_bool(
    "gke_payload_patch_warmpool",
    True,
    "Patch SandboxWarmPool replicas to match concurrent_sessions before measurement.",
)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def GetConfig(user_config):
    """Load and return benchmark config.

    No vm_groups — PKB skips Provision() and Teardown().
    """
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
    """Deploy workloads and verify agent API."""
    logging.info("=== Prepare: deploying workloads ===")
    deploy_utils.DeployWorkloads(benchmark_spec)
    utils.CheckAgentHealthz(required=False)
    utils.EnsurePortForward()
    logging.info("Prepare complete.")


def Run(benchmark_spec):
    """Execute a single payload transfer measurement and return samples.

    Returns:
      List of sample.Sample objects.
    """
    utils.set_benchmark_spec(benchmark_spec)

    ns = FLAGS.k8s_namespace
    payload_size_mb = FLAGS.gke_payload_size_mb
    iterations = FLAGS.gke_payload_iterations
    concurrent = FLAGS.gke_payload_concurrent_sessions

    logging.info(
        "=== Run: payload_size_mb=%s, iterations=%d, concurrent=%d ===",
        payload_size_mb,
        iterations,
        concurrent,
    )

    # Ensure port-forward is active (needed when sweeps skip Prepare)
    utils.EnsurePortForward()

    # Patch warm pool (moved from Prepare for sweep compatibility)
    if FLAGS.gke_payload_patch_warmpool:
        utils.PatchWarmPool(
            namespace=ns,
            warmpool_name=_WARMPOOL_NAME,
            replicas=concurrent,
            label=_WARMPOOL_LABEL,
        )

    # POST to agent API
    payload = {
        "payload_size_mb": payload_size_mb,
        "payload_iterations": iterations,
        "concurrent_sessions": concurrent,
        "sandbox_exec_timeout_s": FLAGS.gke_payload_exec_timeout,
    }

    t0 = time.time()
    result = utils.CallAgentApi("/benchmark/python/payload", payload)
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
        "payload_size_mb": payload_size_mb,
        "payload_iterations": iterations,
        "concurrent_sessions": concurrent,
        "successful_sessions": successful,
        "failed_sessions": failed,
        "wall_time_s": round(wall_time, 2),
    }

    samples = []

    # Orchestrator-side transfer latency
    _emit(
        samples,
        agg,
        "orchestrator_transfer_mean_ms",
        "orchestrator_transfer_mean",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "orchestrator_transfer_p50_ms",
        "orchestrator_transfer_p50",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "orchestrator_transfer_p95_ms",
        "orchestrator_transfer_p95",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "orchestrator_transfer_p99_ms",
        "orchestrator_transfer_p99",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "orchestrator_transfer_min_ms",
        "orchestrator_transfer_min",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "orchestrator_transfer_max_ms",
        "orchestrator_transfer_max",
        "ms",
        ns,
        extra,
    )

    # Payload metadata
    _emit(
        samples,
        agg,
        "sandbox_payload_size_bytes",
        "sandbox_payload_size_bytes",
        "bytes",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_payload_encoded_size_bytes",
        "sandbox_payload_encoded_size_bytes",
        "bytes",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_payload_iterations",
        "sandbox_payload_iterations",
        "count",
        ns,
        extra,
    )

    # Generation time (os.urandom)
    _emit(
        samples,
        agg,
        "sandbox_generation_time_mean_ms",
        "sandbox_generation_time_mean",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_generation_time_p50_ms",
        "sandbox_generation_time_p50",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_generation_time_p95_ms",
        "sandbox_generation_time_p95",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_generation_time_p99_ms",
        "sandbox_generation_time_p99",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_generation_time_min_ms",
        "sandbox_generation_time_min",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_generation_time_max_ms",
        "sandbox_generation_time_max",
        "ms",
        ns,
        extra,
    )

    # Serialization time (base64 encode)
    _emit(
        samples,
        agg,
        "sandbox_serialization_time_mean_ms",
        "sandbox_serialization_time_mean",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_serialization_time_p50_ms",
        "sandbox_serialization_time_p50",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_serialization_time_p95_ms",
        "sandbox_serialization_time_p95",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_serialization_time_p99_ms",
        "sandbox_serialization_time_p99",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_serialization_time_min_ms",
        "sandbox_serialization_time_min",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_serialization_time_max_ms",
        "sandbox_serialization_time_max",
        "ms",
        ns,
        extra,
    )

    # Stdout write time (gVisor Gofer write syscall)
    _emit(
        samples,
        agg,
        "sandbox_stdout_time_mean_ms",
        "sandbox_stdout_time_mean",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_stdout_time_p50_ms",
        "sandbox_stdout_time_p50",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_stdout_time_p95_ms",
        "sandbox_stdout_time_p95",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_stdout_time_p99_ms",
        "sandbox_stdout_time_p99",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_stdout_time_min_ms",
        "sandbox_stdout_time_min",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_stdout_time_max_ms",
        "sandbox_stdout_time_max",
        "ms",
        ns,
        extra,
    )

    # Transfer time (serialization + stdout write — threshold metric)
    _emit(
        samples,
        agg,
        "sandbox_transfer_time_mean_ms",
        "sandbox_transfer_time_mean",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_transfer_time_p50_ms",
        "sandbox_transfer_time_p50",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_transfer_time_p95_ms",
        "sandbox_transfer_time_p95",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_transfer_time_p99_ms",
        "sandbox_transfer_time_p99",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_transfer_time_min_ms",
        "sandbox_transfer_time_min",
        "ms",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_transfer_time_max_ms",
        "sandbox_transfer_time_max",
        "ms",
        ns,
        extra,
    )

    # Throughput
    _emit(
        samples,
        agg,
        "sandbox_throughput_mean_mbps",
        "sandbox_throughput_mean",
        "MB/s",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_throughput_p50_mbps",
        "sandbox_throughput_p50",
        "MB/s",
        ns,
        extra,
    )
    _emit(
        samples,
        agg,
        "sandbox_throughput_min_mbps",
        "sandbox_throughput_min",
        "MB/s",
        ns,
        extra,
    )

    # RSS
    _emit(samples, agg, "sandbox_rss_start_mb", "sandbox_rss_start", "MB", ns, extra)
    _emit(samples, agg, "sandbox_rss_end_mb", "sandbox_rss_end", "MB", ns, extra)
    _emit(samples, agg, "sandbox_rss_growth_mb", "sandbox_rss_growth", "MB", ns, extra)

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

    logging.info(
        "Emitted %d samples for payload_size_mb=%s.", len(samples), payload_size_mb
    )
    return samples


def Cleanup(benchmark_spec):
    """Clean up after measurement. Scale warm pool to 0."""
    ns = FLAGS.k8s_namespace
    logging.info("Cleanup: draining warm pool.")

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


def _emit(samples, agg, agg_key, metric_suffix, unit, namespace, extra):
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
