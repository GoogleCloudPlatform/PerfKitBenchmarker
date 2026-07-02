"""PKB Benchmark: GKE Agent Chromium Density Saturation .

Atomic single-point measurement of Chromium browser sandbox density on a
pre-provisioned GKE cluster with gVisor isolation. Measures interaction
latency, screenshot generation time, cold start, navigation, evaluation,
fill, click latencies, and RSS memory at a given concurrent session count.

This benchmark is designed to be invoked repeatedly by an external sweep
controller that varies the density parameter across iterations to find
the saturation point.

Usage:
  python pkb.py --benchmarks=gke_chromium_density \\
                --k8s_chromium_density_concurrent_sessions=4 \\
                --k8s_chromium_density_task_count=10 \\
                --k8s_chromium_density_warmup_tasks=5 \\
                --k8s_namespace=agentic \\
                --k8s_agent_api_url=http://localhost:8080

Samples emitted (per run):
  - gke_chromium_density_interaction_mean      (ms)
  - gke_chromium_density_interaction_p95       (ms)
  - gke_chromium_density_navigate_mean         (ms)
  - gke_chromium_density_navigate_p95          (ms)
  - gke_chromium_density_evaluate_mean         (ms)
  - gke_chromium_density_evaluate_p95          (ms)
  - gke_chromium_density_fill_mean             (ms)
  - gke_chromium_density_fill_p95              (ms)
  - gke_chromium_density_click_mean            (ms)
  - gke_chromium_density_click_p95             (ms)
  - gke_chromium_density_screenshot_mean       (ms)
  - gke_chromium_density_screenshot_p95        (ms)
  - gke_chromium_density_cold_start_mean       (ms)
  - gke_chromium_density_cold_start_p95        (ms)
  - gke_chromium_density_rss_end               (MB)
  - gke_chromium_density_rss_growth            (MB)
  - gke_chromium_density_wall_time             (seconds)
"""

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

BENCHMARK_NAME = "k8s_chromium_density"
BENCHMARK_CONFIG = """
k8s_chromium_density:
  description: >
    Atomic single-point Chromium browser sandbox density measurement on a
    pre-provisioned GKE cluster with gVisor isolation.
"""

_WARMPOOL_NAME = "chromium-sandbox-warmpool"
_WARMPOOL_LABEL = "sandbox=chromium-sandbox-example"

# ---------------------------------------------------------------------------
# Benchmark-specific flags
# ---------------------------------------------------------------------------

flags.DEFINE_integer(
    "k8s_chromium_density_concurrent_sessions",
    1,
    "Number of concurrent Chromium browser sessions to run.",
)

flags.DEFINE_integer(
    "k8s_chromium_density_task_count",
    10,
    "Number of browser task iterations per Chromium session.",
)

flags.DEFINE_integer(
    "k8s_chromium_density_warmup_tasks",
    5,
    "Number of warmup iterations per session (excluded from stats).",
)

flags.DEFINE_bool(
    "k8s_chromium_density_patch_warmpool",
    True,
    "Patch SandboxWarmPool replicas to match density before measurement.",
)

flags.DEFINE_integer(
    "k8s_chromium_density_exec_timeout",
    120,
    "Sandbox command execution timeout in seconds.",
)

flags.DEFINE_integer(
    "k8s_chromium_density_provision_timeout",
    300,
    "Max seconds to wait for warm pool pods to reach Running.",
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
    """Execute a single Chromium density measurement and return samples.

    Returns:
      List of sample.Sample objects.
    """
    utils.set_benchmark_spec(benchmark_spec)

    ns = FLAGS.k8s_namespace
    density = FLAGS.k8s_chromium_density_concurrent_sessions

    logging.info("=== Run: chromium_density=%d ===", density)

    # Ensure port-forward is active (needed when sweeps skip Prepare)
    utils.EnsurePortForward()

    # Patch warm pool (moved from Prepare for sweep compatibility)
    if FLAGS.k8s_chromium_density_patch_warmpool:
        utils.PatchWarmPool(
            namespace=ns,
            warmpool_name=_WARMPOOL_NAME,
            replicas=density,
            label=_WARMPOOL_LABEL,
            wait_timeout=FLAGS.k8s_chromium_density_provision_timeout,
        )

    # POST to agent API
    payload = {
        "task_count": FLAGS.k8s_chromium_density_task_count,
        "warmup_tasks": FLAGS.k8s_chromium_density_warmup_tasks,
        "concurrent_sessions": density,
        "sandbox_exec_timeout_s": FLAGS.k8s_chromium_density_exec_timeout,
    }

    t0 = time.time()
    result = utils.CallAgentApi("/benchmark/chromium/density", payload)
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
        "task_count": FLAGS.k8s_chromium_density_task_count,
        "warmup_tasks": FLAGS.k8s_chromium_density_warmup_tasks,
        "wall_time_s": round(wall_time, 2),
    }

    samples = []

    # Per-task-type latency: mean and P95 for each
    _emit(samples, agg, "interaction_mean_ms", "interaction_mean", "ms", ns, extra)
    _emit(samples, agg, "interaction_p95_ms", "interaction_p95", "ms", ns, extra)
    _emit(samples, agg, "navigate_mean_ms", "navigate_mean", "ms", ns, extra)
    _emit(samples, agg, "navigate_p95_ms", "navigate_p95", "ms", ns, extra)
    _emit(samples, agg, "evaluate_mean_ms", "evaluate_mean", "ms", ns, extra)
    _emit(samples, agg, "evaluate_p95_ms", "evaluate_p95", "ms", ns, extra)
    _emit(samples, agg, "fill_mean_ms", "fill_mean", "ms", ns, extra)
    _emit(samples, agg, "fill_p95_ms", "fill_p95", "ms", ns, extra)
    _emit(samples, agg, "click_mean_ms", "click_mean", "ms", ns, extra)
    _emit(samples, agg, "click_p95_ms", "click_p95", "ms", ns, extra)
    _emit(samples, agg, "screenshot_mean_ms", "screenshot_mean", "ms", ns, extra)
    _emit(samples, agg, "screenshot_p95_ms", "screenshot_p95", "ms", ns, extra)
    _emit(samples, agg, "cold_start_mean_ms", "cold_start_mean", "ms", ns, extra)
    _emit(samples, agg, "cold_start_p95_ms", "cold_start_p95", "ms", ns, extra)

    # RSS memory
    _emit(samples, agg, "rss_end_mb", "rss_end", "MB", ns, extra)
    _emit(samples, agg, "rss_growth_mb", "rss_growth", "MB", ns, extra)

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

    logging.info("Emitted %d samples for chromium_density=%d.", len(samples), density)
    return samples


def Cleanup(benchmark_spec):
    """Clean up after measurement. Delete claims and drain warm pool."""
    ns = FLAGS.k8s_namespace
    logging.info("Cleanup: deleting SandboxClaims and draining warm pool.")

    # Delete any lingering SandboxClaims to release claimed pods
    utils.RunKubectl(
        [
            "delete",
            "sandboxclaims",
            "--all",
            "-n",
            ns,
            "--ignore-not-found=true",
        ],
        timeout=60,
        raise_on_failure=False,
    )

    # Drain warm pool to 0
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
