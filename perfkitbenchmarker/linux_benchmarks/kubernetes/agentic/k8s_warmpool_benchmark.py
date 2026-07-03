"""PKB Benchmark: GKE Agent Warmpool Scale-Up (Use Case E).

Atomic single-point measurement of warm pool provisioning speed on a
pre-provisioned GKE cluster.  Measures how quickly N sandbox pods can be
provisioned from zero via the SandboxWarmPool controller.  No agent API
is needed; this benchmark interacts directly with the Kubernetes API.

This benchmark is designed to be invoked repeatedly by an external sweep
controller that varies the target_replicas parameter across iterations to
find the provisioning saturation point.

Usage:
  python pkb.py --benchmarks=gke_warmpool \
                --k8s_warmpool_target_replicas=100 \
                --k8s_warmpool_name=python-sandbox-warmpool \
                --k8s_warmpool_pod_label=sandbox=python-sandbox-example \
                --k8s_warmpool_ready_threshold_s=300 \
                --k8s_warmpool_poll_interval_s=2.0 \
                --k8s_warmpool_drain_timeout_s=300 \
                --k8s_namespace=agentic \
                --gke_machine_type=c4-standard-8

Samples emitted (per run):
  - gke_warmpool_total_time_to_ready         (seconds)
  - gke_warmpool_refill_rate                 (pods/sec)
  - gke_warmpool_drain_time                  (seconds)
  - gke_warmpool_first_pod_running           (seconds)
  - gke_warmpool_final_running_count         (count)
  - gke_warmpool_final_pending_count         (count)
  - gke_warmpool_time_to_created_p50         (seconds)
  - gke_warmpool_time_to_created_p95         (seconds)
  - gke_warmpool_time_to_created_max         (seconds)
  - gke_warmpool_time_to_created_count       (count)
  - gke_warmpool_time_to_scheduled_p50       (seconds)
  - gke_warmpool_time_to_scheduled_p95       (seconds)
  - gke_warmpool_time_to_scheduled_max       (seconds)
  - gke_warmpool_time_to_scheduled_count     (count)
  - gke_warmpool_time_to_running_p50         (seconds)
  - gke_warmpool_time_to_running_p95         (seconds)
  - gke_warmpool_time_to_running_max         (seconds)
  - gke_warmpool_time_to_running_count       (count)
  - gke_warmpool_wall_time                   (seconds)
"""

import json
import logging
import time

from absl import flags
from datetime import datetime, timezone
from perfkitbenchmarker import configs
from perfkitbenchmarker.linux_benchmarks.kubernetes.agentic import (
    k8s_benchmark_utils as utils,
)
from perfkitbenchmarker.linux_benchmarks.kubernetes.agentic import (
    gke_deploy_utils as deploy_utils,
)

FLAGS = flags.FLAGS

BENCHMARK_NAME = "k8s_warmpool"
BENCHMARK_CONFIG = """
k8s_warmpool:
  description: >
    Atomic single-point warm pool scale-up measurement on a
    pre-provisioned GKE cluster with gVisor isolation.
"""

# ---------------------------------------------------------------------------
# Benchmark-specific flags
# ---------------------------------------------------------------------------

flags.DEFINE_integer(
    "k8s_warmpool_target_replicas",
    100,
    "Number of warm pool replicas to provision from zero.",
)

flags.DEFINE_string(
    "k8s_warmpool_name",
    "python-sandbox-warmpool",
    "SandboxWarmPool resource name.",
)

flags.DEFINE_string(
    "k8s_warmpool_pod_label",
    "sandbox=python-sandbox-example",
    "Label selector for warm pool pods.",
)

flags.DEFINE_float(
    "k8s_warmpool_ready_threshold_s",
    300.0,
    "Max seconds allowed for all pods to reach Running.",
)

flags.DEFINE_float(
    "k8s_warmpool_poll_interval_s",
    2.0,
    "Seconds between kubectl polls during provisioning.",
)

flags.DEFINE_float(
    "k8s_warmpool_drain_timeout_s",
    300.0,
    "Max seconds to wait for drain to 0.",
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
    """Deploy workloads onto the cluster."""
    benchmark_spec.always_call_cleanup = True
    logging.info("=== Prepare: deploying workloads ===")
    deploy_utils.DeployWorkloads(benchmark_spec)
    utils.EnsurePortForward()
    logging.info("Prepare complete.")


def Run(benchmark_spec):
    """Scale warm pool from 0 to target and measure provisioning time.

    Returns:
      List of sample.Sample objects.
    """
    utils.set_benchmark_spec(benchmark_spec)

    ns = FLAGS.k8s_namespace
    target = FLAGS.k8s_warmpool_target_replicas
    warmpool_name = FLAGS.k8s_warmpool_name
    label = FLAGS.k8s_warmpool_pod_label
    threshold_s = FLAGS.k8s_warmpool_ready_threshold_s
    poll_interval = FLAGS.k8s_warmpool_poll_interval_s

    # Drain to 0 for clean measurement (moved from Prepare for sweep compatibility)
    utils.DrainWarmPool(ns, warmpool_name, label, timeout=int(FLAGS.k8s_warmpool_drain_timeout_s))
    time.sleep(3)

    logging.info("=== Run: scaling %s to %d replicas ===", warmpool_name, target)

    t_wall_start = time.time()

    # 1. Measure drain time (should be near-zero since Prepare drained)
    t0 = time.time()
    utils.DrainWarmPool(ns, warmpool_name, label, timeout=int(FLAGS.k8s_warmpool_drain_timeout_s))
    drain_time_s = round(time.time() - t0, 2)

    time.sleep(2)

    # 2. Scale up
    logging.info("Patching %s replicas -> %d", warmpool_name, target)
    patch_json = json.dumps({"spec": {"replicas": target}})
    utils.RunKubectl(
        [
            "patch",
            "sandboxwarmpool",
            warmpool_name,
            "-n",
            ns,
            "--type=merge",
            f"-p={patch_json}",
        ]
    )

    # 3. Poll until ready or timeout
    t_scale = time.time()
    scale_start_epoch = t_scale
    deadline = t_scale + threshold_s
    first_pod_time = None

    while time.time() < deadline:
        elapsed = time.time() - t_scale
        running = utils.CountPods(ns, label, "Running")
        pending = utils.CountPods(ns, label, "Pending")

        if first_pod_time is None and running > 0:
            first_pod_time = elapsed

        pct = (running / target * 100) if target > 0 else 0
        logging.info(
            "[%.1fs] Running: %d/%d (%.0f%%)  Pending: %d",
            elapsed,
            running,
            target,
            pct,
            pending,
        )

        if running >= target:
            break

        time.sleep(poll_interval)

    total_time = round(time.time() - t_scale, 2)
    final_running = utils.CountPods(ns, label, "Running")
    final_pending = utils.CountPods(ns, label, "Pending")
    rate = round(final_running / total_time, 2) if total_time > 0 else 0

    logging.info(
        "Scale-up complete: %d/%d Running in %.1fs (%.1f pods/sec)",
        final_running,
        target,
        total_time,
        rate,
    )

    # 4. Scrape pod lifecycle timestamps
    lifecycle = _ScrapeLifecycle(ns, label, scale_start_epoch)

    wall_time = round(time.time() - t_wall_start, 2)

    # 5. Build samples
    extra = {
        "target_replicas": target,
        "final_running_count": final_running,
        "final_pending_count": final_pending,
        "wall_time_s": wall_time,
    }

    samples = []

    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_total_time_to_ready",
            total_time,
            "seconds",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_refill_rate",
            rate,
            "pods/sec",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_drain_time",
            drain_time_s,
            "seconds",
            ns,
            extra,
        )
    )

    if first_pod_time is not None:
        samples.append(
            utils.MakeSample(
                f"{BENCHMARK_NAME}_first_pod_running",
                round(first_pod_time, 2),
                "seconds",
                ns,
                extra,
            )
        )

    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_final_running_count",
            float(final_running),
            "count",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_final_pending_count",
            float(final_pending),
            "count",
            ns,
            extra,
        )
    )

    # Pod lifecycle percentiles
    _EmitLifecycleSamples(samples, lifecycle, ns, extra)

    # Wall time
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_wall_time",
            wall_time,
            "seconds",
            ns,
            extra,
        )
    )

    logging.info("Emitted %d samples for target_replicas=%d.", len(samples), target)
    return samples


def Cleanup(benchmark_spec):
    """Drain warm pool back to 0 after measurement."""
    ns = FLAGS.k8s_namespace
    warmpool_name = FLAGS.k8s_warmpool_name
    label = FLAGS.k8s_warmpool_pod_label

    logging.info("Cleanup: draining warm pool to 0.")
    utils.DrainWarmPool(ns, warmpool_name, label, timeout=int(FLAGS.k8s_warmpool_drain_timeout_s))
    utils.StopPortForward()
    logging.info("Cleanup complete.")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ScrapeLifecycle(namespace, label, scale_start_epoch):
    """Scrape pod metadata to compute time-to-created/scheduled/running.

    Returns a dict with P50/P95/max/count for each phase relative to
    scale_start_epoch.
    """
    stdout, _, rc = utils.RunKubectl(
        ["get", "pods", "-n", namespace, "-l", label, "-o", "json"],
        timeout=60,
        raise_on_failure=False,
    )
    if rc != 0 or not stdout:
        return {}

    pods = json.loads(stdout).get("items", [])
    created_deltas = []
    scheduled_deltas = []
    running_deltas = []

    for pod in pods:
        meta = pod.get("metadata", {})
        status = pod.get("status", {})

        # creationTimestamp -> time-to-created
        created_str = meta.get("creationTimestamp")
        if created_str:
            created_ts = datetime.fromisoformat(
                created_str.replace("Z", "+00:00")
            ).timestamp()
            created_deltas.append(created_ts - scale_start_epoch)

        # PodScheduled condition -> time-to-scheduled
        conditions = status.get("conditions", [])
        for cond in conditions:
            if cond.get("type") == "PodScheduled" and cond.get("status") == "True":
                ts_str = cond.get("lastTransitionTime")
                if ts_str:
                    ts = datetime.fromisoformat(
                        ts_str.replace("Z", "+00:00")
                    ).timestamp()
                    scheduled_deltas.append(ts - scale_start_epoch)
            if cond.get("type") == "Ready" and cond.get("status") == "True":
                ts_str = cond.get("lastTransitionTime")
                if ts_str:
                    ts = datetime.fromisoformat(
                        ts_str.replace("Z", "+00:00")
                    ).timestamp()
                    running_deltas.append(ts - scale_start_epoch)

    def _pcts(vals):
        if not vals:
            return {}
        vals.sort()
        n = len(vals)
        return {
            "p50": round(vals[n // 2], 2),
            "p95": round(vals[int(n * 0.95)], 2) if n > 1 else round(vals[-1], 2),
            "max": round(vals[-1], 2),
            "count": n,
        }

    return {
        "time_to_created_s": _pcts(created_deltas),
        "time_to_scheduled_s": _pcts(scheduled_deltas),
        "time_to_running_s": _pcts(running_deltas),
    }


def _EmitLifecycleSamples(samples, lifecycle, namespace, extra):
    """Emit pod lifecycle percentile samples for all three phases."""
    _PHASE_MAP = [
        ("time_to_created_s", "time_to_created"),
        ("time_to_scheduled_s", "time_to_scheduled"),
        ("time_to_running_s", "time_to_running"),
    ]
    for lifecycle_key, metric_base in _PHASE_MAP:
        phase_data = lifecycle.get(lifecycle_key, {})
        for stat in ("p50", "p95", "max"):
            val = phase_data.get(stat)
            if val is not None:
                samples.append(
                    utils.MakeSample(
                        f"{BENCHMARK_NAME}_{metric_base}_{stat}",
                        val,
                        "seconds",
                        namespace,
                        extra,
                    )
                )
        count = phase_data.get("count")
        if count is not None:
            samples.append(
                utils.MakeSample(
                    f"{BENCHMARK_NAME}_{metric_base}_count",
                    float(count),
                    "count",
                    namespace,
                    extra,
                )
            )
