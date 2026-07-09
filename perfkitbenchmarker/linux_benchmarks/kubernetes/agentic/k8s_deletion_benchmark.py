"""PKB Benchmark: GKE Agent Deletion & Cleanup .

Atomic single-point measurement of bulk deletion efficiency and IP
reclamation on a pre-provisioned GKE cluster with gVisor isolation.
Provisions N sandbox pods via SandboxWarmPool, then bulk-deletes them
and measures per-pod deletion latency, aggregate deletion stats, and
IP address reclamation timing.

This benchmark is designed to be invoked repeatedly by an external sweep
controller that varies the batch_size parameter across iterations to find
the deletion saturation point.

Usage:
  python pkb.py --benchmarks=gke_deletion \\
                --k8s_deletion_batch_size=100 \\
                --k8s_deletion_warmpool_name=python-sandbox-warmpool \\
                --k8s_deletion_pod_label=sandbox=python-sandbox-example \\
                --k8s_deletion_poll_interval_s=1.0 \\
                --k8s_deletion_provision_timeout_s=120.0 \\
                --k8s_deletion_drain_timeout_s=300.0 \\
                --k8s_agentic_namespace=agentic \\
                --gke_machine_type=c4-standard-8

Samples emitted (per run):
  - gke_deletion_provision_time              (seconds)
  - gke_deletion_total_drain_time            (seconds)
  - gke_deletion_latency_p50                 (seconds)
  - gke_deletion_latency_p95                 (seconds)
  - gke_deletion_latency_p99                 (seconds)
  - gke_deletion_latency_max                 (seconds)
  - gke_deletion_rate                        (pods/sec)
  - gke_deletion_ip_before                   (count)
  - gke_deletion_ip_after                    (count)
  - gke_deletion_ip_reclaim_time             (seconds)
  - gke_deletion_final_running_count         (count)
  - gke_deletion_wall_time                   (seconds)
"""

from __future__ import annotations


import json
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

BENCHMARK_NAME = "k8s_deletion"
BENCHMARK_CONFIG = """
k8s_deletion:
  description: >
    Atomic single-point bulk deletion and IP reclamation measurement on a
    pre-provisioned GKE cluster with gVisor isolation.
"""

# ---------------------------------------------------------------------------
# Benchmark-specific flags
# ---------------------------------------------------------------------------

flags.DEFINE_integer(
    "k8s_deletion_batch_size",
    100,
    "Number of sandbox pods to provision then bulk-delete.",
)

flags.DEFINE_string(
    "k8s_deletion_warmpool_name",
    "python-sandbox-warmpool",
    "SandboxWarmPool resource name.",
)

flags.DEFINE_string(
    "k8s_deletion_pod_label",
    "sandbox=python-sandbox-example",
    "Label selector for warm pool pods.",
)

flags.DEFINE_float(
    "k8s_deletion_poll_interval_s",
    1.0,
    "Seconds between kubectl polls during deletion.",
)

flags.DEFINE_float(
    "k8s_deletion_provision_timeout_s",
    120.0,
    "Max seconds to wait for pods to reach Running before deletion.",
)

flags.DEFINE_float(
    "k8s_deletion_drain_timeout_s",
    300.0,
    "Max seconds to wait for all pods to terminate after scale-to-0.",
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
    """Deploy workloads onto the cluster."""
    benchmark_spec.always_call_cleanup = True
    logging.info("=== Prepare: deploying workloads ===")
    deploy_utils.DeployWorkloads(benchmark_spec)
    utils.EnsurePortForward()
    logging.info("Prepare complete.")


def Run(benchmark_spec: object) -> list[sample.Sample]:
    """Provision N pods, bulk-delete, measure deletion latency and IP reclamation.

    Returns:
      List of sample.Sample objects.
    """
    utils.set_benchmark_spec(benchmark_spec)

    ns = FLAGS.k8s_agentic_namespace
    batch_size = FLAGS.k8s_deletion_batch_size
    warmpool_name = FLAGS.k8s_deletion_warmpool_name
    label = FLAGS.k8s_deletion_pod_label
    poll_interval = FLAGS.k8s_deletion_poll_interval_s
    provision_timeout = FLAGS.k8s_deletion_provision_timeout_s
    drain_timeout = FLAGS.k8s_deletion_drain_timeout_s

    logging.info("=== Run: batch_size=%d ===", batch_size)

    # Drain to 0 for clean measurement (moved from Prepare for sweep compatibility)
    utils.DrainWarmPool(ns, warmpool_name, label, timeout=int(drain_timeout))
    time.sleep(2)

    t_wall_start = time.time()

    # 1. Provision N pods
    logging.info("Provisioning %d pods...", batch_size)
    provision_start = time.time()
    _PatchReplicas(ns, warmpool_name, batch_size)

    deadline = time.time() + provision_timeout
    while time.time() < deadline:
        running = utils.CountPods(ns, label, phase="Running")
        pct = (running / batch_size * 100) if batch_size > 0 else 0
        logging.info("Provisioning... %d/%d (%.0f%%)", running, batch_size, pct)
        if running >= batch_size:
            break
        time.sleep(3)

    provision_time = time.time() - provision_start
    final_running = utils.CountPods(ns, label, phase="Running")

    logging.info(
        "Provisioned %d/%d pods in %.1fs",
        final_running,
        batch_size,
        provision_time,
    )

    # If not all pods reached Running, this is a failure
    if final_running < batch_size:
        raise RuntimeError(
            f"Provisioning failed: only {final_running}/{batch_size} pods "
            f"reached Running within {provision_timeout}s"
        )

    # 2. Record pod names and IP count before deletion
    pod_names_before = set(_GetPodNames(ns, label))
    ip_before = _CountAllocatedIPs(ns, label)

    logging.info(
        "Recorded %d pods, %d IPs allocated",
        len(pod_names_before),
        ip_before,
    )

    # Brief settle
    time.sleep(1)

    # 3. Bulk delete: scale to 0
    logging.info("Scaling to 0 (bulk delete of %d pods)...", len(pod_names_before))
    _PatchReplicas(ns, warmpool_name, 0)

    # 4. Poll: track pod disappearance and IP reclamation
    t_delete = time.time()
    deadline_drain = t_delete + drain_timeout
    pod_gone_times = {}  # pod_name -> elapsed_s when first absent
    ip_reclaim_time = None

    while time.time() < deadline_drain:
        elapsed = time.time() - t_delete

        # Current pod names still present
        current_pods = set(_GetPodNames(ns, label))
        remaining = len(current_pods)

        # Track which pods have disappeared
        gone_now = pod_names_before - current_pods
        for pn in gone_now:
            if pn not in pod_gone_times:
                pod_gone_times[pn] = elapsed

        # IP count (scoped to warm pool label)
        ips = _CountAllocatedIPs(ns, label)
        if ip_reclaim_time is None and ips == 0:
            ip_reclaim_time = elapsed

        deleted = len(pod_names_before) - remaining
        pct = (deleted / len(pod_names_before) * 100) if pod_names_before else 0
        logging.info(
            "[%.1fs] Deleted: %d/%d (%.0f%%)  IPs: %d",
            elapsed,
            deleted,
            len(pod_names_before),
            pct,
            ips,
        )

        if remaining == 0:
            break

        time.sleep(poll_interval)

    total_drain_time = time.time() - t_delete

    # Pods we never saw disappear (stuck) get the full drain time
    for pn in pod_names_before:
        if pn not in pod_gone_times:
            pod_gone_times[pn] = total_drain_time

    # 5. Compute per-pod deletion latencies
    deletion_latencies = sorted(pod_gone_times.values())
    n = len(deletion_latencies)

    ip_after = _CountAllocatedIPs(ns, label)
    deletion_rate = (
        (len(pod_names_before) / total_drain_time) if total_drain_time > 0 else 0
    )

    logging.info(
        "Drain complete: %.1fs, rate=%.1f pods/sec, IPs: %d->%d",
        total_drain_time,
        deletion_rate,
        ip_before,
        ip_after,
    )

    wall_time = time.time() - t_wall_start

    # 6. Build samples
    extra = {
        "batch_size": batch_size,
        "final_running_count": final_running,
        "ip_before": ip_before,
        "ip_after": ip_after,
        "wall_time_s": round(wall_time, 2),
    }

    samples = []

    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_provision_time",
            round(provision_time, 2),
            "seconds",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_total_drain_time",
            round(total_drain_time, 2),
            "seconds",
            ns,
            extra,
        )
    )

    if n > 0:
        samples.append(
            utils.MakeSample(
                f"{BENCHMARK_NAME}_latency_p50",
                round(_Percentile(deletion_latencies, 50), 3),
                "seconds",
                ns,
                extra,
            )
        )
        samples.append(
            utils.MakeSample(
                f"{BENCHMARK_NAME}_latency_p95",
                round(_Percentile(deletion_latencies, 95), 3),
                "seconds",
                ns,
                extra,
            )
        )
        samples.append(
            utils.MakeSample(
                f"{BENCHMARK_NAME}_latency_p99",
                round(_Percentile(deletion_latencies, 99), 3),
                "seconds",
                ns,
                extra,
            )
        )
        samples.append(
            utils.MakeSample(
                f"{BENCHMARK_NAME}_latency_max",
                round(deletion_latencies[-1], 3),
                "seconds",
                ns,
                extra,
            )
        )

    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_rate",
            round(deletion_rate, 2),
            "pods/sec",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_ip_before",
            float(ip_before),
            "count",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_ip_after",
            float(ip_after),
            "count",
            ns,
            extra,
        )
    )

    if ip_reclaim_time is not None:
        samples.append(
            utils.MakeSample(
                f"{BENCHMARK_NAME}_ip_reclaim_time",
                round(ip_reclaim_time, 2),
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
            f"{BENCHMARK_NAME}_wall_time",
            round(wall_time, 2),
            "seconds",
            ns,
            extra,
        )
    )

    logging.info("Emitted %d samples for batch_size=%d.", len(samples), batch_size)
    return samples


def Cleanup(benchmark_spec: object) -> None:
    """Best-effort drain of warm pool after measurement."""
    ns = FLAGS.k8s_agentic_namespace
    warmpool_name = FLAGS.k8s_deletion_warmpool_name
    label = FLAGS.k8s_deletion_pod_label

    logging.info("Cleanup: draining warm pool to 0.")
    utils.DrainWarmPool(ns, warmpool_name, label, timeout=int(FLAGS.k8s_deletion_drain_timeout_s))
    utils.StopPortForward()
    logging.info("Cleanup complete.")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _PatchReplicas(namespace: str, warmpool_name: str, replicas: int) -> None:
    """Patch SandboxWarmPool to a specific replica count."""
    patch_json = json.dumps({"spec": {"replicas": replicas}})
    utils.RunKubectl(
        [
            "patch",
            "sandboxwarmpool",
            warmpool_name,
            "-n",
            namespace,
            "--type=merge",
            f"-p={patch_json}",
        ],
        raise_on_failure=False,
    )


def _GetPodNames(namespace: str, label: str) -> list[str]:
    """Return list of pod names matching the label selector."""
    stdout, _, rc = utils.RunKubectl(
        [
            "get",
            "pods",
            "-n",
            namespace,
            "-l",
            label,
            "-o",
            "jsonpath={.items[*].metadata.name}",
        ],
        timeout=30,
        raise_on_failure=False,
    )
    if rc != 0 or not stdout:
        return []
    return stdout.split()


def _CountAllocatedIPs(namespace: str, label: str) -> int:
    """Count pod IPs currently allocated for pods matching the label.

    Scoped to the warm pool label to accurately measure IPAM release
    efficiency for the specific pods being deleted.
    """
    stdout, _, rc = utils.RunKubectl(
        [
            "get",
            "pods",
            "-n",
            namespace,
            "-l",
            label,
            "-o",
            "jsonpath={.items[*].status.podIP}",
        ],
        timeout=30,
        raise_on_failure=False,
    )
    if rc != 0 or not stdout:
        return 0
    return len([ip for ip in stdout.split() if ip])


def _Percentile(sorted_values: list[float], pct: float) -> float:
    """Calculate percentile (0-100) with linear interpolation."""
    if not sorted_values:
        return 0.0
    idx = (pct / 100) * (len(sorted_values) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = idx - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac
