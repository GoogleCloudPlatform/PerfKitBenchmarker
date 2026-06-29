"""PKB Benchmark: GKE Agent QPS Saturation .

Atomic single-point measurement of scheduling throughput on a pre-provisioned
GKE cluster.  Fires sandbox claim requests at a controlled QPS rate for a
fixed duration and measures per-request TTFE (Time To First Execution).

Supports two operating modes:
  - **agent**: POST to the orchestrator /benchmark/python/qps endpoint
  - **raw_claim**: Bypass the agent, create SandboxClaims directly via kubectl

This benchmark is designed to be invoked repeatedly by an external sweep
controller that varies the target_qps parameter across iterations to find
the QPS saturation point.

Usage:
  # Agent mode
  python pkb.py --benchmarks=gke_qps \\
                --k8s_qps_target_qps=5.0 \\
                --k8s_qps_pool_size=70 \\
                --k8s_qps_step_duration_s=30.0 \\
                --k8s_qps_mode=agent \\
                --k8s_namespace=agentic \\
                --k8s_agent_api_url=http://localhost:8080

  # Raw claim mode
  python pkb.py --benchmarks=gke_qps \\
                --k8s_qps_target_qps=5.0 \\
                --k8s_qps_pool_size=70 \\
                --k8s_qps_step_duration_s=30.0 \\
                --k8s_qps_mode=raw_claim \\
                --k8s_qps_claim_timeout_s=60.0 \\
                --k8s_namespace=agentic

Samples emitted (per run):
  - gke_qps_ttfe_mean                (ms)
  - gke_qps_ttfe_p50                 (ms)
  - gke_qps_ttfe_p95                 (ms)
  - gke_qps_ttfe_p99                 (ms)
  - gke_qps_ttfe_min                 (ms)
  - gke_qps_ttfe_max                 (ms)
  - gke_qps_claim_mean               (ms)
  - gke_qps_claim_p95                (ms)
  - gke_qps_actual_qps               (requests/sec)
  - gke_qps_duration                 (seconds)
  - gke_qps_total_requests           (count)
  - gke_qps_successful_requests      (count)
  - gke_qps_failed_requests          (count)
  - gke_qps_pool_before              (count)
  - gke_qps_pool_after               (count)
  - gke_qps_wall_time                (seconds)
"""

import json
import os
import logging
import threading
import time
import uuid

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.linux_benchmarks.kubernetes.agentic import (
    k8s_benchmark_utils as utils,
)
from perfkitbenchmarker.linux_benchmarks.kubernetes.agentic import (
    gke_deploy_utils as deploy_utils,
)

FLAGS = flags.FLAGS

BENCHMARK_NAME = "k8s_qps"
BENCHMARK_CONFIG = """
k8s_qps:
  description: >
    Atomic single-point QPS saturation measurement on a
    pre-provisioned GKE cluster with gVisor isolation.
"""

_WARMPOOL_NAME = "python-sandbox-warmpool"
_WARMPOOL_LABEL = "sandbox=python-sandbox-example"
_SANDBOX_TEMPLATE = "python-sandbox-template"
_QPS_CLAIM_LABEL = "created-by=pkb-qps-benchmark"

# ---------------------------------------------------------------------------
# Benchmark-specific flags
# ---------------------------------------------------------------------------

flags.DEFINE_float(
    "k8s_qps_target_qps",
    5.0,
    "Target requests per second (sandbox claims per second).",
)

flags.DEFINE_integer(
    "k8s_qps_pool_size",
    70,
    "Warm pool size maintained during the measurement.",
)

flags.DEFINE_float(
    "k8s_qps_step_duration_s",
    30.0,
    "Duration of the QPS burst in seconds.",
)

flags.DEFINE_integer(
    "k8s_qps_sandbox_exec_timeout_s",
    30,
    "Sandbox command execution timeout in seconds.",
)

flags.DEFINE_float(
    "k8s_qps_provision_timeout_s",
    180.0,
    "Max seconds to wait for pool pods to reach Running.",
)

flags.DEFINE_string(
    "k8s_qps_mode",
    "agent",
    "Operating mode: 'agent' (POST to orchestrator API) or "
    "'raw_claim' (create SandboxClaims directly via kubectl).",
)

flags.DEFINE_float(
    "k8s_qps_claim_timeout_s",
    60.0,
    "Max seconds to wait for a raw claim to bind " "(only used with mode=raw_claim).",
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

    mode = FLAGS.k8s_qps_mode
    if mode == "agent":
        utils.CheckAgentHealthz(required=False)
    utils.EnsurePortForward()
    logging.info("Prepare complete.")


def Run(benchmark_spec):
    """Execute a single QPS measurement and return samples.

    Returns:
      List of sample.Sample objects.
    """
    utils.set_benchmark_spec(benchmark_spec)

    ns = FLAGS.k8s_namespace
    pool_size = FLAGS.k8s_qps_pool_size

    # Scale warm pool (moved from Prepare for sweep compatibility)
    utils.PatchWarmPool(
        namespace=ns,
        warmpool_name=_WARMPOOL_NAME,
        replicas=pool_size,
        label=_WARMPOOL_LABEL,
        wait_timeout=int(FLAGS.k8s_qps_provision_timeout_s),
    )

    mode = FLAGS.k8s_qps_mode

    if mode == "raw_claim":
        return _RunRawClaim(benchmark_spec)
    else:
        return _RunAgent(benchmark_spec)


def Cleanup(benchmark_spec):
    """Delete benchmark claims and drain warm pool."""
    ns = FLAGS.k8s_namespace
    logging.info("Cleanup: deleting benchmark claims and draining warm pool.")

    # Delete any lingering benchmark claims
    _DeleteBenchmarkClaims(ns)

    # Drain warm pool
    utils.DrainWarmPool(
        namespace=ns,
        warmpool_name=_WARMPOOL_NAME,
        label=_WARMPOOL_LABEL,
    )

    utils.StopPortForward()
    logging.info("Cleanup complete.")


# ---------------------------------------------------------------------------
# Agent mode
# ---------------------------------------------------------------------------


def _RunAgent(benchmark_spec):
    """Fire QPS burst via the orchestrator API."""
    ns = FLAGS.k8s_namespace
    target_qps = FLAGS.k8s_qps_target_qps
    pool_size = FLAGS.k8s_qps_pool_size
    step_duration = FLAGS.k8s_qps_step_duration_s

    logging.info(
        "=== Run (agent): target_qps=%s, pool_size=%d, duration=%ss ===",
        target_qps,
        pool_size,
        step_duration,
    )

    # Ensure port-forward is active (needed when sweeps skip Prepare)
    utils.EnsurePortForward()

    # Record pool state before burst
    pool_before = utils.CountPods(ns, _WARMPOOL_LABEL, phase="Running")

    # POST to agent API
    payload = {
        "target_qps": target_qps,
        "duration_s": step_duration,
        "sandbox_exec_timeout_s": FLAGS.k8s_qps_sandbox_exec_timeout_s,
    }

    t0 = time.time()
    api_timeout = int(step_duration + 300)
    result = utils.CallAgentApi("/benchmark/python/qps", payload, timeout=api_timeout)
    wall_time = time.time() - t0

    # Record pool state after burst
    pool_after = utils.CountPods(ns, _WARMPOOL_LABEL, phase="Running")

    # Extract response fields
    aggregate = result.get("aggregate", {})
    successful = result.get("successful_requests", 0)
    failed = result.get("failed_requests", 0)
    total = result.get("total_requests", 0)
    actual_qps = result.get("actual_qps", 0)
    duration_s = result.get("duration_s", 0)

    logging.info(
        "API response: actual_qps=%s, %d/%d requests ok (%.1fs)",
        actual_qps,
        successful,
        total,
        wall_time,
    )

    # Build samples
    extra = {
        "target_qps": target_qps,
        "pool_size": pool_size,
        "step_duration_s": step_duration,
        "mode": "agent",
        "actual_qps": actual_qps,
        "total_requests": total,
        "successful_requests": successful,
        "failed_requests": failed,
        "pool_before": pool_before,
        "pool_after": pool_after,
        "wall_time_s": round(wall_time, 2),
    }

    samples = []

    # TTFE latency stats
    _emit(samples, aggregate, "ttfe_mean_ms", "ttfe_mean", "ms", ns, extra)
    _emit(samples, aggregate, "ttfe_p50_ms", "ttfe_p50", "ms", ns, extra)
    _emit(samples, aggregate, "ttfe_p95_ms", "ttfe_p95", "ms", ns, extra)
    _emit(samples, aggregate, "ttfe_p99_ms", "ttfe_p99", "ms", ns, extra)
    _emit(samples, aggregate, "ttfe_min_ms", "ttfe_min", "ms", ns, extra)
    _emit(samples, aggregate, "ttfe_max_ms", "ttfe_max", "ms", ns, extra)

    # Claim latency stats
    _emit(samples, aggregate, "claim_mean_ms", "claim_mean", "ms", ns, extra)
    _emit(samples, aggregate, "claim_p95_ms", "claim_p95", "ms", ns, extra)

    # Throughput and counts
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_actual_qps",
            actual_qps,
            "requests/sec",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_duration",
            duration_s,
            "seconds",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_total_requests",
            float(total),
            "count",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_successful_requests",
            float(successful),
            "count",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_failed_requests",
            float(failed),
            "count",
            ns,
            extra,
        )
    )

    # Pool state
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_pool_before",
            float(pool_before),
            "count",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_pool_after",
            float(pool_after),
            "count",
            ns,
            extra,
        )
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

    logging.info("Emitted %d samples for target_qps=%s.", len(samples), target_qps)
    return samples


# ---------------------------------------------------------------------------
# Raw claim mode
# ---------------------------------------------------------------------------


def _RunRawClaim(benchmark_spec):
    """Fire SandboxClaims directly at target_qps (no agent)."""
    ns = FLAGS.k8s_namespace
    target_qps = FLAGS.k8s_qps_target_qps
    pool_size = FLAGS.k8s_qps_pool_size
    step_duration = FLAGS.k8s_qps_step_duration_s
    claim_timeout = FLAGS.k8s_qps_claim_timeout_s

    logging.info(
        "=== Run (raw_claim): target_qps=%s, pool_size=%d, duration=%ss ===",
        target_qps,
        pool_size,
        step_duration,
    )

    # Record pool state before burst
    pool_before = utils.CountPods(ns, _WARMPOOL_LABEL, phase="Running")

    # Calculate total claims to fire
    total_claims = max(1, int(target_qps * step_duration))
    interval = 1.0 / target_qps if target_qps > 0 else 1.0

    logging.info(
        "Firing %d raw SandboxClaims at %s req/s",
        total_claims,
        target_qps,
    )

    # Fire claims at target QPS in parallel threads
    claim_results = []
    lock = threading.Lock()

    def _fire_and_wait(idx, fire_time):
        claim_name = f"pkb-qps-0-{idx}-{uuid.uuid4().hex[:6]}"
        result = {"request_id": idx, "fire_time_s": round(fire_time, 3)}
        try:
            t_create = _CreateClaim(ns, _SANDBOX_TEMPLATE, claim_name)
            result["create_ts"] = t_create
            t_bound = _WaitClaimBound(ns, claim_name, claim_timeout)
            if t_bound is not None:
                ttfe_ms = (t_bound - t_create) * 1000.0
                result["ttfe_ms"] = round(ttfe_ms, 3)
                result["claim_ms"] = round(ttfe_ms, 3)
                result["error"] = None
            else:
                result["ttfe_ms"] = None
                result["error"] = "Timeout waiting for claim to bind"
        except Exception as e:
            result["ttfe_ms"] = None
            result["error"] = f"{type(e).__name__}: {e}"
        with lock:
            claim_results.append(result)

    t0 = time.time()
    threads = []
    for i in range(total_claims):
        fire_time = time.time() - t0
        t = threading.Thread(target=_fire_and_wait, args=(i, fire_time), daemon=True)
        threads.append(t)
        t.start()
        if i < total_claims - 1:
            next_fire = t0 + (i + 1) * interval
            sleep_time = next_fire - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)

    for t in threads:
        t.join(timeout=claim_timeout + 30)

    wall_time = time.time() - t0
    actual_qps = round(total_claims / wall_time, 2) if wall_time > 0 else 0

    # Record pool state after burst
    pool_after = utils.CountPods(ns, _WARMPOOL_LABEL, phase="Running")

    # Aggregate results
    successful = [r for r in claim_results if r.get("ttfe_ms") is not None]
    failed = [r for r in claim_results if r.get("error")]
    ttfe_values = sorted(r["ttfe_ms"] for r in successful)

    logging.info(
        "Raw claim burst complete: %d/%d ok, actual_qps=%s (%.1fs)",
        len(successful),
        total_claims,
        actual_qps,
        wall_time,
    )

    # Build samples
    extra = {
        "target_qps": target_qps,
        "pool_size": pool_size,
        "step_duration_s": step_duration,
        "mode": "raw_claim",
        "actual_qps": actual_qps,
        "total_requests": total_claims,
        "successful_requests": len(successful),
        "failed_requests": len(failed),
        "pool_before": pool_before,
        "pool_after": pool_after,
        "wall_time_s": round(wall_time, 2),
    }

    samples = []

    # TTFE latency stats (computed from raw claim results)
    if ttfe_values:
        n = len(ttfe_values)
        samples.append(
            utils.MakeSample(
                f"{BENCHMARK_NAME}_ttfe_mean",
                round(sum(ttfe_values) / n, 3),
                "ms",
                ns,
                extra,
            )
        )
        samples.append(
            utils.MakeSample(
                f"{BENCHMARK_NAME}_ttfe_p50",
                round(_percentile(ttfe_values, 50), 3),
                "ms",
                ns,
                extra,
            )
        )
        samples.append(
            utils.MakeSample(
                f"{BENCHMARK_NAME}_ttfe_p95",
                round(_percentile(ttfe_values, 95), 3),
                "ms",
                ns,
                extra,
            )
        )
        samples.append(
            utils.MakeSample(
                f"{BENCHMARK_NAME}_ttfe_p99",
                round(_percentile(ttfe_values, 99), 3),
                "ms",
                ns,
                extra,
            )
        )
        samples.append(
            utils.MakeSample(
                f"{BENCHMARK_NAME}_ttfe_min",
                round(ttfe_values[0], 3),
                "ms",
                ns,
                extra,
            )
        )
        samples.append(
            utils.MakeSample(
                f"{BENCHMARK_NAME}_ttfe_max",
                round(ttfe_values[-1], 3),
                "ms",
                ns,
                extra,
            )
        )

        # Claim latency (same as TTFE in raw_claim mode)
        samples.append(
            utils.MakeSample(
                f"{BENCHMARK_NAME}_claim_mean",
                round(sum(ttfe_values) / n, 3),
                "ms",
                ns,
                extra,
            )
        )
        samples.append(
            utils.MakeSample(
                f"{BENCHMARK_NAME}_claim_p95",
                round(_percentile(ttfe_values, 95), 3),
                "ms",
                ns,
                extra,
            )
        )

    # Throughput and counts
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_actual_qps",
            actual_qps,
            "requests/sec",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_duration",
            round(wall_time, 2),
            "seconds",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_total_requests",
            float(total_claims),
            "count",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_successful_requests",
            float(len(successful)),
            "count",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_failed_requests",
            float(len(failed)),
            "count",
            ns,
            extra,
        )
    )

    # Pool state
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_pool_before",
            float(pool_before),
            "count",
            ns,
            extra,
        )
    )
    samples.append(
        utils.MakeSample(
            f"{BENCHMARK_NAME}_pool_after",
            float(pool_after),
            "count",
            ns,
            extra,
        )
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

    # Cleanup benchmark claims
    _DeleteBenchmarkClaims(ns)

    logging.info("Emitted %d samples for target_qps=%s.", len(samples), target_qps)
    return samples


# ---------------------------------------------------------------------------
# Raw claim helpers
# ---------------------------------------------------------------------------


def _CreateClaim(namespace, template, claim_name):
    """Create a single SandboxClaim via kubectl and return creation timestamp."""
    manifest = json.dumps(
        {
            "apiVersion": "extensions.agents.x-k8s.io/v1alpha1",
            "kind": "SandboxClaim",
            "metadata": {
                "name": claim_name,
                "namespace": namespace,
                "labels": {"created-by": "pkb-qps-benchmark"},
            },
            "spec": {
                "sandboxTemplateRef": {"name": template},
            },
        }
    )
    tmp_dir = os.path.join(
        data.ResourcePath("k8s_agents/manifests"), "tmp"
    )
    os.makedirs(tmp_dir, exist_ok=True)
    tmp_path = os.path.join(tmp_dir, f"qps-claim-{claim_name}.json")
    try:
        with open(tmp_path, "w") as f:
            f.write(manifest)
        stdout, stderr, retcode = kubectl.RunKubectlCommand(
            ["apply", "-f", tmp_path],
            timeout=30,
            raise_on_failure=False,
        )
    finally:
        if os.path.isfile(tmp_path):
            os.unlink(tmp_path)
    t_create = time.time()
    if retcode != 0:
        raise RuntimeError(
            f"Failed to create claim {claim_name}: {stderr.strip()}"
        )
    return t_create


def _WaitClaimBound(namespace, claim_name, timeout_s):
    """Wait for a SandboxClaim to reach Bound phase. Returns timestamp or None."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        stdout, _, rc = utils.RunKubectl(
            [
                "get",
                "sandboxclaim",
                claim_name,
                "-n",
                namespace,
                "-o",
                "jsonpath={.status.phase}",
            ],
            timeout=10,
            raise_on_failure=False,
        )
        if rc == 0 and stdout.lower() in ("bound", "ready"):
            return time.time()
        time.sleep(0.1)
    return None


def _DeleteBenchmarkClaims(namespace):
    """Delete SandboxClaims labelled created-by=pkb-qps-benchmark."""
    stdout, _, rc = utils.RunKubectl(
        [
            "get",
            "sandboxclaim",
            "-l",
            _QPS_CLAIM_LABEL,
            "-n",
            namespace,
            "-o",
            "jsonpath={.items[*].metadata.name}",
        ],
        timeout=30,
        raise_on_failure=False,
    )
    names = stdout.split() if stdout else []
    if not names or names == [""]:
        return 0

    count = len(names)
    logging.info("Deleting %d pkb-qps SandboxClaim(s)", count)
    utils.RunKubectl(
        [
            "delete",
            "sandboxclaim",
            "-l",
            _QPS_CLAIM_LABEL,
            "-n",
            namespace,
            "--wait=false",
        ],
        timeout=60,
        raise_on_failure=False,
    )

    # Wait for claims to be fully removed
    t0 = time.time()
    while time.time() - t0 < 120:
        stdout, _, _ = utils.RunKubectl(
            [
                "get",
                "sandboxclaim",
                "-l",
                _QPS_CLAIM_LABEL,
                "-n",
                namespace,
                "--no-headers",
                "--ignore-not-found",
            ],
            timeout=10,
            raise_on_failure=False,
        )
        remaining = len([l for l in stdout.splitlines() if l]) if stdout else 0
        if remaining == 0:
            break
        time.sleep(2)

    logging.info("Claims cleaned up in %.1fs", time.time() - t0)
    return count


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _percentile(sorted_values, pct):
    """Calculate percentile (0-100) with linear interpolation."""
    if not sorted_values:
        return 0.0
    idx = (pct / 100) * (len(sorted_values) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = idx - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


def _emit(samples, data, data_key, metric_suffix, unit, namespace, extra):
    """Emit a sample if the key exists in the data dict."""
    value = data.get(data_key)
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
