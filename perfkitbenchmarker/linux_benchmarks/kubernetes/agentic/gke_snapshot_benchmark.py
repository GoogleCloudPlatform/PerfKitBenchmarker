"""PKB Benchmark: GKE Agent Pod Snapshot Saturation (Use Case A).

Atomic single-point measurement of GKE Pod Snapshot create/restore latency
on a pre-provisioned GKE cluster with gVisor isolation.  Measures snapshot
time, restore time, TTFE (Time To First Execution), and restore correctness
at a given preload_mb and burst_size.

This benchmark is designed to be invoked repeatedly by an external sweep
controller that varies the preload_mb parameter across iterations to find
the saturation point.

Usage:
  python pkb.py --benchmarks=gke_snapshot \\
                --gke_snapshot_preload_mb=50 \\
                --gke_snapshot_burst_size=3 \\
                --gke_namespace=agentic \\
                --gke_snapshot_skip_snapshot=false

Samples emitted (per run):
  - gke_snapshot_snapshot_p50        (seconds)
  - gke_snapshot_snapshot_p95        (seconds)
  - gke_snapshot_snapshot_max        (seconds)
  - gke_snapshot_restore_p50         (seconds)
  - gke_snapshot_restore_p95         (seconds)
  - gke_snapshot_restore_max         (seconds)
  - gke_snapshot_ttfe_p50            (seconds)
  - gke_snapshot_ttfe_p95            (seconds)
  - gke_snapshot_ttfe_max            (seconds)
  - gke_snapshot_startup_time        (seconds)
  - gke_snapshot_restore_correct_count (count)
  - gke_snapshot_wall_time           (seconds)
"""

import json
import logging
import os
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks.kubernetes.agentic import (
    gke_benchmark_utils as utils,
)
from perfkitbenchmarker.linux_benchmarks.kubernetes.agentic import (
    gke_deploy_utils as deploy_utils,
)
from perfkitbenchmarker.linux_benchmarks.kubernetes.agentic import gke_provision_utils

FLAGS = flags.FLAGS

BENCHMARK_NAME = "gke_snapshot"
BENCHMARK_CONFIG = """
gke_snapshot:
  description: >
    Atomic single-point Pod Snapshot saturation measurement on a
    pre-provisioned GKE cluster with gVisor isolation.
"""

# ---------------------------------------------------------------------------
# Benchmark-specific flags
# ---------------------------------------------------------------------------

flags.DEFINE_integer(
    "gke_snapshot_preload_mb",
    10,
    "Megabytes of memory to pre-allocate in the sandbox before snapshot.",
)

flags.DEFINE_integer(
    "gke_snapshot_burst_size",
    1,
    "Number of concurrent source/snapshot/restore pods per measurement.",
)

flags.DEFINE_string(
    "gke_snapshot_ksa_name",
    "pod-snapshot-sa",
    "Kubernetes service account for pod snapshots.",
)

flags.DEFINE_integer(
    "gke_snapshot_pod_timeout",
    180,
    "Max seconds to wait for pod Running / preload.",
)

flags.DEFINE_boolean(
    "gke_snapshot_skip_snapshot",
    False,
    "Skip snapshot/restore phases — measure cold-start TTFE only.",
)

flags.DEFINE_string(
    "gke_snapshot_preload_mode",
    "synthetic",
    "Preload mode: 'synthetic' (os.urandom fill) or "
    "'script:<path>' to run a custom startup script.",
)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def Provision(benchmark_spec):
    """Provision GKE cluster and all dependencies."""
    gke_provision_utils.Provision()


def GetConfig(user_config):
    """Load and return benchmark config.

    No vm_groups — PKB skips Provision() and Teardown().
    """
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
    """Deploy workloads, snapshot infra, and validate readiness."""
    ns = FLAGS.gke_namespace
    preload_mb = FLAGS.gke_snapshot_preload_mb

    logging.info(
        "=== Prepare: preload_mb=%d, burst_size=%d ===",
        preload_mb,
        FLAGS.gke_snapshot_burst_size,
    )

    # Deploy Agent Sandbox ecosystem (idempotent)
    deploy_utils.DeployWorkloads()

    # Deploy Pod Snapshot infrastructure (idempotent)
    deploy_utils.DeploySnapshots()

    # 1. Verify PodSnapshotStorageConfig exists (cluster-scoped).
    _, _, retcode = utils.RunKubectl(
        ["get", "podsnapshotstorageconfigs.podsnapshot.gke.io", "--no-headers"],
        timeout=30,
        raise_on_failure=False,
    )
    if retcode != 0:
        raise RuntimeError(
            "PodSnapshotStorageConfig CRD not found. "
            "Ensure pod snapshots are enabled on the cluster."
        )
    logging.info("PodSnapshotStorageConfig verified.")

    # 2. Verify PodSnapshotPolicy exists in the namespace.
    _, _, retcode = utils.RunKubectl(
        ["get", "podsnapshotpolicies.podsnapshot.gke.io", "-n", ns, "--no-headers"],
        timeout=30,
        raise_on_failure=False,
    )
    if retcode != 0:
        logging.warning("PodSnapshotPolicy not found in namespace %s.", ns)

    # 3. Verify the service account exists.
    ksa = FLAGS.gke_snapshot_ksa_name
    _, _, retcode = utils.RunKubectl(
        ["get", "serviceaccount", ksa, "-n", ns],
        timeout=30,
        raise_on_failure=False,
    )
    if retcode != 0:
        raise RuntimeError(
            f"ServiceAccount {ksa} not found in namespace {ns}. "
            "Run setup_snapshot_gke.sh or ensure DeploySnapshots() succeeded."
        )
    logging.info("ServiceAccount %s verified.", ksa)

    # 4. Verify the template file exists.
    template_path = _GetTemplatePath()
    if not os.path.isfile(template_path):
        raise RuntimeError(f"Snapshot template not found: {template_path}")
    logging.info("Template file verified: %s", template_path)

    utils.EnsurePortForward()
    logging.info("Prepare complete.")


def Run(benchmark_spec):
    """Execute a single snapshot/restore measurement and return samples.

    Returns:
      List of sample.Sample objects.
    """
    ns = FLAGS.gke_namespace
    preload_mb = FLAGS.gke_snapshot_preload_mb
    burst_size = FLAGS.gke_snapshot_burst_size
    skip_snapshot = FLAGS.gke_snapshot_skip_snapshot
    preload_mode = FLAGS.gke_snapshot_preload_mode
    ksa_name = FLAGS.gke_snapshot_ksa_name
    pod_timeout = FLAGS.gke_snapshot_pod_timeout

    logging.info(
        "=== Run: preload_mb=%d, burst_size=%d, skip_snapshot=%s ===",
        preload_mb,
        burst_size,
        skip_snapshot,
    )

    template_path = _GetTemplatePath()
    t0 = time.time()

    # Run the snapshot/restore cycle
    step_result = _RunSnapshotCycle(
        namespace=ns,
        preload_mb=preload_mb,
        burst_size=burst_size,
        skip_snapshot=skip_snapshot,
        preload_mode=preload_mode,
        ksa_name=ksa_name,
        pod_timeout=pod_timeout,
        template_path=template_path,
    )

    wall_time = time.time() - t0

    # Build samples
    extra = {
        "preload_mb": preload_mb,
        "burst_size": burst_size,
        "skip_snapshot": skip_snapshot,
        "preload_mode": preload_mode,
        "restore_correct_count": step_result.get("restore_correct_count", 0),
        "wall_time_s": round(wall_time, 2),
    }

    if step_result.get("error"):
        extra["error"] = step_result["error"]

    samples = []

    # Snapshot metrics
    _emit(samples, step_result, "snapshot_p50_s", "snapshot_p50", "seconds", ns, extra)
    _emit(samples, step_result, "snapshot_p95_s", "snapshot_p95", "seconds", ns, extra)
    _emit(samples, step_result, "snapshot_max_s", "snapshot_max", "seconds", ns, extra)

    # Restore metrics
    _emit(samples, step_result, "restore_p50_s", "restore_p50", "seconds", ns, extra)
    _emit(samples, step_result, "restore_p95_s", "restore_p95", "seconds", ns, extra)
    _emit(samples, step_result, "restore_max_s", "restore_max", "seconds", ns, extra)

    # TTFE metrics
    _emit(samples, step_result, "ttfe_p50_s", "ttfe_p50", "seconds", ns, extra)
    _emit(samples, step_result, "ttfe_p95_s", "ttfe_p95", "seconds", ns, extra)
    _emit(samples, step_result, "ttfe_max_s", "ttfe_max", "seconds", ns, extra)

    # Startup time
    _emit(samples, step_result, "startup_time_s", "startup_time", "seconds", ns, extra)

    # Restore correctness
    correct = step_result.get("restore_correct_count")
    if correct is not None:
        samples.append(
            utils.MakeSample(
                f"{BENCHMARK_NAME}_restore_correct_count",
                correct,
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

    logging.info("Emitted %d samples for preload_mb=%d.", len(samples), preload_mb)
    return samples


def Cleanup(benchmark_spec):
    """Clean up any leftover benchmark resources."""
    ns = FLAGS.gke_namespace
    logging.info("Cleanup — deleting any leftover snapshot-benchmark resources.")

    for kind in (
        "sandboxclaim",
        "sandboxtemplate",
        "podsnapshotmanualtrigger",
        "podsnapshots.podsnapshot.gke.io",
    ):
        utils.RunKubectl(
            [
                "delete",
                kind,
                "-l",
                "app=snapshot-benchmark-workload",
                "-n",
                ns,
                "--ignore-not-found=true",
            ],
            timeout=60,
            raise_on_failure=False,
        )
    utils.StopPortForward()
    logging.info("Cleanup complete.")


def Teardown(benchmark_spec):
    """Teardown GKE cluster and all dependencies."""
    gke_provision_utils.Teardown()


# ---------------------------------------------------------------------------
# Core snapshot/restore logic
# ---------------------------------------------------------------------------


def _RunSnapshotCycle(
    namespace,
    preload_mb,
    burst_size,
    skip_snapshot,
    preload_mode,
    ksa_name,
    pod_timeout,
    template_path,
):
    """Execute one full snapshot/restore cycle and return a result dict.

    Handles source creation, snapshot, restore, TTFE measurement,
    correctness verification, and cleanup.
    """
    step_template = f"snap-bench-{preload_mb}mb"
    source_names = [f"snap-src-0-{i}" for i in range(burst_size)]
    restore_names = [f"snap-restore-0-{i}" for i in range(burst_size)]
    trigger_names = [f"snap-trigger-0-{i}" for i in range(burst_size)]

    result = {
        "preload_mb": preload_mb,
        "burst_size": burst_size,
        "snapshot_p50_s": None,
        "snapshot_p95_s": None,
        "snapshot_max_s": None,
        "restore_p50_s": None,
        "restore_p95_s": None,
        "restore_max_s": None,
        "ttfe_p50_s": None,
        "ttfe_p95_s": None,
        "ttfe_max_s": None,
        "startup_time_s": None,
        "snapshot_counter": None,
        "restore_correct_count": 0,
        "burst_results": [],
        "error": None,
    }

    try:
        # 1. Create step-specific SandboxTemplate
        logging.info(
            "Creating SandboxTemplate '%s' (PRELOAD_MB=%d, memory=%dMi)",
            step_template,
            preload_mb,
            max(512, preload_mb + 256),
        )
        if not _RenderAndApplyTemplate(
            template_path,
            step_template,
            namespace,
            ksa_name,
            preload_mb,
            preload_mode,
        ):
            raise RuntimeError("Failed to create SandboxTemplate")

        time.sleep(2)

        # 2. Create source claims and wait for Running + preload
        logging.info("Creating %d source SandboxClaim(s)", burst_size)
        t0_sources = time.time()
        workers = min(burst_size, 50)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for sname in source_names:
                pool.submit(_ApplyClaim, sname, namespace, step_template)

        logging.info("Waiting for %d source pod(s) Running + preload", burst_size)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            source_futs = [
                pool.submit(
                    _MeasureSingleSource,
                    sname,
                    namespace,
                    t0_sources,
                    pod_timeout,
                    preload_mode,
                )
                for sname in source_names
            ]
            source_results = [f.result() for f in source_futs]

        src_failed = [r for r in source_results if r.get("error")]
        if src_failed:
            fail_msgs = "; ".join(f"{r['pod']}: {r['error']}" for r in src_failed)
            raise RuntimeError(
                f"{len(src_failed)}/{burst_size} source pod(s) failed: {fail_msgs}"
            )

        startup_times = [
            r["startup_time_s"]
            for r in source_results
            if r["startup_time_s"] is not None
        ]
        result["startup_time_s"] = (
            round(_Percentile(startup_times, 50), 3) if startup_times else None
        )

        snapshot_counters = {r["pod"]: r["snapshot_counter"] for r in source_results}
        min_counter = min(
            (c for c in snapshot_counters.values() if c is not None), default=None
        )
        result["snapshot_counter"] = min_counter
        logging.info("%d source pod(s) ready. Min counter: %s", burst_size, min_counter)

        # --skip_snapshot: measure cold-start TTFE only
        if skip_snapshot:
            logging.info("skip_snapshot mode: measuring cold-start TTFE")
            ttfe_times = []
            burst_results = []
            for i, sname in enumerate(source_names):
                startup = source_results[i]["startup_time_s"]
                counter = source_results[i]["snapshot_counter"]
                preload_done = source_results[i].get("preload_complete_time_s")
                ttfe_s = preload_done if preload_done else startup
                ttfe_times.append(ttfe_s)
                burst_results.append(
                    {
                        "pod": sname,
                        "source_pod": sname,
                        "startup_time_s": startup,
                        "snapshot_counter": None,
                        "snapshot_time_s": None,
                        "restore_time_s": None,
                        "ttfe_s": ttfe_s,
                        "restore_counter": counter,
                        "restore_correct": True,
                        "error": None,
                    }
                )

            result["burst_results"] = burst_results
            result["restore_correct_count"] = burst_size

            if ttfe_times:
                result["ttfe_p50_s"] = round(_Percentile(ttfe_times, 50), 3)
                result["ttfe_p95_s"] = round(_Percentile(ttfe_times, 95), 3)
                result["ttfe_max_s"] = round(max(ttfe_times), 3)

            # Skip to cleanup
            return result

        # 3. Trigger snapshots concurrently
        logging.info("Triggering %d snapshot(s)", burst_size)
        t0_snap = time.time()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            snap_futs = [
                pool.submit(
                    _TriggerAndWaitSnapshot,
                    tname,
                    sname,
                    namespace,
                    t0_snap,
                )
                for tname, sname in zip(trigger_names, source_names)
            ]
            snap_results = [f.result() for f in snap_futs]

        snap_failed = [r for r in snap_results if r.get("error")]
        snap_times = [
            r["snapshot_time_s"]
            for r in snap_results
            if r["snapshot_time_s"] is not None
        ]
        if snap_times:
            result["snapshot_p50_s"] = round(_Percentile(snap_times, 50), 3)
            result["snapshot_p95_s"] = round(_Percentile(snap_times, 95), 3)
            result["snapshot_max_s"] = round(max(snap_times), 3)

        if snap_failed:
            fail_msgs = "; ".join(f"{r['trigger']}: {r['error']}" for r in snap_failed)
            raise RuntimeError(
                f"{len(snap_failed)}/{burst_size} snapshot(s) failed: {fail_msgs}"
            )

        # 4. Create restore claims concurrently
        logging.info("Creating %d restore SandboxClaim(s)", burst_size)
        t0_burst = time.time()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            create_futs = [
                pool.submit(_ApplyClaim, rname, namespace, step_template)
                for rname in restore_names
            ]
            for f in create_futs:
                f.result()

        # 5. Poll restore pods for Running + TTFE
        logging.info("Measuring restore + TTFE across %d pod(s)", burst_size)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            measure_futs = [
                pool.submit(
                    _MeasureSingleRestore,
                    rname,
                    namespace,
                    t0_burst,
                    min_counter,
                    pod_timeout,
                )
                for rname in restore_names
            ]
            burst_results = [f.result() for f in measure_futs]

        # Merge source + snapshot info
        for i in range(burst_size):
            burst_results[i]["source_pod"] = source_names[i]
            burst_results[i]["startup_time_s"] = source_results[i]["startup_time_s"]
            burst_results[i]["snapshot_counter"] = source_results[i]["snapshot_counter"]
            burst_results[i]["snapshot_time_s"] = snap_results[i]["snapshot_time_s"]

        result["burst_results"] = burst_results

        # 6. Aggregate
        restore_times = [
            r["restore_time_s"]
            for r in burst_results
            if r["restore_time_s"] is not None
        ]
        ttfe_times = [r["ttfe_s"] for r in burst_results if r["ttfe_s"] is not None]
        correct_count = sum(1 for r in burst_results if r["restore_correct"])

        result["restore_correct_count"] = correct_count

        if restore_times:
            result["restore_p50_s"] = round(_Percentile(restore_times, 50), 3)
            result["restore_p95_s"] = round(_Percentile(restore_times, 95), 3)
            result["restore_max_s"] = round(max(restore_times), 3)

        if ttfe_times:
            result["ttfe_p50_s"] = round(_Percentile(ttfe_times, 50), 3)
            result["ttfe_p95_s"] = round(_Percentile(ttfe_times, 95), 3)
            result["ttfe_max_s"] = round(max(ttfe_times), 3)

        logging.info("Counter correct: %d/%d", correct_count, burst_size)

    except Exception as e:
        result["error"] = str(e)
        logging.error("Snapshot cycle failed: %s", e)

    finally:
        # Cleanup
        logging.info("Cleaning up step resources")
        _CleanupStep(
            source_names,
            restore_names,
            trigger_names,
            step_template,
            namespace,
        )
        time.sleep(5)

    return result


# ---------------------------------------------------------------------------
# Kubernetes interaction helpers
# ---------------------------------------------------------------------------


def _ApplyClaim(name, namespace, template_name):
    """Create a SandboxClaim."""
    manifest = json.dumps(
        {
            "apiVersion": "extensions.agents.x-k8s.io/v1alpha1",
            "kind": "SandboxClaim",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {"app": "snapshot-benchmark-workload"},
            },
            "spec": {"sandboxTemplateRef": {"name": template_name}},
        }
    )
    proc = subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=manifest,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"Failed to create SandboxClaim {name}: {proc.stderr}")


def _RenderAndApplyTemplate(
    template_path,
    template_name,
    namespace,
    ksa_name,
    preload_mb,
    preload_mode,
):
    """Render the .yaml.template with step-specific values and kubectl apply."""
    if preload_mode.startswith("script:"):
        return _RenderAndApplyScriptTemplate(
            template_name,
            namespace,
            ksa_name,
            preload_mb,
            preload_mode,
        )

    with open(template_path) as f:
        content = f.read()

    memory_mi = max(512, preload_mb + 256)

    rendered = (
        content.replace("$AGENTIC_NAMESPACE", namespace)
        .replace("$SNAPSHOT_KSA_NAME", ksa_name)
        .replace("$SNAPSHOT_PRELOAD_MB", str(preload_mb))
    )
    rendered = rendered.replace(
        "name: snapshot-benchmark-template",
        f"name: {template_name}",
    )
    rendered = rendered.replace(
        'memory: "512Mi"',
        f'memory: "{memory_mi}Mi"',
    )

    proc = subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=rendered,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if proc.returncode != 0:
        logging.warning("kubectl apply stderr: %s", proc.stderr)
    return proc.returncode == 0


def _get_sandbox_node_selector():
    """Return the correct nodeSelector based on provisioning mode."""
    try:
        mode = FLAGS.gke_provision_mode
    except AttributeError:
        mode = "custom"
    if mode == "native":
        return {"pkb_nodepool": "sandbox"}
    return {"dedicated": "agentic-sandbox"}


def _get_sandbox_tolerations():
    """Return the correct tolerations based on provisioning mode."""
    try:
        mode = FLAGS.gke_provision_mode
    except AttributeError:
        mode = "custom"
    tolerations = [
        {
            "key": "sandbox.gke.io/runtime",
            "operator": "Equal",
            "value": "gvisor",
            "effect": "NoSchedule",
        },
    ]
    if mode != "native":
        tolerations.insert(
            0,
            {
                "key": "dedicated",
                "operator": "Equal",
                "value": "agentic-sandbox",
                "effect": "NoSchedule",
            },
        )
    return tolerations


def _RenderAndApplyScriptTemplate(
    template_name,
    namespace,
    ksa_name,
    preload_mb,
    preload_mode,
):
    """Render a SandboxTemplate that runs a user-provided startup script."""
    script_path = preload_mode.split(":", 1)[1]
    if not os.path.isfile(script_path):
        logging.error("Script not found: %s", script_path)
        return False

    with open(script_path) as f:
        user_script = f.read()

    memory_mi = max(512, preload_mb + 256)

    entrypoint = (
        "#!/bin/bash\n"
        "set -e\n"
        'echo "Running startup script..."\n'
        "# --- User script start ---\n"
        f"{user_script}\n"
        "# --- User script end ---\n"
        'echo "SCRIPT_READY"\n'
        'echo "Starting counter."\n'
        "i=0\n"
        "while true; do\n"
        '  echo "Count: $i"\n'
        "  i=$((i + 1))\n"
        "  sleep 1\n"
        "done\n"
    )

    manifest = {
        "apiVersion": "extensions.agents.x-k8s.io/v1alpha1",
        "kind": "SandboxTemplate",
        "metadata": {
            "name": template_name,
            "namespace": namespace,
        },
        "spec": {
            "podTemplate": {
                "metadata": {
                    "labels": {"app": "snapshot-benchmark-workload"},
                },
                "spec": {
                    "serviceAccountName": ksa_name,
                    "runtimeClassName": "gvisor",
                    "containers": [
                        {
                            "name": "preloader",
                            "image": "python:3.11-slim",
                            "command": ["bash", "-c"],
                            "args": [entrypoint],
                            "env": [{"name": "PRELOAD_MB", "value": str(preload_mb)}],
                            "resources": {
                                "requests": {
                                    "cpu": "250m",
                                    "memory": f"{memory_mi}Mi",
                                    "ephemeral-storage": "512Mi",
                                }
                            },
                        }
                    ],
                    "nodeSelector": _get_sandbox_node_selector(),
                    "tolerations": _get_sandbox_tolerations(),
                    "restartPolicy": "OnFailure",
                },
            }
        },
    }

    proc = subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=json.dumps(manifest),
        capture_output=True,
        text=True,
        timeout=30,
    )
    if proc.returncode != 0:
        logging.warning("kubectl apply stderr: %s", proc.stderr)
    return proc.returncode == 0


def _MeasureSingleSource(name, namespace, t0, pod_timeout, preload_mode):
    """Wait for a source pod to be Running and preloaded."""
    result = {
        "pod": name,
        "startup_time_s": None,
        "preload_complete_time_s": None,
        "snapshot_counter": None,
        "error": None,
    }

    # Wait for Running
    deadline = t0 + pod_timeout
    while time.time() < deadline:
        stdout, _, rc = utils.RunKubectl(
            ["get", "pod", name, "-n", namespace, "-o", "jsonpath={.status.phase}"],
            timeout=10,
            raise_on_failure=False,
        )
        if stdout == "Running":
            result["startup_time_s"] = round(time.time() - t0, 3)
            break
        time.sleep(1)
    else:
        result["error"] = f"Pod {name} did not reach Running within {pod_timeout}s"
        return result

    # Wait for preload
    if not _WaitForPreload(name, namespace, pod_timeout, preload_mode):
        result["error"] = f"Preload did not complete within {pod_timeout}s"
        return result

    result["preload_complete_time_s"] = round(time.time() - t0, 3)

    # Let counter tick
    time.sleep(3)
    result["snapshot_counter"] = _GetLastCounter(name, namespace)
    return result


def _WaitForPreload(name, namespace, timeout_s, preload_mode):
    """Wait for preload to complete."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        stdout, _, rc = utils.RunKubectl(
            ["logs", name, "-n", namespace, "--tail=20"],
            timeout=10,
            raise_on_failure=False,
        )
        if "SCRIPT_READY" in stdout:
            return True
        if "Starting counter" in stdout or re.search(r"Count:\s*\d+", stdout):
            return True
        time.sleep(2)
    return False


def _GetLastCounter(name, namespace):
    """Extract the last Count: N value from pod logs."""
    stdout, _, rc = utils.RunKubectl(
        ["logs", name, "-n", namespace, "--tail=10"],
        timeout=10,
        raise_on_failure=False,
    )
    if rc != 0:
        return None
    matches = re.findall(r"Count:\s*(\d+)", stdout)
    return int(matches[-1]) if matches else None


def _TriggerAndWaitSnapshot(trigger_name, target_pod, namespace, t0, timeout_s=300):
    """Create a snapshot trigger and wait for Complete."""
    result = {
        "trigger": trigger_name,
        "pod": target_pod,
        "snapshot_time_s": None,
        "error": None,
    }
    manifest = json.dumps(
        {
            "apiVersion": "podsnapshot.gke.io/v1",
            "kind": "PodSnapshotManualTrigger",
            "metadata": {"name": trigger_name, "namespace": namespace},
            "spec": {"targetPod": target_pod},
        }
    )
    proc = subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=manifest,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if proc.returncode != 0:
        result["error"] = f"Failed to create trigger: {proc.stderr}"
        return result

    deadline = t0 + timeout_s
    while time.time() < deadline:
        stdout, _, rc = utils.RunKubectl(
            [
                "get",
                "podsnapshotmanualtriggers.podsnapshot.gke.io",
                trigger_name,
                "-n",
                namespace,
                "-o",
                "jsonpath={.status.conditions[0].reason}",
            ],
            timeout=10,
            raise_on_failure=False,
        )
        if stdout == "Complete":
            result["snapshot_time_s"] = round(time.time() - t0, 3)
            return result
        time.sleep(2)
    result["error"] = f"Snapshot {trigger_name} did not complete within {timeout_s}s"
    return result


def _MeasureSingleRestore(name, namespace, t0, snapshot_counter, pod_timeout):
    """Measure restore_time and TTFE for a single pod."""
    result = {
        "pod": name,
        "restore_time_s": None,
        "ttfe_s": None,
        "restore_counter": None,
        "restore_correct": False,
        "error": None,
    }

    # Wait for Running
    deadline = t0 + pod_timeout
    while time.time() < deadline:
        stdout, _, rc = utils.RunKubectl(
            ["get", "pod", name, "-n", namespace, "-o", "jsonpath={.status.phase}"],
            timeout=10,
            raise_on_failure=False,
        )
        if stdout == "Running":
            result["restore_time_s"] = round(time.time() - t0, 3)
            break
        time.sleep(1)
    else:
        result["error"] = f"Pod {name} did not reach Running within {pod_timeout}s"
        return result

    # Wait for first Count (TTFE)
    ttfe_deadline = t0 + pod_timeout
    while time.time() < ttfe_deadline:
        stdout, _, rc = utils.RunKubectl(
            ["logs", name, "-n", namespace, "--tail=50"],
            timeout=10,
            raise_on_failure=False,
        )
        if rc == 0:
            matches = re.findall(r"Count:\s*(\d+)", stdout)
            if matches:
                result["ttfe_s"] = round(time.time() - t0, 3)
                result["restore_counter"] = int(matches[0])
                if (
                    snapshot_counter is not None
                    and result["restore_counter"] >= snapshot_counter
                ):
                    result["restore_correct"] = True
                return result
        time.sleep(1)

    result["error"] = f"Pod {name}: no Count output within timeout"
    return result


def _CleanupStep(source_names, restore_names, trigger_names, template_name, namespace):
    """Delete source claims, restore claims, triggers, snapshots, and template."""
    to_delete = [("sandboxtemplate", template_name)]
    for name in source_names:
        to_delete.append(("sandboxclaim", name))
    for name in restore_names:
        to_delete.append(("sandboxclaim", name))
    for name in trigger_names:
        to_delete.append(("podsnapshotmanualtrigger", name))

    for kind, name in to_delete:
        utils.RunKubectl(
            ["delete", kind, name, "-n", namespace, "--ignore-not-found=true"],
            timeout=60,
            raise_on_failure=False,
        )
    # Delete any PodSnapshot resources
    utils.RunKubectl(
        [
            "delete",
            "podsnapshots.podsnapshot.gke.io",
            "--all",
            "-n",
            namespace,
            "--ignore-not-found=true",
        ],
        timeout=60,
        raise_on_failure=False,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _GetTemplatePath():
    """Return the absolute path to the snapshot SandboxTemplate template."""
    pkg_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(
        pkg_dir,
        "data",
        "k8s_agents",
        "manifests",
        "snapshot-sandbox-template.yaml.template",
    )


def _Percentile(values, pct):
    """Calculate percentile (0-100) from a list of values."""
    if not values:
        return 0.0
    s = sorted(values)
    idx = (pct / 100) * (len(s) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(s) - 1)
    frac = idx - lo
    return s[lo] * (1 - frac) + s[hi] * frac


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
