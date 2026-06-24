# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""GKE vs. AWS EKS Swap Encryption and LSSD Performance Benchmark.

Methodology: go/swap-encryption-and-lssd-performance-comparison:gke-vs-aws

== Architecture ==

Provisions a real GKE (GCP) or EKS (AWS) Kubernetes cluster via PKB's
container_cluster abstraction, then deploys a privileged DaemonSet whose
pod has full host-device access (/dev, /sys, hostPID).  All benchmark
phases execute inside this pod via kubectl exec, so measurements reflect
actual cluster-node behaviour including Kubernetes overhead (kubelet,
containerd cgroup hierarchy, etc.).

  GKE nodes  ── dm-crypt with ephemeral key (go/node:swap-encryption)
                 swap device: /dev/mapper/swap_encrypted (over dedicated
                 hyperdisk or LSSD RAID-0 /dev/md0).
                 Single-disk fallback: plain loop device on
                 /mnt/stateful_partition — dm-crypt is blocked by COS
                 kernel namespace restrictions from inside a pod.

  EKS nodes  ── NVMe Instance Store, Nitro hardware-offloaded encryption
                 swap device: /dev/nvme1n1 (or auto-detected)

== Benchmark Phases ==

  Phase 1 – fio Microbenchmarks
    Run fio directly on the swap block device (swapoff first) to measure
    the hardware + encryption ceiling: random IOPS (4K), sequential
    bandwidth (1M), and completion latency (iodepth=1).

  Phase 2a – CPU Overhead
    stress-ng drives sustained swap I/O; vmstat and pidstat capture
    swap-in/out rates and per-process CPU cost (kswapd, kcryptd,
    dm-crypt threads on GKE; Nitro offload on EKS).

  Phase 2b – I/O Interference
    Baseline fio on a scratch volume → re-run with concurrent swap
    pressure.  IOPS/latency delta = storage contention cost.

  Phase 3a – Redis Latency
    Dataset loaded beyond container memory limit → GET/SET p99 latency
    measured while kernel swaps pages.

  Phase 3b – Kernel Build
    Linux compiled inside a memory-capped cgroup; slowdown ratio vs
    unconstrained baseline.

  Phase 3c – OpenSearch
    Bulk-index + search query under swap pressure (esrally or curl).
"""

import json
import logging
import os
import tempfile
import textwrap
import time
from typing import Any

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec_lib
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.providers.gcp import util as gcp_util
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands

FLAGS = flags.FLAGS

_BenchmarkSpec = bm_spec_lib.BenchmarkSpec

# ---------------------------------------------------------------------------
# Benchmark identity
# ---------------------------------------------------------------------------

BENCHMARK_NAME = "swap_encryption"


BENCHMARK_CONFIG = """
swap_encryption:
  description: >
    GKE vs. EKS swap encryption and LSSD performance comparison.
    Two-step nodepool setup: PKB provisions a minimal cluster with a cheap
    default nodepool (Step 1), then Prepare() adds the real benchmark
    nodepool (n4-highmem-32 / c4-*-lssd, COS_CONTAINERD, 80k IOPS) with a
    node-level startup script that configures dm-crypt swap before any pod
    is scheduled, then removes the default nodepool (Step 2).  All benchmark
    phases run inside a privileged DaemonSet pinned to the benchmark nodepool.
  flags: {}
  container_cluster:
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        # Cheap placeholder — the benchmark nodepool is created in Prepare().
        machine_type: e2-medium
        boot_disk_size: 20
      AWS:
        # Cheap placeholder — the benchmark nodegroup is added in Prepare().
        machine_type: t3.medium
        boot_disk_size: 20
"""


_DAEMONSET_IMAGE = flags.DEFINE_string(
    "swap_encryption_daemonset_image",
    "ubuntu:22.04",
    "Container image used for the privileged benchmark DaemonSet pod.",
)


_NODEPOOL = flags.DEFINE_string(
    "swap_encryption_nodepool",
    "benchmark",
    "Name of the node pool to deploy the benchmark DaemonSet on.",
)


_INSTANCE_SIZE_LABEL = flags.DEFINE_string(
    "swap_encryption_instance_size_label",
    "",
    "Human-readable label for the current instance size being tested, e.g. "
    '"n4-highmem-32" or "i4i.4xlarge".  Stored in sample metadata so that '
    "results from multiple PKB runs across different instance sizes can be "
    "collated and compared.  Defaults to the value reported by the cloud "
    "metadata endpoint inside the pod.",
)


_COLLECT_COST = flags.DEFINE_boolean(
    "swap_encryption_collect_cost",
    False,
    "When True, emit a cost_estimate_usd sample using on-demand pricing "
    "for the instance type detected at runtime.",
)


_FAIL_ON_DEGRADED = flags.DEFINE_boolean(
    "swap_encryption_fail_on_degraded",
    True,
    "When True (default), raise an error at the end of Run() if the run was "
    "catastrophically degraded — e.g. the benchmark pod was OOM-evicted and "
    "replaced mid-run, Gate 1 (fio) produced no samples, or the stress-ng "
    "swap-pressure phase was OOM-killed before completing.  This prevents PKB "
    "from reporting SUCCEEDED for a run whose post-eviction phases produced "
    "empty or meaningless data.  Set False to keep the legacy behaviour of "
    "always returning whatever partial samples were collected.",
)


_PHASES = flags.DEFINE_list(
    "swap_encryption_phases",
    ["all"],
    "Which Run() phases to execute, for fast iteration against an "
    "already-provisioned cluster (e.g. --run_stage=run --run_uri=...).  "
    "Comma-separated subset of: fio (Tier 1 microbenchmarks), 2a (stress-ng "
    "CPU overhead + swap pressure), 2b (I/O interference), 3a (redis), "
    '3b (kernel build), 3c (opensearch).  Default "all" runs everything.  '
    "Example: --swap_encryption_phases=2a runs only the swap-pressure phase. "
    "Phases not listed are skipped and do not affect the degraded-run gate "
    '(e.g. skipping fio will not be reported as "Gate 1 produced no samples").',
)


_BENCHMARK_MACHINE_TYPE = flags.DEFINE_string(
    "swap_encryption_benchmark_machine_type",
    "n4-highmem-32",
    "Machine type for the benchmark nodepool created in Prepare(). "
    "Use n4-highmem-32 (hyperdisk, default) or c4-standard-8-lssd "
    "(LSSD RAID-0).  The matching swap setup is selected automatically.",
)


_BENCHMARK_LSSD = flags.DEFINE_boolean(
    "swap_encryption_lssd",
    False,
    "Force LSSD RAID-0 swap path even when the machine type name does not "
    'contain "lssd".  Auto-detected from machine type when False.',
)


_LSSD_COUNT = flags.DEFINE_integer(
    "swap_encryption_lssd_count",
    1,
    "Number of local NVMe SSDs to attach as raw block devices "
    "(--local-nvme-ssd-block count=N).  Must match the fixed local SSD "
    "count for the chosen machine type: c4-standard-8-lssd=1, "
    "c4-standard-16-lssd=2, i4i.4xlarge has NVMe Instance Store (AWS).  "
    "Default 1 covers most single-lssd machine types.",
)


_NODE_IMAGE_TYPE = flags.DEFINE_string(
    "swap_encryption_node_image_type",
    "UBUNTU_CONTAINERD",
    "GKE node image type for the benchmark nodepool.  "
    "UBUNTU_CONTAINERD is required for dm-crypt measurement: COS locks "
    "down device-mapper at the kernel LSM level and cryptsetup hangs "
    "indefinitely from any pod context (even privileged, even via nsenter "
    "into the host mount namespace).  Ubuntu GKE nodes allow cryptsetup "
    "from privileged pods without restriction.  "
    "Use COS_CONTAINERD only when dm-crypt is disabled "
    "(--noswap_encryption_enable_dmcrypt) to measure plain-swap overhead.  "
    "AL2 on EKS.",
)


_BOOT_DISK_TYPE = flags.DEFINE_string(
    "swap_encryption_boot_disk_type",
    "hyperdisk-balanced",
    "Disk type for the benchmark nodepool boot disk.  Use hyperdisk-balanced "
    "for production machines (n4, c3, c4 families).  Use pd-ssd for n2/e2 "
    "dev/test machines, which do not support hyperdisk-balanced.",
)


_BOOT_DISK_IOPS = flags.DEFINE_integer(
    "swap_encryption_boot_disk_iops",
    80000,
    "Provisioned IOPS for the boot disk (hyperdisk-balanced only).  "
    "80 000 is the COS max-IOPS target.  Ignored for pd-ssd.",
)


_BOOT_DISK_THROUGHPUT = flags.DEFINE_integer(
    "swap_encryption_boot_disk_throughput",
    1200,
    "Provisioned throughput in MB/s for the boot disk (hyperdisk-balanced "
    "only).  Must be set together with iops.  1200 MB/s pairs with 80 000 "
    "IOPS for production; use 140 (minimum) for dev/test.  Ignored for "
    "pd-ssd.",
)


_BOOT_DISK_SIZE_GB = flags.DEFINE_integer(
    "swap_encryption_boot_disk_size_gb",
    500,
    "Boot disk size in GiB for the benchmark nodepool.  500 GiB is "
    "required for the n4-highmem-32 + hyperdisk-balanced Config 2 run "
    "(see Engineer Assignments table in execution-plan.md).  "
    "For LSSD configs the boot disk is smaller; 100 GiB is fine.",
)


_ADD_SWAP_DISK = flags.DEFINE_boolean(
    "swap_encryption_add_swap_disk",
    False,
    "Attach a dedicated second disk to the benchmark nodepool for use as "
    "the swap device.  Required for dm-crypt measurement on single-boot-disk "
    "machines (n4-highmem-32, n4-highmem-8) because COS blocks device-mapper "
    "from pod namespaces.  The second disk is provisioned via "
    "--additional-node-disk using the same type/IOPS/throughput as the boot "
    "disk flags.",
)


_SWAP_DISK_SIZE_GB = flags.DEFINE_integer(
    "swap_encryption_swap_disk_size_gb",
    500,
    "Size in GiB of the dedicated swap disk when "
    "--swap_encryption_add_swap_disk is True.  Must satisfy the "
    "hyperdisk-balanced IOPS constraint: provisioned_iops ≤ size_gb × 80.",
)

_ENABLE_DMCRYPT = flags.DEFINE_boolean(
    "swap_encryption_enable_dmcrypt",
    True,
    "When True (default), wrap the swap device in dm-crypt plain mode "
    "(aes-xts-plain64, ephemeral random key) matching GKE's "
    "go/node:swap-encryption implementation.  Set False to measure plain "
    "(unencrypted) swap overhead as a baseline.",
)

_GKE_KUBELET_MEMORY_SWAP = flags.DEFINE_string(
    "swap_encryption_gke_kubelet_memory_swap",
    "LimitedSwap",
    "Value for kubeletConfig.memorySwapBehavior injected via "
    "--system-config-from-file when creating the GKE benchmark nodepool.  "
    "LimitedSwap (default) — the kubelet allows pods to use swap up to their "
    "memory limit; required for the DaemonSet pod to drive kernel swapping.  "
    "NoSwap — disables swap at the kubelet level (use for a baseline run that "
    "confirms zero swap activity).  Set empty string to omit the flag entirely "
    "and rely on the cluster-level default.",
)

_SWAP_DEVICE = flags.DEFINE_string(
    "swap_encryption_device",
    "",
    "Explicit block device path to use as the swap device, e.g. "
    "/dev/nvme1n1 or /dev/mapper/swap_encrypted.  When empty (default), "
    "the device is auto-detected from /proc/swaps inside the benchmark pod.",
)

_SWAP_TYPE = flags.DEFINE_string(
    "swap_encryption_swap_type",
    "hyperdisk",
    "Storage target for the swap device.  One of: hyperdisk (default), "
    "lssd, instance_store, io2.",
)

_ENABLE_ZSWAP = flags.DEFINE_boolean(
    "swap_encryption_enable_zswap",
    False,
    "When True, enable zswap compressed swap cache on the benchmark node.",
)

_MIN_FREE_KBYTES = flags.DEFINE_integer(
    "swap_encryption_min_free_kbytes",
    0,
    "Value to write to /proc/sys/vm/min_free_kbytes before benchmarking. "
    "0 (default) leaves the kernel default unchanged.",
)

_FIO_RUNTIME_SEC = flags.DEFINE_integer(
    "swap_encryption_fio_runtime_sec",
    60,
    "Wall-clock seconds each fio job runs in Phase 1 microbenchmarks.",
)

_STRESS_VM_BYTES = flags.DEFINE_string(
    "swap_encryption_stress_vm_bytes",
    "28G",
    "stress-ng --vm-bytes value for Phase 2a swap-pressure stressor.  "
    "Should exceed available node RAM to force sustained paging.",
)

_STRESS_VM_BYTES_LIST = flags.DEFINE_list(
    "swap_encryption_stress_vm_bytes_list",
    [],
    "Comma-separated list of --vm-bytes values to sweep in Phase 2a, "
    'e.g. "14G,28G,56G".  Overrides --swap_encryption_stress_vm_bytes.',
)

_STRESS_TIMEOUT_SEC = flags.DEFINE_integer(
    "swap_encryption_stress_timeout_sec",
    300,
    "Maximum seconds to wait for the stress-ng swap-pressure phase.",
)

_DS_NAME = "pkb-swap-benchmark"
_DS_NAMESPACE = "default"
_DS_LABEL = "pkb-swap-benchmark"

# Transient kubectl errors that are safe to retry.
_TRANSIENT_KUBECTL_ERRORS = ("connection reset by peer", "websocket: close")

# Errors indicating the container/pod is gone and needs recovery.
_CONTAINER_GONE_KUBECTL_ERRORS = (
    "container not found",
    "procReady not received",
    "unable to upgrade connection",
    "not found",
    "deleted state",
)

_active_pod: list[str] = []  # single-element list so closures can mutate it


_degraded_reasons: list[str] = []


_pod_lost: list[str] = []


_oom_events: list[str] = []

_BENCHMARK_NODEPOOL = "benchmark"
_DEFAULT_NODEPOOL = "default-pool"


class _GcpZonalResource:
    """Minimal resource shim for gcp_util.GcloudCommand on compute operations.

    gcp_util.GcloudCommand auto-injects --project and --zone from the resource
    object passed to it.  GkeCluster._GcloudCommand() handles container/*
    operations correctly but also switches --zone → --region for multi-zone
    clusters, which is wrong for gcloud compute commands (--region creates
    regional resources, not zonal ones).  This shim pins a single zone so all
    gcloud compute calls target the correct AZ.
    """

    def __init__(self, project: str, zone: str) -> None:
        self.project = project
        self.zone = zone


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
    """Load and return benchmark config spec."""
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(spec: _BenchmarkSpec) -> None:
    """Two-step nodepool setup then DaemonSet deployment.

    Step 1 (handled by PKB infrastructure): cluster provisioned with a cheap
    e2-medium default nodepool.

    Step 2 (this function):
      a. Create the benchmark nodepool (n4-highmem-32 or c4-*-lssd) with
         COS_CONTAINERD, 80 000 IOPS, and a node startup script that configures
         dm-crypt swap at the OS level — before any pod is scheduled.
      b. Delete the dummy default nodepool to stop its cost immediately.
      c. Deploy the privileged DaemonSet (pinned via nodeSelector to the
         benchmark nodepool) and wait for tools to install.
    """
    cluster = spec.container_cluster

    # ── Step 2a: add real benchmark nodepool ────────────────────────────────
    if not getattr(cluster, "project", None):
        # Guard: AWS / EKS path — nodepool management is external.
        # PKB labels nodes pkb_nodepool=default; re-label to match the DaemonSet
        # nodeSelector (pkb_nodepool=benchmark) before deploying the pod.
        logging.info(
            "[swap_encryption] EKS cluster — labelling existing nodes with "
            "pkb_nodepool=%s so the DaemonSet nodeSelector matches.",
            _BENCHMARK_NODEPOOL,
        )
        kubectl.RunKubectlCommand([
            "label",
            "nodes",
            "--all",
            "--overwrite",
            f"pkb_nodepool={_BENCHMARK_NODEPOOL}",
        ])
        # io2 test-matrix row: create + attach a real io2 EBS volume so swap runs
        # on io2 hardware-encrypted storage (no-op unless swap_type=io2).
        _ensure_io2_volume()
    else:
        # GCP path: true two-step nodepool setup.
        logging.info("[swap_encryption] Step 2a: creating benchmark nodepool")
        _create_benchmark_node_pool(cluster)

        # ── Step 2b: wait for the benchmark node to join and be Ready ─────────
        logging.info("[swap_encryption] Step 2b: waiting for benchmark node")
        _wait_for_benchmark_node()

        # ── Step 2b2: attach dedicated swap disk (if requested) ───────────────
        if _ADD_SWAP_DISK.value:
            logging.info(
                "[swap_encryption] Step 2b2: attaching dedicated swap disk"
            )
            _attach_swap_disk(cluster)

    # ── Step 2c: deploy DaemonSet ────────────────────────────────────────────
    # Deploy and wait for the pod BEFORE deleting the default nodepool.
    # Deleting the default pool while the benchmark node is still joining causes
    # a temporary API server i/o timeout (control plane busy with two nodepool
    # ops simultaneously).  Once the pod is Running the cluster is fully stable.
    logging.info("[swap_encryption] Step 2c: deploying privileged DaemonSet")
    _deploy_daemonset()

    pod = _wait_for_benchmark_pod()
    logging.info("[swap_encryption] Benchmark pod ready: %s", pod)

    # ── Step 2d: now safe to remove the dummy default nodepool ───────────────
    if getattr(cluster, "project", None):
        logging.info(
            "[swap_encryption] Step 2d: deleting dummy default nodepool"
        )
        _delete_default_node_pool(cluster)
        # The DaemonSet pod may be evicted and rescheduled with a new name during
        # the nodepool deletion (cluster control plane briefly interrupts pod
        # lifecycle).  Re-resolve the pod name to avoid stale-reference errors on
        # all subsequent _pod_exec calls.
        logging.info(
            "[swap_encryption] Step 2d: re-resolving benchmark pod "
            "after nodepool deletion"
        )
        pod = _wait_for_benchmark_pod()
        logging.info("[swap_encryption] Benchmark pod (post-deletion): %s", pod)


def _phase_selected(token: str) -> bool:
    """Return True if phase `token` should run given --swap_encryption_phases.

    'all' (the default) selects every phase.  Otherwise only the comma-separated
    tokens listed in the flag run.  Tokens: fio, 2a, 2b, 3a, 3b, 3c.
    """
    selected = [p.strip().lower() for p in _PHASES.value if p.strip()]
    return (not selected) or ("all" in selected) or (token.lower() in selected)


def Run(spec: _BenchmarkSpec) -> list[sample.Sample]:
    """Execute all benchmark phases with gate logic.

    Execution is structured in three gated tiers matching the execution plan:

      Tier 1 (Gate 1) — fio microbenchmarks
        Raw I/O ceiling of the swap device.  Gate 1 fails if fio produces
        zero samples (device not found, O_DIRECT error, etc.).

      Tier 2 (Gate 2) — stress-ng CPU overhead + I/O interference
        Requires an active swap device (Gate 1 must pass).  Gate 2 fails if
        stress-ng does not complete within timeout.

      Tier 3 (Gate 3) — real-world workloads (Redis, kernel build, OpenSearch)
        Independent of Tier 2 results; always attempted if Gate 1 passed.
        Individual workload failures are logged but do not abort the others.

    If Gate 1 fails, Tiers 2 and 3 are skipped — there is no point measuring
    application-level swap performance when the raw device is inaccessible.
    """
    pod = _wait_for_benchmark_pod()
    if pod is None:
        raise errors.Benchmarks.RunError(
            "[swap_encryption] Benchmark pod never became ready."
        )
    # Initialise the module-level active-pod tracker so _pod_exec and
    # _recover_pod can transparently redirect to a replacement pod if the
    # original is evicted during the run.
    _active_pod.clear()
    _active_pod.append(pod)
    _degraded_reasons.clear()
    _pod_lost.clear()
    _oom_events.clear()
    original_pod = pod
    swap_dev = _detect_swap_device(pod)
    base_meta = _build_metadata(pod, swap_dev)
    results: list[sample.Sample] = []
    t_run_start = time.time()

    logging.info("[swap_encryption] swap device: %s", swap_dev)

    # ── Phase 1: fio microbenchmarks on raw swap device ─────────────────────────
    if _phase_selected("fio"):
        logging.info(
            "[swap_encryption] Phase 1: fio microbenchmarks on %s", swap_dev
        )
        try:
            phase1_samples = _run_phase1_fio(pod, swap_dev, base_meta)
            results += phase1_samples
            if not phase1_samples:
                _degraded_reasons.append(
                    "Phase 1 (fio) produced no samples — "
                    "check fio install and swap device accessibility"
                )
                logging.error("[swap_encryption] Phase 1: no samples produced")
        except Exception as e:  # pylint: disable=broad-except
            _degraded_reasons.append(f"Phase 1 fio failed: {e}")
            logging.error("[swap_encryption] Phase 1 fio error: %s", e)

    # ── Cost estimate ─────────────────────────────────────────────────────────
    if _COLLECT_COST.value:
        elapsed = time.time() - t_run_start
        results += _collect_cost_sample(pod, elapsed, base_meta)

    # ── Final degradation gate ────────────────────────────────────────────────
    # The phase try/except blocks above keep the run alive so partial data is
    # still collected, but that means a catastrophic failure (pod OOM-evicted
    # mid-run, no fio data, stress-ng killed before it could drive swap I/O)
    # would otherwise be reported by PKB as SUCCEEDED with empty/garbage metrics.
    # Detect those conditions here and surface them explicitly.
    if _active_pod and _active_pod[0] != original_pod:
        _degraded_reasons.append(
            f"benchmark pod was replaced during the run ({original_pod} →"
            f" {_active_pod[0]}) — it was OOM-evicted under swap pressure;"
            " phases executed after the eviction ran against a"
            " freshly-initialised pod (empty /tmp, swap re-setup) and may be"
            " invalid"
        )
    if _pod_lost:
        _degraded_reasons.append(
            "benchmark pod(s) went NotFound during the run"
            f' ({", ".join(_pod_lost)}) — the pod died (node memory-pressure'
            " eviction or container exit) and any phase running at or after"
            " that"
            " point (e.g. kernel-build baseline, OpenSearch) produced invalid"
            " data"
        )
    if _oom_events:
        _degraded_reasons.append(
            "OOM kill(s) (rc=137) occurred during the run on pod(s) "
            f'{", ".join(_oom_events)} — a phase exceeded memory and was'
            " killed by "
            "the OOM killer (the container may have restarted in place), so"
            " the "
            "affected phase(s) produced no or partial data"
        )

    degraded = bool(_degraded_reasons)
    results.append(
        sample.Sample(
            "swap_encryption_run_status",
            0.0 if degraded else 1.0,
            "status",
            dict(
                base_meta,
                degraded=degraded,
                degraded_reasons="; ".join(_degraded_reasons) or "none",
                num_samples=len(results) + 1,
            ),
        )
    )

    if degraded:
        msg = "[swap_encryption] RUN DEGRADED — " + "; ".join(_degraded_reasons)
        logging.error(msg)
        if _FAIL_ON_DEGRADED.value:
            # Raise so PKB marks the benchmark FAILED instead of SUCCEEDED.  The
            # samples collected so far are still published by PKB before the failure
            # is recorded, so no data is lost.
            raise errors.Benchmarks.RunError(msg)
    else:
        logging.info(
            "[swap_encryption] Run completed cleanly (%d samples)", len(results)
        )

    return results


def Cleanup(spec: _BenchmarkSpec) -> None:
    """Remove the DaemonSet and tear down any swap configuration."""
    pod = _wait_for_benchmark_pod(timeout=30)
    if pod:
        _pod_exec(pod, "swapoff -a 2>/dev/null || true", ignore_failure=True)
        _pod_exec(
            pod,
            textwrap.dedent("""
      swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
      dmsetup remove --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    """),
            ignore_failure=True,
        )
        # Clean up loop device backing files (single-disk fallback path).
        _pod_exec(
            pod,
            textwrap.dedent("""
      for backing in /var/pkb_swap_backing /run/pkb_swap_backing \
                     /mnt/stateful_partition/pkb_swap_backing
      do
        losetup -j "$backing" 2>/dev/null | awk -F: '{print $1}' | \
          while read dev
          do
            losetup -d "$dev" 2>/dev/null || true
          done
        rm -f "$backing"
      done
    """),
            ignore_failure=True,
        )
        _pod_exec(
            pod,
            "pkill -9 'stress-ng|fio' 2>/dev/null || true",
            ignore_failure=True,
        )

    _delete_daemonset()

    # Detach and delete the dedicated swap disk if one was provisioned.
    cluster = spec.container_cluster
    if _ADD_SWAP_DISK.value and getattr(cluster, "project", None):
        _detach_and_delete_swap_disk(cluster)


def _configure_eks_kubelet_swap(spec) -> None:
    """Configure EKS kubelet for LimitedSwap via nodeadm bootstrap.

    NOTE: Deferred — requires Ajay's PR #6780 (SwapConfigSpec + nodeadm
    integration) to merge.  When that lands, EKS node pools should include
    a preBootstrapCommands block writing nodeadm config with
    memorySwapBehavior: LimitedSwap before kubelet starts::

      apiVersion: node.eks.aws/v1alpha1
      kind: NodeConfig
      spec:
        kubelet:
          config:
            memorySwapBehavior: LimitedSwap
            failSwapOn: false

    GKE equivalent: linuxConfig.swapConfig + kubeletConfig.memorySwapBehavior
    via --system-config-from-file, already implemented in
    _create_benchmark_node_pool.

    See: https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/pull/6780
    """
    logging.warning(
        "[swap_encryption] EKS kubelet LimitedSwap config via nodeadm is "
        "deferred (blocked on PR #6780 — SwapConfigSpec). "
        "EKS nodes will use default kubelet swap settings until that PR merges."
    )


def _deploy_daemonset() -> None:
    """Apply the swap-infra DaemonSet manifest to the cluster.

    The DaemonSet is intentionally lean: it only verifies the node-level swap
    device is active (configured via linuxConfig.swapConfig on GKE or
    kubelet-config.json on EKS) and writes /tmp/pkb_ready.  No benchmark
    tooling is installed here — workloads are delegated to existing PKB
    benchmark modules (kubernetes_fio, kubernetes_redis_memtier, etc.) which
    manage their own tool installs inside separate benchmark pods.

    Uses kubernetes_commands.ApplyManifest to render the Jinja2 template from
    perfkitbenchmarker/data/cluster/swap_encryption_daemonset.yaml.j2 and
    apply it via kubectl — the standard PKB pattern for deploying manifests.
    """
    kubernetes_commands.ApplyManifest(
        "cluster/swap_encryption_daemonset.yaml.j2",
        ds_name=_DS_NAME,
        ds_namespace=_DS_NAMESPACE,
        ds_label=_DS_LABEL,
        benchmark_nodepool=_BENCHMARK_NODEPOOL,
        image=_DAEMONSET_IMAGE.value,
    )
    logging.info("[swap_encryption] Swap-infra DaemonSet applied")


def _wait_for_benchmark_pod(timeout: int = 600) -> str | None:
    """Wait until the swap-infra DaemonSet pod is Running AND swap is active.

    The DaemonSet installs fio and a small set of measurement tools then
    verifies the swap device before writing /tmp/pkb_ready (~1-2 min on a
    cold apt cache).  Default timeout 600 s covers worst-case APT latency
    on a freshly-started node.

    Uses tab-separated name/phase output so kubectl always exits 0 regardless
    of whether any pods are present, avoiding jsonpath index errors.
    """
    deadline = time.time() + timeout
    last_phase = ""
    ready_pod = None  # pod name once phase == Running

    while time.time() < deadline:
        # ── Step 1: wait for Running phase ──────────────────────────────────────
        if ready_pod is None:
            out, _, rc = kubectl.RunKubectlCommand(
                [
                    "get",
                    "pods",
                    "-l",
                    f"app={_DS_LABEL}",
                    "-n",
                    _DS_NAMESPACE,
                    "-o",
                    (
                        r"jsonpath={range"
                        r' .items[*]}{.metadata.name}{"\t"}{.status.phase}{"\n"}{end}'
                    ),
                ],
                raise_on_failure=False,
            )

            if rc == 0 and out.strip():
                for line in out.strip().splitlines():
                    parts = line.split("\t")
                    if len(parts) == 2:
                        pod_name, phase = parts[0].strip(), parts[1].strip()
                        if phase == "Running":
                            logging.info(
                                "[swap_encryption] Pod %s is Running – "
                                "waiting for swap device readiness sentinel...",
                                pod_name,
                            )
                            ready_pod = pod_name
                            break
                        if phase != last_phase:
                            logging.info(
                                "[swap_encryption] Pod %s phase: %s",
                                pod_name,
                                phase,
                            )
                            last_phase = phase
                            if phase in ("Pending",):
                                _log_pod_events(pod_name)
            else:
                logging.info(
                    "[swap_encryption] Waiting for DaemonSet pod to appear..."
                )

        # ── Step 2: poll for /tmp/pkb_ready sentinel ────────────────────────────
        if ready_pod is not None:
            sentinel_out, sentinel_err, sentinel_rc = kubectl.RunKubectlCommand(
                [
                    "exec",
                    ready_pod,
                    "-n",
                    _DS_NAMESPACE,
                    "--",
                    "test",
                    "-f",
                    "/tmp/pkb_ready",
                ],
                raise_on_failure=False,
            )
            if sentinel_rc == 0:
                logging.info(
                    "[swap_encryption] Pod %s ready (swap device active)",
                    ready_pod,
                )
                return ready_pod
            # "container not found" means the container crashed (CrashLoopBackOff or
            # exited) — treat it as a hard reset: re-check pod phase on next iteration.
            if (
                "container not found" in sentinel_err
                or "unable to upgrade connection" in sentinel_err
            ):
                logging.warning(
                    "[swap_encryption] Pod %s: container not running (%s) "
                    "— will re-check pod state",
                    ready_pod,
                    sentinel_err.strip(),
                )
                ready_pod = None
                last_phase = ""
            else:
                logging.info(
                    "[swap_encryption] Pod %s: still installing tools...",
                    ready_pod,
                )

        time.sleep(15)

    logging.warning(
        "[swap_encryption] Benchmark pod not ready after %ds", timeout
    )
    return None


def _log_pod_events(pod_name: str) -> None:
    """Dump recent Kubernetes events for the pod to help diagnose startup hangs."""
    events_out, _, _ = kubectl.RunKubectlCommand(
        [
            "describe",
            "pod",
            pod_name,
            "-n",
            _DS_NAMESPACE,
        ],
        raise_on_failure=False,
    )
    # Only log the Events section to keep output manageable
    in_events = False
    lines = []
    for line in events_out.splitlines():
        if line.startswith("Events:"):
            in_events = True
        if in_events:
            lines.append(line)
    if lines:
        logging.info("[swap_encryption] Pod events:\n%s", "\n".join(lines[:30]))
    else:
        logging.info(
            "[swap_encryption] kubectl describe output:\n%s",
            events_out[-2000:] if len(events_out) > 2000 else events_out,
        )


def _delete_daemonset() -> None:
    """Delete the benchmark DaemonSet."""
    kubectl.RunKubectlCommand(
        [
            "delete",
            "daemonset",
            _DS_NAME,
            "-n",
            _DS_NAMESPACE,
            "--ignore-not-found",
        ],
        raise_on_failure=False,
    )
    logging.info("[swap_encryption] DaemonSet deleted")


_HYPERDISK_MAX_IOPS_PER_MBPS = (
    256  # GCP Hyperdisk Balanced: IOPS <= 256 x MiB/s
)


def _valid_hyperdisk_throughput(iops: int, throughput: int) -> int:
    """Return a throughput (MiB/s) that satisfies GCP's Hyperdisk constraint.

    Hyperdisk Balanced rejects disk creation when provisioned IOPS exceed
    256 x provisioned throughput (MiB/s) — e.g. 80000 IOPS with 300 MiB/s fails
    with "Requested provisioned throughput is too low for the provisioned iops".
    Clamp throughput UP to the minimum the requested IOPS need (plus a small
    margin) and warn, so a mismatched flag pairing cannot abort node-pool/disk
    creation.
    """
    min_tput = -(-int(iops) // _HYPERDISK_MAX_IOPS_PER_MBPS)  # ceil(iops/256)
    if throughput < min_tput:
        logging.warning(
            "[swap_encryption] boot/swap disk throughput %d MiB/s is too low"
            " for %d IOPS (Hyperdisk needs >= ceil(iops/256) = %d MiB/s);"
            " raising to %d",
            throughput,
            iops,
            min_tput,
            min_tput,
        )
        return min_tput
    return throughput


def _create_benchmark_node_pool(cluster) -> None:
    """Add the benchmark nodepool to the existing cluster (Step 2 of setup).

    Uses:
      --swap_encryption_benchmark_machine_type  (default n4-highmem-32)
      --swap_encryption_node_image_type         (default COS_CONTAINERD)
      --swap_encryption_boot_disk_iops          (default 80000)
      --swap_encryption_enable_dmcrypt          (default True)

    The nodepool is labelled pkb_nodepool=benchmark so the DaemonSet
    nodeSelector targets it exclusively.  dm-crypt swap setup is performed
    from within the privileged DaemonSet pod (see _setup_gke_hyperdisk_swap /
    _setup_gke_lssd_swap) — we do NOT inject a startup-script via node metadata
    because GKE reserves that metadata key and rejects it at the API level.
    """
    machine_type = _BENCHMARK_MACHINE_TYPE.value
    # Auto-detect LSSD from machine type name; flag overrides only when True.
    is_lssd = _BENCHMARK_LSSD.value or "lssd" in machine_type.lower()

    # Determine zone/region from the cluster object.
    # LSSD configs only need a small boot disk (OS only; swap is on local NVMe).
    # Hyperdisk configs need 500 GiB to hit 80 000 IOPS (the IOPS/GiB ratio on
    # hyperdisk-balanced is 1:1 up to the provisioned ceiling, so a 100 GiB disk
    # can only provision up to 100 000 IOPS but a 500 GiB gives comfortable
    # headroom and matches the Config 2 spec in the Engineer Assignments table).
    disk_size_gb = 100 if is_lssd else _BOOT_DISK_SIZE_GB.value

    disk_type = _BOOT_DISK_TYPE.value

    # Use PKB's GcloudCommand wrapper: auto-injects --project, --zone/--region,
    # and auth token refresh.  GkeCluster._GcloudCommand also handles the
    # zone → region promotion for multi-zone / regional clusters.
    cmd = cluster._GcloudCommand(
        "container",
        "node-pools",
        "create",
        _BENCHMARK_NODEPOOL,
        "--cluster",
        cluster.name,
    )
    cmd.flags["machine-type"] = machine_type
    cmd.flags["image-type"] = _NODE_IMAGE_TYPE.value
    cmd.flags["disk-type"] = disk_type
    cmd.flags["disk-size"] = disk_size_gb
    cmd.flags["num-nodes"] = 1
    cmd.flags["node-labels"] = f"pkb_nodepool={_BENCHMARK_NODEPOOL}"
    cmd.args += ["--no-enable-autoupgrade", "--no-enable-autorepair"]

    # IOPS and throughput provisioning only applies to hyperdisk-* types AND
    # only when the boot disk is also the swap device (non-LSSD configs).
    # For LSSD machines the boot disk is OS-only; swap is on local NVMe.
    # Provisioning 80k IOPS on a 100 GiB boot disk would exceed the
    # hyperdisk-balanced per-GiB cap (80 IOPS/GiB × 100 GiB = 8 000 max).
    if disk_type.startswith("hyperdisk") and not is_lssd:
        # Hyperdisk boot-disk IOPS/throughput provisioning — not covered by
        # GkeCluster._AddNodeParamsToCmd (which only handles secondary disks).
        cmd.flags["boot-disk-provisioned-iops"] = _BOOT_DISK_IOPS.value
        cmd.flags["boot-disk-provisioned-throughput"] = (
            _valid_hyperdisk_throughput(
                _BOOT_DISK_IOPS.value, _BOOT_DISK_THROUGHPUT.value
            )
        )

    # For LSSD machines, expose local NVMe as raw block devices so fio/mdadm
    # can access them directly (go/gke-swap-lssd uses local-nvme-ssd-block).
    if is_lssd:
        cmd.flags["local-nvme-ssd-block"] = f"count={_LSSD_COUNT.value}"

    # ── GKE kubelet swap config ───────────────────────────────────────────────
    # Per Ajay's review comment (go/pkb-swap-encryption-pr1): the benchmark
    # nodepool must be created with kubeletConfig.memorySwapBehavior=LimitedSwap
    # so that the kubelet allocates swap to the DaemonSet pod.  Without this flag
    # the Linux kernel swap device may exist but the kubelet blocks pod-level
    # swap usage and the benchmark pod cannot drive swap I/O.
    #
    # Passed as --system-config-from-file pointing to a temp YAML, which is the
    # same mechanism PKB's gke_node_system_config flag uses:
    #   perfkitbenchmarker/providers/gcp/google_kubernetes_engine.py
    swap_behavior = _GKE_KUBELET_MEMORY_SWAP.value
    system_config_tmp = None
    if swap_behavior:
        # Build system-config YAML for --system-config-from-file.
        # Per Ajay's review (go/pkb-swap-encryption-pr1 #r3457877984):
        #   kubeletConfig.memorySwapBehavior: kubelet allocates swap to pods.
        #   linuxConfig.swapConfig: GKE enables node-level swap device.
        #     For LSSD machines, dedicatedLocalSsdProfile tells GKE to use
        #     the local NVMe as the swap device (avoids boot-disk overhead).
        #   linuxConfig.sysctl: swap aggressiveness tuning so the benchmark
        #     workloads can drive sustained swap I/O.
        # Reference:
        #   https://docs.cloud.google.com/kubernetes-engine/docs/how-to/
        #   node-memory-swap#enable
        if is_lssd:
            swap_config_block = (
                "  swapConfig:\n"
                "    enabled: true\n"
                "    dedicatedLocalSsdProfile:\n"
                f"      diskCount: {_LSSD_COUNT.value}\n"
            )
        else:
            swap_config_block = "  swapConfig:\n    enabled: true\n"
        kubelet_yaml = (
            "kubeletConfig:\n  memorySwapBehavior:"
            f" {swap_behavior}\nlinuxConfig:\n"
            + swap_config_block
            + "  sysctl:\n"
            "    vm.min_free_kbytes: 200\n"
            "    vm.watermark_scale_factor: 500\n"
            "    vm.swappiness: 100\n"
        )
        system_config_tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        )
        system_config_tmp.write(kubelet_yaml)
        system_config_tmp.flush()
        cmd.flags["system-config-from-file"] = system_config_tmp.name
        logging.info(
            "[swap_encryption] system-config-from-file: "
            "kubelet_swap=%s lssd=%s (written to %s):\n%s",
            swap_behavior,
            is_lssd,
            system_config_tmp.name,
            kubelet_yaml,
        )

    logging.info(
        "[swap_encryption] Creating benchmark nodepool: %s / %s / "
        "image=%s / disk=%dGiB / iops=%d / dmcrypt=%s / lssd=%s / "
        "add_swap_disk=%s / kubelet_swap=%s",
        _BENCHMARK_NODEPOOL,
        machine_type,
        _NODE_IMAGE_TYPE.value,
        disk_size_gb,
        _BOOT_DISK_IOPS.value,
        _ENABLE_DMCRYPT.value,
        is_lssd,
        _ADD_SWAP_DISK.value,
        swap_behavior or "unset",
    )

    # LSSD nodepools take longer to provision than PD-only nodepools because
    # GKE must also initialise the local NVMe devices before marking nodes Ready.
    # 1200 s (20 min) covers observed worst-case times on c4-lssd and n4 configs.
    try:
        _, stderr, rc = cmd.Issue(timeout=1200, raise_on_failure=False)
    finally:
        if system_config_tmp is not None:
            try:
                os.unlink(system_config_tmp.name)
            except OSError:
                pass

    if rc != 0:
        # Idempotent prepare: if the nodepool already exists (e.g. re-running
        # --run_stage=prepare,run to redeploy the DaemonSet onto an existing
        # cluster), reuse it instead of failing.  gcloud returns a 409 /
        # "Already exists" message in this case.
        low = (stderr or "").lower()
        if (
            "already exists" in low
            or "alreadyexists" in low
            or "code=409" in low
        ):
            logging.info(
                "[swap_encryption] Benchmark nodepool already exists — "
                "reusing it (idempotent prepare); proceeding to DaemonSet"
            )
            return
        raise errors.Benchmarks.RunError(
            "[swap_encryption] Failed to create benchmark nodepool "
            f"(rc={rc}): {stderr}"
        )
    logging.info("[swap_encryption] Benchmark nodepool ready")


def _wait_for_benchmark_node(timeout: int = 900) -> None:
    """Block until a node labelled pkb_nodepool=benchmark is Ready.

    gcloud container node-pools create returns as soon as the API accepts the
    request — the actual node VM may take another 2-4 minutes to boot, join the
    cluster, and pass its readiness checks.  Deploying the DaemonSet before that
    point leaves the pod Pending indefinitely because the nodeSelector finds no
    eligible node.

    This function polls kubectl every 15 s until at least one node with
    pkb_nodepool=benchmark has Ready=True, then returns.
    """
    deadline = time.time() + timeout
    logging.info(
        "[swap_encryption] Waiting for benchmark node "
        "(pkb_nodepool=benchmark) to be Ready..."
    )
    while time.time() < deadline:
        out, _, rc = kubectl.RunKubectlCommand(
            [
                "get",
                "nodes",
                "-l",
                f"pkb_nodepool={_BENCHMARK_NODEPOOL}",
                "-o",
                r"jsonpath={range .items[*]}"
                r'{.metadata.name}{"\t"}'
                r'{range .status.conditions[?(@.type=="Ready")]}'
                r'{.status}{"\n"}{end}{end}',
            ],
            raise_on_failure=False,
        )

        if rc == 0 and out.strip():
            for line in out.strip().splitlines():
                parts = line.split("\t")
                if len(parts) == 2 and parts[1].strip() == "True":
                    logging.info(
                        "[swap_encryption] Benchmark node ready: %s",
                        parts[0].strip(),
                    )
                    return

        logging.info(
            "[swap_encryption] Benchmark node not yet Ready — retrying in 15"
            " s..."
        )
        time.sleep(15)

    raise errors.Benchmarks.RunError(
        "[swap_encryption] Timed out waiting for benchmark node "
        f"(pkb_nodepool={_BENCHMARK_NODEPOOL}) to become Ready "
        f"after {timeout}s"
    )


def _attach_swap_disk(cluster) -> None:
    """Create a dedicated hyperdisk and attach it to the benchmark node.

    gcloud container node-pools create --additional-node-disk is not available
    in all gcloud SDK versions, so we use gcloud compute to create the disk and
    attach it after the node is ready.  In GKE the Kubernetes node name is the
    same as the GCE instance name, so no translation is needed.

    After attachment the disk appears as /dev/sdb (or /dev/nvme1n1 on NVMe
    nodes) inside the pod, and _setup_gke_hyperdisk_swap detects it via lsblk.

    The disk is named pkb-swap-<cluster-name> to avoid name collisions across
    concurrent runs.  Cleanup deletes it in Cleanup() if it exists.
    """
    # Resolve zone from cluster
    zone = None
    if getattr(cluster, "zones", None):
        zone = cluster.zones[0]
    elif getattr(cluster, "region", None):
        zone = cluster.region
    if not zone:
        raise errors.Benchmarks.RunError(
            "[swap_encryption] Cannot attach swap disk: cluster zone unknown"
        )

    project = cluster.project
    disk_name = f"pkb-swap-{cluster.name}"
    disk_type = _BOOT_DISK_TYPE.value
    disk_size_gb = _SWAP_DISK_SIZE_GB.value

    # ── Step 1: get the GCE instance name of the benchmark node ───────────────
    node_out, _, rc = kubectl.RunKubectlCommand(
        [
            "get",
            "nodes",
            "-l",
            f"pkb_nodepool={_BENCHMARK_NODEPOOL}",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
        raise_on_failure=False,
    )
    instance_name = node_out.strip()
    if rc != 0 or not instance_name:
        raise errors.Benchmarks.RunError(
            "[swap_encryption] Cannot find benchmark node for swap disk attach"
        )
    logging.info("[swap_encryption] Benchmark node instance: %s", instance_name)

    # ── Step 2: create the hyperdisk ──────────────────────────────────────────
    logging.info(
        "[swap_encryption] Creating swap disk %s (%dGiB %s)",
        disk_name,
        disk_size_gb,
        disk_type,
    )
    # Use PKB's GcloudCommand via _GcpZonalResource: auto-injects --project
    # and --zone (always zonal — gcloud compute --region creates regional
    # resources, which is not what we want for a node-attached swap disk).
    gcp_res = _GcpZonalResource(project, zone)
    create_cmd = gcp_util.GcloudCommand(
        gcp_res, "compute", "disks", "create", disk_name
    )
    create_cmd.flags["type"] = disk_type
    create_cmd.flags["size"] = f"{disk_size_gb}GB"
    create_cmd.args.append("--quiet")
    if disk_type.startswith("hyperdisk"):
        create_cmd.flags["provisioned-iops"] = _BOOT_DISK_IOPS.value
        create_cmd.flags["provisioned-throughput"] = (
            _valid_hyperdisk_throughput(
                _BOOT_DISK_IOPS.value, _BOOT_DISK_THROUGHPUT.value
            )
        )
    _, stderr, rc = create_cmd.Issue(timeout=120, raise_on_failure=False)
    if rc != 0:
        raise errors.Benchmarks.RunError(
            f"[swap_encryption] Failed to create swap disk {disk_name}:"
            f" {stderr}"
        )

    # ── Step 3: attach the disk to the node VM ────────────────────────────────
    logging.info(
        "[swap_encryption] Attaching swap disk %s to %s",
        disk_name,
        instance_name,
    )
    attach_cmd = gcp_util.GcloudCommand(
        gcp_res, "compute", "instances", "attach-disk", instance_name
    )
    attach_cmd.flags["disk"] = disk_name
    attach_cmd.flags["device-name"] = "pkb-swap"
    attach_cmd.args.append("--quiet")
    _, stderr, rc = attach_cmd.Issue(timeout=120, raise_on_failure=False)
    if rc != 0:
        raise errors.Benchmarks.RunError(
            f"[swap_encryption] Failed to attach swap disk to {instance_name}: "
            f"{stderr}"
        )
    logging.info(
        "[swap_encryption] Swap disk attached: %s → %s",
        disk_name,
        instance_name,
    )


def _delete_disk_by_name(disk_name: str, project: str, zone: str) -> bool:
    """Detach (if attached) and delete a GCE disk, robustly, with retries.

    Finds the attached instance from the disk's own `users` field rather than
    kubectl — kubectl is often unavailable during teardown (cluster being
    deleted), which previously left the disk attached and undeletable, so it
    leaked.  Returns True if the disk is gone (deleted or already absent).
    """
    for attempt in range(1, 5):
        gcp_res = _GcpZonalResource(project, zone)
        describe_cmd = gcp_util.GcloudCommand(
            gcp_res, "compute", "disks", "describe", disk_name
        )
        describe_cmd.flags["format"] = "value(users)"
        users, _, rc = describe_cmd.Issue(timeout=60, raise_on_failure=False)
        if rc != 0:
            logging.info(
                "[swap_encryption] Swap disk %s not present — nothing to"
                " delete",
                disk_name,
            )
            return True  # already gone
        user = users.strip()
        if user:
            inst = user.split("/")[-1]
            logging.info(
                "[swap_encryption] Detaching swap disk %s from %s",
                disk_name,
                inst,
            )
            detach_cmd = gcp_util.GcloudCommand(
                gcp_res, "compute", "instances", "detach-disk", inst
            )
            detach_cmd.flags["disk"] = disk_name
            detach_cmd.args.append("--quiet")
            detach_cmd.Issue(timeout=120, raise_on_failure=False)
        delete_cmd = gcp_util.GcloudCommand(
            gcp_res, "compute", "disks", "delete", disk_name
        )
        delete_cmd.args.append("--quiet")
        _, derr, drc = delete_cmd.Issue(timeout=180, raise_on_failure=False)
        if drc == 0:
            logging.info("[swap_encryption] Swap disk deleted: %s", disk_name)
            return True
        logging.warning(
            "[swap_encryption] Swap disk delete attempt %d/4 failed "
            "(%s); retrying in 10s",
            attempt,
            derr.strip()[:160],
        )
        time.sleep(10)
    logging.error(
        "[swap_encryption] Could NOT delete swap disk %s after retries "
        "— delete it manually: gcloud compute disks delete %s "
        "--zone %s --quiet",
        disk_name,
        disk_name,
        zone,
    )
    return False


def _detach_and_delete_swap_disk(cluster) -> None:
    """Detach and delete the dedicated swap disk created by _attach_swap_disk."""
    zone = None
    if getattr(cluster, "zones", None):
        zone = cluster.zones[0]
    elif getattr(cluster, "region", None):
        zone = cluster.region
    if not zone or not getattr(cluster, "project", None):
        return
    _delete_disk_by_name(f"pkb-swap-{cluster.name}", cluster.project, zone)


def _delete_default_node_pool(cluster) -> None:
    """Delete the dummy default nodepool after the benchmark pool is ready.

    The default nodepool (e2-medium) was only needed to satisfy GKE's
    requirement that a cluster must have at least one nodepool at creation time.
    Removing it stops the clock on its cost immediately.
    """
    # Use PKB's GcloudCommand: auto-injects --project, --zone/--region.
    cmd = cluster._GcloudCommand(
        "container",
        "node-pools",
        "delete",
        _DEFAULT_NODEPOOL,
        "--cluster",
        cluster.name,
    )
    cmd.args.append("--quiet")

    logging.info(
        "[swap_encryption] Deleting default nodepool: %s", _DEFAULT_NODEPOOL
    )
    _, stderr, rc = cmd.Issue(timeout=300, raise_on_failure=False)
    if rc != 0:
        logging.warning(
            "[swap_encryption] Could not delete default nodepool (rc=%d): %s",
            rc,
            stderr,
        )
    else:
        logging.info("[swap_encryption] Default nodepool deleted")


def _is_pod_gone(pod: str) -> bool:
    """Return True if the named pod no longer exists in the cluster.

    Used to distinguish OOM-killed container processes (pod still alive, rc=137)
    from OOM-evicted pods (pod gone, DaemonSet will create a replacement).
    """
    try:
        _, err, rc = kubectl.RunKubectlCommand(
            [
                "get",
                "pod",
                pod,
                "-n",
                _DS_NAMESPACE,
                "-o",
                "jsonpath={.metadata.name}",
            ],
            raise_on_failure=False,
            timeout=15,
        )
        return rc != 0 and "not found" in (err or "").lower()
    except Exception:  # pylint: disable=broad-except
        return False


def _pod_exec(
    pod: str,
    cmd: str,
    ignore_failure: bool = False,
    timeout: int = 300,
    _retries: int = 2,
) -> tuple[str, str]:
    """Run a shell command inside the benchmark pod via kubectl exec.

    Args:
      pod: Pod name returned by _wait_for_benchmark_pod.
      cmd: Shell command string passed to bash -c.
      ignore_failure: When True, non-zero exit codes are logged but not
        raised.
      timeout: Seconds before PKB kills the kubectl exec process. Default
        300 s matches PKB's IssueCommand default. Pass a larger value for
        long-running jobs (fio, stress-ng, kernel build).
      _retries: Number of automatic retries on transient GKE websocket
        resets ("connection reset by peer").  Set to 0 to disable retries
        for idempotent-sensitive commands.

    Returns:
      Tuple of (stdout, stderr) strings.
    """
    # Use module-level constants for error strings (defined at top of module).
    # Use the globally-tracked active pod name — it may have been updated by
    # a previous _recover_pod call when eviction replaced the pod.
    active = _active_pod[0] if _active_pod else pod

    for attempt in range(_retries + 1):
        out, err, rc = kubectl.RunKubectlCommand(
            ["exec", active, "-n", _DS_NAMESPACE, "--", "bash", "-c", cmd],
            raise_on_failure=False,
            raise_on_timeout=False,  # let _pod_exec's own retry loop handle transient resets
            timeout=timeout,
        )
        is_transient = rc != 0 and any(
            e in err for e in _TRANSIENT_KUBECTL_ERRORS
        )
        if is_transient and attempt < _retries:
            logging.warning(
                "[swap_encryption] kubectl exec connection reset (attempt"
                " %d/%d); retrying in 10 s",
                attempt + 1,
                _retries + 1,
            )
            time.sleep(10)
            continue
        # rc=137 (SIGKILL): the OOM killer terminated the container process.
        # Two sub-cases:
        #   A) Pod eviction: pod is gone, DaemonSet recreates it under a new name.
        #   B) Container OOM restart: pod still exists, container restarts in place.
        #      (DaemonSet restartPolicy=Always restarts the container, /tmp is lost,
        #      tools must be re-installed before subsequent commands can run.)
        # In both cases we call _recover_pod to wait for tools + sentinel, and
        # we do NOT retry the OOM-triggering command itself.
        if rc == 137:
            # Record the OOM so the run-level gate can flag it even if the container
            # restarts in place under the same pod name (which leaves both the
            # "pod replaced" and "pod NotFound" checks silent).
            if active not in _oom_events:
                _oom_events.append(active)
            # CRITICAL: sleep before checking pod state.  Kubernetes takes a few
            # seconds to mark a just-evicted pod as Terminating / NotFound.  Without
            # this delay _recover_pod sees the pod still in "Running" phase, returns
            # the old pod name immediately, and every subsequent command fails with
            # "Error from server (NotFound): pods … not found".
            logging.warning(
                "[swap_encryption] rc=137 — sleeping 15s for Kubernetes to"
                " update pod state before recovery check"
            )
            time.sleep(15)
            pod_gone = _is_pod_gone(active)
            if pod_gone:
                logging.warning(
                    "[swap_encryption] OOM-eviction detected (rc=137, pod gone)"
                    " — recovering pod name for subsequent commands (not"
                    " retrying this cmd)"
                )
            else:
                logging.warning(
                    "[swap_encryption] Container OOM-killed (rc=137, pod still"
                    " exists) — waiting for container restart and tool"
                    " re-install before continuing"
                )
            new_pod = _recover_pod(active)
            if new_pod != active:
                logging.info(
                    "[swap_encryption] Pod name updated: %s → %s",
                    active,
                    new_pod,
                )
                if _active_pod:
                    _active_pod[0] = new_pod
                active = new_pod
            break  # Do NOT retry — the OOM cmd itself is not re-run on the new pod.

        is_container_gone = rc != 0 and any(
            e in err.lower() for e in _CONTAINER_GONE_KUBECTL_ERRORS
        )
        if is_container_gone:
            # Record the loss for the run-level degradation gate REGARDLESS of retry
            # budget or ignore_failure.  A "pods … not found" on a best-effort command
            # (kernel build, opensearch, cleanup of a dead pod) still means the pod
            # died; without this the gate stays blind because _active_pod is only
            # renamed on the retry path below, which _retries=0 callers never reach.
            if active and active not in _pod_lost:
                _pod_lost.append(active)
                logging.error(
                    "[swap_encryption] Benchmark pod %s is gone (%s) —"
                    " recording run as degraded",
                    active,
                    (err or "").strip()[:160],
                )
            if attempt < _retries:
                logging.warning(
                    "[swap_encryption] Container gone/restarting (attempt"
                    " %d/%d) — waiting for pod to recover...",
                    attempt + 1,
                    _retries + 1,
                )
                new_pod = _recover_pod(active)
                if new_pod != active:
                    logging.info(
                        "[swap_encryption] Pod name updated: %s → %s",
                        active,
                        new_pod,
                    )
                    if _active_pod:
                        _active_pod[0] = new_pod
                    active = new_pod
                continue
        break

    if rc != 0 and not ignore_failure:
        raise errors.VmUtil.IssueCommandError(
            f"[swap_encryption] _pod_exec failed (rc={rc}): {err}"
        )
    return out, err


def _recover_pod(pod: str, timeout_sec: int = 600) -> str:
    """Wait for a DaemonSet container to recover after OOM kill or eviction.

    Handles two scenarios:
    1. Container OOM restart: same pod name, container restarting in place.
       DaemonSet restartPolicy=Always brings it back under the same pod name.
    2. Pod eviction/deletion: the pod is gone entirely; the DaemonSet creates
       a new pod with a DIFFERENT name.  We detect this by checking whether
       the named pod still exists; if not, we search by the DaemonSet label
       selector for a Running pod.

    Returns the (possibly new) pod name once it is Running and ready.
    """
    deadline = time.time() + timeout_sec
    logging.info(
        "[swap_encryption] Waiting for pod %s to recover (up to %ds)...",
        pod,
        timeout_sec,
    )

    # Phase 1: wait for a Running pod — either the named one (container
    # restart) or a replacement pod found via label selector (eviction).
    #
    # IMPORTANT: we query BOTH status.phase AND metadata.deletionTimestamp in a
    # single call.  When a pod is evicted, Kubernetes first sets deletionTimestamp
    # (the pod is "Terminating") while status.phase may still read "Running" for
    # several seconds.  Checking only status.phase causes a false-positive: we
    # return the old pod name immediately and every subsequent command fails with
    # "Error from server (NotFound)".  Checking deletionTimestamp catches this.
    recovered_pod = pod
    while time.time() < deadline:
        # IMPORTANT: capture stderr — kubectl writes "not found" to stderr, not
        # stdout.  When the pod is gone, status_out is empty and the error text
        # lives entirely in status_err.  Discarding stderr (using _) means the
        # 'not found' check below never fires and we spin until deadline.
        status_out, status_err, status_rc = kubectl.RunKubectlCommand(
            [
                "get",
                "pod",
                pod,
                "-n",
                _DS_NAMESPACE,
                "-o",
                "jsonpath={.status.phase}|{.metadata.deletionTimestamp}",
            ],
            raise_on_failure=False,
            timeout=30,
        )
        # Parse "Running|" (no deletionTimestamp) vs "Running|2026-…" (terminating)
        fields = status_out.strip().split("|")
        phase = fields[0].strip() if fields else ""
        is_terminating = len(fields) > 1 and bool(fields[1].strip())

        # Pod is genuinely Running and NOT being deleted — recovery complete.
        if status_rc == 0 and phase == "Running" and not is_terminating:
            break

        # Pod no longer exists, OR it exists but is being terminated (Terminating
        # state or deletionTimestamp set) — look for a replacement pod by label.
        pod_gone_or_terminating = (
            status_rc != 0 and "not found" in (status_out + status_err).lower()
        ) or is_terminating
        if pod_gone_or_terminating:
            label_out, _, label_rc = kubectl.RunKubectlCommand(
                [
                    "get",
                    "pods",
                    "-n",
                    _DS_NAMESPACE,
                    "-l",
                    f"app={_DS_LABEL}",
                    "-o",
                    (
                        'jsonpath={range .items[?(@.status.phase=="Running")]}'
                        '{.metadata.name}{"\\n"}{end}'
                    ),
                ],
                raise_on_failure=False,
                timeout=30,
            )
            new_pods = [
                p.strip()
                for p in label_out.strip().splitlines()
                if p.strip() and p.strip() != pod
            ]  # exclude the dying pod
            if label_rc == 0 and new_pods:
                recovered_pod = new_pods[0]
                logging.info(
                    "[swap_encryption] Original pod %s gone/terminating; "
                    "found replacement %s",
                    pod,
                    recovered_pod,
                )
                break

        time.sleep(10)
    else:
        raise errors.VmUtil.IssueCommandError(
            f"[swap_encryption] No Running pod found (original: {pod}) "
            f"within {timeout_sec}s after OOM kill / eviction"
        )

    # Phase 2: wait for init script to finish (sentinel written last).
    while time.time() < deadline:
        ready_out, _, ready_rc = kubectl.RunKubectlCommand(
            [
                "exec",
                recovered_pod,
                "-n",
                _DS_NAMESPACE,
                "--",
                "bash",
                "-c",
                "test -f /tmp/pkb_ready && echo READY",
            ],
            raise_on_failure=False,
            timeout=30,
        )
        if ready_rc == 0 and "READY" in ready_out:
            logging.info(
                "[swap_encryption] Pod %s recovered (swap device active)",
                recovered_pod,
            )
            return recovered_pod
        time.sleep(15)

    raise errors.VmUtil.IssueCommandError(
        f"[swap_encryption] Pod {recovered_pod} did not become ready "
        f"within {timeout_sec}s after OOM kill / eviction"
    )


def _run_phase1_fio(
    pod: str, swap_dev: str, base_meta: dict[str, Any]
) -> list[sample.Sample]:
    """Run fio microbenchmarks on the raw swap block device (Phase 1).

    Calls swapoff before running fio so measurements reflect the raw
    hardware + encryption ceiling with no swap-daemon overhead.  Re-enables
    swap unconditionally after all jobs complete.

    Jobs:
      4k_randread   iodepth=32  → random read IOPS
      4k_randwrite  iodepth=32  → random write IOPS
      1m_seqread    iodepth=8   → sequential read bandwidth
      1m_seqwrite   iodepth=8   → sequential write bandwidth
      4k_lat_read   iodepth=1   → completion latency floor (read)

    Args:
      pod: Benchmark pod name.
      swap_dev: Block device path, e.g. /dev/mapper/swap_encrypted.
      base_meta: Shared metadata dict from _build_metadata().

    Returns:
      List of Sample objects with IOPS, bandwidth and latency metrics.
    """
    samples: list[sample.Sample] = []

    # swapoff before fio — running fio with --direct=1 on an active swap
    # device races with kernel page-reclaim on the same dm-crypt target
    # and can cause kernel panics on some kernels.
    logging.info("[swap_encryption] Phase 1: swapoff %s", swap_dev)
    _pod_exec(
        pod,
        f"swapoff {swap_dev} 2>/dev/null || swapoff -a 2>/dev/null || true",
        timeout=30,
        ignore_failure=True,
    )

    # (name, rw_mode, block_size, iodepth)
    fio_jobs = [
        ("4k_randread", "randread", "4k", 32),
        ("4k_randwrite", "randwrite", "4k", 32),
        ("1m_seqread", "read", "1m", 8),
        ("1m_seqwrite", "write", "1m", 8),
        ("4k_lat_read", "randread", "4k", 1),
    ]

    runtime = _FIO_RUNTIME_SEC.value
    try:
        for name, rw, bs, iodepth in fio_jobs:
            cmd = (
                f"fio --name={name} --filename={swap_dev}"
                f" --rw={rw} --bs={bs} --iodepth={iodepth}"
                " --ioengine=libaio --direct=1"
                f" --runtime={runtime} --time_based --group_reporting"
                " --output-format=json 2>/dev/null"
            )
            logging.info("[swap_encryption] Phase 1: fio job %s", name)
            out, _ = _pod_exec(pod, cmd, timeout=runtime + 120)
            samples += _parse_fio_json(out, name, base_meta)
    finally:
        # Always re-enable swap so subsequent phases can drive swap I/O.
        logging.info("[swap_encryption] Phase 1: swapon %s", swap_dev)
        _pod_exec(
            pod,
            f"swapon {swap_dev} 2>/dev/null || true",
            timeout=30,
            ignore_failure=True,
        )

    logging.info(
        "[swap_encryption] Phase 1 complete (%d samples)", len(samples)
    )
    return samples


def _parse_fio_json(
    fio_output: str, job_name: str, base_meta: dict[str, Any]
) -> list[sample.Sample]:
    """Parse fio --output-format=json output into PKB Sample objects.

    Extracts per-direction (read/write) IOPS, bandwidth (MB/s) and completion
    latency (mean + p50/p99/p999 percentiles).

    Args:
      fio_output: Raw stdout from fio with --output-format=json.
      job_name: Short identifier embedded in metric names, e.g. '4k_randread'.
      base_meta: Shared metadata dict copied into each sample.

    Returns:
      List of Sample objects; empty if output cannot be parsed or is zero.
    """
    # fio sometimes emits kernel warnings before the JSON object.
    json_start = fio_output.find("{")
    if json_start == -1:
        logging.warning(
            "[swap_encryption] Phase 1: no JSON in fio output for %s", job_name
        )
        return []

    try:
        data = json.loads(fio_output[json_start:])
    except json.JSONDecodeError as e:
        logging.warning(
            "[swap_encryption] Phase 1: fio JSON parse error (%s): %s",
            job_name,
            e,
        )
        return []

    jobs = data.get("jobs", [])
    if not jobs:
        return []

    job = jobs[0]
    samples: list[sample.Sample] = []
    meta = dict(base_meta, fio_job=job_name)

    for direction in ("read", "write"):
        d = job.get(direction, {})
        iops = float(d.get("iops", 0))
        bw_kbps = float(d.get("bw", 0))  # fio reports KiB/s
        bw_mbps = bw_kbps / 1024.0

        # Skip directions with near-zero throughput (e.g. write on a randread job).
        if iops < 1 and bw_kbps < 1:
            continue

        prefix = f"phase1_fio_{job_name}_{direction}"
        samples.append(sample.Sample(f"{prefix}_iops", iops, "IOPS", meta))
        samples.append(
            sample.Sample(f"{prefix}_bw_mbps", bw_mbps, "MB/s", meta)
        )

        # Completion latency — fio reports nanoseconds; emit microseconds.
        clat = d.get("clat_ns", d.get("lat_ns", {}))
        lat_mean_ns = float(clat.get("mean", 0))
        if lat_mean_ns > 0:
            samples.append(
                sample.Sample(
                    f"{prefix}_lat_mean_us", lat_mean_ns / 1000.0, "us", meta
                )
            )
            for pct_key, label in (
                ("50.000000", "p50"),
                ("99.000000", "p99"),
                ("99.900000", "p999"),
            ):
                val_ns = clat.get("percentile", {}).get(pct_key, 0)
                if val_ns:
                    samples.append(
                        sample.Sample(
                            f"{prefix}_lat_{label}_us",
                            val_ns / 1000.0,
                            "us",
                            meta,
                        )
                    )

    return samples


_INSTANCE_PRICE_USD_PER_HR: dict[str, float] = {
    # GCP  (on-demand, us-central1 unless noted)
    "c4-standard-8-lssd": 0.5888,  # 8 vCPU, 32 GB RAM + 1×375 GB LSSD
    "c4-standard-8": 0.5008,  # 8 vCPU, 32 GB RAM, no LSSD
    "n4-highmem-32": 3.0256,  # 32 vCPU, 256 GB RAM
    "n2-highmem-32": 2.5216,  # 32 vCPU, 256 GB RAM
    "n2-standard-32": 1.5264,  # 32 vCPU, 120 GB RAM
    "z3-highmem-8": 2.7248,  # 8 vCPU + 4× LSSD
    # AWS
    "i4i.4xlarge": 1.4960,  # 16 vCPU, 128 GB RAM, NVMe Instance Store
    "i4i.2xlarge": 0.7480,
    "m6id.4xlarge": 0.9072,  # 16 vCPU, 64 GB RAM, NVMe Instance Store
    "m6i.4xlarge": 0.7680,  # 16 vCPU, 64 GB RAM, no Instance Store
    "r6i.4xlarge": 1.0080,  # 16 vCPU, 128 GB RAM, no Instance Store
}


def _collect_cost_sample(
    pod: str, elapsed_sec: float, base_meta: dict
) -> list[sample.Sample]:
    """Emit a cost_estimate_usd sample for the benchmark run (gap 7).

    Instance type is read from cloud metadata inside the pod.  Price is looked
    up from _INSTANCE_PRICE_USD_PER_HR; if unknown, the sample is omitted and
    a warning is logged.

    Args:
      pod: Benchmark pod name.
      elapsed_sec: Wall-clock seconds the benchmark phases took.
      base_meta: Shared metadata dict.

    Returns:
      A list of zero or one sample.Sample.
    """
    # Detect instance type from cloud metadata
    instance_type = ""

    # GCP: machine type is the last segment of the metadata URL value
    gcp_type_out, _ = _pod_exec(
        pod,
        "curl -s -m 3 --fail"
        " http://metadata.google.internal/computeMetadata/v1/instance/machine-type"
        ' -H "Metadata-Flavor: Google" 2>/dev/null || echo ""',
        ignore_failure=True,
    )
    if gcp_type_out.strip():
        instance_type = gcp_type_out.strip().split("/")[-1]

    if not instance_type:
        # AWS: instance-type is a plain string
        aws_type_out, _ = _pod_exec(
            pod,
            "curl -s -m 3 --fail "
            "http://169.254.169.254/latest/meta-data/instance-type "
            '2>/dev/null || echo ""',
            ignore_failure=True,
        )
        instance_type = aws_type_out.strip()

    # Allow explicit override (useful when running on custom/renamed machine
    # types or when the pod was unavailable during cost collection).
    if _INSTANCE_SIZE_LABEL.value:
        instance_type = _INSTANCE_SIZE_LABEL.value

    # Last resort: fall back to the benchmark machine type flag.  This ensures
    # cost tracking works even when the pod was evicted before cost collection
    # ran (in which case the metadata curl above returned empty).
    if not instance_type and _BENCHMARK_MACHINE_TYPE.value:
        instance_type = _BENCHMARK_MACHINE_TYPE.value
        logging.info(
            "[swap_encryption] Instance type from metadata unavailable; using"
            " --swap_encryption_benchmark_machine_type=%s for cost tracking",
            instance_type,
        )

    price = _INSTANCE_PRICE_USD_PER_HR.get(instance_type)
    if price is None:
        logging.warning(
            '[swap_encryption] Unknown instance type "%s" – skipping cost'
            " sample. Add it to _INSTANCE_PRICE_USD_PER_HR to enable cost"
            " tracking.",
            instance_type,
        )
        return []

    hours = elapsed_sec / 3600.0
    cost = hours * price
    meta = dict(
        base_meta,
        instance_type=instance_type,
        price_usd_per_hr=price,
        benchmark_elapsed_sec=round(elapsed_sec, 1),
    )
    return [sample.Sample("cost_estimate_usd", cost, "USD", meta)]


def _detect_swap_device(pod: str) -> str:
    """Return the active swap device path on the cluster node."""
    if _SWAP_DEVICE.value:
        return _SWAP_DEVICE.value

    # /proc/swaps is the source of truth: it lists the swap device that is
    # ACTUALLY active.  We must NOT just `test -e /dev/mapper/swap_encrypted`,
    # because a stale dm-crypt mapping from a previous run on a reused node can
    # still exist as a /dev node while being non-functional (fio/swapoff then
    # fail with "No such device or address").  So read the active device from
    # /proc/swaps first; only fall back to the mapper path if /proc/swaps is
    # somehow empty but the mapper is genuinely present.
    dm_out, _ = _pod_exec(
        pod,
        textwrap.dedent("""
        ACTIVE=$(awk 'NR==2{print $1}' /proc/swaps 2>/dev/null)
        if [ -n "$ACTIVE" ]
        then
          echo "$ACTIVE"
        elif test -e /dev/mapper/swap_encrypted
        then
          echo /dev/mapper/swap_encrypted
        fi
      """),
        ignore_failure=True,
    )
    dev = dm_out.strip().splitlines()[-1].strip() if dm_out.strip() else ""
    if dev:
        return dev
    raise ValueError(
        "No active swap device found in the benchmark pod. "
        "Use --swap_encryption_device to specify one."
    )


def _build_metadata(pod: str, swap_dev: str) -> dict[str, Any]:
    """Collect node environment, encryption type, and config into a dict."""

    kernel_out, _ = _pod_exec(pod, "uname -r", ignore_failure=True)
    mem_out, _ = _pod_exec(
        pod,
        "awk '/MemTotal/{print $2}' /proc/meminfo",
        ignore_failure=True,
    )
    swap_out, _ = _pod_exec(
        pod,
        "awk 'NR>1{sum+=$3} END{print sum+0}' /proc/swaps",
        ignore_failure=True,
    )

    try:
        mem_gb = round(int(mem_out.strip()) / (1024 * 1024), 1)
    except ValueError:
        mem_gb = 0
    try:
        swap_gb = round(int(swap_out.strip()) / (1024 * 1024), 1)
    except ValueError:
        swap_gb = 0

    # Encryption type — key off dm-crypt presence + the swap target, NOT the
    # device path.  A GKE plain Local SSD is /dev/nvme0n1 but is NOT Nitro-
    # encrypted; only the AWS targets (instance_store / io2) are.
    enc = "unknown"
    if "/dev/mapper/" in swap_dev:
        table_out, _ = _pod_exec(
            pod,
            f'dmsetup table {swap_dev.split("/")[-1]} 2>/dev/null || echo ""',
            ignore_failure=True,
        )
        enc = "dm-crypt-plain" if "crypt" in table_out.lower() else "dm-other"
    elif _SWAP_TYPE.value in ("instance_store", "io2"):
        enc = "nitro_hardware_offload"  # AWS: encrypted by the Nitro card
    elif not _ENABLE_DMCRYPT.value:
        enc = "none"  # GKE plain swap (encryption OFF)

    cloud = _detect_cloud(pod)

    # Gap 6: instance size label for multi-size comparison runs.
    # If the flag is set use it directly; otherwise try to read it from
    # cloud metadata so that the field is always populated.
    instance_label = _INSTANCE_SIZE_LABEL.value
    if not instance_label:
        gcp_type_out, _ = _pod_exec(
            pod,
            "curl -s -m 3 --fail"
            " http://metadata.google.internal/computeMetadata/v1/instance/machine-type"
            ' -H "Metadata-Flavor: Google" 2>/dev/null || echo ""',
            ignore_failure=True,
        )
        if gcp_type_out.strip():
            instance_label = gcp_type_out.strip().split("/")[-1]
    if not instance_label:
        aws_type_out, _ = _pod_exec(
            pod,
            "curl -s -m 3 --fail "
            "http://169.254.169.254/latest/meta-data/instance-type "
            '2>/dev/null || echo ""',
            ignore_failure=True,
        )
        instance_label = aws_type_out.strip()

    return {
        "benchmark": BENCHMARK_NAME,
        "execution_mode": "kubernetes_privileged_pod",
        "cloud": cloud,
        "instance_size": instance_label,
        "kernel_version": kernel_out.strip(),
        "host_memory_gb": mem_gb,
        "swap_device": swap_dev,
        "swap_size_gb": swap_gb,
        "swap_encryption": enc,
        # Test-matrix columns: storage target, encryption on/off, image, IOPS
        "storage_target": _SWAP_TYPE.value,
        "boot_disk_type": _BOOT_DISK_TYPE.value,
        "dmcrypt_enabled": _ENABLE_DMCRYPT.value,
        "node_image_type": _NODE_IMAGE_TYPE.value,
        "boot_disk_iops_target": _BOOT_DISK_IOPS.value,
        "benchmark_machine_type": _BENCHMARK_MACHINE_TYPE.value,
        # Other config
        "zswap_enabled": _ENABLE_ZSWAP.value,
        "min_free_kbytes": _MIN_FREE_KBYTES.value,
        "fio_runtime_sec": _FIO_RUNTIME_SEC.value,
        # Requested config value only.  The *effective* stress-ng footprint may
        # be autoscaled per node (see _autoscale_vm_bytes); Phase 2a records the
        # actual value it ran with as 'stress_vm_bytes' so the two never conflict.
        "stress_vm_bytes_requested": _STRESS_VM_BYTES.value,
        "stress_vm_bytes_list": _STRESS_VM_BYTES_LIST.value,
        "stress_timeout_sec": _STRESS_TIMEOUT_SEC.value,
        "nodepool": _NODEPOOL.value,
    }


def _detect_cloud(pod: str) -> str:
    """Detect whether the benchmark pod is running on GCP or AWS.

    Queries the cloud instance metadata endpoint inside the pod.  Returns
    'GCP' if the GCP metadata server responds, 'AWS' otherwise.
    """
    gcp_out, _ = _pod_exec(
        pod,
        "curl -s -m 2 --fail "
        "http://metadata.google.internal/computeMetadata/v1/project/project-id"
        ' -H "Metadata-Flavor: Google" 2>/dev/null || echo ""',
        ignore_failure=True,
    )
    if gcp_out.strip():
        return "GCP"
    return "AWS"


def _ensure_io2_volume() -> None:
    """Create and attach an io2 EBS volume for swap on EKS (no-op if not io2).

    Only executed when --swap_encryption_swap_type=io2.  Full implementation
    is deferred to PR2 (swap-capability layer).
    """
    if _SWAP_TYPE.value != "io2":
        return
    logging.info(
        "[swap_encryption] io2 swap volume provisioning deferred to PR2"
    )
