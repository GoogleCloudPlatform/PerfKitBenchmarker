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

== Resource pattern ==

Infrastructure lifecycle lives in two BaseResource subclasses:

  SwapNodePool  (perfkitbenchmarker/resources/container_service/swap_nodepool.py)
    _Create():  gcloud container node-pools create with linuxConfig.swapConfig
                + sysctl via --system-config-from-file; waits for node Ready;
                optionally creates and attaches a dedicated swap disk.
    _Delete():  detach+delete disk; delete the nodepool.
    DeleteDefaultPool(): remove the dummy e2-medium default pool after the
                DaemonSet pod is Running (separate step to avoid API-server
                contention during nodepool ops).

  SwapDaemonSet  (perfkitbenchmarker/resources/container_service/swap_daemonset.py)
    _Create():  apply Jinja2 manifest; wait for Running + /tmp/pkb_ready.
    _Delete():  in-pod swapoff / dmsetup / losetup teardown; kubectl delete.
    PodExec():  kubectl exec wrapper with transient-reset retry, OOM-kill
                detection (rc=137), and automatic pod recovery.

Both resources are added to spec.resources in Prepare() and are auto-deleted
by the PKB framework in Cleanup().

== Benchmark Phases ==

  Phase 1 – fio Microbenchmarks (this PR)
    Run fio directly on the swap block device (swapoff first) to measure
    the hardware + encryption ceiling: random IOPS (4K), sequential
    bandwidth (1M), and completion latency (iodepth=1).

  Phase 2a – CPU Overhead  (PR2/PR4)
  Phase 2b – I/O Interference  (PR4)
  Phase 3a – Redis Latency  (PR5)
  Phase 3b – Kernel Build  (PR5)
  Phase 3c – OpenSearch  (PR5)
"""

import json
import logging
import textwrap
import time
from typing import Any

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec_lib
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import swap_daemonset as _ds_mod
from perfkitbenchmarker.resources.container_service import swap_nodepool as _np_mod

FLAGS = flags.FLAGS

_BenchmarkSpec = bm_spec_lib.BenchmarkSpec

# ---------------------------------------------------------------------------
# Benchmark identity
# ---------------------------------------------------------------------------

BENCHMARK_NAME = 'swap_encryption'


BENCHMARK_CONFIG = """
swap_encryption:
  description: >
    GKE vs. EKS swap encryption and LSSD performance comparison.
    Two-step nodepool setup: PKB provisions a minimal cluster with a cheap
    default nodepool (Step 1), then Prepare() adds the real benchmark
    nodepool (n4-highmem-32 / c4-*-lssd, UBUNTU_CONTAINERD, 80k IOPS) with a
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
    'swap_encryption_daemonset_image',
    'ubuntu:22.04',
    'Container image used for the privileged benchmark DaemonSet pod.',
)


_NODEPOOL = flags.DEFINE_string(
    'swap_encryption_nodepool',
    'benchmark',
    'Name of the node pool to deploy the benchmark DaemonSet on.',
)


_INSTANCE_SIZE_LABEL = flags.DEFINE_string(
    'swap_encryption_instance_size_label',
    '',
    'Human-readable label for the current instance size being tested, e.g. '
    '"n4-highmem-32" or "i4i.4xlarge".  Stored in sample metadata so that '
    'results from multiple PKB runs across different instance sizes can be '
    'collated and compared.  Defaults to the value reported by the cloud '
    'metadata endpoint inside the pod.',
)


_COLLECT_COST = flags.DEFINE_boolean(
    'swap_encryption_collect_cost',
    False,
    'When True, emit a cost_estimate_usd sample using on-demand pricing '
    'for the instance type detected at runtime.',
)


_FAIL_ON_DEGRADED = flags.DEFINE_boolean(
    'swap_encryption_fail_on_degraded',
    True,
    'When True (default), raise an error at the end of Run() if the run was '
    'catastrophically degraded — e.g. the benchmark pod was OOM-evicted and '
    'replaced mid-run, Gate 1 (fio) produced no samples, or the stress-ng '
    'swap-pressure phase was OOM-killed before completing.  This prevents PKB '
    'from reporting SUCCEEDED for a run whose post-eviction phases produced '
    'empty or meaningless data.  Set False to keep the legacy behaviour of '
    'always returning whatever partial samples were collected.',
)


_PHASES = flags.DEFINE_list(
    'swap_encryption_phases',
    ['all'],
    'Which Run() phases to execute, for fast iteration against an '
    'already-provisioned cluster (e.g. --run_stage=run --run_uri=...).  '
    'Comma-separated subset of: fio (Tier 1 microbenchmarks), 2a (stress-ng '
    'CPU overhead + swap pressure), 2b (I/O interference), 3a (redis), '
    '3b (kernel build), 3c (opensearch).  Default "all" runs everything.  '
    'Example: --swap_encryption_phases=2a runs only the swap-pressure phase. '
    'Phases not listed are skipped and do not affect the degraded-run gate '
    '(e.g. skipping fio will not be reported as "Gate 1 produced no samples").',
)


_BENCHMARK_MACHINE_TYPE = flags.DEFINE_string(
    'swap_encryption_benchmark_machine_type',
    'n4-highmem-32',
    'Machine type for the benchmark nodepool created in Prepare(). '
    'Use n4-highmem-32 (hyperdisk, default) or c4-standard-8-lssd '
    '(LSSD RAID-0).  The matching swap setup is selected automatically.',
)


_BENCHMARK_LSSD = flags.DEFINE_boolean(
    'swap_encryption_lssd',
    False,
    'Force LSSD RAID-0 swap path even when the machine type name does not '
    'contain "lssd".  Auto-detected from machine type when False.',
)


_LSSD_COUNT = flags.DEFINE_integer(
    'swap_encryption_lssd_count',
    1,
    'Number of local NVMe SSDs to attach as raw block devices '
    '(--local-nvme-ssd-block count=N).  Must match the fixed local SSD '
    'count for the chosen machine type: c4-standard-8-lssd=1, '
    'c4-standard-16-lssd=2, i4i.4xlarge has NVMe Instance Store (AWS).  '
    'Default 1 covers most single-lssd machine types.',
)


_NODE_IMAGE_TYPE = flags.DEFINE_string(
    'swap_encryption_node_image_type',
    'UBUNTU_CONTAINERD',
    'GKE node image type for the benchmark nodepool.  '
    'UBUNTU_CONTAINERD is required for dm-crypt measurement: COS locks '
    'down device-mapper at the kernel LSM level and cryptsetup hangs '
    'indefinitely from any pod context (even privileged, even via nsenter '
    'into the host mount namespace).  Ubuntu GKE nodes allow cryptsetup '
    'from privileged pods without restriction.  '
    'Use COS_CONTAINERD only when dm-crypt is disabled '
    '(--noswap_encryption_enable_dmcrypt) to measure plain-swap overhead.  '
    'AL2 on EKS.',
)


_BOOT_DISK_TYPE = flags.DEFINE_string(
    'swap_encryption_boot_disk_type',
    'hyperdisk-balanced',
    'Disk type for the benchmark nodepool boot disk.  Use hyperdisk-balanced '
    'for production machines (n4, c3, c4 families).  Use pd-ssd for n2/e2 '
    'dev/test machines, which do not support hyperdisk-balanced.',
)


_BOOT_DISK_IOPS = flags.DEFINE_integer(
    'swap_encryption_boot_disk_iops',
    80000,
    'Provisioned IOPS for the boot disk (hyperdisk-balanced only).  '
    '80 000 is the COS max-IOPS target.  Ignored for pd-ssd.',
)


_BOOT_DISK_THROUGHPUT = flags.DEFINE_integer(
    'swap_encryption_boot_disk_throughput',
    1200,
    'Provisioned throughput in MB/s for the boot disk (hyperdisk-balanced '
    'only).  Must be set together with iops.  1200 MB/s pairs with 80 000 '
    'IOPS for production; use 140 (minimum) for dev/test.  Ignored for '
    'pd-ssd.',
)


_BOOT_DISK_SIZE_GB = flags.DEFINE_integer(
    'swap_encryption_boot_disk_size_gb',
    500,
    'Boot disk size in GiB for the benchmark nodepool.  500 GiB is '
    'required for the n4-highmem-32 + hyperdisk-balanced Config 2 run '
    '(see Engineer Assignments table in execution-plan.md).  '
    'For LSSD configs the boot disk is smaller; 100 GiB is fine.',
)


_ADD_SWAP_DISK = flags.DEFINE_boolean(
    'swap_encryption_add_swap_disk',
    False,
    'Attach a dedicated second disk to the benchmark nodepool for use as '
    'the swap device.  Required for dm-crypt measurement on single-boot-disk '
    'machines (n4-highmem-32, n4-highmem-8) because COS blocks device-mapper '
    'from pod namespaces.  The second disk is provisioned via '
    '--additional-node-disk using the same type/IOPS/throughput as the boot '
    'disk flags.',
)


_SWAP_DISK_SIZE_GB = flags.DEFINE_integer(
    'swap_encryption_swap_disk_size_gb',
    500,
    'Size in GiB of the dedicated swap disk when '
    '--swap_encryption_add_swap_disk is True.  Must satisfy the '
    'hyperdisk-balanced IOPS constraint: provisioned_iops ≤ size_gb × 80.',
)

_ENABLE_DMCRYPT = flags.DEFINE_boolean(
    'swap_encryption_enable_dmcrypt',
    True,
    'When True (default), wrap the swap device in dm-crypt plain mode '
    '(aes-xts-plain64, ephemeral random key) matching GKE\'s '
    'go/node:swap-encryption implementation.  Set False to measure plain '
    '(unencrypted) swap overhead as a baseline.',
)


_SWAP_DEVICE = flags.DEFINE_string(
    'swap_encryption_device',
    '',
    'Explicit block device path to use as the swap device, e.g. '
    '/dev/nvme1n1 or /dev/mapper/swap_encrypted.  When empty (default), '
    'the device is auto-detected from /proc/swaps inside the benchmark pod.',
)

_SWAP_TYPE = flags.DEFINE_string(
    'swap_encryption_swap_type',
    'hyperdisk',
    'Storage target for the swap device.  One of: hyperdisk (default), '
    'lssd, instance_store, io2.',
)

_ENABLE_ZSWAP = flags.DEFINE_boolean(
    'swap_encryption_enable_zswap',
    False,
    'When True, enable zswap compressed swap cache on the benchmark node.',
)

_MIN_FREE_KBYTES = flags.DEFINE_integer(
    'swap_encryption_min_free_kbytes',
    0,
    'Value to write to /proc/sys/vm/min_free_kbytes before benchmarking. '
    '0 (default) leaves the kernel default unchanged.',
)

_FIO_RUNTIME_SEC = flags.DEFINE_integer(
    'swap_encryption_fio_runtime_sec',
    60,
    'Wall-clock seconds each fio job runs in Phase 1 microbenchmarks.',
)

_STRESS_VM_BYTES = flags.DEFINE_string(
    'swap_encryption_stress_vm_bytes',
    '28G',
    'stress-ng --vm-bytes value for Phase 2a swap-pressure stressor.  '
    'Should exceed available node RAM to force sustained paging.',
)

_STRESS_VM_BYTES_LIST = flags.DEFINE_list(
    'swap_encryption_stress_vm_bytes_list',
    [],
    'Comma-separated list of --vm-bytes values to sweep in Phase 2a, '
    'e.g. "14G,28G,56G".  Overrides --swap_encryption_stress_vm_bytes.',
)

_STRESS_TIMEOUT_SEC = flags.DEFINE_integer(
    'swap_encryption_stress_timeout_sec',
    300,
    'Maximum seconds to wait for the stress-ng swap-pressure phase.',
)

# DaemonSet constants used by both SwapDaemonSet construction and the EKS path.
_DS_NAME = 'pkb-swap-benchmark'
_DS_NAMESPACE = 'default'
_DS_LABEL = 'pkb-swap-benchmark'
_BENCHMARK_NODEPOOL = 'benchmark'


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
    """Load and return benchmark config spec."""
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(spec: _BenchmarkSpec) -> None:
    """Two-step nodepool setup then DaemonSet deployment.

    Step 1 (handled by PKB infrastructure): cluster provisioned with a cheap
    e2-medium default nodepool.

    Step 2 (this function):
      a. GCP: Create SwapNodePool (benchmark nodepool + optional swap disk).
         EKS: label existing nodes with pkb_nodepool=benchmark.
      b. Create SwapDaemonSet: deploy manifest + wait for Running + sentinel.
      c. GCP: DeleteDefaultPool() — safe now that DaemonSet pod is Running.
      d. GCP: re-resolve pod name in case default-pool deletion evicts the pod.

    Both resources are appended to spec.resources for auto-cleanup.
    """
    cluster = spec.container_cluster
    is_gcp = getattr(cluster, 'project', None) is not None

    if is_gcp:
        # ── Step 2a (GCP): create benchmark nodepool + wait for node ──────────
        logging.info('[swap_encryption] Step 2a: creating benchmark nodepool')
        nodepool = _np_mod.SwapNodePool(
            cluster=cluster,
            machine_type=_BENCHMARK_MACHINE_TYPE.value,
            node_image_type=_NODE_IMAGE_TYPE.value,
            disk_type=_BOOT_DISK_TYPE.value,
            disk_size_gb=_BOOT_DISK_SIZE_GB.value,
            disk_iops=_BOOT_DISK_IOPS.value,
            disk_throughput=_BOOT_DISK_THROUGHPUT.value,
            lssd=_BENCHMARK_LSSD.value,
            lssd_count=_LSSD_COUNT.value,
            add_swap_disk=_ADD_SWAP_DISK.value,
            swap_disk_size_gb=_SWAP_DISK_SIZE_GB.value,
        )
        nodepool.Create()
        spec.resources.append(nodepool)
    else:
        # ── Step 2a (EKS): label existing nodes to match DaemonSet selector ──
        logging.info(
            '[swap_encryption] EKS cluster — labelling existing nodes with'
            ' pkb_nodepool=%s so the DaemonSet nodeSelector matches.',
            _BENCHMARK_NODEPOOL,
        )
        kubectl.RunKubectlCommand([
            'label',
            'nodes',
            '--all',
            '--overwrite',
            f'pkb_nodepool={_BENCHMARK_NODEPOOL}',
        ])
        _ensure_io2_volume()

    # ── Step 2b: deploy DaemonSet and wait for pod ────────────────────────────
    # Deploy BEFORE deleting the default pool: deleting the default pool while
    # the benchmark node is still joining causes a brief API-server I/O timeout.
    # The pod being Running means the cluster is fully stable.
    logging.info('[swap_encryption] Step 2b: deploying privileged DaemonSet')
    daemonset = _ds_mod.SwapDaemonSet(
        name=_DS_NAME,
        namespace=_DS_NAMESPACE,
        label=_DS_LABEL,
        nodepool=_BENCHMARK_NODEPOOL,
        image=_DAEMONSET_IMAGE.value,
    )
    daemonset.Create()
    spec.resources.append(daemonset)
    logging.info(
        '[swap_encryption] Benchmark pod ready: %s', daemonset.pod_name
    )

    # ── Step 2c+d (GCP): delete dummy default nodepool, re-resolve pod name ──
    if is_gcp:
        logging.info(
            '[swap_encryption] Step 2c: deleting dummy default nodepool'
        )
        nodepool.DeleteDefaultPool()
        # The pod may be evicted and rescheduled with a new name during the
        # default nodepool deletion.  Re-resolve to avoid stale references.
        logging.info(
            '[swap_encryption] Step 2d: re-resolving benchmark pod after'
            ' nodepool deletion'
        )
        daemonset.WaitForPod()
        logging.info(
            '[swap_encryption] Benchmark pod (post-deletion): %s',
            daemonset.pod_name,
        )


def Run(spec: _BenchmarkSpec) -> list[sample.Sample]:
    """Execute all benchmark phases with gate logic.

    Execution is structured in three gated tiers matching the execution plan:

      Tier 1 (Gate 1) — fio microbenchmarks
        Raw I/O ceiling of the swap device.  Gate 1 fails if fio produces
        zero samples (device not found, O_DIRECT error, etc.).

      Tier 2 (Gate 2) — stress-ng CPU overhead + I/O interference (PR4)
        Requires an active swap device (Gate 1 must pass).

      Tier 3 (Gate 3) — real-world workloads (PR5)
        Independent of Tier 2 results.

    If Gate 1 fails, Tiers 2 and 3 are skipped.
    """
    daemonset = _get_daemonset(spec)

    pod = daemonset.WaitForPod()
    if pod is None:
        raise errors.Benchmarks.RunError(
            '[swap_encryption] Benchmark pod never became ready.'
        )
    # Reset per-run accumulators before starting phases.
    daemonset.oom_events.clear()
    daemonset.pod_lost.clear()
    original_pod = pod
    degraded_reasons: list[str] = []

    swap_dev = _detect_swap_device(daemonset)
    base_meta = _build_metadata(daemonset, swap_dev)
    results: list[sample.Sample] = []
    t_run_start = time.time()

    logging.info('[swap_encryption] swap device: %s', swap_dev)

    # ── Phase 1: fio microbenchmarks on raw swap device ───────────────────────
    if _phase_selected('fio'):
        logging.info(
            '[swap_encryption] Phase 1: fio microbenchmarks on %s', swap_dev
        )
        try:
            phase1_samples = _run_phase1_fio(daemonset, swap_dev, base_meta)
            results += phase1_samples
            if not phase1_samples:
                degraded_reasons.append(
                    'Phase 1 (fio) produced no samples — '
                    'check fio install and swap device accessibility'
                )
                logging.error('[swap_encryption] Phase 1: no samples produced')
        except Exception as e:  # pylint: disable=broad-except
            degraded_reasons.append(f'Phase 1 fio failed: {e}')
            logging.error('[swap_encryption] Phase 1 fio error: %s', e)

    # ── Cost estimate ─────────────────────────────────────────────────────────
    if _COLLECT_COST.value:
        elapsed = time.time() - t_run_start
        results += _collect_cost_sample(daemonset, elapsed, base_meta)

    # ── Final degradation gate ────────────────────────────────────────────────
    if daemonset.pod_name and daemonset.pod_name != original_pod:
        degraded_reasons.append(
            f'benchmark pod was replaced during the run ({original_pod} →'
            f' {daemonset.pod_name}) — it was OOM-evicted under swap'
            ' pressure; phases executed after the eviction ran against a'
            ' freshly-initialised pod (empty /tmp, swap re-setup) and may'
            ' be invalid'
        )
    if daemonset.pod_lost:
        degraded_reasons.append(
            'benchmark pod(s) went NotFound during the run'
            f' ({", ".join(daemonset.pod_lost)}) — the pod died (node'
            ' memory-pressure eviction or container exit) and any phase'
            ' running at or after that point produced invalid data'
        )
    if daemonset.oom_events:
        degraded_reasons.append(
            'OOM kill(s) (rc=137) occurred during the run on pod(s) '
            f'{", ".join(daemonset.oom_events)} — a phase exceeded memory'
            ' and was killed by the OOM killer; the affected phase(s)'
            ' produced no or partial data'
        )

    degraded = bool(degraded_reasons)
    results.append(
        sample.Sample(
            'swap_encryption_run_status',
            0.0 if degraded else 1.0,
            'status',
            dict(
                base_meta,
                degraded=degraded,
                degraded_reasons='; '.join(degraded_reasons) or 'none',
                num_samples=len(results) + 1,
            ),
        )
    )

    if degraded:
        msg = '[swap_encryption] RUN DEGRADED — ' + '; '.join(degraded_reasons)
        logging.error(msg)
        if _FAIL_ON_DEGRADED.value:
            raise errors.Benchmarks.RunError(msg)
    else:
        logging.info(
            '[swap_encryption] Run completed cleanly (%d samples)',
            len(results),
        )

    return results


def Cleanup(spec: _BenchmarkSpec) -> None:
    """Resources in spec.resources are auto-deleted by the PKB framework.

    SwapDaemonSet._Delete() runs in-pod teardown (swapoff, dmsetup remove,
    losetup cleanup, pkill fio/stress-ng) then deletes the DaemonSet.
    SwapNodePool._Delete() detaches+deletes the swap disk (if any) then
    deletes the benchmark nodepool.
    """


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_daemonset(spec: _BenchmarkSpec) -> _ds_mod.SwapDaemonSet:
    """Retrieve the SwapDaemonSet resource from spec.resources."""
    daemonset = next(
        (r for r in spec.resources if isinstance(r, _ds_mod.SwapDaemonSet)),
        None,
    )
    if daemonset is None:
        raise errors.Benchmarks.RunError(
            '[swap_encryption] SwapDaemonSet not found in spec.resources —'
            ' was Prepare() called?'
        )
    return daemonset


def _phase_selected(token: str) -> bool:
    """Return True if phase `token` should run given --swap_encryption_phases.

    'all' (the default) selects every phase.  Otherwise only the
    comma-separated tokens listed in the flag run.
    """
    selected = [p.strip().lower() for p in _PHASES.value if p.strip()]
    return (not selected) or ('all' in selected) or (token.lower() in selected)


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

    GKE equivalent: linuxConfig.swapConfig via --system-config-from-file
    (swapConfig automatically enables memorySwapBehavior=LimitedSwap),
    already implemented in SwapNodePool._CreateNodePool().

    See: https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/pull/6780
    """
    logging.warning(
        '[swap_encryption] EKS kubelet LimitedSwap config via nodeadm is '
        'deferred (blocked on PR #6780 — SwapConfigSpec). '
        'EKS nodes will use default kubelet swap settings until that PR merges.'
    )


def _ensure_io2_volume() -> None:
    """Create and attach an io2 EBS volume for swap on EKS (no-op if not io2).

    Only executed when --swap_encryption_swap_type=io2.  Full implementation
    is deferred to PR2 (swap-capability layer).
    """
    if _SWAP_TYPE.value != 'io2':
        return
    logging.info(
        '[swap_encryption] io2 swap volume provisioning deferred to PR2'
    )


def _detect_swap_device(
    daemonset: _ds_mod.SwapDaemonSet,
) -> str:
    """Return the active swap device path on the cluster node."""
    if _SWAP_DEVICE.value:
        return _SWAP_DEVICE.value

    # /proc/swaps is the source of truth — it lists the device ACTUALLY active.
    # Do NOT just test -e /dev/mapper/swap_encrypted: a stale dm-crypt mapping
    # from a previous run on a reused node can still appear as a /dev node while
    # being non-functional (fio/swapoff fail with "No such device or address").
    dm_out, _ = daemonset.PodExec(
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
    dev = dm_out.strip().splitlines()[-1].strip() if dm_out.strip() else ''
    if dev:
        return dev
    raise ValueError(
        'No active swap device found in the benchmark pod. '
        'Use --swap_encryption_device to specify one.'
    )


def _build_metadata(
    daemonset: _ds_mod.SwapDaemonSet, swap_dev: str
) -> dict[str, Any]:
    """Collect node environment, encryption type, and config into a dict."""
    kernel_out, _ = daemonset.PodExec('uname -r', ignore_failure=True)
    mem_out, _ = daemonset.PodExec(
        "awk '/MemTotal/{print $2}' /proc/meminfo", ignore_failure=True
    )
    swap_out, _ = daemonset.PodExec(
        "awk 'NR>1{sum+=$3} END{print sum+0}' /proc/swaps", ignore_failure=True
    )

    try:
        mem_gb = round(int(mem_out.strip()) / (1024 * 1024), 1)
    except ValueError:
        mem_gb = 0
    try:
        swap_gb = round(int(swap_out.strip()) / (1024 * 1024), 1)
    except ValueError:
        swap_gb = 0

    # Encryption type — key off dm-crypt presence + swap target.
    enc = 'unknown'
    if '/dev/mapper/' in swap_dev:
        table_out, _ = daemonset.PodExec(
            f'dmsetup table {swap_dev.split("/")[-1]} 2>/dev/null || echo ""',
            ignore_failure=True,
        )
        enc = 'dm-crypt-plain' if 'crypt' in table_out.lower() else 'dm-other'
    elif _SWAP_TYPE.value in ('instance_store', 'io2'):
        enc = 'nitro_hardware_offload'
    elif not _ENABLE_DMCRYPT.value:
        enc = 'none'

    cloud = _detect_cloud(daemonset)

    instance_label = _INSTANCE_SIZE_LABEL.value
    if not instance_label:
        gcp_type_out, _ = daemonset.PodExec(
            'curl -s -m 3 --fail'
            ' http://metadata.google.internal/computeMetadata/v1/instance/machine-type'
            ' -H "Metadata-Flavor: Google" 2>/dev/null || echo ""',
            ignore_failure=True,
        )
        if gcp_type_out.strip():
            instance_label = gcp_type_out.strip().split('/')[-1]
    if not instance_label:
        aws_type_out, _ = daemonset.PodExec(
            'curl -s -m 3 --fail '
            'http://169.254.169.254/latest/meta-data/instance-type '
            '2>/dev/null || echo ""',
            ignore_failure=True,
        )
        instance_label = aws_type_out.strip()

    return {
        'benchmark': BENCHMARK_NAME,
        'execution_mode': 'kubernetes_privileged_pod',
        'cloud': cloud,
        'instance_size': instance_label,
        'kernel_version': kernel_out.strip(),
        'host_memory_gb': mem_gb,
        'swap_device': swap_dev,
        'swap_size_gb': swap_gb,
        'swap_encryption': enc,
        'storage_target': _SWAP_TYPE.value,
        'boot_disk_type': _BOOT_DISK_TYPE.value,
        'dmcrypt_enabled': _ENABLE_DMCRYPT.value,
        'node_image_type': _NODE_IMAGE_TYPE.value,
        'boot_disk_iops_target': _BOOT_DISK_IOPS.value,
        'benchmark_machine_type': _BENCHMARK_MACHINE_TYPE.value,
        'zswap_enabled': _ENABLE_ZSWAP.value,
        'min_free_kbytes': _MIN_FREE_KBYTES.value,
        'fio_runtime_sec': _FIO_RUNTIME_SEC.value,
        'stress_vm_bytes_requested': _STRESS_VM_BYTES.value,
        'stress_vm_bytes_list': _STRESS_VM_BYTES_LIST.value,
        'stress_timeout_sec': _STRESS_TIMEOUT_SEC.value,
        'nodepool': _NODEPOOL.value,
    }


def _detect_cloud(daemonset: _ds_mod.SwapDaemonSet) -> str:
    """Detect whether the benchmark pod is running on GCP or AWS."""
    gcp_out, _ = daemonset.PodExec(
        'curl -s -m 2 --fail '
        'http://metadata.google.internal/computeMetadata/v1/project/project-id'
        ' -H "Metadata-Flavor: Google" 2>/dev/null || echo ""',
        ignore_failure=True,
    )
    if gcp_out.strip():
        return 'GCP'
    return 'AWS'


def _run_phase1_fio(
    daemonset: _ds_mod.SwapDaemonSet,
    swap_dev: str,
    base_meta: dict[str, Any],
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
      daemonset: Active SwapDaemonSet resource.
      swap_dev: Block device path, e.g. /dev/mapper/swap_encrypted.
      base_meta: Shared metadata dict from _build_metadata().

    Returns:
      List of Sample objects with IOPS, bandwidth and latency metrics.
    """
    samples: list[sample.Sample] = []

    # swapoff before fio — running fio with --direct=1 on an active swap device
    # races with kernel page-reclaim on the same dm-crypt target.
    logging.info('[swap_encryption] Phase 1: swapoff %s', swap_dev)
    daemonset.PodExec(
        f'swapoff {swap_dev} 2>/dev/null || swapoff -a 2>/dev/null || true',
        timeout=30,
        ignore_failure=True,
    )

    # (name, rw_mode, block_size, iodepth)
    fio_jobs = [
        ('4k_randread', 'randread', '4k', 32),
        ('4k_randwrite', 'randwrite', '4k', 32),
        ('1m_seqread', 'read', '1m', 8),
        ('1m_seqwrite', 'write', '1m', 8),
        ('4k_lat_read', 'randread', '4k', 1),
    ]

    runtime = _FIO_RUNTIME_SEC.value
    try:
        for name, rw, bs, iodepth in fio_jobs:
            cmd = (
                f'fio --name={name} --filename={swap_dev}'
                f' --rw={rw} --bs={bs} --iodepth={iodepth}'
                ' --ioengine=libaio --direct=1'
                f' --runtime={runtime} --time_based --group_reporting'
                ' --output-format=json 2>/dev/null'
            )
            logging.info('[swap_encryption] Phase 1: fio job %s', name)
            out, _ = daemonset.PodExec(cmd, timeout=runtime + 120)
            samples += _parse_fio_json(out, name, base_meta)
    finally:
        # Always re-enable swap so subsequent phases can drive swap I/O.
        logging.info('[swap_encryption] Phase 1: swapon %s', swap_dev)
        daemonset.PodExec(
            f'swapon {swap_dev} 2>/dev/null || true',
            timeout=30,
            ignore_failure=True,
        )

    logging.info(
        '[swap_encryption] Phase 1 complete (%d samples)', len(samples)
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
    json_start = fio_output.find('{')
    if json_start == -1:
        logging.warning(
            '[swap_encryption] Phase 1: no JSON in fio output for %s', job_name
        )
        return []

    try:
        data = json.loads(fio_output[json_start:])
    except json.JSONDecodeError as e:
        logging.warning(
            '[swap_encryption] Phase 1: fio JSON parse error (%s): %s',
            job_name,
            e,
        )
        return []

    jobs = data.get('jobs', [])
    if not jobs:
        return []

    job = jobs[0]
    samples: list[sample.Sample] = []
    meta = dict(base_meta, fio_job=job_name)

    for direction in ('read', 'write'):
        d = job.get(direction, {})
        iops = float(d.get('iops', 0))
        bw_kbps = float(d.get('bw', 0))  # fio reports KiB/s
        bw_mbps = bw_kbps / 1024.0

        # Skip directions with near-zero throughput.
        if iops < 1 and bw_kbps < 1:
            continue

        prefix = f'phase1_fio_{job_name}_{direction}'
        samples.append(sample.Sample(f'{prefix}_iops', iops, 'IOPS', meta))
        samples.append(
            sample.Sample(f'{prefix}_bw_mbps', bw_mbps, 'MB/s', meta)
        )

        # Completion latency — fio reports nanoseconds; emit microseconds.
        clat = d.get('clat_ns', d.get('lat_ns', {}))
        lat_mean_ns = float(clat.get('mean', 0))
        if lat_mean_ns > 0:
            samples.append(
                sample.Sample(
                    f'{prefix}_lat_mean_us', lat_mean_ns / 1000.0, 'us', meta
                )
            )
            for pct_key, label in (
                ('50.000000', 'p50'),
                ('99.000000', 'p99'),
                ('99.900000', 'p999'),
            ):
                val_ns = clat.get('percentile', {}).get(pct_key, 0)
                if val_ns:
                    samples.append(
                        sample.Sample(
                            f'{prefix}_lat_{label}_us',
                            val_ns / 1000.0,
                            'us',
                            meta,
                        )
                    )

    return samples


_INSTANCE_PRICE_USD_PER_HR: dict[str, float] = {
    # GCP  (on-demand, us-central1 unless noted)
    'c4-standard-8-lssd': 0.5888,
    'c4-standard-8': 0.5008,
    'n4-highmem-32': 3.0256,
    'n2-highmem-32': 2.5216,
    'n2-standard-32': 1.5264,
    'z3-highmem-8': 2.7248,
    # AWS
    'i4i.4xlarge': 1.4960,
    'i4i.2xlarge': 0.7480,
    'm6id.4xlarge': 0.9072,
    'm6i.4xlarge': 0.7680,
    'r6i.4xlarge': 1.0080,
}


def _collect_cost_sample(
    daemonset: _ds_mod.SwapDaemonSet,
    elapsed_sec: float,
    base_meta: dict,
) -> list[sample.Sample]:
    """Emit a cost_estimate_usd sample for the benchmark run."""
    instance_type = ''

    gcp_type_out, _ = daemonset.PodExec(
        'curl -s -m 3 --fail'
        ' http://metadata.google.internal/computeMetadata/v1/instance/machine-type'
        ' -H "Metadata-Flavor: Google" 2>/dev/null || echo ""',
        ignore_failure=True,
    )
    if gcp_type_out.strip():
        instance_type = gcp_type_out.strip().split('/')[-1]

    if not instance_type:
        aws_type_out, _ = daemonset.PodExec(
            'curl -s -m 3 --fail '
            'http://169.254.169.254/latest/meta-data/instance-type '
            '2>/dev/null || echo ""',
            ignore_failure=True,
        )
        instance_type = aws_type_out.strip()

    if _INSTANCE_SIZE_LABEL.value:
        instance_type = _INSTANCE_SIZE_LABEL.value

    if not instance_type and _BENCHMARK_MACHINE_TYPE.value:
        instance_type = _BENCHMARK_MACHINE_TYPE.value
        logging.info(
            '[swap_encryption] Instance type from metadata unavailable; using'
            ' --swap_encryption_benchmark_machine_type=%s for cost tracking',
            instance_type,
        )

    price = _INSTANCE_PRICE_USD_PER_HR.get(instance_type)
    if price is None:
        logging.warning(
            '[swap_encryption] Unknown instance type "%s" — skipping cost'
            ' sample. Add it to _INSTANCE_PRICE_USD_PER_HR to enable cost'
            ' tracking.',
            instance_type,
        )
        return []

    hours = elapsed_sec / 3600.0
    meta = dict(
        base_meta,
        instance_type=instance_type,
        price_usd_per_hr=price,
        benchmark_elapsed_sec=round(elapsed_sec, 1),
    )
    return [sample.Sample('cost_estimate_usd', hours * price, 'USD', meta)]
