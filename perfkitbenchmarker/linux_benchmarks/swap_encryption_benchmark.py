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

  Phase 1 – fio Microbenchmarks
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
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import swap_daemonset as _ds_mod

FLAGS = flags.FLAGS

_BenchmarkSpec = bm_spec_lib.BenchmarkSpec

# ---------------------------------------------------------------------------
# Benchmark identity
# ---------------------------------------------------------------------------

BENCHMARK_NAME = 'swap_encryption'


BENCHMARK_CONFIG = """
swap_encryption:
  description: >
    Verify dm-crypt encrypted swap on GKE/EKS nodes. Swap-enabled 'benchmark' nodepool declared in BENCHMARK_CONFIG;
    GKE cluster creation applies --system-config-from-file (dm-crypt swapConfig)
    automatically via swap_config field on NodepoolSpec.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        machine_type: e2-medium
        boot_disk_size: 20
        zone: us-central1-a
    nodepools:
      benchmark:
        vm_count: 1
        vm_spec:
          GCP:
            machine_type: n4-highmem-32
            boot_disk_type: hyperdisk-balanced
            boot_disk_size: 500
            zone: us-central1-a
        swap_config:
          enabled: true
          swappiness: 100
          min_free_kbytes: 200
          watermark_scale_factor: 500
          boot_disk_iops: 160000
          boot_disk_throughput: 2400
"""


_SWAP_DEVICE = flags.DEFINE_string(
    'swap_encryption_device',
    '',
    'Explicit swap block-device path on the cluster node, e.g. '
    '/dev/nvme1n1 or /dev/dm-0.  When empty the benchmark auto-detects '
    'via /proc/swaps after setup.',
)


_SWAP_SIZE_GB = flags.DEFINE_integer(
    'swap_encryption_swap_size_gb',
    32,
    'Size in GB of the swap space to configure on the node. '
    'Ignored when a ready swap device already exists.',
)


_SWAP_TYPE = flags.DEFINE_enum(
    'swap_encryption_swap_type',
    'auto',
    ['auto', 'hyperdisk', 'lssd', 'boot_disk', 'instance_store', 'io2'],
    'Swap backing storage target, one per methodology test-matrix row:\n'
    '  GKE:  boot_disk (swap file on the OS boot disk — pd-balanced or '
    'hyperdisk-balanced, chosen via --swap_encryption_boot_disk_type),\n'
    '        hyperdisk (dedicated hyperdisk-balanced data disk),\n'
    '        lssd (dedicated Local SSD RAID-0).\n'
    '  AWS:  instance_store (NVMe Instance Store, Nitro-encrypted),\n'
    '        io2 (EBS io2 data/root volume).\n'
    'dm-crypt is applied on the GKE targets when '
    '--swap_encryption_enable_dmcrypt is set; AWS targets are encrypted by '
    'Nitro at the hardware level.  auto = detect from cloud + instance type.',
)


_ENABLE_ZSWAP = flags.DEFINE_boolean(
    'swap_encryption_enable_zswap',
    False,
    'Enable zswap (lz4 compressor, 20%% max pool) before running tests.',
)


_MIN_FREE_KBYTES = flags.DEFINE_integer(
    'swap_encryption_min_free_kbytes',
    65536,
    'Value written to /proc/sys/vm/min_free_kbytes to trigger earlier '
    'swapping. Set 0 to leave the kernel default unchanged.',
)


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


_IO2_ENCRYPTED = flags.DEFINE_boolean(
    'swap_encryption_io2_encrypted',
    True,
    'When True (default), the dedicated io2 swap volume is created with EBS '
    'encryption (Nitro/KMS) -> matrix row "io2 + hardware encryption". '
    'Set False for the unencrypted io2 baseline row. Only applies when '
    '--swap_encryption_swap_type=io2 on AWS/EKS.',
)


_IO2_KMS_KEY_ID = flags.DEFINE_string(
    'swap_encryption_io2_kms_key_id',
    '',
    'Optional KMS key id/ARN for the encrypted io2 volume. Empty = the '
    'account default aws/ebs key. Ignored unless io2_encrypted is True.',
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


_ENABLE_DMCRYPT = flags.DEFINE_boolean(
    'swap_encryption_enable_dmcrypt',
    True,
    'When True (default), configure dm-crypt on the swap device — the '
    '"encryption enabled" column of the test matrix.  Set False to use '
    'plain swap (encryption disabled column).',
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

# Module-level stash for the io2 volume id created by _ensure_io2_volume().
_IO2_VOLUME_ID = ''


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
    """Load and return benchmark config spec."""
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(spec: _BenchmarkSpec) -> None:
    """Two-step nodepool setup then DaemonSet deployment.

    PKB cluster creation automatically provisions the swap-enabled 'benchmark'
    nodepool (swap_config in BENCHMARK_CONFIG). This function only:
      1. Deploys the privileged SwapDaemonSet and waits for Running.
      2. Deletes the cheap e2-medium default-pool (required at cluster create).

    DaemonSet is appended to spec.resources for PKB auto-cleanup.
    """
    cluster = spec.container_cluster

    # The swap-enabled 'benchmark' nodepool is already provisioned by GKE
    # cluster creation (swap_config declared in BENCHMARK_CONFIG).
    # Prepare() only deploys the privileged DaemonSet + deletes the cheap
    # e2-medium default pool that GKE requires at cluster creation time.
    logging.info('[swap_encryption] Deploying privileged DaemonSet')
    daemonset = _ds_mod.SwapDaemonSet(
        name=_DS_NAME,
        namespace=_DS_NAMESPACE,
        label=_DS_LABEL,
        nodepool=_BENCHMARK_NODEPOOL,
        image=_DAEMONSET_IMAGE.value,
    )
    # Register before Create() so PKB auto-deletes on failure/cleanup.
    spec.resources.append(daemonset)
    daemonset.Create()
    logging.info('[swap_encryption] Benchmark pod ready: %s', daemonset.pod_name)
    _delete_default_pool(cluster)
    daemonset.WaitForPod()
    logging.info(
        '[swap_encryption] Benchmark pod (post-deletion): %s', daemonset.pod_name
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

    # WaitForPod raises PrepareException on timeout — never returns None.
    pod = daemonset.WaitForPod()
    # Reset per-run accumulators before starting phases.
    daemonset.oom_events.clear()
    daemonset.pod_lost.clear()
    original_pod = pod
    degraded_reasons: list[str] = []

    # ── Swap setup (cloud-specific) ───────────────────────────────────────────
    daemonset.PodExec('sysctl -w vm.swappiness=100', ignore_failure=True)
    if _MIN_FREE_KBYTES.value > 0:
        daemonset.PodExec(
            f'sysctl -w vm.min_free_kbytes={_MIN_FREE_KBYTES.value}'
        )
    daemonset.PodExec(
        textwrap.dedent("""
    PKB_CG=$(awk -F: '/^0::/{print $3; exit}' /proc/self/cgroup 2>/dev/null)
    if [ -n "$PKB_CG" ] && [ -f "/sys/fs/cgroup${PKB_CG}/memory.swap.max" ]; then
      echo max > "/sys/fs/cgroup${PKB_CG}/memory.swap.max" 2>/dev/null || true
    fi
    PKB_CG1=$(awk -F: '/:memory:/{print $3; exit}' /proc/self/cgroup 2>/dev/null)
    if [ -n "$PKB_CG1" ] && \
       [ -f "/sys/fs/cgroup/memory${PKB_CG1}/memory.memsw.limit_in_bytes" ]; then
      echo -1 > "/sys/fs/cgroup/memory${PKB_CG1}/memory.memsw.limit_in_bytes" \
        2>/dev/null || true
    fi
  """),
        ignore_failure=True,
    )
    if _ENABLE_ZSWAP.value:
        _enable_zswap(daemonset)

    cloud = _detect_cloud(daemonset)
    logging.info('[swap_encryption] Detected cloud: %s', cloud)
    if cloud == 'gcp':
        _setup_gke_swap(daemonset)
    elif cloud == 'aws':
        _setup_eks_swap(daemonset)
    else:
        raise errors.Benchmarks.RunError(
            f'[swap_encryption] Unknown cloud {cloud!r}; cannot set up swap.'
        )

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



def _delete_default_pool(cluster) -> None:
  """Delete the dummy e2-medium default-pool once the benchmark pod is Running.

  GKE requires at least one nodepool at cluster creation time; the e2-medium
  default-pool satisfies that requirement. Deleting it before the DaemonSet
  pod is Running can trigger a brief API-server timeout while two concurrent
  nodepool operations are in progress.
  """
  try:
    cmd = cluster._GcloudCommand(  # pylint: disable=protected-access
        'container', 'node-pools', 'delete', _DEFAULT_POOL,
        '--cluster', cluster.name,
    )
    cmd.args.append('--quiet')
    logging.info('[swap_encryption] Deleting default nodepool: %s', _DEFAULT_POOL)
    _, stderr, rc = cmd.Issue(timeout=300, raise_on_failure=False)
    if rc != 0:
      raise errors.Benchmarks.RunError(
          f'[swap_encryption] Could not delete default nodepool (rc={rc}): {stderr}'
      )
    logging.info('[swap_encryption] Default nodepool deleted')
  except errors.Benchmarks.RunError:
    raise
  except Exception as e:  # pylint: disable=broad-except
    raise errors.Benchmarks.RunError(
        f'[swap_encryption] _delete_default_pool failed: {e}'
    ) from e
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


def _ensure_io2_volume() -> None:
    """Create + attach a dedicated io2 EBS volume to the benchmark node so the
    io2 test-matrix row swaps on real io2 hardware-encrypted storage.

    No-op unless --swap_encryption_swap_type=io2 on an AWS/EKS cluster.
    Best-effort: logs and returns on failure.  Stashes the created volume id in
    _IO2_VOLUME_ID for serial-based device detection in _setup_eks_io2_swap.
    """
    global _IO2_VOLUME_ID
    if _SWAP_TYPE.value != 'io2':
        return
    out, _, rc = kubectl.RunKubectlCommand(
        ['get', 'nodes', '-o', 'jsonpath={.items[0].spec.providerID}'],
        raise_on_failure=False,
    )
    provider = (out or '').strip()  # aws:///us-east-1a/i-0abc...
    if rc != 0 or 'aws://' not in provider:
        raise errors.Benchmarks.RunError(
            f'[swap_encryption] io2 setup: could not resolve EC2 instance'
            f' from providerID={provider!r}'
        )
    parts = [p for p in provider.split('/') if p]
    instance_id, az = parts[-1], parts[-2]
    region = az[:-1]
    base = ['aws', 'ec2', '--region', region]
    try:
        create_args = [
            'create-volume',
            '--volume-type',
            'io2',
            '--size',
            '500',
            '--iops',
            '16000',
            '--availability-zone',
            az,
            '--tag-specifications',
            'ResourceType=volume,Tags=[{Key=pkb,Value=swap_encryption}]',
        ]
        if _IO2_ENCRYPTED.value:
            create_args.append('--encrypted')
            if _IO2_KMS_KEY_ID.value:
                create_args += ['--kms-key-id', _IO2_KMS_KEY_ID.value]
            logging.info(
                '[swap_encryption] io2 volume will be EBS-encrypted '
                '(row: hardware encryption)'
            )
        else:
            logging.info(
                '[swap_encryption] io2 volume UNENCRYPTED (baseline row)'
            )
        create_args += ['--query', 'VolumeId', '--output', 'text']
        vol_id, _, vrc = vm_util.IssueCommand(
            base + create_args, raise_on_failure=False
        )
        vol_id = (vol_id or '').strip()
        if vrc != 0 or not vol_id.startswith('vol-'):
            raise errors.Benchmarks.RunError(
                f'[swap_encryption] io2 create-volume failed: {vol_id!r}'
            )
        vm_util.IssueCommand(
            base + ['wait', 'volume-available', '--volume-ids', vol_id],
            raise_on_failure=False,
        )
        vm_util.IssueCommand(
            base
            + [
                'attach-volume',
                '--volume-id',
                vol_id,
                '--instance-id',
                instance_id,
                '--device',
                '/dev/sdf',
            ],
            raise_on_failure=False,
        )
        vm_util.IssueCommand(
            base + ['wait', 'volume-in-use', '--volume-ids', vol_id],
            raise_on_failure=False,
        )
        _IO2_VOLUME_ID = vol_id
        logging.info(
            '[swap_encryption] Attached io2 volume %s to %s as /dev/sdf',
            vol_id,
            instance_id,
        )
        time.sleep(15)  # allow the NVMe device node to appear
    except Exception as e:  # pylint: disable=broad-except
        raise errors.Benchmarks.RunError(
            f'[swap_encryption] io2 attach error: {e}'
        ) from e


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
    raise NotImplementedError(
        '[swap_encryption] EKS kubelet LimitedSwap config via nodeadm is '
        'deferred (blocked on PR #6780). EKS swap setup not yet implemented.'
    )


def _detect_cloud(daemonset: _ds_mod.SwapDaemonSet) -> str:
    """Detect GCP vs AWS from DMI product info exposed via /sys hostPath mount.

    DMI is the most reliable in-container detection method because it reads
    directly from the host kernel's SMBIOS table via /sys (already mounted).
    It avoids HTTP metadata endpoint quoting issues and network timeouts.

    Falls back to metadata HTTP endpoints if DMI is inconclusive.
    """
    # Primary: DMI product name / vendor (available via /sys hostPath mount)
    dmi_out, _ = daemonset.PodExec(
        'cat /sys/class/dmi/id/sys_vendor /sys/class/dmi/id/product_name '
        '/sys/class/dmi/id/bios_vendor 2>/dev/null || echo ""',
        ignore_failure=True,
    )
    dmi = dmi_out.strip().lower()
    if 'google' in dmi:
        logging.info(
            '[swap_encryption] Cloud detected via DMI: gcp (%s)',
            dmi_out.strip(),
        )
        return 'gcp'
    if any(k in dmi for k in ('amazon', 'ec2', 'aws')):
        logging.info(
            '[swap_encryption] Cloud detected via DMI: aws (%s)',
            dmi_out.strip(),
        )
        return 'aws'

    # Secondary: GCP metadata endpoint.
    gcp_out, _ = daemonset.PodExec(
        'curl -s -m 3 '
        'http://metadata.google.internal/computeMetadata/v1/instance/zone '
        '-H Metadata-Flavor:Google 2>/dev/null || echo ""',
        ignore_failure=True,
    )
    if gcp_out.strip():
        logging.info('[swap_encryption] Cloud detected via metadata: gcp')
        return 'gcp'

    # Tertiary: AWS IMDS (IMDSv2 token-based; IMDSv1 is often disabled).
    aws_out, _ = daemonset.PodExec(
        'T=$(curl -s -m 3 -X PUT '
        'http://169.254.169.254/latest/api/token '
        '-H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null); '
        'curl -s -m 3 -H "X-aws-ec2-metadata-token: $T" '
        'http://169.254.169.254/latest/meta-data/instance-id '
        '2>/dev/null || echo ""',
        ignore_failure=True,
    )
    if aws_out.strip():
        logging.info('[swap_encryption] Cloud detected via IMDS: aws')
        return 'aws'

    raise errors.Benchmarks.RunError(
        '[swap_encryption] Could not detect cloud from DMI or metadata.'
    )


def _setup_gke_swap(daemonset: _ds_mod.SwapDaemonSet) -> None:
    """Configure dm-crypt swap on the GKE node, mirroring go/node:swap-encryption.

    GKE nodes use dm-crypt with an ephemeral random key so that swap contents
    are encrypted at rest without requiring persistent key management.
    We replicate this exactly using cryptsetup in plain mode (no LUKS header).
    """
    swap_type = _SWAP_TYPE.value
    if swap_type == 'auto':
        # Check whether Local SSDs are present
        lssd_out, _ = daemonset.PodExec(
            "lsblk -d -o NAME,MODEL | grep -i 'local\\|nvme' | "
            "grep -v 'nvme0' | awk '{print $1}' | head -1",
            ignore_failure=True,
        )
        swap_type = 'lssd' if lssd_out.strip() else 'hyperdisk'

    if swap_type == 'lssd':
        _setup_gke_lssd_swap(daemonset)
    elif swap_type == 'boot_disk':
        _setup_gke_bootdisk_swap(daemonset)
    else:
        _setup_gke_hyperdisk_swap(daemonset)


def _setup_gke_hyperdisk_swap(daemonset: _ds_mod.SwapDaemonSet) -> None:
    """Configure dm-crypt swap on hyperdisk-balanced (GKE default).

    Disk detection is split into two separate commands so that the boot-device
    name is resolved first and then substituted as a literal string — nested
    $() expansions inside a kubectl exec bash -c argument are unreliable.

    If no dedicated data disk is attached (single-disk node) dm-crypt is set up
    over a loop device backed by a file on the boot hyperdisk, which still
    exercises the full encryption path on the same storage tier.
    """
    logging.info('[swap_encryption] GKE: setting up dm-crypt on hyperdisk')

    # Step 1: identify the boot device name (e.g. "nvme0n1", "sda")
    boot_out, _ = daemonset.PodExec(
        'lsblk -no pkname "$(findmnt -n -o SOURCE /)" 2>/dev/null | head -1',
        ignore_failure=True,
    )
    boot_base = boot_out.strip() or 'nvme0n1'
    logging.info('[swap_encryption] GKE: boot device: %s', boot_base)

    # Step 2: find a non-boot disk using the literal name from step 1
    disk_out, _ = daemonset.PodExec(
        "lsblk -d -o NAME,TYPE | awk '$2==\"disk\"{print $1}' "
        f"| grep -v '^{boot_base}$' | head -1",
        ignore_failure=True,
    )
    disk_name = disk_out.strip()

    if not disk_name:
        logging.info(
            '[swap_encryption] No dedicated data disk found – '
            'falling back to loop device on /mnt/stateful_partition '
            '(direct-io=on, dm-crypt=%s)',
            _ENABLE_DMCRYPT.value,
        )
        _setup_gke_loop_device_swap(daemonset)
        return

    disk = f'/dev/{disk_name}'
    logging.info(
        '[swap_encryption] GKE: swap target disk: %s  dmcrypt=%s',
        disk,
        _ENABLE_DMCRYPT.value,
    )

    # Clean up any stale mapping from a previous failed run.
    daemonset.PodExec(
        textwrap.dedent(f"""
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    dmsetup remove --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    wipefs -a {disk} 2>/dev/null || true
  """),
        ignore_failure=True,
    )

    if _ENABLE_DMCRYPT.value:
        # We cannot use cryptsetup open from inside a container because
        # libdevmapper calls dm_udev_wait() after creating the target, which
        # blocks on /run/udev/control.  That socket belongs to udevd which is
        # not running inside the container — so cryptsetup hangs forever.
        #
        # Instead we drive dmsetup directly with --noudevrules --noudevsync,
        # which skips all udev synchronisation, and call dmsetup mknodes to
        # ensure /dev/mapper/swap_encrypted appears without udev.
        #
        # insmod (not modprobe) loads the kernel module: modprobe also talks to
        # systemd-udevd and can deadlock from a container for the same reason.
        daemonset.PodExec(
            textwrap.dedent(f"""
      grep -q dm_crypt /proc/modules 2>/dev/null || {{
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }}
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz {disk})
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "{disk}" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted
      swapon /dev/mapper/swap_encrypted
    """),
        )
        logging.info(
            '[swap_encryption] GKE: dm-crypt swap active on '
            '/dev/mapper/swap_encrypted'
        )
    else:
        # Encryption-disabled column of the test matrix
        daemonset.PodExec(
            textwrap.dedent(f"""
      mkswap {disk} && \\
      swapon {disk}
    """),
        )
        logging.info(
            '[swap_encryption] GKE: plain (unencrypted) swap active on %s', disk
        )


def _setup_gke_loop_device_swap(daemonset: _ds_mod.SwapDaemonSet) -> None:
    """Plain loop-device swap for single-disk GKE nodes (no dedicated swap disk).

    Used when _setup_gke_hyperdisk_swap finds no dedicated second disk (e.g.
    n2-highmem-32 / n4-highmem-32 single-boot-disk nodes, regardless of image
    type).

    dm-crypt is skipped on this path for two reasons:
    1. On COS (Container-Optimised OS): the device-mapper kernel subsystem is
       inaccessible from inside a Kubernetes pod (even privileged).  Calls to
       cryptsetup/dmsetup block indefinitely and are killed by the PKB timeout.
       This is a deliberate COS security restriction, not a permissions issue.
    2. On UBUNTU_CONTAINERD: the loop device is created in the container
       namespace; its behaviour under nsenter (needed for dm-crypt on dedicated
       disks) is untested, so plain loop swap is used for safety.
    For dedicated block devices (hyperdisk, LSSD) nsenter into the host mount
    namespace works around the COS restriction (see _setup_gke_hyperdisk_swap).
    The loop device path skips dm-crypt on all image types; plain loop swap is
    used instead.

    Therefore this path uses a plain loop device as swap without dm-crypt.
    Phase 1 (fio) is skipped for plain loop devices — the goal is enc-on vs
    enc-off comparison, and fio on a plain loop device measures the backing
    filesystem rather than the swap stack.  Tiers 2–6 (stress-ng, Redis,
    kernel build, OpenSearch) run normally.

    For dm-crypt measurement on GCP use a machine type with local NVMe (LSSD)
    or provision a dedicated hyperdisk on a second disk slot (n4-highmem-32+).

    Improvements over the old /var path:
    - Backing file on /mnt/stateful_partition (ext4), not the container
      overlayfs — avoids overlayfs O_DIRECT limitation.
    - losetup --direct-io=on passes I/O through to the host ext4, reducing
      double-buffering for Tiers 2–6 workloads.
    """
    size_gb = _SWAP_SIZE_GB.value
    backing = '/mnt/stateful_partition/pkb_swap_backing'

    # ── Step 0: detach any stale loop device from a previous failed run ───────
    daemonset.PodExec(
        textwrap.dedent(f"""
    losetup -j {backing} 2>/dev/null | awk -F: '{{print $1}}' | \
      while read dev
      do
        swapoff "$dev" 2>/dev/null || true
        losetup -d "$dev" 2>/dev/null || true
      done
    rm -f {backing}
  """),
        ignore_failure=True,
    )

    # ── Step 1: allocate backing file on stateful partition (ext4) ───────────
    logging.info(
        '[swap_encryption] GKE: creating %dG backing file on'
        ' stateful_partition',
        size_gb,
    )
    daemonset.PodExec(
        textwrap.dedent(f"""
    fallocate -l {size_gb}G {backing} 2>/dev/null || \\
      truncate -s {size_gb}G {backing}
  """),
    )

    # ── Step 2: loop device with direct-io passthrough ───────────────────────
    loop_out, _ = daemonset.PodExec(
        textwrap.dedent(f"""
    LOOP=$(losetup -f) && \\
    losetup --direct-io=on "$LOOP" {backing} && \\
    echo "$LOOP"
  """),
    )
    loop_dev = loop_out.strip()
    if not loop_dev.startswith('/dev/loop'):
        raise errors.Benchmarks.RunError(
            f'[swap_encryption] losetup failed – output: {loop_out!r}'
        )
    logging.info(
        '[swap_encryption] GKE: loop device: %s  direct-io=on', loop_dev
    )

    # ── Step 3: plain mkswap + swapon (dm-crypt skipped on loop devices) ────────
    daemonset.PodExec(f'mkswap {loop_dev}')
    daemonset.PodExec(f'swapon {loop_dev}')
    logging.info(
        '[swap_encryption] GKE: plain loop swap active on %s '
        '(dm-crypt unavailable from COS pod — device-mapper is blocked by '
        'COS kernel namespace restrictions). '
        'Phase 1 (fio) will be skipped. '
        'Use a machine with LSSD (c4-*-lssd) or attach a dedicated second '
        'hyperdisk for dm-crypt measurement.',
        loop_dev,
    )


def _setup_gke_bootdisk_swap(daemonset: _ds_mod.SwapDaemonSet) -> None:
    """Swap on the OS BOOT disk — methodology Table 0 rows 1-4.

    Creates a loop-backed swap file on /mnt/stateful_partition (the node's boot
    disk, whose type — pd-balanced or hyperdisk-balanced — is chosen at
    nodepool-creation time via --swap_encryption_boot_disk_type).  dm-crypt is
    layered on the loop device when --swap_encryption_enable_dmcrypt is set
    (encryption-on rows 2/4); otherwise plain swap is used (encryption-off rows
    1/3).

    Reuses the same loop-creation and dmsetup patterns as the LSSD/hyperdisk
    paths — no shared provider module is touched.  Requires an Ubuntu node image
    (dm-crypt from a pod is blocked on COS).
    """
    size_gb = _SWAP_SIZE_GB.value
    backing = '/mnt/stateful_partition/pkb_swap_backing'
    logging.info(
        '[swap_encryption] GKE: boot-disk swap (%dG backing, dmcrypt=%s)',
        size_gb,
        _ENABLE_DMCRYPT.value,
    )

    # Clean up any stale loop/mapping from a previous run.
    daemonset.PodExec(
        textwrap.dedent(f"""
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    dmsetup remove --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    losetup -j {backing} 2>/dev/null | awk -F: '{{print $1}}' | while read d
    do
      swapoff "$d" 2>/dev/null || true
      losetup -d "$d" 2>/dev/null || true
    done
    rm -f {backing}
  """),
        ignore_failure=True,
    )

    # Allocate the backing file on the boot-disk ext4 stateful partition.
    daemonset.PodExec(
        textwrap.dedent(f"""
    fallocate -l {size_gb}G {backing} 2>/dev/null || truncate -s {size_gb}G {backing}
  """),
    )

    loop_out, _ = daemonset.PodExec(
        textwrap.dedent(f"""
    LOOP=$(losetup -f) && losetup --direct-io=on "$LOOP" {backing} && echo "$LOOP"
  """),
    )
    loop_dev = (
        loop_out.strip().splitlines()[-1].strip() if loop_out.strip() else ''
    )
    if not loop_dev.startswith('/dev/loop'):
        raise errors.Benchmarks.RunError(
            f'[swap_encryption] boot-disk losetup failed: {loop_out!r}'
        )
    logging.info('[swap_encryption] GKE: boot-disk loop device: %s', loop_dev)

    if _ENABLE_DMCRYPT.value:
        daemonset.PodExec(
            textwrap.dedent(f"""
      grep -q dm_crypt /proc/modules 2>/dev/null || {{
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }}
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz {loop_dev})
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "{loop_dev}" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted
      swapon /dev/mapper/swap_encrypted
    """),
        )
        logging.info(
            '[swap_encryption] GKE: boot-disk dm-crypt swap active on '
            '/dev/mapper/swap_encrypted'
        )
    else:
        daemonset.PodExec(
            textwrap.dedent(f"""
      mkswap {loop_dev} && swapon {loop_dev}
    """),
        )
        logging.info(
            '[swap_encryption] GKE: boot-disk plain swap active on %s', loop_dev
        )


def _setup_gke_lssd_swap(daemonset: _ds_mod.SwapDaemonSet) -> None:
    """Configure dm-crypt on LSSD RAID-0 array (go/gke-swap-lssd)."""
    logging.info('[swap_encryption] GKE: setting up LSSD RAID-0 swap')

    # Reused-node hygiene: a previous run on this node may have left an ACTIVE
    # dm-crypt swap (e.g. /dev/nvme0n1 └─swap_encrypted [SWAP]).  That makes the
    # LSSD look "unclean/busy" to the device selector below, which then wrongly
    # falls back to the hyperdisk path and tries the boot disk.  Tear down any
    # prior PKB swap mapping FIRST so the underlying LSSD is freed and selectable.
    daemonset.PodExec(
        textwrap.dedent("""
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    swapoff -a 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
  """),
        ignore_failure=True,
    )

    # Log the full block-device topology up front for diagnosis (every prior
    # swap failure traced back to picking the wrong device).
    topo, _ = daemonset.PodExec(
        'lsblk -o NAME,TYPE,SIZE,ROTA,MOUNTPOINT 2>/dev/null',
        ignore_failure=True,
    )
    logging.info(
        '[swap_encryption] block device topology:\n%s', (topo or '').strip()
    )

    # Identify candidate swap devices = whole disks that are NOT the boot/OS
    # disk.  We must NOT rely on a device name (boot disk enumerates as nvme0n1
    # on some nodes, nvme1n1 on others) and we cannot use `findmnt /` because the
    # container root is an overlay.  Instead we EXCLUDE any disk that:
    #   * has partition children (boot disk has p1/p14/p15/p16), or
    #   * has any mounted filesystem (itself or a child).
    # A raw local SSD intended for swap has neither.  This robustly prevents the
    # catastrophic bug where the 100 GB boot disk (root mounted) was RAIDed into
    # the swap device, yielding a non-functional swap (fio empty + stress OOM).
    lssd_out, _ = daemonset.PodExec(
        textwrap.dedent("""
        for d in $(lsblk -dno NAME,ROTA | awk '$2==0{print $1}')
        do
          if lsblk -no TYPE "/dev/$d" 2>/dev/null | grep -q '^part$'; then
            continue   # has partitions -> boot/OS disk
          fi
          if lsblk -no MOUNTPOINT "/dev/$d" 2>/dev/null | grep -q '[^[:space:]]'; then
            continue   # mounted somewhere -> not a free swap device
          fi
          echo "/dev/$d"
        done
      """),
        ignore_failure=True,
    )
    devices = [d.strip() for d in lssd_out.strip().splitlines() if d.strip()]
    if not devices:
        logging.info(
            '[swap_encryption] No clean local SSD found — falling back to'
            ' hyperdisk swap path'
        )
        _setup_gke_hyperdisk_swap(daemonset)
        return

    device_list = ' '.join(devices)
    n = len(devices)
    logging.info(
        '[swap_encryption] GKE: LSSD RAID-0 across %d clean device(s): '
        '%s  dmcrypt=%s',
        n,
        device_list,
        _ENABLE_DMCRYPT.value,
    )

    # Clean up stale mappings, RAID arrays, and GKE-managed mounts.
    #
    # GKE UBUNTU nodes run google-ssd-startup.service at boot which formats
    # local NVMe SSDs as ext4 and mounts them at /mnt/disks/ssd0 etc. even
    # when --local-nvme-ssd-block is set.  The mount makes the block device
    # busy so mdadm/wipefs fail silently (we had || true).  We must unmount
    # those paths first.  /proc-host/mounts reflects the host mount table
    # (hostPID:true + privileged gives us access).
    #
    # pkb_swap is the dm-crypt device created by the node startup script (for
    # single-LSSD nodes it holds /dev/nvme1n1 directly without an md0 layer).
    daemonset.PodExec(
        textwrap.dedent(f"""
    echo "[pkb-lssd-cleanup] /proc/mdstat:" >&2
    cat /proc/mdstat 2>/dev/null || true
    echo "[pkb-lssd-cleanup] dmsetup ls:" >&2
    dmsetup ls 2>/dev/null || true
    echo "[pkb-lssd-cleanup] /proc/swaps:" >&2
    cat /proc/swaps 2>/dev/null || true
    echo "[pkb-lssd-cleanup] host mounts on {device_list}:" >&2
    grep -E '{('|'.join(devices))}' /proc-host/mounts 2>/dev/null || true
    echo "[pkb-lssd-cleanup] sysfs holders:" >&2
    for dev in {device_list}
    do
      devname=$(basename "$dev")
      ls -1 /sys/block/$devname/holders/ 2>/dev/null | while read h
      do
        echo "[pkb-lssd-cleanup]   $dev held by $h" >&2
      done
    done
    echo "[pkb-lssd-cleanup] --- begin teardown ---" >&2
    for dev in {device_list}
    do
      test -b "$dev" || continue
      devname=$(basename "$dev")
      for holder in /sys/block/$devname/holders/*
      do
        test -e "$holder" || continue
        h=$(basename "$holder")
        echo "[pkb-lssd-cleanup] removing holder /dev/$h from $dev" >&2
        if echo "$h" | grep -q "^md"
        then
          mdadm --stop /dev/$h 2>/dev/null || true
        else
          dmsetup remove --force --noudevrules --noudevsync /dev/$h 2>/dev/null || true
        fi
      done
      mounts=$(awk -v d="$dev" '$1==d{{print $2}}' /proc-host/mounts 2>/dev/null || true)
      for mp in $mounts
      do
        echo "[pkb-lssd-cleanup] unmounting $mp from $dev" >&2
        umount -f "$mp" 2>/dev/null || true
      done
    done
    swapoff -a 2>/dev/null || true
    swapoff /dev/mapper/pkb_swap 2>/dev/null || true
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync pkb_swap 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    mdadm --stop --scan 2>/dev/null || true
    mdadm --zero-superblock {device_list} 2>/dev/null || true
    wipefs -a {device_list} 2>/dev/null || true
    echo "[pkb-lssd-cleanup] lsblk after wipefs:" >&2
    lsblk {device_list} 2>/dev/null || true
    partx -u {device_list} 2>/dev/null || true
    losetup -D 2>/dev/null || true
    rm -f /mnt/stateful_partition/pkb_swap.img 2>/dev/null || true
    sleep 2
  """),
        ignore_failure=True,
    )

    # Step 3: verify the devices are truly raw (unpartitioned).  On GKE Ubuntu
    # nodes the local NVMe device may be partitioned by node startup scripts
    # even when --local-nvme-ssd-block is specified.  The kernel refuses a
    # whole-disk exclusive open (DM_TABLE_LOAD → EBUSY) when any partition of
    # the disk is open by another process (e.g. the container overlay FS is
    # backed by nvme1n1p1).  Detect this and fall back to a loop device backed
    # by a file on /mnt/stateful_partition (which IS the SSD partition).
    raw_check_out, _ = daemonset.PodExec(
        textwrap.dedent(f"""
        for dev in {device_list}
        do
          if lsblk -ln -o TYPE "$dev" 2>/dev/null | grep -q '^part$'
          then
            echo "[pkb-lssd] $dev is partitioned — cannot use as raw block device" >&2
          else
            echo "$dev"
          fi
        done
      """),
        ignore_failure=True,
    )
    raw_devices = [
        d.strip() for d in raw_check_out.strip().splitlines() if d.strip()
    ]

    if not raw_devices:
        logging.info(
            '[swap_encryption] GKE: all LSSD devices are partitioned — '
            'falling back to loop device on /mnt/stateful_partition'
        )
        _setup_gke_lssd_stateful_loop_swap(daemonset)
        return

    # Use only raw (unpartitioned) devices going forward.
    devices = raw_devices
    device_list = ' '.join(devices)
    n = len(devices)
    logging.info(
        '[swap_encryption] GKE: using %d raw LSSD device(s): %s  dmcrypt=%s',
        n,
        device_list,
        _ENABLE_DMCRYPT.value,
    )

    # For N=1 LSSD, skip mdadm entirely and target the raw device directly.
    # For N>1 we stripe across multiple NVMe devices.
    if n > 1:
        daemonset.PodExec(
            textwrap.dedent(f"""
      mdadm --create /dev/md0 --force \\
        --level=0 --raid-devices={n} \\
        {device_list}
      test -b /dev/md0 || {{ echo "mdadm: /dev/md0 not created" >&2; exit 1; }}
    """),
        )
        swap_block_dev = '/dev/md0'
    else:
        swap_block_dev = devices[0]
        logging.info(
            '[swap_encryption] GKE: single LSSD — skipping mdadm, '
            'using %s directly',
            swap_block_dev,
        )

    if _ENABLE_DMCRYPT.value:
        # Same dmsetup --noudevrules --noudevsync approach as _setup_gke_hyperdisk_swap.
        daemonset.PodExec(
            textwrap.dedent(f"""
      grep -q dm_crypt /proc/modules 2>/dev/null || {{
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }}
      udevadm control --stop-exec-queue 2>/dev/null || true
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz {swap_block_dev})
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "{swap_block_dev}" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      udevadm control --start-exec-queue 2>/dev/null || true
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted
      swapon /dev/mapper/swap_encrypted
    """),
        )
        logging.info(
            '[swap_encryption] GKE: LSSD dm-crypt swap active on %s',
            swap_block_dev,
        )
    else:
        daemonset.PodExec(
            textwrap.dedent(f"""
      mkswap {swap_block_dev}
      swapon {swap_block_dev}
    """),
        )
        logging.info(
            '[swap_encryption] GKE: LSSD plain swap active on %s',
            swap_block_dev,
        )


def _setup_gke_lssd_stateful_loop_swap(daemonset: _ds_mod.SwapDaemonSet) -> None:
    """Set up swap on the LSSD partition via a loop device.

    Used when the local NVMe device is partitioned by GKE startup scripts
    and cannot be opened as a whole raw block device (DM_TABLE_LOAD EBUSY).
    The DaemonSet mounts /mnt/stateful_partition (hostPath) from the host's
    nvme1n1p1 — which is still local SSD storage.  We create a large file
    there and layer loop → dm-crypt → swap on top of it.
    """
    img_path = '/mnt/stateful_partition/pkb_swap.img'

    # Clean up any previous run artifacts.
    daemonset.PodExec(
        textwrap.dedent(f"""
    swapoff -a 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    losetup -D 2>/dev/null || true
    rm -f {img_path} 2>/dev/null || true
  """),
        ignore_failure=True,
    )

    # Determine file size: 80% of available space, at least 16 GB.
    size_out, _ = daemonset.PodExec(
        "df -P /mnt/stateful_partition | awk 'NR==2{print $4}'",
        ignore_failure=True,
    )
    avail_kb = int(size_out.strip() or '0')
    swap_gb = max(16, int(avail_kb * 0.8 / 1024 / 1024))
    logging.info(
        '[swap_encryption] GKE: LSSD stateful-loop: %d GB image at %s',
        swap_gb,
        img_path,
    )

    # Allocate file (fallocate is instant on ext4; dd fallback for others).
    daemonset.PodExec(
        textwrap.dedent(f"""
    fallocate -l {swap_gb}G {img_path} 2>/dev/null || \\
      dd if=/dev/zero of={img_path} bs=1G count={swap_gb}
    chmod 600 {img_path}
    losetup --direct-io=on -f {img_path}
  """),
        timeout=300,
    )

    loop_out, _ = daemonset.PodExec(
        f"losetup -j {img_path} | awk -F: '{{print $1}}' | head -1",
        ignore_failure=True,
    )
    loop_dev = loop_out.strip()
    if not loop_dev.startswith('/dev/loop'):
        raise errors.Benchmarks.RunError(
            f'[swap_encryption] losetup failed for {img_path} — got:'
            f' {loop_out!r}'
        )
    logging.info(
        '[swap_encryption] GKE: LSSD stateful-loop device: %s', loop_dev
    )

    if _ENABLE_DMCRYPT.value:
        daemonset.PodExec(
            textwrap.dedent(f"""
      grep -q dm_crypt /proc/modules 2>/dev/null || {{
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }}
      udevadm control --stop-exec-queue 2>/dev/null || true
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz {loop_dev})
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "{loop_dev}" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      udevadm control --start-exec-queue 2>/dev/null || true
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted
      swapon /dev/mapper/swap_encrypted
    """),
        )
        logging.info(
            '[swap_encryption] GKE: LSSD stateful-loop dm-crypt swap active '
            'on %s → %s',
            img_path,
            loop_dev,
        )
    else:
        daemonset.PodExec(
            textwrap.dedent(f"""
      mkswap {loop_dev}
      swapon {loop_dev}
    """),
        )
        logging.info(
            '[swap_encryption] GKE: LSSD stateful-loop plain swap active '
            'on %s → %s',
            img_path,
            loop_dev,
        )


def _setup_eks_swap(daemonset: _ds_mod.SwapDaemonSet) -> None:
    """Configure swap on EKS nodes — Instance Store OR io2 root disk.

    Swap type is selected by --swap_encryption_swap_type:
      instance_store (default) – NVMe SSD attached by Nitro (i4i, m6id, c6id).
        Nitro encrypts all block-device writes at hardware level; no extra
        cryptsetup needed.
      io2 – EBS io2 volume provisioned as the node root/data disk.
        Used for apples-to-apples comparison against GKE hyperdisk-balanced.
    """
    swap_type = _SWAP_TYPE.value
    if swap_type in ('auto', 'instance_store'):
        _setup_eks_instance_store_swap(daemonset)
    elif swap_type == 'io2':
        _setup_eks_io2_swap(daemonset)
    else:
        raise errors.Benchmarks.RunError(
            f'[swap_encryption] Unknown EKS swap type {swap_type!r}.'
        )


def _setup_eks_instance_store_swap(daemonset: _ds_mod.SwapDaemonSet) -> None:
    """Swap on AWS NVMe Instance Store (Nitro hardware-offloaded encryption)."""
    logging.info('[swap_encryption] EKS: setting up Instance Store swap')

    # Find the Instance Store NVMe device (not the root EBS volume)
    nvme_out, _ = daemonset.PodExec(
        "nvme list 2>/dev/null | awk '/Instance Storage/{print $1}' | head -1"
        " || lsblk -d -o NAME,MODEL | grep -i 'instance\\|nvme' | grep -v"
        " 'nvme0' | awk '{print \"/dev/\"$1}' | head -1",
        ignore_failure=True,
    )
    device = nvme_out.strip()
    if not device:
        # Common Instance Store device paths on AWS
        for candidate in ['/dev/nvme1n1', '/dev/nvme2n1', '/dev/xvdb']:
            exists_out, _ = daemonset.PodExec(
                f'test -b {candidate} && echo yes || echo no',
                ignore_failure=True,
            )
            if exists_out.strip() == 'yes':
                device = candidate
                break

    if not device:
        logging.info(
            '[swap_encryption] No Instance Store NVMe found – creating swapfile'
        )
        _setup_plain_swap_file(daemonset, _SWAP_SIZE_GB.value)
        return

    logging.info('[swap_encryption] EKS: Instance Store device: %s', device)

    # Nitro encrypts all Instance Store writes automatically.
    # No additional cryptsetup required.
    daemonset.PodExec(
        textwrap.dedent(f"""
    mkswap {device} && \\
    swapon {device}
  """),
    )
    logging.info(
        '[swap_encryption] EKS: Instance Store swap active on %s', device
    )


def _setup_eks_io2_swap(daemonset: _ds_mod.SwapDaemonSet) -> None:
    """Swap on AWS EBS io2 volume – apples-to-apples comparison vs GKE hyperdisk.

    EBS io2 volumes on Nitro instances are encrypted at rest by AWS KMS (if
    enabled on the volume) or via Nitro-level hardware encryption.  No additional
    cryptsetup is needed here; we simply format the attached data disk as swap.

    Device discovery order:
      1. Match the io2 volume created by _ensure_io2_volume() by its NVMe serial
         (serial == volume id without the dash).  This is unambiguous and never
         picks the root disk or the instance store regardless of nvmeXn1
         enumeration order on Nitro.
      2. First non-root EBS ("Elastic Block Store") block device that is not
         currently mounted.
    """
    logging.info('[swap_encryption] EKS: setting up io2 EBS swap')

    # Identify root device so we can exclude it.
    root_out, _ = daemonset.PodExec(
        'lsblk -no pkname $(findmnt -n -o SOURCE /) 2>/dev/null || echo'
        ' nvme0n1',
        ignore_failure=True,
    )
    root_base = root_out.strip() or 'nvme0n1'

    # Identify the io2 volume UNAMBIGUOUSLY by its NVMe serial == volume id.
    # An EBS NVMe device's serial equals the volume id minus the dash
    # (vol-0abc... -> serial vol0abc...).
    device = ''
    target = _IO2_VOLUME_ID.replace('-', '')
    if target:
        ser_out, _ = daemonset.PodExec(
            'for d in /sys/block/nvme*n1; do '
            '[ -e "$d" ] || continue; '
            's=$(cat "$d/device/serial" 2>/dev/null | tr -d "-" | tr -d " "); '
            f'[ "$s" = "{target}" ] && {{ echo "/dev/$(basename "$d")"; break;'
            ' }; '
            'done',
            ignore_failure=True,
        )
        device = ser_out.strip()
        if device:
            logging.info(
                '[swap_encryption] EKS: io2 matched by serial %s -> %s',
                target,
                device,
            )

    if not device:
        # Fallback: first non-root EBS device, excluding any device that is
        # currently mounted (root) or already active swap.
        disk_out, _ = daemonset.PodExec(
            'for d in /sys/block/nvme*n1 /sys/block/xvd[b-z]'
            ' /sys/block/sd[b-z];'
            ' do [ -e "$d" ] || continue; n=$(basename "$d"); [ "$n" ='
            f' "{root_base}" ] && continue; m=$(cat "$d/device/model"'
            ' 2>/dev/null);'
            ' echo "$m" | grep -qi "Elastic Block Store" || continue;'
            ' mnt=$(lsblk'
            ' -no MOUNTPOINT "/dev/$n" 2>/dev/null | tr -d " "); [ -n "$mnt"'
            ' ] &&'
            ' continue; echo "/dev/$n"; break; done',
            ignore_failure=True,
        )
        device = disk_out.strip()
        if device:
            logging.info(
                '[swap_encryption] EKS: io2 fallback EBS device: %s', device
            )

    if not device:
        logging.info(
            '[swap_encryption] No io2 EBS disk found – creating plain swapfile'
        )
        _setup_plain_swap_file(daemonset, _SWAP_SIZE_GB.value)
        return

    logging.info('[swap_encryption] EKS: io2 EBS device: %s', device)

    # EBS io2 encryption is handled at the AWS level (Nitro / KMS).
    out, _ = daemonset.PodExec(
        textwrap.dedent(f"""
    swapoff {device} 2>/dev/null || true
    wipefs -a {device} 2>/dev/null || true
    mkswap -f {device} && swapon {device}
    swapon --show
  """),
        ignore_failure=True,
    )
    if device not in out:
        raise errors.Benchmarks.RunError(
            f'[swap_encryption] io2 swap did not activate on {device}; '
            f'swapon --show output: {out!r}. The device may be busy/mounted '
            '(wrong device picked) or mkswap failed.'
        )
    logging.info('[swap_encryption] EKS: io2 EBS swap active on %s', device)


def _setup_plain_swap_file(
    daemonset: _ds_mod.SwapDaemonSet, size_gb: int
) -> None:
    """Fallback: create a loop-device-backed swapfile.

    A plain file on overlayfs (the container root) cannot be used as swap —
    the kernel rejects it with EINVAL.  Routing it through a loop device
    presents a proper block device to the mm subsystem and succeeds.
    """
    logging.info('[swap_encryption] Creating %dGB loop-device swap', size_gb)
    daemonset.PodExec(
        textwrap.dedent(f"""
    fallocate -l {size_gb}G /tmp/pkb_swapfile && \\
    chmod 600 /tmp/pkb_swapfile && \\
    LOOP=$(losetup -f) && \\
    losetup "$LOOP" /tmp/pkb_swapfile && \\
    mkswap "$LOOP" && \\
    swapon "$LOOP" && \\
    echo "swap loop device: $LOOP"
  """),
    )


def _enable_zswap(daemonset: _ds_mod.SwapDaemonSet) -> None:
    """Enable zswap with lz4 compressor and 20% pool limit inside the pod."""
    logging.info('[swap_encryption] Enabling zswap (lz4, 20%% pool)')
    for cmd in [
        'echo 1      > /sys/module/zswap/parameters/enabled',
        'echo lz4    > /sys/module/zswap/parameters/compressor',
        'echo 20     > /sys/module/zswap/parameters/max_pool_percent',
        'echo z3fold > /sys/module/zswap/parameters/zpool',
    ]:
        daemonset.PodExec(cmd, ignore_failure=True)


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
    raise errors.Benchmarks.RunError(
        '[swap_encryption] No active swap device found in the benchmark pod. '
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
        raise errors.Benchmarks.RunError(
            f'[swap_encryption] Phase 1: no JSON in fio output for {job_name}'
        )

    try:
        data = json.loads(fio_output[json_start:])
    except json.JSONDecodeError as e:
        raise errors.Benchmarks.RunError(
            f'[swap_encryption] Phase 1: fio JSON parse error ({job_name}): {e}'
        ) from e

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
    'c4-standard-8-lssd': 0.5888,  # 8 vCPU, 32 GB RAM + 1×375 GB LSSD
    'c4-standard-8': 0.5008,  # 8 vCPU, 32 GB RAM, no LSSD
    'n4-highmem-32': 3.0256,  # 32 vCPU, 256 GB RAM
    'n2-highmem-32': 2.5216,  # 32 vCPU, 256 GB RAM
    'n2-standard-32': 1.5264,  # 32 vCPU, 120 GB RAM
    'z3-highmem-8': 2.7248,  # 8 vCPU + 4× LSSD
    # AWS
    'i4i.4xlarge': 1.4960,  # 16 vCPU, 128 GB RAM, NVMe Instance Store
    'i4i.2xlarge': 0.7480,
    'm6id.4xlarge': 0.9072,  # 16 vCPU, 64 GB RAM, NVMe Instance Store
    'm6i.4xlarge': 0.7680,  # 16 vCPU, 64 GB RAM, no Instance Store
    'r6i.4xlarge': 1.0080,  # 16 vCPU, 128 GB RAM, no Instance Store
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
        logging.info(
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
