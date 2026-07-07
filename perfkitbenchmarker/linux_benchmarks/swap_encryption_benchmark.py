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

    SwapNodePool  (provisioned via swap_config field on NodepoolSpec)
    SwapDaemonSet (perfkitbenchmarker/resources/container_service/swap_daemonset.py)

Both resources are added to spec.resources in Prepare() and are
auto-deleted by the PKB framework in Cleanup().

== Benchmark Phases ==

  Phase 1 – fio Microbenchmarks
    Run fio directly on the swap block device (swapoff first) to measure
    the hardware + encryption ceiling: random IOPS (4K), sequential
    bandwidth (1M), and completion latency (iodepth=1).
"""

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
from perfkitbenchmarker.linux_benchmarks import swap_encryption_phases as _phases
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
    Verify dm-crypt encrypted swap on GKE/EKS nodes. Swap-enabled 'benchmark'
    nodepool declared in BENCHMARK_CONFIG; GKE cluster creation applies
    --system-config-from-file (dm-crypt swapConfig) automatically via
    swap_config field on NodepoolSpec.
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

# ---------------------------------------------------------------------------
# Flags
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

# DaemonSet identity — used by Prepare() and _get_daemonset().
_DS_NAME = 'pkb-swap-benchmark'
_DS_NAMESPACE = 'default'
_DS_LABEL = 'pkb-swap-benchmark'
_BENCHMARK_NODEPOOL = 'benchmark'
_DEFAULT_POOL = 'default-pool'

# Module-level stash for the io2 volume id created by _ensure_io2_volume().
_IO2_VOLUME_ID = ''

# On-demand instance pricing (USD/hr) for cost_estimate_usd sample.
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


# ---------------------------------------------------------------------------
# PKB benchmark API
# ---------------------------------------------------------------------------


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
    """Load and return benchmark config spec."""
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(spec: _BenchmarkSpec) -> None:
    """Deploy privileged SwapDaemonSet and delete the dummy default nodepool.

    PKB cluster creation automatically provisions the swap-enabled 'benchmark'
    nodepool (swap_config declared in BENCHMARK_CONFIG). This function only:
      1. Deploys the privileged SwapDaemonSet and waits for Running.
      2. Deletes the cheap e2-medium default-pool (required at cluster create).

    DaemonSet is appended to spec.resources for PKB auto-cleanup.
    """
    cluster = spec.container_cluster
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

    Execution is structured in gated tiers matching the execution plan:

      Tier 1 (Gate 1) — fio microbenchmarks
        Raw I/O ceiling of the swap device.  Gate 1 fails if fio produces
        zero samples (device not found, O_DIRECT error, etc.).

    If Gate 1 fails, subsequent tiers (added in later PRs) are skipped.
    """
    daemonset = _get_daemonset(spec)

    pod = daemonset.WaitForPod()
    daemonset.oom_events.clear()
    daemonset.pod_lost.clear()
    original_pod = pod
    degraded_reasons: list[str] = []

    # ── Swap setup ────────────────────────────────────────────────────────────
    daemonset.PodExec('sysctl -w vm.swappiness=100', ignore_failure=True)
    if _MIN_FREE_KBYTES.value > 0:
        daemonset.PodExec(
            f'sysctl -w vm.min_free_kbytes={_MIN_FREE_KBYTES.value}'
        )
    # Lift cgroup swap limits so the pod can actually use swap.
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
        daemonset.EnableZswap()

    cloud = daemonset.DetectCloud()
    logging.info('[swap_encryption] Detected cloud: %s', cloud)
    daemonset.SetupSwap(
        cloud=cloud,
        swap_type=_SWAP_TYPE.value,
        enable_dmcrypt=_ENABLE_DMCRYPT.value,
        swap_size_gb=_SWAP_SIZE_GB.value,
        io2_volume_id=_IO2_VOLUME_ID,
    )

    swap_dev = daemonset.GetActiveSwapDevice(_SWAP_DEVICE.value)
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
            phase1_samples = _phases.RunPhase1Fio(
                daemonset, swap_dev, base_meta, _FIO_RUNTIME_SEC.value
            )
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
    """


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _delete_default_pool(cluster) -> None:
    """Delete the dummy e2-medium default-pool once the benchmark pod is Running.

    GKE requires at least one nodepool at cluster creation time; the e2-medium
    default-pool satisfies that requirement.
    """
    try:
        cmd = cluster._GcloudCommand(  # pylint: disable=protected-access
            'container', 'node-pools', 'delete', _DEFAULT_POOL,
            '--cluster', cluster.name,
        )
        cmd.args.append('--quiet')
        logging.info(
            '[swap_encryption] Deleting default nodepool: %s', _DEFAULT_POOL
        )
        _, stderr, rc = cmd.Issue(timeout=300, raise_on_failure=False)
        if rc != 0:
            raise errors.Benchmarks.RunError(
                f'[swap_encryption] Could not delete default nodepool '
                f'(rc={rc}): {stderr}'
            )
        logging.info('[swap_encryption] Default nodepool deleted')
    except errors.Benchmarks.RunError:
        raise
    except Exception as e:  # pylint: disable=broad-except
        raise errors.Benchmarks.RunError(
            f'[swap_encryption] _delete_default_pool failed: {e}'
        ) from e


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
    """Create + attach a dedicated io2 EBS volume to the benchmark node.

    NOTE: Deferred — requires EKS kubelet LimitedSwap support to be
    enabled.  Stashes the created volume id in _IO2_VOLUME_ID for
    serial-based device detection in SwapDaemonSet._SetupEksIo2Swap.
    """
    global _IO2_VOLUME_ID
    if _SWAP_TYPE.value != 'io2':
        return
    out, _, rc = kubectl.RunKubectlCommand(
        ['get', 'nodes', '-o', 'jsonpath={.items[0].spec.providerID}'],
        raise_on_failure=False,
    )
    provider = (out or '').strip()
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
            'create-volume', '--volume-type', 'io2',
            '--size', '500', '--iops', '16000',
            '--availability-zone', az,
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
            base + [
                'attach-volume', '--volume-id', vol_id,
                '--instance-id', instance_id, '--device', '/dev/sdf',
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
            vol_id, instance_id,
        )
        time.sleep(15)
    except Exception as e:  # pylint: disable=broad-except
        raise errors.Benchmarks.RunError(
            f'[swap_encryption] io2 attach error: {e}'
        ) from e


def _configure_eks_kubelet_swap(spec) -> None:
    """Configure EKS kubelet for LimitedSwap via nodeadm bootstrap.

    NOTE: Deferred — requires Ajay's PR #6780 (SwapConfigSpec + nodeadm
    integration) to merge.  When that lands, EKS node pools should include
    a preBootstrapCommands block writing nodeadm config with
    memorySwapBehavior: LimitedSwap before kubelet starts.

    GKE equivalent: linuxConfig.swapConfig via --system-config-from-file
    (already implemented in SwapNodePool._CreateNodePool()).
    """
    raise NotImplementedError(
        '[swap_encryption] EKS kubelet LimitedSwap config via nodeadm is '
        'deferred (blocked on PR #6780). EKS swap setup not yet implemented.'
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

    cloud = daemonset.DetectCloud()

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
