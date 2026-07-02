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
    fio microbenchmarks (Tier 1) on swap-encrypted GKE/EKS nodes. Swap-enabled 'benchmark' nodepool declared in BENCHMARK_CONFIG;
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


_SWAP_TYPE = flags.DEFINE_string(
    'swap_encryption_swap_type',
    'hyperdisk',
    'Storage target for the swap device.  One of: hyperdisk (default), '
    'lssd, boot_disk, instance_store, io2.',
)


_FIO_RUNTIME_SEC = flags.DEFINE_integer(
    'swap_encryption_fio_runtime_sec',
    60,
    'Wall-clock runtime in seconds for each individual fio job.',
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
    'hyperdisk-balanced IOPS constraint: provisioned_iops <= size_gb * 80.',
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

_FIO_JOBS = (
    ('rand_write_iops', 'randwrite', '4k', 256, 'Random write IOPS'),
    ('rand_read_iops', 'randread', '4k', 256, 'Random read IOPS'),
    ('rand_rw_mixed', 'randrw', '4k', 256, 'Mixed random R/W (50/50)'),
    ('seq_write_bw', 'write', '1m', 64, 'Sequential write bandwidth'),
    ('seq_read_bw', 'read', '1m', 64, 'Sequential read bandwidth'),
    ('lat_write', 'randwrite', '4k', 1, 'Random write latency'),
    ('lat_read', 'randread', '4k', 1, 'Random read latency'),
)

# Module-level stash for the io2 volume created in _ensure_io2_volume.
_IO2_VOLUME_ID = ''


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
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
    daemonset.Create()
    spec.resources.append(daemonset)
    logging.info('[swap_encryption] Benchmark pod ready: %s', daemonset.pod_name)
    _delete_default_pool(cluster)
    daemonset.WaitForPod()
    logging.info(
        '[swap_encryption] Benchmark pod (post-deletion): %s', daemonset.pod_name
    )

    # Tune kernel swap aggressiveness.
    daemonset.PodExec('sysctl -w vm.swappiness=100', ignore_failure=True)
    if _MIN_FREE_KBYTES.value > 0:
        daemonset.PodExec(
            f'sysctl -w vm.min_free_kbytes={_MIN_FREE_KBYTES.value}'
        )

    # Unlock container cgroup swap.
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

    # Enable zswap if requested.
    if _ENABLE_ZSWAP.value:
        _enable_zswap(daemonset)

    # Configure cloud-specific swap.
    cloud = _detect_cloud(daemonset)
    logging.info('[swap_encryption] Detected cloud: %s', cloud)

    if cloud == 'gcp':
        _setup_gke_swap(daemonset)
    elif cloud == 'aws':
        _setup_eks_swap(daemonset)
    else:
        logging.warning(
            '[swap_encryption] Unknown cloud – falling back to plain swapfile'
        )
        _setup_plain_swap_file(daemonset, _SWAP_SIZE_GB.value)


def _phase_selected(token: str) -> bool:
    """Return True if phase `token` should run given --swap_encryption_phases.

    'all' (the default) selects every phase.  Otherwise only the comma-separated
    tokens listed in the flag run.  Tokens: fio, 2a, 2b, 3a, 3b, 3c.
    """
    selected = [p.strip().lower() for p in _PHASES.value if p.strip()]
    return (not selected) or ('all' in selected) or (token.lower() in selected)


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

    # ── Tier 1 / Gate 1: fio microbenchmarks ─────────────────────────────────
    tier1_results = []
    if _phase_selected('fio'):
        logging.info(
            '[swap_encryption] ── Tier 1 / Gate 1: fio microbenchmarks ──'
        )
        try:
            tier1_results = _phase1_fio(daemonset, swap_dev, base_meta)
            results += tier1_results
        except Exception as e:  # pylint: disable=broad-except
            logging.error(
                '[swap_encryption] Gate 1 FAILED — fio phase error: %s', e
            )
            logging.error(
                '[swap_encryption] Skipping Tiers 2 and 3 (no swap device)'
            )
            return results

        if not tier1_results:
            logging.warning(
                '[swap_encryption] Gate 1 produced no samples '
                '(loop-device skip or parse error) — '
                'continuing to Tier 2 with caution'
            )
    else:
        logging.info(
            '[swap_encryption] Skipping Tier 1 (fio) — not selected by '
            '--swap_encryption_phases=%s',
            ','.join(_PHASES.value),
        )

    # ── Cost estimate ─────────────────────────────────────────────────────────
    if _COLLECT_COST.value:
        elapsed = time.time() - t_run_start
        results += _collect_cost_sample(daemonset, elapsed, base_meta)

    # ── Final degradation gate ────────────────────────────────────────────────
    if daemonset.pod_name and daemonset.pod_name != original_pod:
        degraded_reasons.append(
            f'benchmark pod was replaced during the run ({original_pod} →'
            f' {daemonset.pod_name}) — it was OOM-evicted under swap pressure;'
            ' phases executed after the eviction ran against a'
            ' freshly-initialised pod (empty /tmp, swap re-setup) and may be'
            ' invalid'
        )
    if daemonset.pod_lost:
        degraded_reasons.append(
            'benchmark pod(s) went NotFound during the run'
            f' ({", ".join(daemonset.pod_lost)}) — the pod died (node memory-pressure'
            ' eviction or container exit) and any phase running at or after'
            ' that point (e.g. kernel-build baseline, OpenSearch) produced'
            ' invalid data'
        )
    if daemonset.oom_events:
        degraded_reasons.append(
            f'OOM kill(s) (rc=137) occurred during the run on pod(s) '
            f'{", ".join(daemonset.oom_events)} — a phase exceeded memory and was'
            ' killed by the OOM killer (the container may have restarted in place),'
            ' so the affected phase(s) produced no or partial data'
        )

    if _phase_selected('fio') and not tier1_results:
        if swap_dev.startswith('/dev/loop'):
            # Expected: COS blocks device-mapper from pod namespaces on single-disk
            # nodes.  Tier 2/3 results are still valid; do NOT mark the run as degraded.
            logging.warning(
                '[swap_encryption] Gate 1 (fio) skipped — loop device %s has no'
                ' dm-crypt support from inside a pod.  Tier 2/3 results are'
                ' valid. Use c4-*-lssd or --swap_encryption_add_swap_disk for'
                ' fio data.',
                swap_dev,
            )
        else:
            degraded_reasons.append(
                'Gate 1 (fio microbenchmarks) produced no samples — the raw'
                ' swap device was never characterised'
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
            '[swap_encryption] Run completed cleanly (%d samples)', len(results)
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
      logging.warning(
          '[swap_encryption] Could not delete default nodepool (rc=%d): %s',
          rc, stderr,
      )
    else:
      logging.info('[swap_encryption] Default nodepool deleted')
  except Exception as e:  # pylint: disable=broad-except
    logging.warning('[swap_encryption] _delete_default_pool failed: %s', e)
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


def _configure_eks_kubelet_swap(spec) -> None:
    """Configure EKS kubelet for LimitedSwap via nodeadm bootstrap.

    NOTE: Deferred — requires Ajay's PR #6780 (SwapConfigSpec + nodeadm
    integration) to merge.  When that lands, EKS node pools should include
    a preBootstrapCommands block writing nodeadm config with
    memorySwapBehavior: LimitedSwap before kubelet starts.

    See also: https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/pull/6780
    """
    logging.warning(
        '[swap_encryption] EKS kubelet LimitedSwap config via nodeadm is '
        'deferred (blocked on PR #6780 — SwapConfigSpec). '
        'EKS nodes will use default kubelet swap settings until that PR merges.'
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

    logging.warning(
        '[swap_encryption] Could not detect cloud from DMI or metadata'
    )
    return 'unknown'


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
       inaccessible from inside a Kubernetes pod (even privileged).
    2. On UBUNTU_CONTAINERD: the loop device is created in the container
       namespace; its behaviour under nsenter is untested.

    Therefore this path uses a plain loop device as swap without dm-crypt.
    Phase 1 (fio) is skipped for plain loop devices.
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
        raise RuntimeError(
            f'[swap_encryption] losetup failed – output: {loop_out!r}'
        )
    logging.info(
        '[swap_encryption] GKE: loop device: %s  direct-io=on', loop_dev
    )

    # ── Step 3: plain mkswap + swapon (dm-crypt skipped on loop devices) ────────
    daemonset.PodExec(f'mkswap {loop_dev}')
    daemonset.PodExec(f'swapon {loop_dev}')
    logging.warning(
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
        raise RuntimeError(
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

    # Reused-node hygiene: tear down any prior PKB swap mapping FIRST.
    daemonset.PodExec(
        textwrap.dedent("""
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    swapoff -a 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
  """),
        ignore_failure=True,
    )

    # Log the full block-device topology up front for diagnosis.
    topo, _ = daemonset.PodExec(
        'lsblk -o NAME,TYPE,SIZE,ROTA,MOUNTPOINT 2>/dev/null',
        ignore_failure=True,
    )
    logging.info(
        '[swap_encryption] block device topology:\n%s', (topo or '').strip()
    )

    # Identify candidate swap devices = whole disks that are NOT the boot/OS disk.
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
        logging.warning(
            '[swap_encryption] No clean (unpartitioned, unmounted) local SSD'
            ' found — falling back to hyperdisk swap path'
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

    # Verify the devices are truly raw (unpartitioned).
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


def _setup_gke_lssd_stateful_loop_swap(
    daemonset: _ds_mod.SwapDaemonSet,
) -> None:
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
        f"df -P /mnt/stateful_partition | awk 'NR==2{{print $4}}'",
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
        raise RuntimeError(
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


def _ensure_io2_volume() -> None:
    """Create + attach a dedicated io2 EBS volume to the benchmark node.

    No-op unless --swap_encryption_swap_type=io2 on an AWS/EKS cluster.
    Best-effort: logs and returns on failure.  Stashes the created volume id in
    the module-level _IO2_VOLUME_ID for serial-based device detection in
    _setup_eks_io2_swap.
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
        logging.warning(
            '[swap_encryption] io2 attach skipped: could not resolve '
            'EC2 instance from providerID=%r',
            provider,
        )
        return
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
            logging.warning(
                '[swap_encryption] io2 create-volume failed: %r', vol_id
            )
            return
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
        logging.warning(
            '[swap_encryption] io2 attach error (continuing): %s', e
        )


def _setup_eks_swap(daemonset: _ds_mod.SwapDaemonSet) -> None:
    """Configure swap on EKS nodes — Instance Store OR io2 root disk.

    Swap type is selected by --swap_encryption_swap_type:
      instance_store (default) – NVMe SSD attached by Nitro (i4i, m6id, c6id).
        Nitro encrypts all block-device writes at hardware level.
      io2 – EBS io2 volume provisioned as the node root/data disk.
        Used for apples-to-apples comparison against GKE hyperdisk-balanced.
    """
    swap_type = _SWAP_TYPE.value
    if swap_type in ('auto', 'instance_store'):
        _setup_eks_instance_store_swap(daemonset)
    elif swap_type == 'io2':
        _setup_eks_io2_swap(daemonset)
    else:
        logging.warning(
            '[swap_encryption] Unknown EKS swap type %s – fallback', swap_type
        )
        _setup_eks_instance_store_swap(daemonset)


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
        logging.warning(
            '[swap_encryption] No Instance Store NVMe found – creating swapfile'
        )
        _setup_plain_swap_file(daemonset, _SWAP_SIZE_GB.value)
        return

    logging.info('[swap_encryption] EKS: Instance Store device: %s', device)

    # Nitro encrypts all Instance Store writes automatically.
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
    enabled on the volume) or via Nitro-level hardware encryption.

    Device discovery order:
      1. Match the io2 volume created by _ensure_io2_volume() by its NVMe serial
         (serial == volume id without the dash).
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
        # Fallback: first non-root EBS device that is not currently mounted.
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
        logging.warning(
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
        raise RuntimeError(
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


def _phase1_fio(
    daemonset: _ds_mod.SwapDaemonSet, swap_dev: str, base_meta: dict
) -> list[sample.Sample]:
    """Run fio directly on the swap block device for raw I/O characterisation.

    Skipped only for an UNINTENTIONAL loop fallback (a single-disk node with no
    dedicated swap disk, where fio on the loop would measure the boot ext4
    filesystem rather than the swap stack).  When the user explicitly selects the
    boot_disk target (--swap_encryption_swap_type=boot_disk, methodology rows
    1-4), the loop over the boot disk IS the device under test, so fio runs and
    characterises it.
    """
    if swap_dev.startswith('/dev/loop') and _SWAP_TYPE.value != 'boot_disk':
        logging.warning(
            '[swap_encryption] Phase 1 (fio) SKIPPED for plain loop device %s'
            ' (unintentional single-disk fallback). fio on a loop-backed device'
            ' measures the underlying ext4 filesystem (stateful_partition), not'
            ' the swap stack. Use c4-*-lssd, --swap_encryption_add_swap_disk,'
            ' or --swap_encryption_swap_type=boot_disk for fio data.',
            swap_dev,
        )
        return []

    results = []

    daemonset.PodExec(f'swapoff {swap_dev}', ignore_failure=True)

    # Pre-fill device so read tests have real data.
    # Cap at 20 GiB — enough to warm up the dm-crypt pipeline.
    _PREFILL_GIB = 20
    prefill_timeout = _PREFILL_GIB * 1024 // 150 + 60
    prefill_timeout = max(prefill_timeout, 300)
    logging.info(
        '[swap_encryption] Pre-filling %d GiB of %s', _PREFILL_GIB, swap_dev
    )
    daemonset.PodExec(
        (
            f'fio --name=prefill --filename={swap_dev} --ioengine=libaio'
            f' --direct=1 --rw=write --bs=1m --size={_PREFILL_GIB}g --verify=0'
            ' --output=/tmp/pkb_fio_prefill.log'
        ),
        timeout=prefill_timeout,
        ignore_failure=True,
    )

    # Each fio job: runtime + 90 s buffer (run + JSON write + file read).
    fio_run_timeout = _FIO_RUNTIME_SEC.value + 90
    fio_read_timeout = 60

    for name, rw, bs, depth, label in _FIO_JOBS:
        logging.info('[swap_encryption] fio: %s', name)
        out_file = f'/tmp/pkb_fio_{name}.json'
        # Remove stale output first to avoid silently reusing a previous result.
        daemonset.PodExec(
            f'rm -f {out_file}',
            ignore_failure=True,
            _retries=0,
            timeout=15,
        )
        run_cmd = (
            f'fio --name={name} --filename={swap_dev} '
            '--ioengine=libaio --direct=1 --verify=0 --randrepeat=0 '
            f'--bs={bs} --iodepth={depth} --rw={rw} '
            f'--time_based --runtime={_FIO_RUNTIME_SEC.value}s '
            f'--output-format=json --output={out_file}'
        )
        _, err = daemonset.PodExec(
            run_cmd,
            timeout=fio_run_timeout,
            ignore_failure=True,
            _retries=0,
        )
        if 'connection reset by peer' in err:
            logging.warning(
                '[swap_encryption] fio %s: kubectl exec connection '
                'reset; result may be incomplete',
                name,
            )
        out, _ = daemonset.PodExec(
            f'cat {out_file} 2>/dev/null || echo ""',
            timeout=fio_read_timeout,
            ignore_failure=True,
        )
        results += _parse_fio_json(out, name, label, base_meta)

    # fio prefill overwrites the entire device, destroying the mkswap header.
    # Re-stamp and re-enable before the remaining phases need active swap.
    daemonset.PodExec(
        f'mkswap {swap_dev} && swapon {swap_dev}',
        ignore_failure=True,
        timeout=120,
    )
    return results


def _parse_fio_json(
    stdout: str, job_name: str, label: str, base_meta: dict
) -> list[sample.Sample]:
    """Parse fio JSON output into PKB Samples."""
    results = []
    try:
        data = json.loads(stdout)
    except (json.JSONDecodeError, ValueError):
        logging.warning(
            '[swap_encryption] fio JSON parse failed for %s', job_name
        )
        return results

    meta = dict(base_meta, fio_job=job_name, fio_label=label)
    for job in data.get('jobs', []):
        for direction in ('read', 'write'):
            d = job.get(direction, {})
            if not d or d.get('io_bytes', 0) == 0:
                continue
            iops = float(d.get('iops', 0))
            bw_kib = float(d.get('bw', 0))
            clat = d.get('clat_ns', {})
            pct = clat.get('percentile', {})
            lat_mean = float(clat.get('mean', 0)) / 1000.0
            lat_p50 = float(pct.get('50.000000', 0)) / 1000.0
            lat_p99 = float(pct.get('99.000000', 0)) / 1000.0
            lat_p999 = float(pct.get('99.900000', 0)) / 1000.0
            m = dict(meta, direction=direction)
            results += [
                sample.Sample(f'{job_name}_{direction}_iops', iops, 'iops', m),
                sample.Sample(
                    f'{job_name}_{direction}_bw_mbps', bw_kib / 1024, 'MB/s', m
                ),
                sample.Sample(
                    f'{job_name}_{direction}_lat_mean', lat_mean, 'us', m
                ),
                sample.Sample(
                    f'{job_name}_{direction}_lat_p50', lat_p50, 'us', m
                ),
                sample.Sample(
                    f'{job_name}_{direction}_lat_p99', lat_p99, 'us', m
                ),
                sample.Sample(
                    f'{job_name}_{direction}_lat_p999', lat_p999, 'us', m
                ),
            ]
    return results


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
    """Emit a cost_estimate_usd sample for the benchmark run.

    Instance type is read from cloud metadata inside the pod.  Price is looked
    up from _INSTANCE_PRICE_USD_PER_HR; if unknown, the sample is omitted and
    a warning is logged.

    Args:
      daemonset: Active SwapDaemonSet resource.
      elapsed_sec: Wall-clock seconds the benchmark phases took.
      base_meta: Shared metadata dict.

    Returns:
      A list of zero or one sample.Sample.
    """
    instance_type = ''

    # GCP: machine type is the last segment of the metadata URL value
    gcp_type_out, _ = daemonset.PodExec(
        'curl -s -m 3 --fail'
        ' http://metadata.google.internal/computeMetadata/v1/instance/machine-type'
        ' -H "Metadata-Flavor: Google" 2>/dev/null || echo ""',
        ignore_failure=True,
    )
    if gcp_type_out.strip():
        instance_type = gcp_type_out.strip().split('/')[-1]

    if not instance_type:
        # AWS: instance-type is a plain string
        aws_type_out, _ = daemonset.PodExec(
            'curl -s -m 3 --fail '
            'http://169.254.169.254/latest/meta-data/instance-type '
            '2>/dev/null || echo ""',
            ignore_failure=True,
        )
        instance_type = aws_type_out.strip()

    # Allow explicit override.
    if _INSTANCE_SIZE_LABEL.value:
        instance_type = _INSTANCE_SIZE_LABEL.value

    # Last resort: fall back to the benchmark machine type flag.
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
    cost = hours * price
    meta = dict(
        base_meta,
        instance_type=instance_type,
        price_usd_per_hr=price,
        benchmark_elapsed_sec=round(elapsed_sec, 1),
    )
    return [sample.Sample('cost_estimate_usd', cost, 'USD', meta)]


def _detect_swap_device(daemonset: _ds_mod.SwapDaemonSet) -> str:
    """Return the active swap device path on the cluster node."""
    if _SWAP_DEVICE.value:
        return _SWAP_DEVICE.value

    # /proc/swaps is the source of truth: it lists the swap device that is
    # ACTUALLY active.  We must NOT just `test -e /dev/mapper/swap_encrypted`,
    # because a stale dm-crypt mapping from a previous run on a reused node can
    # still exist as a /dev node while being non-functional.
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
        "awk '/MemTotal/{print $2}' /proc/meminfo",
        ignore_failure=True,
    )
    swap_out, _ = daemonset.PodExec(
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
    enc = 'unknown'
    if '/dev/mapper/' in swap_dev:
        table_out, _ = daemonset.PodExec(
            f'dmsetup table {swap_dev.split("/")[-1]} 2>/dev/null || echo ""',
            ignore_failure=True,
        )
        enc = 'dm-crypt-plain' if 'crypt' in table_out.lower() else 'dm-other'
    elif _SWAP_TYPE.value in ('instance_store', 'io2'):
        enc = 'nitro_hardware_offload'  # AWS: encrypted by the Nitro card
    elif not _ENABLE_DMCRYPT.value:
        enc = 'none'  # GKE plain swap (encryption OFF)

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
        # Test-matrix columns: storage target, encryption on/off, image, IOPS
        'storage_target': _SWAP_TYPE.value,
        'boot_disk_type': _BOOT_DISK_TYPE.value,
        'dmcrypt_enabled': _ENABLE_DMCRYPT.value,
        'node_image_type': _NODE_IMAGE_TYPE.value,
        'boot_disk_iops_target': _BOOT_DISK_IOPS.value,
        'benchmark_machine_type': _BENCHMARK_MACHINE_TYPE.value,
        # Other config
        'zswap_enabled': _ENABLE_ZSWAP.value,
        'min_free_kbytes': _MIN_FREE_KBYTES.value,
        'fio_runtime_sec': _FIO_RUNTIME_SEC.value,
        # Requested config value only.
        'stress_vm_bytes_requested': _STRESS_VM_BYTES.value,
        'stress_vm_bytes_list': _STRESS_VM_BYTES_LIST.value,
        'stress_timeout_sec': _STRESS_TIMEOUT_SEC.value,
        'nodepool': _NODEPOOL.value,
    }
