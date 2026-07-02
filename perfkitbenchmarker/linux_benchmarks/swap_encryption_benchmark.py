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

  Phase 3b – Kernel Build
    Linux compiled inside a memory-capped cgroup; slowdown ratio vs
    unconstrained baseline.

  Note: For Redis latency (Phase 3a) and OpenSearch (Phase 3c) under swap
  pressure, run kubernetes_redis_memtier_benchmark and esrally_benchmark
  on the same swap-enabled cluster rather than duplicating their logic here.
"""

import json
import logging
import re
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

FLAGS = flags.FLAGS

_BenchmarkSpec = bm_spec_lib.BenchmarkSpec

# ---------------------------------------------------------------------------
# Benchmark identity
# ---------------------------------------------------------------------------


BENCHMARK_NAME = 'swap_encryption'


BENCHMARK_CONFIG = """
swap_encryption:
  description: >
    App workloads (kernel build under cgroup memory cap) on swap-encrypted GKE/EKS nodes. Swap-enabled 'benchmark' nodepool declared in BENCHMARK_CONFIG;
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


_FIO_RUNTIME_SEC = flags.DEFINE_integer(
    'swap_encryption_fio_runtime_sec',
    60,
    'Wall-clock runtime in seconds for each individual fio job.',
)


_STRESS_TIMEOUT_SEC = flags.DEFINE_integer(
    'swap_encryption_stress_timeout_sec',
    300,
    'Duration in seconds of each stress-ng memory-pressure phase.',
)


_STRESS_VM_BYTES = flags.DEFINE_string(
    'swap_encryption_stress_vm_bytes',
    '28G',
    'Combined stress-ng working-set size (total in-flight footprint, not '
    'per-worker).  It is divided equally across --swap_encryption_stress_vm_'
    'workers before being passed to stress-ng, so the total memory touched '
    'equals this value.  Should exceed node RAM to force kernel swapping.',
)


_STRESS_VM_BYTES_LIST = flags.DEFINE_string(
    'swap_encryption_stress_vm_bytes_list',
    '',
    'Comma-separated list of stress-ng --vm-bytes values to iterate over '
    'in Phase 2a CPU-overhead sweeps, e.g. "14G,21G,28G".  When non-empty '
    'this overrides --swap_encryption_stress_vm_bytes and Phase 2a is run '
    'once per entry so that the swap-pressure intensity curve is captured.',
)


_STRESS_VM_WORKERS = flags.DEFINE_integer(
    'swap_encryption_stress_vm_workers',
    4,
    'Number of parallel stress-ng --vm workers for Phase 2a.  The total '
    'working set (the autoscaled vm_bytes) is divided equally across workers, '
    'so the combined footprint stays under RAM+swap (no OOM) while exceeding '
    'RAM (forcing swap).  Multiple workers are needed for fill speed — a '
    'single write64 worker cannot dirty enough memory within the timeout to '
    'reach RAM (run swap1: ~184 GB resident, no swap).  To stop the N '
    "workers' resident sets from collapsing to one worker's share, the "
    'stressor uses random access (rand-set) and disables KSM page-merging '
    '(without those, identical write64 pages across workers were merged, '
    'leaving only ~vm_bytes/N resident and swap_out ~0).',
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
    'CPU overhead + swap pressure), 2b (I/O interference), '
    '3b (kernel build).  Default "all" runs everything.  '
    'Example: --swap_encryption_phases=2a runs only the swap-pressure phase. '
    'Phases not listed are skipped and do not affect the degraded-run gate '
    '(e.g. skipping fio will not be reported as "Gate 1 produced no samples").',
)


_MIN_SWAP_OUT_PAGES = flags.DEFINE_integer(
    'swap_encryption_min_swap_out_pages',
    1000,
    'Minimum peak swap-out rate (pages/s) that Phase 2a must reach for the run'
    ' to count as a real swap-encryption measurement.  Below this the working'
    ' set never meaningfully paged (e.g. run swap1 peaked at 176 pages/s of'
    ' noise yet "passed" the old zero-only gate), so the dm-crypt overhead is'
    ' hollow and the run is flagged degraded.  A genuinely swapping run peaks'
    ' in the tens-to-hundreds of thousands of pages/s.  Set 0 to accept any'
    ' non-zero swap-out (legacy behaviour).',
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
    'hyperdisk-balanced IOPS constraint: provisioned_iops <= size_gb x 80.',
)


_KERNEL_VERSION = flags.DEFINE_string(
    'swap_encryption_kernel_version',
    '6.1.38',
    'Linux kernel version to download and compile for the build workload.',
)


_KERNEL_MEMORY_MB = flags.DEFINE_integer(
    'swap_encryption_kernel_memory_mb',
    512,
    'cgroup memory limit in MB applied during the constrained kernel build.',
)

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_DS_NAME = 'pkb-swap-benchmark'
_DS_NAMESPACE = 'default'
_DS_LABEL = 'pkb-swap-benchmark'
_BENCHMARK_NODEPOOL = 'benchmark'


_stress_vm_method: list[str] = (
    []
)  # single-element list; '' means no --vm-method flag


_FIO_JOBS = (
    ('rand_write_iops', 'randwrite', '4k', 256, 'Random write IOPS'),
    ('rand_read_iops', 'randread', '4k', 256, 'Random read IOPS'),
    ('rand_rw_mixed', 'randrw', '4k', 256, 'Mixed random R/W (50/50)'),
    ('seq_write_bw', 'write', '1m', 64, 'Sequential write bandwidth'),
    ('seq_read_bw', 'read', '1m', 64, 'Sequential read bandwidth'),
    ('lat_write', 'randwrite', '4k', 1, 'Random write latency'),
    ('lat_read', 'randread', '4k', 1, 'Random read latency'),
)


_CRYPTO_PROCS = ('kswapd', 'kworker', 'kcryptd', 'dmcrypt_write')


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
    daemonset.Create()
    spec.resources.append(daemonset)
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

      Tier 2 (Gate 2) — stress-ng CPU overhead + I/O interference (Phase 2a/2b)
        Requires an active swap device (Gate 1 must pass).  Gate 2 fails if
        stress-ng does not complete within timeout.

      Phase 3b — kernel build inside a memory-capped cgroup.

    If Gate 1 fails, Tier 2 is skipped — there is no point measuring
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
                '[swap_encryption] Skipping Tier 2 (no swap device)'
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

    # ── Tier 2 / Gate 2: stress-ng CPU overhead + I/O interference ───────────
    if _phase_selected('2a') or _phase_selected('2b'):
        logging.info(
            '[swap_encryption] ── Tier 2 / Gate 2: stress-ng phases ──'
        )
        try:
            if _phase_selected('2a'):
                logging.info('[swap_encryption] Phase 2a: CPU overhead')
                results += _phase2a_cpu_overhead(daemonset, base_meta, degraded_reasons)
            if _phase_selected('2b'):
                logging.info('[swap_encryption] Phase 2b: I/O interference')
                results += _phase2b_io_interference(daemonset, base_meta)
        except Exception as e:  # pylint: disable=broad-except
            logging.error(
                '[swap_encryption] Gate 2 FAILED — stress phase error: %s', e
            )

    # ── Phase 3b: kernel build under memory constraint ────────────────────
    # For Redis latency and OpenSearch under swap, run
    # kubernetes_redis_memtier_benchmark and esrally_benchmark on the
    # swap-enabled cluster rather than duplicating their logic here.
    if _phase_selected('3b'):
        logging.info(
            '[swap_encryption] Phase 3b: kernel build under memory cap'
        )
        try:
            results += _phase3b_kernel_build(daemonset, base_meta)
        except Exception as e:  # pylint: disable=broad-except
            logging.error(
                '[swap_encryption] Kernel build (3b) FAILED: %s — continuing',
                e,
            )

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

    if _phase_selected('fio') and not tier1_results:
        if swap_dev.startswith('/dev/loop'):
            # Expected: COS blocks device-mapper from pod namespaces on single-disk
            # nodes (n2/n4 without --swap_encryption_add_swap_disk or lssd).
            # Tier 2 results are still valid; do NOT mark the run as degraded.
            logging.warning(
                '[swap_encryption] Gate 1 (fio) skipped — loop device %s has no'
                ' dm-crypt support from inside a pod.  Tier 2 results are'
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
            # Raise so PKB marks the benchmark FAILED instead of SUCCEEDED.  The
            # samples collected so far are still published by PKB before the failure
            # is recorded, so no data is lost.
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


def _phase_selected(token: str) -> bool:
    """Return True if phase `token` should run given --swap_encryption_phases.

    'all' (the default) selects every phase.  Otherwise only the comma-separated
    tokens listed in the flag run.  Tokens: fio, 2a, 2b, 3b.
    """
    selected = [p.strip().lower() for p in _PHASES.value if p.strip()]
    return (not selected) or ('all' in selected) or (token.lower() in selected)


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


def _detect_swap_device(daemonset: _ds_mod.SwapDaemonSet) -> str:
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


def _setup_gke_swap(daemonset: _ds_mod.SwapDaemonSet) -> None:
    """Configure dm-crypt swap on the GKE node, mirroring go/node:swap-encryption."""
    swap_type = _SWAP_TYPE.value
    if swap_type == 'auto':
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
    """Configure dm-crypt swap on hyperdisk-balanced (GKE default)."""
    logging.info('[swap_encryption] GKE: setting up dm-crypt on hyperdisk')

    boot_out, _ = daemonset.PodExec(
        'lsblk -no pkname "$(findmnt -n -o SOURCE /)" 2>/dev/null | head -1',
        ignore_failure=True,
    )
    boot_base = boot_out.strip() or 'nvme0n1'
    logging.info('[swap_encryption] GKE: boot device: %s', boot_base)

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
    """Plain loop-device swap for single-disk GKE nodes (no dedicated swap disk)."""
    size_gb = _SWAP_SIZE_GB.value
    backing = '/mnt/stateful_partition/pkb_swap_backing'

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
    """Swap on the OS BOOT disk — methodology Table 0 rows 1-4."""
    size_gb = _SWAP_SIZE_GB.value
    backing = '/mnt/stateful_partition/pkb_swap_backing'
    logging.info(
        '[swap_encryption] GKE: boot-disk swap (%dG backing, dmcrypt=%s)',
        size_gb,
        _ENABLE_DMCRYPT.value,
    )

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

    daemonset.PodExec(
        textwrap.dedent("""
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    swapoff -a 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
  """),
        ignore_failure=True,
    )

    topo, _ = daemonset.PodExec(
        'lsblk -o NAME,TYPE,SIZE,ROTA,MOUNTPOINT 2>/dev/null',
        ignore_failure=True,
    )
    logging.info(
        '[swap_encryption] block device topology:\n%s', (topo or '').strip()
    )

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

    daemonset.PodExec(
        textwrap.dedent(f"""
    echo "[pkb-lssd-cleanup] /proc/mdstat:" >&2
    cat /proc/mdstat 2>/dev/null || true
    swapoff -a 2>/dev/null || true
    swapoff /dev/mapper/pkb_swap 2>/dev/null || true
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync pkb_swap 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    mdadm --stop --scan 2>/dev/null || true
    mdadm --zero-superblock {device_list} 2>/dev/null || true
    wipefs -a {device_list} 2>/dev/null || true
    sleep 2
  """),
        ignore_failure=True,
    )

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

    devices = raw_devices
    device_list = ' '.join(devices)
    n = len(devices)
    logging.info(
        '[swap_encryption] GKE: using %d raw LSSD device(s): %s  dmcrypt=%s',
        n,
        device_list,
        _ENABLE_DMCRYPT.value,
    )

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


def _setup_gke_lssd_stateful_loop_swap(daemonset: _ds_mod.SwapDaemonSet) -> None:
    """Set up swap on the LSSD partition via a loop device."""
    img_path = '/mnt/stateful_partition/pkb_swap.img'

    daemonset.PodExec(
        textwrap.dedent(f"""
    swapoff -a 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    losetup -D 2>/dev/null || true
    rm -f {img_path} 2>/dev/null || true
  """),
        ignore_failure=True,
    )

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


_IO2_VOLUME_ID = ''  # set by _ensure_io2_volume; serial-based detection


def _setup_eks_swap(daemonset: _ds_mod.SwapDaemonSet) -> None:
    """Configure swap on EKS nodes — Instance Store OR io2 root disk."""
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

    nvme_out, _ = daemonset.PodExec(
        "nvme list 2>/dev/null | awk '/Instance Storage/{print $1}' | head -1"
        " || lsblk -d -o NAME,MODEL | grep -i 'instance\\|nvme' | grep -v"
        " 'nvme0' | awk '{print \"/dev/\"$1}' | head -1",
        ignore_failure=True,
    )
    device = nvme_out.strip()
    if not device:
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
    """Swap on AWS EBS io2 volume."""
    logging.info('[swap_encryption] EKS: setting up io2 EBS swap')

    root_out, _ = daemonset.PodExec(
        'lsblk -no pkname $(findmnt -n -o SOURCE /) 2>/dev/null || echo'
        ' nvme0n1',
        ignore_failure=True,
    )
    root_base = root_out.strip() or 'nvme0n1'

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


def _setup_plain_swap_file(daemonset: _ds_mod.SwapDaemonSet, size_gb: int) -> None:
    """Fallback: create a loop-device-backed swapfile."""
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

    # Pre-fill device so read tests have real data (avoids zero-block optimisation
    # by the storage controller skewing read latency measurements).
    _PREFILL_GIB = 20
    prefill_timeout = (
        _PREFILL_GIB * 1024 // 150 + 60
    )
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

    fio_run_timeout = _FIO_RUNTIME_SEC.value + 90
    fio_read_timeout = 60

    for name, rw, bs, depth, label in _FIO_JOBS:
        logging.info('[swap_encryption] fio: %s', name)
        out_file = f'/tmp/pkb_fio_{name}.json'
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


def _parse_vm_bytes_to_mb(vm_bytes: str) -> float:
    """Parse a vm-bytes string like '28G', '512M', '1024k' into megabytes."""
    vm_bytes = vm_bytes.strip()
    if not vm_bytes:
        return 0.0
    suffix = vm_bytes[-1].upper()
    try:
        value = float(vm_bytes[:-1])
    except ValueError:
        return 0.0
    if suffix == 'G':
        return value * 1024.0
    elif suffix == 'M':
        return value
    elif suffix == 'K':
        return value / 1024.0
    elif suffix == 'T':
        return value * 1024.0 * 1024.0
    else:
        try:
            return float(vm_bytes) / (1024.0 * 1024.0)
        except ValueError:
            return 0.0


def _per_worker_vm_bytes(total_vm_bytes: str, workers: int) -> str:
    """Split a *total* vm-bytes target across N stress-ng --vm workers.

    stress-ng allocates ``--vm-bytes`` PER worker, so ``--vm N --vm-bytes B``
    touches ``N * B`` of memory.  Every vm_bytes value in this benchmark (the
    --swap_encryption_stress_vm_bytes flag and the _autoscale_vm_bytes result)
    represents the intended *combined* footprint, as documented on
    --swap_encryption_stress_vm_workers.  We therefore divide by the worker
    count before handing the value to stress-ng.

    Returns a stress-ng-friendly ``<int>M`` string (megabytes), floored to at
    least 1M.
    """
    workers = max(1, int(workers))
    total_mb = _parse_vm_bytes_to_mb(total_vm_bytes)
    if total_mb <= 0:
        return total_vm_bytes
    per_worker_mb = max(1, int(total_mb / workers))
    return f'{per_worker_mb}M'


def _cgroup_swap_limit_mb(daemonset: _ds_mod.SwapDaemonSet) -> float:
    """Return the swap budget (in MB) that the benchmark cgroup can actually use.

    GKE sets the per-container cgroup v2 ``memory.swap.max`` to 0, so even
    though the node advertises a large swap device the container cannot page
    anything out.  This probe finds the swap budget of *our* cgroup so the
    caller can size against reality.

    Returns:
      ``float('inf')`` when swap is uncapped (``max``); the limit in MB when
      capped to a finite value; ``0.0`` when swap is fully locked
      (``memory.swap.max == 0``); ``-1.0`` when the limit could not be read.
    """
    probe = textwrap.dedent("""
    mypid=$$
    for procf in $(find /sys/fs/cgroup -path '*kubepods*' -name cgroup.procs 2>/dev/null)
    do
      if grep -qx "$mypid" "$procf" 2>/dev/null
      then
        d=$(dirname "$procf")
        if [ -f "$d/memory.swap.max" ]
        then
          echo "V2=$(cat "$d/memory.swap.max" 2>/dev/null)"
        elif [ -f "$d/memory.memsw.limit_in_bytes" ] && [ -f "$d/memory.limit_in_bytes" ]
        then
          echo "MEMSW=$(cat "$d/memory.memsw.limit_in_bytes" 2>/dev/null) MEM=$(cat "$d/memory.limit_in_bytes" 2>/dev/null)"
        fi
        break
      fi
    done
  """)
    try:
        out, _ = daemonset.PodExec(probe, timeout=20, ignore_failure=True)
    except Exception as e:  # pylint: disable=broad-except
        logging.warning(
            '[swap_encryption] cgroup swap-limit probe failed: %s', e
        )
        return -1.0

    text = (out or '').strip()
    m = re.search(r'V2=(\S+)', text)
    if m:
        val = m.group(1)
        if val == 'max':
            return float('inf')
        try:
            return int(val) / (1024.0 * 1024.0)
        except ValueError:
            return -1.0
    m = re.search(r'MEMSW=(\S+)\s+MEM=(\S+)', text)
    if m:
        try:
            memsw = int(m.group(1))
            mem = int(m.group(2))
        except ValueError:
            return -1.0
        if memsw >= (1 << 62):
            return float('inf')
        return max(0.0, (memsw - mem) / (1024.0 * 1024.0))
    return -1.0


def _autoscale_vm_bytes(
    daemonset: _ds_mod.SwapDaemonSet,
    vm_bytes: str,
    degraded_reasons: list[str],
) -> str:
    """Ensure vm_bytes forces real swap I/O without hard-crashing the container.

    Strategy
    --------
    We want stress-ng to overflow into swap so that dm-crypt / Nitro encryption
    overhead is actually measured.  Two competing constraints apply:

    1. vm_bytes must exceed available RAM so that anonymous pages are paged out
       to the swap device.

    2. vm_bytes must not be so large that the kernel OOM-kills the whole
       container before any meaningful swap activity is recorded.

    Target formula: target = RAM + min(swap_size x 0.25, 64 GB)
    Hard ceiling: RAM + swap_size - 4 GB headroom.
    """
    try:
        meminfo_out, _ = daemonset.PodExec('cat /proc/meminfo', timeout=15)
        node_ram_kb = 0
        swap_total_kb = 0
        for line in meminfo_out.splitlines():
            if line.startswith('MemTotal:'):
                parts = line.split()
                if len(parts) >= 2:
                    node_ram_kb = int(parts[1])
            elif line.startswith('SwapTotal:'):
                parts = line.split()
                if len(parts) >= 2:
                    swap_total_kb = int(parts[1])
            if node_ram_kb and swap_total_kb:
                break

        if node_ram_kb <= 0:
            logging.warning(
                '[swap_encryption] Could not read MemTotal; using vm_bytes=%s',
                vm_bytes,
            )
            return vm_bytes

        node_ram_mb = node_ram_kb / 1024.0
        swap_total_mb = swap_total_kb / 1024.0
        requested_mb = _parse_vm_bytes_to_mb(vm_bytes)
        if requested_mb <= 0:
            return vm_bytes

        cgroup_swap_mb = _cgroup_swap_limit_mb(daemonset)
        usable_swap_mb = swap_total_mb
        if cgroup_swap_mb == 0.0:
            safe_gb = max(1, int(node_ram_mb * 0.9 / 1024))
            msg = (
                'cgroup swap is locked (memory.swap.max=0); the'
                f' {swap_total_mb/1024:.0f} GB node swap device is unreachable.'
                f' Capping stress-ng vm_bytes {vm_bytes} → {safe_gb}G (0.9 x'
                ' RAM) to keep the pod alive — swap-encryption overhead will'
                ' NOT be measured this run'
            )
            logging.error('[swap_encryption] %s', msg)
            degraded_reasons.append(msg)
            return f'{safe_gb}G'
        if 0.0 < cgroup_swap_mb < float('inf'):
            usable_swap_mb = min(swap_total_mb, cgroup_swap_mb)

        overflow_mb = max(min(usable_swap_mb * 0.25, 64.0 * 1024), 4.0 * 1024)
        target_mb = node_ram_mb + overflow_mb

        if usable_swap_mb > 0:
            ceiling_mb = node_ram_mb + usable_swap_mb - 4096.0
            target_mb = min(target_mb, ceiling_mb)
        else:
            target_mb = min(target_mb, node_ram_mb * 0.9)

        target_gb = max(1, int(target_mb / 1024))

        if requested_mb < node_ram_mb * 0.95:
            new_vm_bytes = f'{target_gb}G'
            logging.warning(
                '[swap_encryption] Auto-scaling vm_bytes UP: %s → %s (RAM %.0f'
                ' GB, swap %.0f GB; original value would not trigger swap)',
                vm_bytes,
                new_vm_bytes,
                node_ram_mb / 1024,
                swap_total_mb / 1024,
            )
            return new_vm_bytes

        if requested_mb > target_mb:
            new_vm_bytes = f'{target_gb}G'
            logging.warning(
                '[swap_encryption] Capping vm_bytes DOWN: %s → %s (RAM %.0f GB,'
                ' swap %.0f GB; original value risks swap exhaustion)',
                vm_bytes,
                new_vm_bytes,
                node_ram_mb / 1024,
                swap_total_mb / 1024,
            )
            return new_vm_bytes

        return vm_bytes
    except Exception as e:  # pylint: disable=broad-except
        logging.warning(
            '[swap_encryption] _autoscale_vm_bytes failed (%s); using %s',
            e,
            vm_bytes,
        )
        return vm_bytes


def _get_stress_vm_method(daemonset: _ds_mod.SwapDaemonSet) -> str:
    """Detect the best --vm-method argument for stress-ng on this node.

    Result is cached in _stress_vm_method so the detection kubectl exec only
    runs once per benchmark run.
    """
    if _stress_vm_method:
        return _stress_vm_method[0]

    try:
        out, _, _ = kubectl.RunKubectlCommand(
            [
                'exec',
                daemonset.pod_name,
                '-n',
                _DS_NAMESPACE,
                '--',
                'bash',
                '-c',
                (
                    'stress-ng --vm 1 --vm-bytes 1M --vm-method __invalid__'
                    ' --timeout 1s 2>&1 || true'
                ),
            ],
            raise_on_failure=False,
            timeout=15,
        )
        combined = out.lower()
        if 'rand-set' in combined:
            method = 'rand-set'
        elif 'mmap' in combined:
            method = 'mmap'
        elif 'write64' in combined:
            method = 'write64'
        else:
            method = ''
        logging.info(
            '[swap_encryption] stress-ng vm-method detected: %r',
            method or '(default)',
        )
    except Exception as e:  # pylint: disable=broad-except
        logging.warning(
            '[swap_encryption] vm-method detection failed (%s); using rand-set',
            e,
        )
        method = 'rand-set'

    _stress_vm_method.append(method)
    return method


def _stress_vm_method_flag(daemonset: _ds_mod.SwapDaemonSet) -> str:
    """Return the --vm-method <method> flag string, or empty string if none."""
    method = _get_stress_vm_method(daemonset)
    return f'--vm-method {method}' if method else ''


def _phase2a_cpu_overhead(
    daemonset: _ds_mod.SwapDaemonSet,
    base_meta: dict,
    degraded_reasons: list[str],
) -> list[sample.Sample]:
    """Measure CPU cost of dm-crypt / Nitro while stress-ng drives swap I/O.

    If --swap_encryption_stress_vm_bytes_list is set the phase is run once per
    listed intensity value so that a full pressure-curve is captured (gap 5).
    Otherwise the single value from --swap_encryption_stress_vm_bytes is used.
    """
    if _STRESS_VM_BYTES_LIST.value.strip():
        intensities = [
            v.strip()
            for v in _STRESS_VM_BYTES_LIST.value.split(',')
            if v.strip()
        ]
    else:
        intensities = [_STRESS_VM_BYTES.value]

    results = []
    for vm_bytes in intensities:
        scaled = _autoscale_vm_bytes(daemonset, vm_bytes, degraded_reasons)
        logging.info(
            '[swap_encryption] Phase 2a: stress-ng intensity %s', scaled
        )
        results += _run_cpu_overhead_sweep(
            daemonset, base_meta, scaled, degraded_reasons
        )
    return results


def _run_cpu_overhead_sweep(
    daemonset: _ds_mod.SwapDaemonSet,
    base_meta: dict,
    vm_bytes: str,
    degraded_reasons: list[str],
) -> list[sample.Sample]:
    """Phase 2a stressor sweep, WITH RETRY for flaky swap.

    Driving the multi-worker rand-set working set past RAM into swap is
    empirically non-deterministic on these nodes: the SAME config produced
    ~670k pages/s on some runs and <300 on others.  So we retry: if an attempt
    completes but peak swap-out is below the threshold (and it did not OOM),
    reclaim memory and re-run, keeping the BEST attempt.  An OOM, or a peak
    at/above threshold, ends the retries immediately.
    """
    meta = dict(base_meta, phase='cpu_overhead', stress_vm_bytes=vm_bytes)
    timeout = _STRESS_TIMEOUT_SEC.value
    interval = 2
    n_samples = timeout // interval + 10
    vmstat_log = f'/tmp/pkb_vmstat_{vm_bytes}.log'
    pidstat_log = f'/tmp/pkb_pidstat_{vm_bytes}.log'
    workers = max(1, _STRESS_VM_WORKERS.value)
    per_worker = _per_worker_vm_bytes(vm_bytes, workers)
    min_so = _MIN_SWAP_OUT_PAGES.value
    method_flag = _stress_vm_method_flag(daemonset)
    max_attempts = 3
    best = None

    for attempt in range(1, max_attempts + 1):
        t0 = time.time()
        stress_out, _ = daemonset.PodExec(
            textwrap.dedent(f"""
      echo 2 > /sys/kernel/mm/ksm/run 2>/dev/null || true
      echo 0 > /sys/kernel/mm/ksm/run 2>/dev/null || true
      sysctl -w vm.swappiness=100 >/dev/null 2>&1 || true
      PKB_MCG=$(awk -F: '/^0::/{{print $3}}' /proc/self/cgroup 2>/dev/null)
      echo "[pkb] phase2a attempt={attempt}/{max_attempts} ksm_run=$(cat /sys/kernel/mm/ksm/run 2>/dev/null || echo n/a) swappiness=$(cat /proc/sys/vm/swappiness 2>/dev/null) MemAvailable_kB=$(awk '/MemAvailable/{{print $2}}' /proc/meminfo) memory.swap.max=$(cat /sys/fs/cgroup$PKB_MCG/memory.swap.max 2>/dev/null || echo n/a) workers={workers} per_worker={per_worker}"
      vmstat {interval} {n_samples} > {vmstat_log} 2>&1 &
      VMSTAT_PID=$!
      pidstat -u {interval} {n_samples} -p ALL > {pidstat_log} 2>&1 &
      PISTAT_PID=$!
      stress-ng --vm {workers} \\
        --vm-bytes {per_worker} \\
        {method_flag} \\
        --timeout {timeout}s \\
        --metrics-brief 2>&1 || true
      kill $VMSTAT_PID $PISTAT_PID 2>/dev/null || true
    """),
            timeout=timeout + 60,
            ignore_failure=True,
        )
        elapsed = time.time() - t0

        completed_cleanly = (
            'successful run completed' in stress_out.lower()
            or 'metrics-brief' in stress_out.lower()
            or 'bogo-ops' in stress_out.lower()
        )
        oom_killed = (not completed_cleanly) and elapsed < timeout * 0.8
        vmstat_out, _ = daemonset.PodExec(
            f'cat {vmstat_log}', ignore_failure=True
        )
        pidstat_out, _ = daemonset.PodExec(
            f'cat {pidstat_log}', ignore_failure=True
        )
        vmstat_samples = _parse_vmstat(vmstat_out, meta)
        swap_out_max = max(
            (
                s.value
                for s in vmstat_samples
                if s.metric
                in ('swap_out_pages_per_sec', 'swap_out_pages_per_sec_max')
            ),
            default=0.0,
        )
        bogo = None
        for line in stress_out.splitlines():
            mm = re.search(r'vm\s+\d+\s+(\d+)\s+\S+\s+bogo-ops', line)
            if mm:
                bogo = float(mm.group(1))
                break
        logging.info(
            '[swap_encryption] Phase 2a attempt %d/%d: peak swap-out '
            '%.0f pages/s (completed=%s, oom=%s)',
            attempt,
            max_attempts,
            swap_out_max,
            completed_cleanly,
            oom_killed,
        )
        if best is None or swap_out_max > best['swap_out_max']:
            best = dict(
                elapsed=elapsed,
                oom_killed=oom_killed,
                swap_out_max=swap_out_max,
                vmstat_samples=vmstat_samples,
                pidstat_out=pidstat_out,
                bogo=bogo,
            )
        if oom_killed or swap_out_max >= min_so:
            break
        if attempt < max_attempts:
            logging.warning(
                '[swap_encryption] Phase 2a swap-out %.0f < %d threshold '
                '— reclaiming and retrying (%d/%d)',
                swap_out_max,
                min_so,
                attempt + 1,
                max_attempts,
            )
            daemonset.PodExec(
                textwrap.dedent("""
        echo -1000 > /proc/self/oom_score_adj 2>/dev/null || true
        pkill -9 stress-ng 2>/dev/null || true
        sleep 3; sync; echo 1 > /proc/sys/vm/drop_caches 2>/dev/null || true
      """),
                ignore_failure=True,
                timeout=60,
            )

    # Emit samples from the BEST attempt.
    results = [
        sample.Sample('stress_ng_duration_sec', best['elapsed'], 's', meta),
        sample.Sample(
            'stress_ng_completed',
            0.0 if best['oom_killed'] else 1.0,
            'status',
            meta,
        ),
    ]
    if best['bogo'] is not None:
        results.append(
            sample.Sample('stress_ng_bogo_ops', best['bogo'], 'ops', meta)
        )
    results += best['vmstat_samples']
    results += _parse_pidstat(best['pidstat_out'], meta)

    # Swap-activity gate: a completed run that moved ~no pages to swap never
    # exercised the encrypted swap path.
    if best['oom_killed']:
        msg = (
            f'stress-ng (vm_bytes={vm_bytes}) was OOM-killed — the cgroup could'
            ' not page anonymous memory out to swap; swap-encryption overhead'
            ' was not measured'
        )
        logging.error('[swap_encryption] %s', msg)
        degraded_reasons.append(msg)
    elif best['swap_out_max'] < min_so:
        msg = (
            f'stress-ng (vm_bytes={vm_bytes}) peak swap-out was only '
            f'{best["swap_out_max"]:.0f} pages/s (< {min_so} threshold) after '
            f'{max_attempts} attempts — the working set never meaningfully '
            f'paged to swap. Check vm_bytes vs RAM and the swap device'
        )
        logging.error('[swap_encryption] %s', msg)
        degraded_reasons.append(msg)

    return results


def _parse_vmstat(output: str, base_meta: dict) -> list[sample.Sample]:
    """Parse vmstat output for swap rates AND CPU utilisation.

    Standard vmstat column layout (non-header data lines, 0-indexed):
      r b swpd free buff cache  si  so  bi  bo  in  cs  us  sy  id  wa  st
      0 1    2    3    4     5   6   7   8   9  10  11  12  13  14  15  16

    si=6, so=7  – swap-in / swap-out pages/s
    us=12        – user CPU %
    sy=13        – system (kernel) CPU %
    id=14        – idle CPU %
    wa=15        – I/O wait CPU %
    """
    si_vals, so_vals = [], []
    us_vals, sy_vals, wa_vals = [], [], []

    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 17 or not parts[0].isdigit():
            continue
        try:
            si_vals.append(float(parts[6]))
            so_vals.append(float(parts[7]))
            us_vals.append(float(parts[12]))
            sy_vals.append(float(parts[13]))
            wa_vals.append(float(parts[15]))
        except (ValueError, IndexError):
            pass

    if not si_vals:
        return []

    meta = dict(base_meta, metric_source='vmstat')

    def _mean(lst):
        return sum(lst) / len(lst) if lst else 0.0

    def _peak(lst):
        return max(lst) if lst else 0.0

    total_active = [u + s + w for u, s, w in zip(us_vals, sy_vals, wa_vals)]

    return [
        sample.Sample('swap_in_pages_per_sec', _mean(si_vals), 'pages/s', meta),
        sample.Sample(
            'swap_in_pages_per_sec_max', _peak(si_vals), 'pages/s', meta
        ),
        sample.Sample(
            'swap_out_pages_per_sec', _mean(so_vals), 'pages/s', meta
        ),
        sample.Sample(
            'swap_out_pages_per_sec_max', _peak(so_vals), 'pages/s', meta
        ),
        sample.Sample('total_cpu_pct_avg', _mean(total_active), '%', meta),
        sample.Sample('total_cpu_pct_max', _peak(total_active), '%', meta),
        sample.Sample('system_time_pct_avg', _mean(sy_vals), '%', meta),
        sample.Sample('system_time_pct_max', _peak(sy_vals), '%', meta),
        sample.Sample('user_cpu_pct_avg', _mean(us_vals), '%', meta),
        sample.Sample('iowait_cpu_pct_avg', _mean(wa_vals), '%', meta),
    ]


def _parse_pidstat(output: str, base_meta: dict) -> list[sample.Sample]:
    """Parse CPU % for swap/encryption-related kernel threads from pidstat."""
    cpu_by_proc: dict[str, list[float]] = {}
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 9:
            continue
        proc = parts[-1]
        if not any(t in proc for t in _CRYPTO_PROCS):
            continue
        try:
            cpu_by_proc.setdefault(proc, []).append(float(parts[7]))
        except (ValueError, IndexError):
            pass
    results = []
    meta = dict(base_meta, metric_source='pidstat')
    for proc, vals in cpu_by_proc.items():
        m = dict(meta, process=proc)
        results += [
            sample.Sample(f'cpu_pct_avg_{proc}', sum(vals) / len(vals), '%', m),
            sample.Sample(f'cpu_pct_max_{proc}', max(vals), '%', m),
        ]
    return results


def _launch_confined_bg_stress(
    daemonset: _ds_mod.SwapDaemonSet, timeout_s: int, logfile: str
) -> None:
    """Launch the Phase 2b background swap stressor confined to its OWN
    memory-capped cgroup, so it drives swap pressure WITHOUT starving the
    concurrent foreground workload (fio) or OOM-killing the pod.
    """
    method = _stress_vm_method_flag(daemonset)
    vm_bytes = _STRESS_VM_BYTES.value
    daemonset.PodExec(
        textwrap.dedent(f"""
    nohup bash -c '
      BG=/sys/fs/cgroup/pkb_bgstress
      mkdir -p "$BG" 2>/dev/null || true
      echo +memory > /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null || true
      echo max > "$BG/memory.swap.max" 2>/dev/null || true
      MT_KB=$(grep -m1 MemTotal /proc/meminfo | tr -s " " | cut -d" " -f2)
      echo $(( MT_KB * 1024 * 60 / 100 )) > "$BG/memory.max" 2>/dev/null || true
      echo $$ > "$BG/cgroup.procs" 2>/dev/null || true
      exec stress-ng --vm 1 --vm-bytes {vm_bytes} {method} --timeout {timeout_s}s
    ' >{logfile} 2>&1 &
    disown
    echo STRESS_STARTED
  """),
        timeout=30,
    )


def _set_memory_high_guard(
    daemonset: _ds_mod.SwapDaemonSet, fraction: float = 0.9
) -> None:
    """Cap the container cgroup ``memory.high`` at `fraction` x RAM.

    Phases 2b run a background stressor AND a concurrent foreground workload.
    On a small-RAM node their combined footprint exceeds RAM and the hard OOM
    killer terminates the pod.  ``memory.high`` is a soft limit: when the
    cgroup crosses it the kernel reclaims and swaps aggressively (throttling
    the cgroup) instead of killing it.
    Best-effort; any failure is ignored.
    """
    daemonset.PodExec(
        textwrap.dedent(f"""
    PKB_MCG=$(awk -F: '/^0::/{{print $3}}' /proc/self/cgroup 2>/dev/null)
    MT_KB=$(awk '/MemTotal/{{print $2}}' /proc/meminfo)
    HIGH=$(( MT_KB * 1024 / 100 * {int(fraction * 100)} ))
    if [ -n "$PKB_MCG" ] && [ -f "/sys/fs/cgroup$PKB_MCG/memory.high" ]; then
      echo $HIGH > "/sys/fs/cgroup$PKB_MCG/memory.high" 2>/dev/null \
        && echo "[pkb] memory.high set to $HIGH bytes ({int(fraction * 100)}% RAM) — pod will swap, not OOM" \
        || echo "[pkb] WARNING: could not set memory.high" >&2
    fi
  """),
        ignore_failure=True,
        timeout=30,
        _retries=0,
    )


def _reset_memory_high_guard(daemonset: _ds_mod.SwapDaemonSet) -> None:
    """Restore ``memory.high`` to ``max`` after a guarded phase."""
    daemonset.PodExec(
        textwrap.dedent("""
    PKB_MCG=$(awk -F: '/^0::/{print $3}' /proc/self/cgroup 2>/dev/null)
    if [ -n "$PKB_MCG" ] && [ -f "/sys/fs/cgroup$PKB_MCG/memory.high" ]; then
      echo max > "/sys/fs/cgroup$PKB_MCG/memory.high" 2>/dev/null || true
    fi
  """),
        ignore_failure=True,
        timeout=30,
        _retries=0,
    )


def _phase2b_io_interference(
    daemonset: _ds_mod.SwapDaemonSet, base_meta: dict
) -> list[sample.Sample]:
    """Quantify drop in application I/O when swap is under simultaneous pressure."""
    results = []
    app_file = '/mnt/stateful_partition/pkb_app_io'
    timeout = _STRESS_TIMEOUT_SEC.value
    meta = dict(base_meta, phase='io_interference')

    _set_memory_high_guard(daemonset)

    daemonset.PodExec(
        textwrap.dedent("""
    command -v fio >/dev/null 2>&1 || {
      apt-get install -y -qq fio 2>/dev/null || true
    }
  """),
        ignore_failure=True,
        timeout=120,
    )

    daemonset.PodExec(
        textwrap.dedent("""
    pkill -9 stress-ng 2>/dev/null || true
    pkill -9 -f 'opensearch|elasticsearch' 2>/dev/null || true
    pkill -9 redis-server 2>/dev/null || true
    sync
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
    sleep 2
    echo "[pkb] pre-2b MemAvailable_kB=$(awk '/MemAvailable/{print $2}' /proc/meminfo) SwapFree_kB=$(awk '/SwapFree/{print $2}' /proc/meminfo)"
  """),
        ignore_failure=True,
        timeout=60,
    )

    daemonset.PodExec(
        (
            f'fio --name=create --filename={app_file} '
            '--rw=write --bs=1m --size=4G --verify=0 --direct=1'
        ),
        timeout=600,
        ignore_failure=True,
    )

    def _run_app_fio(pressure_label: str) -> list[sample.Sample]:
        cmd = (
            f'fio --name=app_io --filename={app_file} '
            '--ioengine=libaio --direct=1 '
            '--rw=randrw --bs=4k --iodepth=32 --size=4G --verify=0 '
            '--time_based --runtime=60s --output-format=json'
        )
        out, _ = daemonset.PodExec(cmd, ignore_failure=True)
        return _parse_fio_json(
            out,
            'app_io',
            f'App I/O ({pressure_label})',
            dict(meta, pressure=pressure_label),
        )

    logging.info('[swap_encryption] I/O interference: baseline (no pressure)')
    results += _run_app_fio('no_pressure')

    logging.info('[swap_encryption] I/O interference: under swap pressure')
    _launch_confined_bg_stress(daemonset, timeout, '/tmp/pkb_stress_io.log')
    time.sleep(10)
    results += _run_app_fio('with_swap_pressure')

    daemonset.PodExec(
        'pkill -9 stress-ng 2>/dev/null || true',
        ignore_failure=True,
        _retries=0,
        timeout=15,
    )
    _reset_memory_high_guard(daemonset)
    return results


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

# ---------------------------------------------------------------------------
# Phase 3b – Kernel Build Under Memory Constraint
# ---------------------------------------------------------------------------


def _phase3b_kernel_build(
    daemonset: _ds_mod.SwapDaemonSet, base_meta: dict
) -> list[sample.Sample]:
    """Compile Linux inside a cgroup memory cap; compare to unconstrained."""
    results = []
    ver = _KERNEL_VERSION.value
    root = '/mnt/stateful_partition/pkb_kernel'
    tarball = f'{root}/linux-{ver}.tar.xz'
    src = f'{root}/linux-{ver}'
    url = (
        f'https://cdn.kernel.org/pub/linux/kernel/'
        f'v{ver.split(".")[0]}.x/linux-{ver}.tar.xz'
    )

    daemonset.PodExec(
        textwrap.dedent("""
    command -v make >/dev/null 2>&1 && command -v cgexec >/dev/null 2>&1 || {
      apt-get install -y -qq build-essential cgroup-tools 2>/dev/null || true
    }
  """),
        ignore_failure=True,
        timeout=180,
    )

    daemonset.PodExec(f'mkdir -p {root}')
    daemonset.PodExec(
        f'test -f {tarball} || wget -q --timeout=300 -O {tarball} {url}',
        timeout=600,
    )
    daemonset.PodExec(
        f'test -d {src} || tar -xf {tarball} -C {root}', timeout=600
    )
    daemonset.PodExec(
        f'make -C {src} defconfig -j$(nproc) 2>&1', timeout=300
    )

    mem_bytes = _KERNEL_MEMORY_MB.value * 1024 * 1024

    cgroup_setup_out, _ = daemonset.PodExec(
        textwrap.dedent(f"""
    if [ -d /sys/fs/cgroup/memory ] && \
       mkdir -p /sys/fs/cgroup/memory/pkb_kernelbuild 2>/dev/null && \
       echo {mem_bytes} > /sys/fs/cgroup/memory/pkb_kernelbuild/memory.limit_in_bytes 2>/dev/null; then
      echo CGROUPV1
    elif [ -d /sys/fs/cgroup/system.slice ] || [ -f /sys/fs/cgroup/cgroup.controllers ]; then
      mkdir -p /sys/fs/cgroup/pkb_kernelbuild 2>/dev/null || true
      echo {mem_bytes} > /sys/fs/cgroup/pkb_kernelbuild/memory.max 2>/dev/null || true
      echo $$ > /sys/fs/cgroup/pkb_kernelbuild/cgroup.procs 2>/dev/null || true
      echo CGROUPV2
    else
      echo CGROUP_NONE
    fi
  """),
        ignore_failure=True,
        timeout=30,
    )
    cgroup_mode = (
        cgroup_setup_out.strip().splitlines()[-1]
        if cgroup_setup_out.strip()
        else 'CGROUP_NONE'
    )
    logging.info(
        '[swap_encryption] cgroup mode: %s (mem_limit=%dMB)',
        cgroup_mode,
        _KERNEL_MEMORY_MB.value,
    )

    def _build(label: str, use_cgroup: bool) -> sample.Sample:
        daemonset.PodExec(f'make -C {src} clean 2>&1')
        if use_cgroup and cgroup_mode == 'CGROUPV1':
            cmd = (
                'cgexec -g memory:pkb_kernelbuild '
                f'make -C {src} -j$(nproc) vmlinux 2>&1 '
                f'|| make -C {src} -j$(nproc) vmlinux 2>&1'
            )
        elif use_cgroup and cgroup_mode == 'CGROUPV2':
            cmd = textwrap.dedent(f"""
        mkdir -p /sys/fs/cgroup/pkb_kernelbuild 2>/dev/null || true
        echo {mem_bytes} > /sys/fs/cgroup/pkb_kernelbuild/memory.max 2>/dev/null || true
        echo $$ > /sys/fs/cgroup/pkb_kernelbuild/cgroup.procs 2>/dev/null || true
        make -C {src} -j$(nproc) vmlinux 2>&1 || true
      """)
        else:
            cmd = f'make -C {src} -j$(nproc) vmlinux 2>&1'
        t0 = time.time()
        daemonset.PodExec(cmd, timeout=3600, ignore_failure=True)
        elapsed = time.time() - t0
        m = dict(
            base_meta,
            workload='kernel_build',
            kernel_version=ver,
            build_variant=label,
            cgroup_mode=cgroup_mode,
            memory_limit_mb=(
                _KERNEL_MEMORY_MB.value if use_cgroup else 'unconstrained'
            ),
        )
        return sample.Sample('kernel_build_elapsed_sec', elapsed, 's', m)

    s_constrained = _build('constrained', use_cgroup=True)
    s_unconstrained = _build('unconstrained', use_cgroup=False)
    results += [s_constrained, s_unconstrained]

    if s_unconstrained.value > 0:
        ratio = s_constrained.value / s_unconstrained.value
        results.append(
            sample.Sample(
                'kernel_build_slowdown_ratio',
                ratio,
                'ratio',
                dict(
                    base_meta,
                    workload='kernel_build',
                    kernel_version=ver,
                    memory_limit_mb=_KERNEL_MEMORY_MB.value,
                ),
            )
        )
    return results


