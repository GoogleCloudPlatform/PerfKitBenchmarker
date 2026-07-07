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
  EKS nodes  ── NVMe Instance Store, Nitro hardware-offloaded encryption.

== Phases ==

  Tier 1 (Gate 1) — fio microbenchmarks (Phase 1)
    Raw I/O ceiling of the swap device.

  Tier 2 (Gate 2) — stress-ng CPU overhead + I/O interference (Phase 2a/2b)
    Requires an active swap device (Gate 1 must pass).

Phase 2 flags (_STRESS_*) are defined in phases.py alongside the phase code.
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
from perfkitbenchmarker.linux_benchmarks.swap_encryption import (
    phases as _phases,
    utils as _utils,
)
from perfkitbenchmarker.resources.container_service import (
    swap_daemonset as _ds_mod,
)

_BenchmarkSpec = bm_spec_lib.BenchmarkSpec

# ---------------------------------------------------------------------------
# Benchmark identity
# ---------------------------------------------------------------------------

BENCHMARK_NAME = 'swap_encryption'

BENCHMARK_CONFIG = """
swap_encryption:
  description: >
    CPU/IO overhead benchmarks (Tier 2) on swap-encrypted GKE/EKS nodes.
    Swap-enabled 'benchmark' nodepool declared in BENCHMARK_CONFIG;
    GKE cluster creation applies --system-config-from-file (dm-crypt
    swapConfig) automatically via swap_config field on NodepoolSpec.
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
    'swap_encryption_swap_device',
    '',
    'Override the auto-detected swap device (e.g. /dev/mapper/swap_encrypted).',
)

_SWAP_SIZE_GB = flags.DEFINE_integer(
    'swap_encryption_swap_size_gb',
    32,
    'Size of the swap partition/file in GiB.',
)

_SWAP_TYPE = flags.DEFINE_enum(
    'swap_encryption_swap_type',
    'hyperdisk',
    [
        'hyperdisk', 'lssd', 'lssd_stateful_loop', 'loop',
        'boot_disk', 'io2', 'instance_store',
    ],
    'Storage target for the swap device.',
)

_FIO_RUNTIME_SEC = flags.DEFINE_integer(
    'swap_encryption_fio_runtime_sec',
    60,
    'Wall-clock seconds each fio job runs in Phase 1.',
)

_ENABLE_ZSWAP = flags.DEFINE_boolean(
    'swap_encryption_enable_zswap',
    False,
    'Enable zswap compressed swap cache on the node.',
)

_MIN_FREE_KBYTES = flags.DEFINE_integer(
    'swap_encryption_min_free_kbytes',
    0,
    'Override vm.min_free_kbytes (0 = leave at system default).',
)

_DAEMONSET_IMAGE = flags.DEFINE_string(
    'swap_encryption_daemonset_image',
    '',
    'Container image for the benchmark DaemonSet pod.',
)

_NODEPOOL = flags.DEFINE_string(
    'swap_encryption_nodepool',
    'benchmark',
    'Name of the nodepool where the benchmark DaemonSet runs.',
)

_INSTANCE_SIZE_LABEL = flags.DEFINE_string(
    'swap_encryption_instance_size_label',
    '',
    'Human-readable instance size label stored in sample metadata.',
)

_COLLECT_COST = flags.DEFINE_boolean(
    'swap_encryption_collect_cost',
    False,
    'Emit a cost_estimate_usd sample at the end of the run.',
)

_IO2_ENCRYPTED = flags.DEFINE_boolean(
    'swap_encryption_io2_encrypted',
    True,
    'Whether the io2 EBS volume is encrypted (EKS only).',
)

_IO2_KMS_KEY_ID = flags.DEFINE_string(
    'swap_encryption_io2_kms_key_id',
    '',
    'KMS key ID for io2 EBS volume encryption (EKS only).',
)

_FAIL_ON_DEGRADED = flags.DEFINE_boolean(
    'swap_encryption_fail_on_degraded',
    False,
    'Raise RunError if the run is marked degraded.',
)

_PHASES = flags.DEFINE_list(
    'swap_encryption_phases',
    ['all'],
    "Comma-separated phases: fio, 2a, 2b. Default 'all' runs every phase.",
)

_BENCHMARK_MACHINE_TYPE = flags.DEFINE_string(
    'swap_encryption_benchmark_machine_type',
    '',
    'Machine type for cost estimation when cloud metadata is unavailable.',
)

_BENCHMARK_LSSD = flags.DEFINE_boolean(
    'swap_encryption_benchmark_lssd',
    False,
    'Node has local SSD(s) for swap (affects BENCHMARK_CONFIG nodepool spec).',
)

_LSSD_COUNT = flags.DEFINE_integer(
    'swap_encryption_lssd_count',
    1,
    'Number of local SSD disks to stripe into a RAID-0 swap device.',
)

_ENABLE_DMCRYPT = flags.DEFINE_boolean(
    'swap_encryption_enable_dmcrypt',
    True,
    'Enable dm-crypt encryption on the swap device.',
)

_NODE_IMAGE_TYPE = flags.DEFINE_string(
    'swap_encryption_node_image_type',
    'COS_CONTAINERD',
    'GKE node image type (e.g. COS_CONTAINERD, UBUNTU_CONTAINERD).',
)

_BOOT_DISK_TYPE = flags.DEFINE_string(
    'swap_encryption_boot_disk_type',
    'hyperdisk-balanced',
    'GKE boot disk type.',
)

_BOOT_DISK_IOPS = flags.DEFINE_integer(
    'swap_encryption_boot_disk_iops',
    0,
    'Provisioned IOPS for the boot disk (0 = cloud default).',
)

_BOOT_DISK_THROUGHPUT = flags.DEFINE_integer(
    'swap_encryption_boot_disk_throughput',
    0,
    'Provisioned throughput for the boot disk in MiB/s (0 = cloud default).',
)

_BOOT_DISK_SIZE_GB = flags.DEFINE_integer(
    'swap_encryption_boot_disk_size_gb',
    500,
    'Boot disk size in GiB.',
)

_ADD_SWAP_DISK = flags.DEFINE_boolean(
    'swap_encryption_add_swap_disk',
    False,
    'Attach a dedicated swap disk to the benchmark node.',
)

_SWAP_DISK_SIZE_GB = flags.DEFINE_integer(
    'swap_encryption_swap_disk_size_gb',
    375,
    'Size of the dedicated swap disk in GiB.',
)

# ---------------------------------------------------------------------------
# Internal constants
# ---------------------------------------------------------------------------

_DS_NAME = 'pkb-swap-benchmark'
_DS_NAMESPACE = 'default'
_DS_LABEL = 'pkb-swap-benchmark'
_BENCHMARK_NODEPOOL = 'benchmark'

# Set by _ensure_io2_volume(); passed to SetupSwap for serial-based detection.
_IO2_VOLUME_ID = ''


# ---------------------------------------------------------------------------
# PKB entry points
# ---------------------------------------------------------------------------


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:  # pylint: disable=invalid-name
  """Load and return benchmark config spec."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(spec: _BenchmarkSpec) -> None:  # pylint: disable=invalid-name
  """Deploy DaemonSet, tune kernel swap settings, and configure swap device.

  PKB cluster creation automatically provisions the swap-enabled 'benchmark'
  nodepool (swap_config declared in BENCHMARK_CONFIG). This function:
    1. Deploys the privileged SwapDaemonSet and waits for Running.
    2. Deletes the cheap e2-medium default-pool.
    3. Tunes kernel swap aggressiveness (swappiness, min_free_kbytes).
    4. Unlocks container cgroup swap limits.
    5. Optionally enables zswap.
    6. Configures cloud-specific swap via SwapDaemonSet.SetupSwap().
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
  spec.resources.append(daemonset)
  daemonset.Create()
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
    daemonset.EnableZswap()

  # Configure cloud-specific swap via the daemonset abstraction.
  _ensure_io2_volume()
  cloud = daemonset.DetectCloud()
  logging.info('[swap_encryption] Detected cloud: %s', cloud)
  daemonset.SetupSwap(
      cloud=cloud,
      swap_type=_SWAP_TYPE.value,
      enable_dmcrypt=_ENABLE_DMCRYPT.value,
      swap_size_gb=_SWAP_SIZE_GB.value,
      io2_volume_id=_IO2_VOLUME_ID,
  )


def Run(spec: _BenchmarkSpec) -> list[sample.Sample]:  # pylint: disable=invalid-name
  """Execute all benchmark phases with gate logic.

  Tier 1 (Gate 1): fio microbenchmarks — raw I/O ceiling of the swap device.
  Tier 2 (Gate 2): stress-ng CPU overhead (2a) + IO interference (2b).
    Requires Gate 1 to pass (no point measuring app-level swap when the raw
    device is inaccessible).
  """
  daemonset = _get_daemonset(spec)

  pod = daemonset.WaitForPod()
  daemonset.oom_events.clear()
  daemonset.pod_lost.clear()
  original_pod = pod
  degraded_reasons: list[str] = []

  swap_dev = daemonset.GetActiveSwapDevice(_SWAP_DEVICE.value)
  base_meta = _utils.BuildMetadata(
      daemonset,
      swap_dev,
      swap_type=_SWAP_TYPE.value,
      enable_dmcrypt=_ENABLE_DMCRYPT.value,
      node_image_type=_NODE_IMAGE_TYPE.value,
      boot_disk_type=_BOOT_DISK_TYPE.value,
      boot_disk_iops=_BOOT_DISK_IOPS.value,
      benchmark_machine_type=_BENCHMARK_MACHINE_TYPE.value,
      enable_zswap=_ENABLE_ZSWAP.value,
      min_free_kbytes=_MIN_FREE_KBYTES.value,
      fio_runtime_sec=_FIO_RUNTIME_SEC.value,
      nodepool=_NODEPOOL.value,
      instance_size_label=_INSTANCE_SIZE_LABEL.value,
  )
  results: list[sample.Sample] = []
  t_run_start = time.time()

  logging.info('[swap_encryption] swap device: %s', swap_dev)

  # ── Tier 1 / Gate 1: fio microbenchmarks ──────────────────────────────────
  tier1_results = []
  if _phase_selected('fio'):
    logging.info('[swap_encryption] ── Tier 1 / Gate 1: fio microbenchmarks ──')
    try:
      tier1_results = _phases.RunPhase1Fio(
          daemonset,
          swap_dev,
          base_meta,
          _FIO_RUNTIME_SEC.value,
          _SWAP_TYPE.value,
      )
      results += tier1_results
    except Exception as e:  # pylint: disable=broad-except
      logging.error('[swap_encryption] Gate 1 FAILED — fio phase error: %s', e)
      return results

    if not tier1_results:
      logging.info(
          '[swap_encryption] Gate 1 produced no samples '
          '(loop-device skip or parse error) — continuing to Tier 2'
      )
  else:
    logging.info(
        '[swap_encryption] Skipping Tier 1 (fio)'
        ' — not in --swap_encryption_phases'
    )

  # ── Tier 2 / Gate 2: stress-ng CPU overhead + IO interference ─────────────
  if _phase_selected('2a') or _phase_selected('2b'):
    logging.info('[swap_encryption] ── Tier 2 / Gate 2: stress-ng phases ──')
    try:
      if _phase_selected('2a'):
        logging.info('[swap_encryption] Phase 2a: CPU overhead')
        results += _phases.RunPhase2a(daemonset, base_meta, degraded_reasons)
      if _phase_selected('2b'):
        logging.info('[swap_encryption] Phase 2b: I/O interference')
        results += _phases.RunPhase2b(daemonset, base_meta)
    except Exception as e:  # pylint: disable=broad-except
      logging.error(
          '[swap_encryption] Gate 2 FAILED — stress phase error: %s', e
      )

  # ── Cost estimate ──────────────────────────────────────────────────────────
  if _COLLECT_COST.value:
    elapsed = time.time() - t_run_start
    results += _utils.CollectCostSample(
        daemonset,
        elapsed,
        base_meta,
        _INSTANCE_SIZE_LABEL.value,
        _BENCHMARK_MACHINE_TYPE.value,
    )

  # ── Degradation gate ───────────────────────────────────────────────────────
  if daemonset.pod_name and daemonset.pod_name != original_pod:
    degraded_reasons.append(
        f'benchmark pod was replaced during the run ({original_pod} →'
        f' {daemonset.pod_name}) — OOM-evicted under swap pressure'
    )
  if daemonset.pod_lost:
    degraded_reasons.append(
        'benchmark pod(s) went NotFound during the run'
        f' ({", ".join(daemonset.pod_lost)})'
    )
  if daemonset.oom_events:
    degraded_reasons.append(
        'OOM kill(s) (rc=137) occurred during the run on pod(s) '
        f'{", ".join(daemonset.oom_events)}'
    )

  if _phase_selected('fio') and not tier1_results:
    if swap_dev.startswith('/dev/loop'):
      logging.info(
          '[swap_encryption] Gate 1 (fio) skipped (loop) — Tier 2 valid: %s',
          swap_dev,
      )
    else:
      degraded_reasons.append(
          'Gate 1 (fio microbenchmarks) produced no samples'
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


def Cleanup(spec: _BenchmarkSpec) -> None:  # pylint: disable=unused-argument,invalid-name
  """Resources in spec.resources are auto-deleted by the PKB framework.

  SwapDaemonSet._Delete() runs in-pod teardown (swapoff, dmsetup remove,
  losetup cleanup, pkill fio/stress-ng) then deletes the DaemonSet.
  """


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _delete_default_pool(cluster) -> None:
  """Delete the cheap e2-medium default nodepool after DaemonSet is Running."""
  try:
    cluster.DeleteNodePool('default-pool')
  except Exception as e:  # pylint: disable=broad-except
    logging.warning(
        '[swap_encryption] Could not delete default-pool: %s', e
    )


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
  """Return True if phase token should run given --swap_encryption_phases."""
  selected = [p.strip().lower() for p in _PHASES.value if p.strip()]
  return (not selected) or ('all' in selected) or (token.lower() in selected)


def _configure_eks_kubelet_swap(spec) -> None:
  """Configure EKS kubelet for LimitedSwap (deferred — blocked on PR #6780)."""
  raise NotImplementedError(
      '[swap_encryption] EKS kubelet LimitedSwap config via nodeadm is '
      'deferred (blocked on PR #6780).'
  )


def _ensure_io2_volume() -> None:
  """Create and attach an io2 EBS volume for swap on EKS (deferred to PR2)."""
  if _SWAP_TYPE.value != 'io2':
    return
  logging.info('[swap_encryption] io2 swap volume provisioning deferred to PR2')
