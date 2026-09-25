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
workloads execute inside this pod via kubectl exec, so measurements reflect
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

  SwapDaemonSet
  (perfkitbenchmarker/resources/container_service/swap_daemonset.py)
    _Create():  apply Jinja2 manifest; wait for Running + /tmp/pkb_ready.
    _Delete():  in-pod swapoff / dmsetup / losetup teardown; kubectl delete.
    PodExec():  kubectl exec wrapper with transient-reset retry, OOM-kill
                detection (rc=137), and automatic pod recovery.

Both resources are added to spec.resources in Prepare() and are auto-deleted
by the PKB framework in Cleanup().

== Benchmark Workloads ==

  FIO Microbenchmarks
    Run fio directly on the swap block device (swapoff first) to measure
    the hardware + encryption ceiling: random IOPS (4K), sequential
    bandwidth (1M), and completion latency (iodepth=1).

  CPU Overhead
    stress-ng drives sustained swap I/O; vmstat and pidstat capture
    swap-in/out rates and per-process CPU cost (kswapd, kcryptd,
    dm-crypt threads on GKE; Nitro offload on EKS).

  I/O Interference
    Baseline fio on a scratch volume → re-run with concurrent swap
    pressure.  IOPS/latency delta = storage contention cost.

  Redis Latency
    Dataset loaded beyond container memory limit → GET/SET p99 latency
    measured while kernel swaps pages.

  Kernel Build
    Linux compiled inside a memory-capped cgroup; slowdown ratio vs
    unconstrained baseline.

  OpenSearch
    Bulk-index + search query under swap pressure (esrally or curl).
"""

import logging
import textwrap
from typing import Any

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec_lib
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks.kubernetes.swap_encryption import utils as _utils
from perfkitbenchmarker.linux_benchmarks.kubernetes.swap_encryption import workloads as _workloads
from perfkitbenchmarker.resources.container_service import swap_daemonset as _ds_mod

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

_ENABLE_DMCRYPT = flags.DEFINE_boolean(
    'swap_encryption_enable_dmcrypt',
    True,
    'When True (default), configure dm-crypt on the swap device — the '
    '"encryption enabled" column of the test matrix.  Set False to use '
    'plain swap (encryption disabled column).',
)

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

# ---------------------------------------------------------------------------
# Flags
# ---------------------------------------------------------------------------

_FIO_RUNTIME_SEC = flags.DEFINE_integer(
    'swap_encryption_fio_runtime_sec',
    60,
    'Wall-clock runtime in seconds for each individual fio job.',
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
    'When True (default), raise an error at the end of Run() if the run was'
    ' catastrophically degraded — e.g. the benchmark pod was OOM-evicted and'
    ' replaced mid-run, Gate 1 (fio) produced no samples, or the stress-ng'
    ' swap-pressure workload was OOM-killed before completing.  This prevents'
    ' PKB from reporting SUCCEEDED for a run whose post-eviction workloads'
    ' produced empty or meaningless data.  Set False to keep the legacy'
    ' behaviour of always returning whatever partial samples were collected.',
)

_WORKLOADS = flags.DEFINE_list(
    'swap_encryption_workloads',
    ['all'],
    'Which Run() workloads to execute, for fast iteration against an'
    ' already-provisioned cluster (e.g. --run_stage=run --run_uri=...). '
    ' Comma-separated subset of: fio (Tier 1 microbenchmarks), cpu_overhead'
    ' (stress-ng CPU overhead + swap pressure), io_interference (I/O'
    ' interference), redis (redis), kernel_build (kernel build), opensearch'
    ' (opensearch).  Default "all" runs everything.  Example:'
    ' --swap_encryption_workloads=cpu_overhead runs only the swap-pressure'
    ' workload. Workloads not listed are skipped and do not affect the'
    ' degraded-run gate (e.g. skipping fio will not be reported as "Gate 1'
    ' produced no samples").',
)

_BENCHMARK_MACHINE_TYPE = flags.DEFINE_string(
    'swap_encryption_benchmark_machine_type',
    'n4-highmem-32',
    'Machine type for the benchmark nodepool created in Prepare(). '
    'Use n4-highmem-32 (hyperdisk, default) or c4-standard-8-lssd '
    '(LSSD RAID-0).  The matching swap setup is selected automatically.',
)

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

# DaemonSet identity — used by Prepare() and _GetDaemonset().
_DS_NAME = 'pkb-swap-benchmark'
_DS_NAMESPACE = 'default'
_DS_LABEL = 'pkb-swap-benchmark'
_BENCHMARK_NODEPOOL = 'benchmark'
_DEFAULT_POOL = 'default-pool'


# ---------------------------------------------------------------------------
# PKB benchmark API
# ---------------------------------------------------------------------------


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:  # pylint: disable=invalid-name
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(spec: _BenchmarkSpec) -> None:  # pylint: disable=invalid-name
  """Two-step nodepool setup then DaemonSet deployment.

  Args:
    spec: The benchmark specification.

  PKB cluster creation automatically provisions the swap-enabled 'benchmark'
  nodepool (swap_config declared in BENCHMARK_CONFIG). This function:
    1. Deploys the privileged SwapDaemonSet and waits for Running.
    2. Deletes the cheap e2-medium default-pool (required at cluster create).
    3. Tunes kernel swap aggressiveness (swappiness, min_free_kbytes).
    4. Unlocks container cgroup swap limits.
    5. Optionally enables zswap.
    6. Configures cloud-specific swap via SwapDaemonSet.SetupSwap().

  DaemonSet is appended to spec.resources for PKB auto-cleanup.
  """
  cluster = spec.container_cluster

  logging.info('[swap_encryption] Deploying privileged DaemonSet')
  daemonset_class = resource.GetResourceClass(
      _ds_mod.SwapDaemonSet, CLOUD=spec.container_cluster.CLOUD
  )
  daemonset = daemonset_class(
      name=_DS_NAME,
      namespace=_DS_NAMESPACE,
      label=_DS_LABEL,
      nodepool=_BENCHMARK_NODEPOOL,
      image=FLAGS.swap_encryption_daemonset_image,
  )
  # Register before Create() so PKB auto-deletes on failure/cleanup.
  spec.resources.append(daemonset)
  daemonset.Create()
  logging.info('[swap_encryption] Benchmark pod ready: %s', daemonset.pod_name)
  try:
    cluster.DeleteNodePool(_DEFAULT_POOL)  # pytype: disable=attribute-error
  except NotImplementedError:
    pass
  except Exception as e:  # pylint: disable=broad-except
    logging.warning('[swap_encryption] Could not delete default-pool: %s', e)
  daemonset.WaitForPod()
  logging.info(
      '[swap_encryption] Benchmark pod (post-deletion): %s', daemonset.pod_name
  )

  # Tune kernel swap aggressiveness.
  daemonset.PodExec('sysctl -w vm.swappiness=100', ignore_failure=True)
  if FLAGS.swap_encryption_min_free_kbytes > 0:
    daemonset.PodExec(
        f'sysctl -w vm.min_free_kbytes={FLAGS.swap_encryption_min_free_kbytes}'
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
  if FLAGS.swap_encryption_enable_zswap:
    daemonset.EnableZswap()

  # Configure cloud-specific swap via the daemonset abstraction.
  daemonset.SetupSwap(
      swap_type=FLAGS.swap_encryption_swap_type,
      enable_dmcrypt=FLAGS.swap_encryption_enable_dmcrypt,
      swap_size_gb=FLAGS.swap_encryption_swap_size_gb,
      io2_volume_id='',
  )


def _WorkloadSelected(token: str) -> bool:
  """Return True if workload `token` should run given --swap_encryption_workloads.

  Args:
    token: The workload token to check.

  'all' (the default) selects every workload.  Otherwise only the
    comma-separated
  tokens listed in the flag run.  Tokens: fio, cpu_overhead, io_interference,
    redis, kernel_build, opensearch.
  """
  selected = [p.strip().lower() for p in _WORKLOADS.value if p.strip()]
  return (not selected) or ('all' in selected) or (token.lower() in selected)


def Run(spec: _BenchmarkSpec) -> list[sample.Sample]:  # pylint: disable=invalid-name
  """Execute all benchmark workloads with gate logic.

  Args:
    spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects.

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
  daemonset = _GetDaemonset(spec)

  # WaitForPod raises PrepareException on timeout — never returns None.
  pod = daemonset.WaitForPod()
  # Reset per-run accumulators before starting workloads.
  daemonset.oom_events.clear()
  daemonset.pod_lost.clear()
  original_pod = pod
  degraded_reasons: list[str] = []

  swap_dev = daemonset.GetActiveSwapDevice(FLAGS.swap_encryption_device)
  base_metadata = _utils.BuildMetadata(
      daemonset,
      swap_dev,
      swap_type=FLAGS.swap_encryption_swap_type,
      enable_dmcrypt=FLAGS.swap_encryption_enable_dmcrypt,
      node_image_type=FLAGS.swap_encryption_node_image_type,
      boot_disk_type=FLAGS.swap_encryption_boot_disk_type,
      boot_disk_iops=FLAGS.swap_encryption_boot_disk_iops,
      benchmark_machine_type=_BENCHMARK_MACHINE_TYPE.value,
      enable_zswap=FLAGS.swap_encryption_enable_zswap,
      min_free_kbytes=FLAGS.swap_encryption_min_free_kbytes,
      fio_runtime_sec=_FIO_RUNTIME_SEC.value,
      stress_vm_bytes=FLAGS.swap_encryption_stress_vm_bytes,
      stress_vm_bytes_list=FLAGS.swap_encryption_stress_vm_bytes_list,
      stress_timeout_sec=FLAGS.swap_encryption_stress_timeout_sec,
      nodepool=_NODEPOOL.value,
      instance_size_label=_INSTANCE_SIZE_LABEL.value,
      benchmark_name=BENCHMARK_NAME,
  )
  results: list[sample.Sample] = []

  logging.info('[swap_encryption] swap device: %s', swap_dev)

  # ── Tier 1 / Gate 1: fio microbenchmarks ─────────────────────────────────
  tier1_results = []
  if _WorkloadSelected('fio'):
    logging.info('[swap_encryption] ── Tier 1 / Gate 1: fio microbenchmarks ──')
    try:
      tier1_results = _workloads.RunFio(
          daemonset,
          swap_dev,
          base_metadata,
          _FIO_RUNTIME_SEC.value,
          FLAGS.swap_encryption_swap_type,
      )
      results += tier1_results
    except Exception as e:  # pylint: disable=broad-except
      logging.error(
          '[swap_encryption] Gate 1 FAILED — fio workload error: %s', e
      )
      logging.error('[swap_encryption] Skipping Tiers 2 and 3 (no swap device)')
      return results

    if not tier1_results:
      logging.info(
          '[swap_encryption] Gate 1 produced no samples '
          '(loop-device skip or parse error) — '
          'continuing to Tier 2; degradation gate will assess'
      )
  else:
    logging.info(
        '[swap_encryption] Skipping Tier 1 (fio) — not selected by '
        '--swap_encryption_workloads=%s',
        ','.join(_WORKLOADS.value),
    )

  if _WorkloadSelected('redis'):
    logging.info('[swap_encryption] Workload: redis benchmark')
    results += _workloads.run_redis(daemonset, base_metadata)

  # ── Kernel build under memory constraint ───────────────────────────────────
  if _WorkloadSelected('kernel_build'):
    logging.info('[swap_encryption] Workload: kernel build under memory cap')
    results += _workloads.run_kernel_build(
        daemonset,
        base_metadata,
        kernel_version=_workloads._KERNEL_VERSION.value,  # pylint: disable=protected-access
        kernel_memory_mb=_workloads._KERNEL_MEMORY_MB.value,  # pylint: disable=protected-access
    )

  # ── Cost estimate ─────────────────────────────────────────────────────────
  if _COLLECT_COST.value:
    pass

  # ── Final degradation gate ────────────────────────────────────────────────
  if daemonset.pod_name and daemonset.pod_name != original_pod:
    degraded_reasons.append(
        f'benchmark pod was replaced during the run ({original_pod} →'
        f' {daemonset.pod_name}) — it was OOM-evicted under swap pressure;'
        ' workloads executed after the eviction ran against a'
        ' freshly-initialised pod (empty /tmp, swap re-setup) and may be'
        ' invalid'
    )
  if daemonset.pod_lost:
    degraded_reasons.append(
        'benchmark pod(s) went NotFound during the run'
        f' ({", ".join(daemonset.pod_lost)}) — the pod died (node'
        ' memory-pressure eviction or container exit) and any workload running'
        ' at or after that point (e.g. kernel-build baseline, OpenSearch)'
        ' produced invalid data'
    )
  if daemonset.oom_events:
    degraded_reasons.append(
        'OOM kill(s) (rc=137) occurred during the run on pod(s)'
        f' {", ".join(daemonset.oom_events)} — a workload exceeded memory and'
        ' was killed by the OOM killer (the container may have restarted in'
        ' place), so the affected workload(s) produced no or partial data'
    )

  if _WorkloadSelected('fio') and not tier1_results:
    if swap_dev.startswith('/dev/loop'):
      # Expected: COS blocks device-mapper from pod namespaces on single-disk
      # nodes. Tier 2/3 results are still valid; do NOT mark run as degraded.
      logging.info(
          '[swap_encryption] Gate 1 (fio) skipped — loop device %s has no'
          ' dm-crypt support from inside a pod. Tier 2/3 results are'
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
              base_metadata,
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


def Cleanup(spec: _BenchmarkSpec) -> None:  # pylint: disable=invalid-name
  """Resources in spec.resources are auto-deleted by the PKB framework.

  Args:
    spec: The benchmark specification.

  SwapDaemonSet._Delete() runs in-pod teardown (swapoff, dmsetup remove,
  losetup cleanup, pkill fio/stress-ng) then deletes the DaemonSet.
  SwapNodePool._Delete() detaches+deletes the swap disk (if any) then
  deletes the benchmark nodepool.
  """
  del spec  # Unused.


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _GetDaemonset(spec: _BenchmarkSpec) -> _ds_mod.SwapDaemonSet:
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
