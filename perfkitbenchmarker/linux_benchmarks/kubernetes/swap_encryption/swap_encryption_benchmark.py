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

  SwapDaemonSet
  (perfkitbenchmarker/resources/container_service/swap_daemonset.py)
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

import logging
import textwrap
import time
from typing import Any

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec_lib
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks.kubernetes.swap_encryption import phases as _phases
from perfkitbenchmarker.linux_benchmarks.kubernetes.swap_encryption import utils as _utils
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

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

# DaemonSet identity — used by Prepare() and _GetDaemonset().
_DS_NAME = 'pkb-swap-benchmark'
_DS_NAMESPACE = 'default'
_DS_LABEL = 'pkb-swap-benchmark'
_BENCHMARK_NODEPOOL = 'benchmark'
_DEFAULT_POOL = 'default-pool'

# Module-level stash for the io2 volume id created by _EnsureIo2Volume().
_IO2_VOLUME_ID = ''


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
  _DeleteDefaultPool(cluster)
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
      cloud=spec.container_cluster.CLOUD,
      swap_type=FLAGS.swap_encryption_swap_type,
      enable_dmcrypt=FLAGS.swap_encryption_enable_dmcrypt,
      swap_size_gb=FLAGS.swap_encryption_swap_size_gb,
      io2_volume_id=_IO2_VOLUME_ID,
  )


def _PhaseSelected(token: str) -> bool:
  """Return True if phase `token` should run given --swap_encryption_phases.

  Args:
    token: The phase token to check.

  'all' (the default) selects every phase.  Otherwise only the comma-separated
  tokens listed in the flag run.  Tokens: fio, 2a, 2b, 3a, 3b, 3c.
  """
  selected = [p.strip().lower() for p in _PHASES.value if p.strip()]
  return (not selected) or ('all' in selected) or (token.lower() in selected)


def Run(spec: _BenchmarkSpec) -> list[sample.Sample]:  # pylint: disable=invalid-name
  """Execute all benchmark phases with gate logic.

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
  # Reset per-run accumulators before starting phases.
  daemonset.oom_events.clear()
  daemonset.pod_lost.clear()
  original_pod = pod
  degraded_reasons: list[str] = []

  swap_dev = daemonset.GetActiveSwapDevice(FLAGS.swap_encryption_device)
  base_meta = _utils.BuildMetadata(
      daemonset,
      swap_dev,
      swap_type=FLAGS.swap_encryption_swap_type,
      enable_dmcrypt=FLAGS.swap_encryption_enable_dmcrypt,
      node_image_type=_NODE_IMAGE_TYPE.value,
      boot_disk_type=_BOOT_DISK_TYPE.value,
      boot_disk_iops=_BOOT_DISK_IOPS.value,
      benchmark_machine_type=_BENCHMARK_MACHINE_TYPE.value,
      enable_zswap=FLAGS.swap_encryption_enable_zswap,
      min_free_kbytes=FLAGS.swap_encryption_min_free_kbytes,
      fio_runtime_sec=_FIO_RUNTIME_SEC.value,
      stress_vm_bytes=_STRESS_VM_BYTES.value,
      stress_vm_bytes_list=_STRESS_VM_BYTES_LIST.value,
      stress_timeout_sec=_STRESS_TIMEOUT_SEC.value,
      nodepool=_NODEPOOL.value,
      instance_size_label=_INSTANCE_SIZE_LABEL.value,
      benchmark_name=BENCHMARK_NAME,
  )
  results: list[sample.Sample] = []
  t_run_start = time.time()

  logging.info('[swap_encryption] swap device: %s', swap_dev)

  # ── Tier 1 / Gate 1: fio microbenchmarks ─────────────────────────────────
  tier1_results = []
  if _PhaseSelected('fio'):
    logging.info('[swap_encryption] ── Tier 1 / Gate 1: fio microbenchmarks ──')
    try:
      tier1_results = _phases.RunPhase1Fio(
          daemonset,
          swap_dev,
          base_meta,
          _FIO_RUNTIME_SEC.value,
          FLAGS.swap_encryption_swap_type,
      )
      results += tier1_results
    except Exception as e:  # pylint: disable=broad-except
      logging.error('[swap_encryption] Gate 1 FAILED — fio phase error: %s', e)
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
        '--swap_encryption_phases=%s',
        ','.join(_PHASES.value),
    )

  # ── Cost estimate ─────────────────────────────────────────────────────────
  if _COLLECT_COST.value:
    elapsed = time.time() - t_run_start
    results += _utils.CollectCostSample(
        daemonset,
        elapsed,
        base_meta,
        _INSTANCE_SIZE_LABEL.value,
        _BENCHMARK_MACHINE_TYPE.value,
    )

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
        f' ({", ".join(daemonset.pod_lost)}) — the pod died (node'
        ' memory-pressure eviction or container exit) and any phase running at'
        ' or after that point (e.g. kernel-build baseline, OpenSearch)'
        ' produced invalid data'
    )
  if daemonset.oom_events:
    degraded_reasons.append(
        'OOM kill(s) (rc=137) occurred during the run on pod(s) '
        f'{", ".join(daemonset.oom_events)} — a phase exceeded memory and was'
        ' killed by the OOM killer (the container may have restarted in place),'
        ' so the affected phase(s) produced no or partial data'
    )

  if _PhaseSelected('fio') and not tier1_results:
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


def _DeleteDefaultPool(cluster: Any) -> None:
  """Delete the dummy e2-medium default-pool once the benchmark pod is Running.

  Args:
    cluster: The PKB container cluster instance.

  GKE requires at least one nodepool at cluster creation time; the e2-medium
  default-pool satisfies that requirement. Deleting it before the DaemonSet
  pod is Running can trigger a brief API-server timeout while two concurrent
  nodepool operations are in progress.
  """
  try:
    cmd = cluster._GcloudCommand(  # pylint: disable=protected-access
        'container',
        'node-pools',
        'delete',
        _DEFAULT_POOL,
        '--cluster',
        cluster.name,
    )
    cmd.args.append('--quiet')
    logging.info(
        '[swap_encryption] Deleting default nodepool: %s', _DEFAULT_POOL
    )
    _, stderr, rc = cmd.Issue(timeout=300, raise_on_failure=False)
    if rc != 0:
      raise errors.Benchmarks.RunError(
          '[swap_encryption] Could not delete default nodepool '
          f'(rc={rc}): {stderr}'
      )
    logging.info('[swap_encryption] Default nodepool deleted')
  except errors.Benchmarks.RunError:
    raise
  except Exception as e:  # pylint: disable=broad-except
    raise errors.Benchmarks.RunError(
        f'[swap_encryption] _DeleteDefaultPool failed: {e}'
    ) from e


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


def _EnsureIo2Volume() -> None:
  """Create + attach a dedicated io2 EBS volume to the benchmark node.

  No-op unless --swap_encryption_swap_type=io2 on an AWS/EKS cluster.
  Best-effort: logs and returns on failure.  Stashes the created volume id in
  the module-level _IO2_VOLUME_ID for serial-based device detection in
  SwapDaemonSet._SetupEksIo2Swap.
  """
  global _IO2_VOLUME_ID
  if FLAGS.swap_encryption_swap_type != 'io2':
    return
  out, _, rc = kubectl.RunKubectlCommand(
      ['get', 'nodes', '-o', 'jsonpath={.items[0].spec.providerID}'],
      raise_on_failure=False,
  )
  provider = (out or '').strip()  # aws:///us-east-1a/i-0abc...
  if rc != 0 or 'aws://' not in provider:
    raise errors.Benchmarks.RunError(
        '[swap_encryption] io2 setup: could not resolve EC2 instance'
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
    if FLAGS.swap_encryption_io2_encrypted:
      create_args.append('--encrypted')
      if FLAGS.swap_encryption_io2_kms_key_id:
        create_args += ['--kms-key-id', FLAGS.swap_encryption_io2_kms_key_id]
      logging.info(
          '[swap_encryption] io2 volume will be EBS-encrypted '
          '(row: hardware encryption)'
      )
    else:
      logging.info('[swap_encryption] io2 volume UNENCRYPTED (baseline row)')
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


def _ConfigureEksKubeletSwap(spec: _BenchmarkSpec) -> None:
  """Configure EKS kubelet for LimitedSwap via nodeadm bootstrap.

  Args:
    spec: The benchmark specification.

  NOTE: Deferred — requires Ajay's PR #6780 (SwapConfigSpec + nodeadm
  integration) to merge.  When that lands, EKS node pools should include
  a preBootstrapCommands block writing nodeadm config with
  memorySwapBehavior: LimitedSwap before kubelet starts.
  """
  del spec  # Unused until implemented.
  raise NotImplementedError(
      '[swap_encryption] EKS kubelet LimitedSwap config via nodeadm is '
      'deferred (blocked on PR #6780). EKS swap setup not yet implemented.'
  )
