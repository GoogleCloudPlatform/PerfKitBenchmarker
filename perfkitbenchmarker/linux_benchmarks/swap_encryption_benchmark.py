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
"""swap_encryption_benchmark: verifies encrypted swap on GKE/EKS nodepools.

Architecture:
  BENCHMARK_CONFIG declares a 'benchmark' nodepool with swap_config.
  GkeCluster._AddNodeParamsToCmd() reads nodepool_config.swap_config and
  applies --system-config-from-file (linuxConfig.swapConfig + sysctl) + sets
  UBUNTU_CONTAINERD + boot-disk-provisioned-iops/throughput automatically
  during cluster creation. No separate nodepool lifecycle management needed.

  Prepare() deploys a privileged SwapDaemonSet on the swap-enabled nodepool
  for in-pod benchmark execution (fio / stress-ng / kernel build in later PRs).

  Run() verifies swap is active and dm-crypt encryption is configured, then
  reports swap device metadata as PKB samples.

  Cleanup() is empty — PKB auto-deletes spec.resources (SwapDaemonSet).

Subsequent PRs add phases:
  PR3: fio microbenchmarks on raw swap device (Tier 1)
  PR4: stress-ng CPU overhead + I/O interference (Tier 2)
  PR5: kernel build under cgroup memory constraint (Phase 3b)
"""

import logging
from typing import Any

from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker.resources.container_service import swap_daemonset

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'swap_encryption'
BENCHMARK_CONFIG = """
swap_encryption:
  description: >
    Verify dm-crypt encrypted swap on GKE/EKS. Subsequent PRs add fio,
    stress-ng, and kernel build phases.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        machine_type: e2-medium
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
          min_free_kbytes: 67584
          watermark_scale_factor: 500
          boot_disk_iops: 160000
          boot_disk_throughput: 2400
"""

_MACHINE_TYPE = flags.DEFINE_string(
    'swap_encryption_machine_type',
    None,
    'Override machine type for the benchmark nodepool.',
)
_DISK_TYPE = flags.DEFINE_string(
    'swap_encryption_disk_type',
    None,
    'Override disk type for the benchmark nodepool.',
)

_DAEMONSET_IMAGE = flags.DEFINE_string(
    'swap_encryption_daemonset_image',
    'ubuntu:22.04',
    'Container image for the privileged benchmark DaemonSet.',
)

_BenchmarkSpec = benchmark_spec.BenchmarkSpec
_BENCHMARK_NODEPOOL = 'benchmark'
_DEFAULT_POOL = 'default-pool'
_DS_NAME = 'pkb-swap-benchmark'
_DS_NAMESPACE = 'default'
_DS_LABEL = 'pkb-swap-benchmark'


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Load and return benchmark config spec."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  nodepool = config['container_cluster']['nodepools'][_BENCHMARK_NODEPOOL]
  if _MACHINE_TYPE.value:
    for cloud in nodepool['vm_spec']:
      nodepool['vm_spec'][cloud]['machine_type'] = _MACHINE_TYPE.value
  if _DISK_TYPE.value:
    for cloud in nodepool['vm_spec']:
      nodepool['vm_spec'][cloud]['boot_disk_type'] = _DISK_TYPE.value
  return config


def CheckPrerequisites(_) -> None:
  """Verifies that benchmark setup is correct."""


def Prepare(spec: _BenchmarkSpec) -> None:
  """Deploys the privileged benchmark DaemonSet on the swap-enabled nodepool.

  The swap-enabled 'benchmark' nodepool is already created by GKE cluster
  creation (swap_config declared in BENCHMARK_CONFIG). Prepare() deploys the
  privileged DaemonSet used for in-pod command execution across all phases.

  After the DaemonSet pod is Running the dummy e2-medium default-pool is
  deleted to stop its cost.

  Args:
    spec: PKB BenchmarkSpec with spec.container_cluster already created.
  """
  cluster = spec.container_cluster
  daemonset = swap_daemonset.SwapDaemonSet(
      name=_DS_NAME,
      namespace=_DS_NAMESPACE,
      label=_DS_LABEL,
      nodepool=_BENCHMARK_NODEPOOL,
      image=_DAEMONSET_IMAGE.value,
  )
  daemonset.Create()
  spec.resources.append(daemonset)
  pod = daemonset.WaitForPod()
  logging.info('[swap_encryption] Benchmark pod ready: %s', pod)
  _delete_default_pool(cluster)


def Run(spec: _BenchmarkSpec) -> list[sample.Sample]:
  """Verify swap is active and dm-crypt encryption is configured.

  Returns:
    PKB samples: swap_active, swap_encrypted, swap_cipher, swap_total_kb.
  """
  daemonset = _get_daemonset(spec)
  daemonset.WaitForPod()
  daemonset.oom_events.clear()
  daemonset.pod_lost.clear()

  swap_dev = _detect_swap_device(daemonset)
  base_meta = _build_metadata(daemonset, swap_dev)
  results: list[sample.Sample] = []

  # ── Verify swap is active ──────────────────────────────────────────────────
  try:
    swap_out, _ = daemonset.PodExec('cat /proc/swaps')
    active = any(
        l and not l.startswith('Filename') for l in swap_out.splitlines()
    )
    results.append(sample.Sample('swap_active', int(active), 'bool', base_meta))
    logging.info('[swap_encryption] swap_active=%s /proc/swaps:\n%s', active, swap_out)
  except Exception as e:  # pylint: disable=broad-except
    logging.warning('[swap_encryption] Could not read /proc/swaps: %s', e)

  # ── Verify dm-crypt encryption ─────────────────────────────────────────────
  if swap_dev:
    try:
      dm_out, _ = daemonset.PodExec(
          f'dmsetup status {swap_dev} 2>/dev/null || echo not_encrypted'
      )
      encrypted = 'crypt' in dm_out.lower()
      cipher = _parse_cipher(dm_out)
      meta = {**base_meta, 'dmsetup_status': dm_out.strip()[:200]}
      results.append(sample.Sample('swap_encrypted', int(encrypted), 'bool', meta))
      if cipher:
        results.append(sample.Sample('swap_cipher', 0, cipher, meta))
      logging.info('[swap_encryption] encrypted=%s cipher=%s', encrypted, cipher)
    except Exception as e:  # pylint: disable=broad-except
      logging.warning('[swap_encryption] dm-crypt check failed: %s', e)

  # ── Swap size ──────────────────────────────────────────────────────────────
  try:
    sz_out, _ = daemonset.PodExec(
        "awk '/^SwapTotal/ {print $2}' /proc/meminfo"
    )
    swap_kb = int(sz_out.strip() or '0')
    results.append(sample.Sample('swap_total_kb', swap_kb, 'KB', base_meta))
    logging.info(
        '[swap_encryption] SwapTotal: %d KB (%.1f GiB)',
        swap_kb, swap_kb / 1024 / 1024,
    )
  except Exception as e:  # pylint: disable=broad-except
    logging.warning('[swap_encryption] Could not read SwapTotal: %s', e)

  if daemonset.oom_events:
    results.append(
        sample.Sample('oom_events', len(daemonset.oom_events), 'count', base_meta)
    )
  return results


def Cleanup(_: _BenchmarkSpec) -> None:
  """Empty — PKB auto-deletes spec.resources (SwapDaemonSet)."""


# ── Helpers ────────────────────────────────────────────────────────────────────


def _get_daemonset(spec: _BenchmarkSpec) -> swap_daemonset.SwapDaemonSet:
  for r in spec.resources:
    if isinstance(r, swap_daemonset.SwapDaemonSet):
      return r
  raise RuntimeError('[swap_encryption] SwapDaemonSet not found in spec.resources')


def _detect_swap_device(ds: swap_daemonset.SwapDaemonSet) -> str:
  """Return the first active swap device name (e.g. 'dm-0') or ''."""
  try:
    out, _ = ds.PodExec("awk 'NR>1 {print $1}' /proc/swaps")
    dev = out.strip().split('\n')[0].strip()
    return dev.split('/')[-1] if dev else ''
  except Exception as e:  # pylint: disable=broad-except
    logging.warning('[swap_encryption] _detect_swap_device: %s', e)
    return ''


def _build_metadata(
    ds: swap_daemonset.SwapDaemonSet, swap_dev: str
) -> dict[str, Any]:
  """Build base metadata dict for all samples."""
  meta: dict[str, Any] = {'swap_device': swap_dev or 'unknown'}
  try:
    kver, _ = ds.PodExec('uname -r')
    meta['kernel_version'] = kver.strip()
  except Exception:  # pylint: disable=broad-except
    pass
  return meta


def _parse_cipher(dmsetup_status: str) -> str:
  """Extract cipher name from dmsetup status output."""
  parts = dmsetup_status.split()
  try:
    idx = parts.index('crypt')
    return parts[idx + 1] if idx + 1 < len(parts) else ''
  except ValueError:
    return ''


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
