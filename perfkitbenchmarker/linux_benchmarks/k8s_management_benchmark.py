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

"""Benchmark for Kubernetes management plane operations.

Measures GKE/EKS/AKS control-plane API responsiveness via three scenarios:
  A. Concurrent node-pool create/upgrade/delete.
  B. Node-pool create overlapping with a long-running cluster update.
  C. Large-scale node-pool provisioning (single scale or sweep).

Optimizations for minimum run time:
  - Streaming concurrency in Scenario C (no batch barriers)
  - Optional pipelined Scenario A (create->upgrade->delete per thread)
  - Reduced poll_interval in provider WaitForOperation (5s vs 10s)
  - Per-op threads capped at _MAX_CONCURRENT to avoid OS limits
  - Accurate delete success rate via attempted_ops denominator
"""

import copy
import threading
import time
from typing import Callable

from absl import flags
from absl import logging
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.resources.container_service import container as container_lib
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_cluster

_SLEEP_POD_NAME = 'pkb-mgmt-sleep'

BENCHMARK_NAME = 'k8s_management'

BENCHMARK_CONFIG = """
k8s_management:
  description: >
    Benchmarks GKE/EKS/AKS management plane operations: concurrent node pool
    create/upgrade/delete, overlapping cluster + node-pool ops, and large-scale
    provisioning. Focused on control-plane API responsiveness.
    Spec regions: GCP us-central1, AWS us-east-1 (closest), Azure eastus (closest).
    Equivalent machine types across clouds per Google benchmark spec.
  container_cluster:
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        # us-central1-a: spec primary region for GCP
        # e2-standard-2: 2 vCPU 8GB — equivalent to t3.medium / Standard_D2s_v3
        machine_type: e2-standard-2
        zone: us-central1-a
      AWS:
        # us-east-1a: closest comparable region to GCP us-central1
        # t3.medium: 2 vCPU 4GB — closest equivalent to e2-standard-2 (Google spec)
        machine_type: t3.medium
        zone: us-east-1a
      Azure:
        # eastus: closest comparable region to GCP us-central1
        # Standard_D2s_v3: 2 vCPU 8GB — equivalent to e2-standard-2
        machine_type: Standard_D2s_v3
        zone: eastus
"""

_VALID_SCENARIOS = frozenset({'A', 'B', 'C'})

_CONCURRENT_NODEPOOLS = flags.DEFINE_integer(
    'k8s_mgmt_concurrent_nodepools',
    5,
    'Number of node pools to create/upgrade/delete concurrently in Scenario A.',
)
_LARGE_SCALE_NODEPOOLS = flags.DEFINE_integer(
    'k8s_mgmt_large_scale_nodepools',
    1000,
    'Number of node pools to provision in the large-scale Scenario C. '
    'Spec target is 1000; ensure VPC/quota is available before running.',
)
_NODES_PER_NODEPOOL = flags.DEFINE_integer(
    'k8s_mgmt_nodes_per_nodepool',
    2,
    'Number of nodes per node pool. Google spec: 2 nodes per pool.',
)
_INITIAL_VERSION = flags.DEFINE_string(
    'k8s_mgmt_initial_version',
    None,
    'Kubernetes version for newly-created node pools (N-1). None = auto.',
)
_TARGET_VERSION = flags.DEFINE_string(
    'k8s_mgmt_target_version',
    None,
    'Kubernetes version to upgrade node pools to (N). None = cluster version.',
)
_SCENARIOS = flags.DEFINE_list(
    'k8s_mgmt_scenarios',
    ['A', 'B', 'C'],
    'Comma-separated subset of scenarios to run. Valid values: A, B, C.',
)
_SCALE_SWEEP = flags.DEFINE_list(
    'k8s_mgmt_scale_sweep',
    [],
    'Comma-separated list of node-pool counts for Scenario C scale sweep. '
    'Each scale runs as a separate sub-run with full create/delete cycle. '
    'Example: --k8s_mgmt_scale_sweep=10,50,100,500,1000. '
    'If empty, uses --k8s_mgmt_large_scale_nodepools.',
)
_MAX_CONCURRENT = flags.DEFINE_integer(
    'k8s_mgmt_max_concurrent',
    50,
    'Cap on concurrent provider API calls within a batch. '
    'Higher = faster but more aggressive on connection pools.',
)
_PIPELINE_SCENARIO_A = flags.DEFINE_boolean(
    'k8s_mgmt_pipeline_scenario_a',
    False,
    'If True, run Scenario A as a per-pool pipeline (create->upgrade->delete '
    'back-to-back per thread). Minimizes wall time but measures ops under '
    'mixed-type concurrent load. Default False = phase-by-phase (spec-strict).',
)

# AKS caps node-pool names at 12 chars — keep all names within that limit.
_PREFIX = 'pkbm'
_SCENARIO_A_NAME = lambda i: f'{_PREFIX}a{i:03d}'
_SCENARIO_B_NAME = f'{_PREFIX}b'
_SCENARIO_C_NAME = lambda i: f'{_PREFIX}c{i:04d}'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(
    benchmark_config: benchmark_config_spec.BenchmarkConfigSpec,
):
  """Validates flag values and cluster type before any cloud calls."""
  invalid = [
      s for s in _SCENARIOS.value if s.strip().upper() not in _VALID_SCENARIOS
  ]
  if invalid:
    raise errors.Config.InvalidValue(
        f'Invalid value(s) for --k8s_mgmt_scenarios: {invalid}. '
        f'Valid options: {sorted(_VALID_SCENARIOS)}.'
    )
  for s in _SCALE_SWEEP.value:
    try:
      int(s.strip())
    except ValueError as e:
      raise errors.Config.InvalidValue(
          f'Non-integer value in --k8s_mgmt_scale_sweep: {s!r}'
      ) from e
  if benchmark_config.container_cluster.type != 'Kubernetes':
    raise errors.Config.InvalidValue(
        'k8s_management benchmark requires a Kubernetes container cluster.'
    )


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Asserts the cluster is reachable; deploys spec-defined sleep workload."""
  cluster = benchmark_spec.container_cluster
  assert isinstance(cluster, kubernetes_cluster.KubernetesCluster)
  benchmark_spec.always_call_cleanup = True
  logging.info(
      'k8s_management Prepare: cluster=%s, version=%s',
      cluster.name,
      cluster.k8s_version,
  )
  # Spec workload: "a simple container that sleeps for a given time".
  # Confirms data-plane reachability; generates no data-plane load.
  _, _, rc = kubectl.RunKubectlCommand(
      [
          'run', _SLEEP_POD_NAME,
          '--image=busybox',
          '--restart=Never',
          '--', 'sleep', '86400',
      ],
      raise_on_failure=False,
  )
  if rc:
    logging.warning(
        'Sleep workload deploy returned rc=%d (non-fatal; continuing)', rc
    )


def _CleanStartSweep(cluster: kubernetes_cluster.KubernetesCluster) -> None:
  """Deletes any stale pkbm* node pools so each run starts clean (spec C.2)."""
  try:
    stale = [n for n in cluster.GetNodePoolNames() if n.startswith(_PREFIX)]
  except Exception:  # pylint: disable=broad-except
    logging.exception('CleanStart: failed to list node pools')
    return
  if not stale:
    logging.info('CleanStart: no stale pools found — clean start confirmed.')
    return
  logging.warning('CleanStart: deleting %d stale pools: %s', len(stale), stale)
  background_tasks.RunThreaded(cluster.DeleteNodePool, stale)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the selected scenarios and returns flat list of samples."""
  cluster = benchmark_spec.container_cluster
  assert isinstance(cluster, kubernetes_cluster.KubernetesCluster)

  # Spec C.2: start clean.
  _CleanStartSweep(cluster)

  # Resolve versions once; log clearly; tag every sample.
  # Google spec: initial=N-1, target=N (adjacent minor upgrade).
  flag_initial = _INITIAL_VERSION.value
  flag_target  = _TARGET_VERSION.value
  if flag_initial and flag_target:
    initial, target = flag_initial, flag_target
    source = 'flags'
  else:
    resolved_initial, resolved_target = cluster.ResolveNodePoolVersions()
    initial = flag_initial or resolved_initial
    target  = flag_target  or resolved_target
    source  = 'auto-resolved' if not (flag_initial or flag_target) else 'mixed'

  logging.info(
      'NodePool versions (%s): initial=%s -> target=%s '
      '(cluster k8s_version=%s) | nodes_per_pool=%d | machine_type=%s',
      source, initial, target, cluster.k8s_version,
      _NODES_PER_NODEPOOL.value,
      cluster.default_nodepool.machine_type
      if hasattr(cluster, 'default_nodepool') else 'unknown',
  )

  scenarios = {s.strip().upper() for s in _SCENARIOS.value}
  samples: list[sample.Sample] = []

  if 'A' in scenarios:
    samples += _RunScenarioA(cluster, initial, target)
  if 'B' in scenarios:
    samples += _RunScenarioB(cluster, initial)
  if 'C' in scenarios:
    scales = (
        [int(x.strip()) for x in _SCALE_SWEEP.value]
        if _SCALE_SWEEP.value
        else [_LARGE_SCALE_NODEPOOLS.value]
    )
    logging.info('Scenario C: scale sweep = %s', scales)
    for scale in scales:
      scenario_c_samples = _RunScenarioC(cluster, initial, scale)
      for s in scenario_c_samples:
        s.metadata['scenario_c_scale'] = str(scale)
      samples += scenario_c_samples

  # Tag all samples with version path and run config for published results.
  run_meta = {
      'initial_version':      str(initial),
      'target_version':       str(target),
      'cluster_k8s_version':  str(cluster.k8s_version),
      'nodes_per_nodepool':   str(_NODES_PER_NODEPOOL.value),
      'concurrent_nodepools': str(_CONCURRENT_NODEPOOLS.value),
  }
  for s in samples:
    s.metadata.update(run_meta)

  return samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Best-effort delete of leftover benchmark node pools and sleep pod."""
  cluster = benchmark_spec.container_cluster
  if cluster is None:
    return
  kubectl.RunKubectlCommand(
      ['delete', 'pod', _SLEEP_POD_NAME, '--ignore-not-found'],
      raise_on_failure=False,
  )
  try:
    leftover = [
        n for n in cluster.GetNodePoolNames() if n.startswith(_PREFIX)
    ]
  except Exception:  # pylint: disable=broad-except
    logging.exception('Cleanup: failed to list node pools')
    return
  if not leftover:
    return
  logging.info('Cleanup: deleting %d leftover node pools', len(leftover))
  background_tasks.RunThreaded(cluster.DeleteNodePool, leftover)


# ---------------------------------------------------------------------------
# Scenario A
# ---------------------------------------------------------------------------

def _RunScenarioA(
    cluster: kubernetes_cluster.KubernetesCluster,
    initial: str,
    target: str,
) -> list[sample.Sample]:
  """Concurrent CreateNodePool, UpgradeNodePool, DeleteNodePool."""
  n = _CONCURRENT_NODEPOOLS.value
  if _PIPELINE_SCENARIO_A.value:
    logging.info(
        'Scenario A (pipelined): %d pools, initial=%s, target=%s', n, initial, target)
    return _RunScenarioAPipelined(cluster, n, initial, target)

  logging.info(
      'Scenario A (phase-by-phase): %d pools, initial=%s, target=%s', n, initial, target)
  pool_names = [_SCENARIO_A_NAME(i) for i in range(n)]
  configs_   = [_MakeNodePoolConfig(cluster, name) for name in pool_names]
  samples: list[sample.Sample] = []

  # ── Phase 1: concurrent creates ──────────────────────────────────────────
  create_results = _RunAsync(
      kickoff  = lambda cfg: cluster.CreateNodePoolAsync(cfg, node_version=initial),
      wait_fn  = cluster.WaitForOperation,
      items    = configs_,
      get_name = lambda cfg: cfg.name,
  )
  samples += _OpSamples('ScenarioA_Create', create_results,
                        attempted_ops=len(pool_names))

  # ── Phase 2: concurrent upgrades (only successfully created pools) ────────
  created = [name for name, _, _, err in create_results if err is None]
  logging.info('Scenario A: %d/%d pools created — proceeding to upgrade',
               len(created), n)
  upgrade_results = _RunAsync(
      kickoff  = lambda name: cluster.UpgradeNodePoolAsync(name, target),
      wait_fn  = cluster.WaitForOperation,
      items    = created,
      get_name = str,
  )
  samples += _OpSamples('ScenarioA_Upgrade', upgrade_results,
                        attempted_ops=len(created))
  # ── Dynamic Wait: Wait until the cluster control plane reports STATUS_RUNNING ──
  logging.info('Scenario A upgrades finished. Checking control plane status...')
  poll_start = time.time()
  timeout_seconds = 300
  status = None
  
  while status != 'RUNNING' and (time.time() - poll_start) < timeout_seconds:
    try:
      if hasattr(cluster, 'GetStatus'):
        status = cluster.GetStatus()
        logging.info('Current cluster control plane status: %s', status)
      else:
        logging.warning('Cluster provider does not support GetStatus(). Falling back to 30s cooldown.')
        time.sleep(30)
        break
    except Exception as e:
      logging.warning('Transient error querying cluster status: %s. Retrying...', e)
    
    # Only sleep if we need to poll again (status is still updating)
    if status != 'RUNNING':
      logging.info('Control plane busy or locking. Waiting 30 seconds before checking again...')
      time.sleep(30)
  if status == 'RUNNING':
    logging.info('Cluster control plane is stable and RUNNING. Proceeding to deletes.')
  else:
    logging.warning('Control plane did not return to RUNNING within safety limit. Proceeding anyway.')

  # ── Phase 3: concurrent deletes (live-list to catch EKS rollbacks) ────────
  alive = [p for p in cluster.GetNodePoolNames() if p.startswith(f'{_PREFIX}a')]
  logging.info('Scenario A: %d live pools found for delete (originally %d)',
               len(alive), n)
  delete_results = _RunAsync(
      kickoff  = cluster.DeleteNodePoolAsync,
      wait_fn  = cluster.WaitForOperation,
      items    = alive,
      get_name = str,
  )
  # attempted_ops=n: success rate reflects original request, not just live pools.
  # EKS rolls back timed-out pools silently — without this fix delete shows 100%.
  samples += _OpSamples('ScenarioA_Delete', delete_results,
                        attempted_ops=n)
  return samples


def _RunScenarioAPipelined(
    cluster: kubernetes_cluster.KubernetesCluster,
    n: int,
    initial: str,
    target: str,
) -> list[sample.Sample]:
  """Per-pool pipeline: create->upgrade->delete back-to-back per thread.

  Minimizes wall time: max_i(create_i + upgrade_i + delete_i) vs
  max(creates)+max(upgrades)+max(deletes) in phase-by-phase mode.
  Trade-off: ops run under mixed-type concurrent load.
  """
  pool_names = [_SCENARIO_A_NAME(i) for i in range(n)]
  creates  = _Results()
  upgrades = _Results()
  deletes  = _Results()

  def _do_pool(name: str):
    cfg = _MakeNodePoolConfig(cluster, name)
    init, e2e, err = _TimedAsync(
        lambda: cluster.CreateNodePoolAsync(cfg, node_version=initial),
        cluster.WaitForOperation,
    )
    creates.add(name, init, e2e, err)
    if err is not None:
      return
    init, e2e, err = _TimedAsync(
        lambda: cluster.UpgradeNodePoolAsync(name, target),
        cluster.WaitForOperation,
    )
    upgrades.add(name, init, e2e, err)
    init, e2e, err = _TimedAsync(
        lambda: cluster.DeleteNodePoolAsync(name),
        cluster.WaitForOperation,
    )
    deletes.add(name, init, e2e, err)

  background_tasks.RunThreaded(
      _do_pool, pool_names,
      max_concurrent_threads=min(n, _MAX_CONCURRENT.value),
  )
  samples: list[sample.Sample] = []
  samples += _OpSamples('ScenarioA_Create',  creates.entries,  attempted_ops=n)
  samples += _OpSamples('ScenarioA_Upgrade', upgrades.entries, attempted_ops=n)
  samples += _OpSamples('ScenarioA_Delete',  deletes.entries,  attempted_ops=n)
  return samples


# ---------------------------------------------------------------------------
# Scenario B
# ---------------------------------------------------------------------------

def _RunScenarioB(
    cluster: kubernetes_cluster.KubernetesCluster,
    initial: str,
) -> list[sample.Sample]:
  """CreateNodePool fired concurrently with a long-running cluster update.

  Both ops kick off async on separate threads; initiation + E2E latency
  recorded independently. Overlap window duration = ClusterUpdate E2E latency.
  """
  logging.info('Scenario B: overlapping cluster update + node-pool create')
  cfg = _MakeNodePoolConfig(cluster, _SCENARIO_B_NAME)
  results = _Results()

  def _do_cluster_update():
    init, e2e, err = _TimedAsync(
        cluster.UpdateClusterAsync, cluster.WaitForOperation)
    results.add('ScenarioB_ClusterUpdate', init, e2e, err)
    logging.info('Scenario B ClusterUpdate: init=%.2fs e2e=%.2fs ok=%s',
                 init, e2e, err is None)

  def _do_create():
    init, e2e, err = _TimedAsync(
        lambda: cluster.CreateNodePoolAsync(cfg, node_version=initial),
        cluster.WaitForOperation,
    )
    results.add('ScenarioB_NodePoolCreate', init, e2e, err)
    logging.info('Scenario B NodePoolCreate: init=%.2fs e2e=%.2fs ok=%s',
                 init, e2e, err is None)

  background_tasks.RunThreaded(lambda fn: fn(), [_do_cluster_update, _do_create])

  samples: list[sample.Sample] = []
  for entry in results.entries:
    name, init_dur, e2e_dur, err = entry
    samples += _OpSamples(name, [(name, init_dur, e2e_dur, err)], attempted_ops=1)

  # Remove test pool (best-effort).
  try:
    cluster.DeleteNodePool(_SCENARIO_B_NAME)
  except Exception:  # pylint: disable=broad-except
    logging.exception('Scenario B: failed to delete test pool')
  return samples


# ---------------------------------------------------------------------------
# Scenario C
# ---------------------------------------------------------------------------

def _RunScenarioC(
    cluster: kubernetes_cluster.KubernetesCluster,
    initial: str,
    scale: int,
) -> list[sample.Sample]:
  """Large-scale node-pool provisioning at a given scale.

  Streams all `scale` creates through a single executor capped at
  _MAX_CONCURRENT workers — as each op completes the next starts immediately
  (no batch barriers). Delete uses a live-list so EKS-rolled-back pools are
  excluded from the denominator correctly.
  """
  logging.info(
      'Scenario C: scale=%d, max_concurrent=%d, initial_version=%s',
      scale, _MAX_CONCURRENT.value, initial,
  )
  pool_names = [_SCENARIO_C_NAME(i) for i in range(scale)]
  configs_   = [_MakeNodePoolConfig(cluster, name) for name in pool_names]
  samples: list[sample.Sample] = []

  # ── Creates ───────────────────────────────────────────────────────────────
  create_results = _RunAsync(
      kickoff  = lambda cfg: cluster.CreateNodePoolAsync(
          cfg, node_version=initial),
      wait_fn  = cluster.WaitForOperation,
      items    = configs_,
      get_name = lambda cfg: cfg.name,
  )
  created_ok = sum(1 for _, _, _, err in create_results if err is None)
  logging.info('Scenario C scale=%d: %d/%d creates succeeded',
               scale, created_ok, scale)
  samples += _OpSamples('ScenarioC_Create', create_results,
                        attempted_ops=scale)

  # ── Deletes (live-list) ───────────────────────────────────────────────────
  alive = [p for p in cluster.GetNodePoolNames() if p.startswith(f'{_PREFIX}c')]
  logging.info(
      'Scenario C scale=%d: %d live pools for delete (originally requested %d; '
      '%d rolled back by cloud)',
      scale, len(alive), scale, scale - len(alive),
  )
  if not alive:
    logging.warning(
        'Scenario C scale=%d: 0 live pools — all timed-out creates were '
        'rolled back. Recording 0%% delete success rate.', scale)
    samples += _OpSamples('ScenarioC_Delete', [], attempted_ops=scale)
    return samples

  delete_results = _RunAsync(
      kickoff  = cluster.DeleteNodePoolAsync,
      wait_fn  = cluster.WaitForOperation,
      items    = alive,
      get_name = str,
  )
  # attempted_ops=scale: accurate rate against original request count.
  samples += _OpSamples('ScenarioC_Delete', delete_results,
                        attempted_ops=scale)
  return samples


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _Results:
  """Thread-safe collector for (name, init_latency, e2e_latency, error)."""

  def __init__(self):
    self._lock = threading.Lock()
    self.entries: list[tuple[str, float, float, Exception | None]] = []

  def add(self, name: str, init_dur: float, e2e_dur: float,
          err: Exception | None) -> None:
    with self._lock:
      self.entries.append((name, init_dur, e2e_dur, err))


def _TimedAsync(
    kickoff: Callable[[], str],
    wait_fn: Callable[[str], None],
) -> tuple[float, float, Exception | None]:
  """Runs kickoff() then wait_fn(handle); returns (init_lat, e2e_lat, err).

  init_lat = time for kickoff() to return (API accepted).
  e2e_lat  = total wall time including wait. On kickoff failure both are set
             to elapsed time at failure point.
  """
  init_start = time.time()
  try:
    handle = kickoff()
  except Exception as exc:  # pylint: disable=broad-except
    elapsed = time.time() - init_start
    return elapsed, elapsed, exc
  init_dur = time.time() - init_start
  try:
    wait_fn(handle)
    return init_dur, time.time() - init_start, None
  except Exception as exc:  # pylint: disable=broad-except
    return init_dur, time.time() - init_start, exc


def _RunAsync(
    kickoff: Callable,
    wait_fn: Callable[[str], None],
    items: list,
    get_name: Callable[[object], str],
) -> list[tuple[str, float, float, Exception | None]]:
  """Fires kickoff(item) concurrently for all items; returns timed results.

  Uses background_tasks.RunThreaded with a concurrency cap for streaming
  execution — completed ops free their slot immediately for the next one.
  """
  if not items:
    return []
  results = _Results()
  cap = min(len(items), _MAX_CONCURRENT.value)

  def _wrap(item):
    init_dur, e2e_dur, err = _TimedAsync(lambda: kickoff(item), wait_fn)
    name = get_name(item)
    results.add(name, init_dur, e2e_dur, err)
    logging.info('%s ok=%s initiation=%.2fs end_to_end=%.2fs',
                 name, err is None, init_dur, e2e_dur)

  background_tasks.RunThreaded(_wrap, items, max_concurrent_threads=cap)
  return results.entries


def _MakeNodePoolConfig(
    cluster: kubernetes_cluster.KubernetesCluster,
    name: str,
) -> container_lib.BaseNodePoolConfig:
  """Builds a node-pool config from the cluster's default pool."""
  cfg = copy.copy(cluster.default_nodepool)
  cfg.name       = name
  cfg.num_nodes  = _NODES_PER_NODEPOOL.value
  cfg.min_nodes  = _NODES_PER_NODEPOOL.value
  cfg.max_nodes  = _NODES_PER_NODEPOOL.value
  return cfg


def _OpSamples(
    metric_prefix: str,
    results: list[tuple[str, float, float, Exception | None]],
    attempted_ops: int = None,
) -> list[sample.Sample]:
  """Per-op + aggregate samples for initiation and end-to-end latency.

  Args:
    metric_prefix: prefix for all metric names.
    results:       list of (operation_name, init_lat, e2e_lat, err).
    attempted_ops: total ops originally requested. Used as the denominator
                   for SuccessRate so EKS-rolled-back pools (which never
                   appear in results) are counted as failures, not ignored.
                   If None, len(results) is used (original behavior).
  """
  samples: list[sample.Sample] = []
  init_latencies: list[float] = []
  e2e_latencies:  list[float] = []
  success = 0

  for name, init_dur, e2e_dur, err in results:
    meta = {'operation_name': name, 'success': str(err is None)}
    if err is not None:
      meta['error'] = str(err)[:200]
    else:
      success += 1
      init_latencies.append(init_dur)
      e2e_latencies.append(e2e_dur)
    samples.append(sample.Sample(
        f'{metric_prefix}_InitiationLatency', init_dur, 'seconds', dict(meta)))
    samples.append(sample.Sample(
        f'{metric_prefix}_EndToEndLatency',   e2e_dur,  'seconds', dict(meta)))

  # ── Success rate ──────────────────────────────────────────────────────────
  total    = attempted_ops if attempted_ops is not None else len(results)
  executed = len(results)
  if total > 0:
    samples.append(sample.Sample(
        f'{metric_prefix}_SuccessRate',
        100.0 * success / total,
        'percent',
        {
            'total_ops':      str(total),
            'executed_ops':   str(executed),
            'successful_ops': str(success),
            'skipped_ops':    str(total - executed),   # cloud-rolled-back ops
        },
    ))

  # ── Aggregate stats (successful ops only) ────────────────────────────────
  for phase_label, latencies in (
      ('InitiationLatency', init_latencies),
      ('EndToEndLatency',   e2e_latencies),
  ):
    if len(latencies) >= 2:
      samples += _AggregateSamples(metric_prefix, phase_label, latencies)
    if len(latencies) >= 4:
      samples += _OutlierSamples(metric_prefix, phase_label, latencies)

  return samples


def _AggregateSamples(
    metric_prefix: str, phase_label: str, latencies: list[float]
) -> list[sample.Sample]:
  """Emits Mean/StdDev/Min/Median/P90/P99/Max samples."""
  pcts = sample.PercentileCalculator(
      latencies, percentiles=(0, 50, 90, 99, 100))
  agg_meta = {'sample_count': str(len(latencies))}
  out: list[sample.Sample] = []
  for label, key in (
      ('Mean',   'average'),
      ('StdDev', 'stddev'),
      ('Min',    'p0'),
      ('Median', 'p50'),
      ('P90',    'p90'),
      ('P99',    'p99'),
      ('Max',    'p100'),
  ):
    if key in pcts:
      out.append(sample.Sample(
          f'{metric_prefix}_{phase_label}_{label}',
          pcts[key], 'seconds', agg_meta))
  return out


def _OutlierSamples(
    metric_prefix: str, phase_label: str, latencies: list[float]
) -> list[sample.Sample]:
  """Tukey IQR outlier detection; emits OutlierCount sample with metadata."""
  sorted_lats = sorted(latencies)
  n  = len(sorted_lats)
  q1 = sorted_lats[n // 4]
  q3 = sorted_lats[(3 * n) // 4]
  iqr = q3 - q1
  upper_fence = q3 + 1.5 * iqr
  lower_fence = q1 - 1.5 * iqr
  outliers = [v for v in latencies if v > upper_fence or v < lower_fence]
  meta = {
      'sample_count':  str(n),
      'q1':            f'{q1:.3f}',
      'q3':            f'{q3:.3f}',
      'iqr':           f'{iqr:.3f}',
      'upper_fence':   f'{upper_fence:.3f}',
      'lower_fence':   f'{lower_fence:.3f}',
      'outlier_values': ','.join(f'{v:.2f}' for v in outliers),
  }
  if outliers:
    logging.warning(
        '[Outliers] %s %s: %d outlier(s) detected: %s (fence: %.2f-%.2f)',
        metric_prefix, phase_label, len(outliers),
        [f'{v:.2f}s' for v in outliers], lower_fence, upper_fence,
    )
  return [sample.Sample(
      f'{metric_prefix}_{phase_label}_OutlierCount',
      len(outliers), 'count', meta)]
