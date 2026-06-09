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
  concurrent_node_pool_ops: concurrent node-pool create/delete.
  overlapping_cluster_update: node-pool create overlapping a cluster update.
  large_scale_provisioning: large-scale node-pool provisioning (scale/sweep).

Optimizations for minimum run time:
  - Streaming concurrency in large_scale_provisioning (no batch barriers)
  - Reduced poll_interval in provider WaitForOperation (5s vs 10s)
  - Per-op threads capped at _MAX_CONCURRENT to avoid OS limits
  - Accurate delete success rate via attempted_ops denominator
"""

import copy
import dataclasses
import statistics
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
from perfkitbenchmarker.resources.container_service import (
    container as container_lib,
)
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_cluster

_SLEEP_POD_NAME = "pkb-mgmt-sleep"

BENCHMARK_NAME = "kubernetes_management"

BENCHMARK_CONFIG = """
kubernetes_management:
  description: >
    Benchmarks GKE/EKS/AKS management plane operations: concurrent node pool
    create/delete, overlapping cluster + node-pool ops, and large-scale
    provisioning. Focused on control-plane API responsiveness.
  container_cluster:
    type: Kubernetes
    vm_count: 1
    vm_spec: *default_dual_core
"""

# Scenarios measured by this benchmark (select via --k8s_mgmt_scenarios):
#   concurrent_node_pool_ops: concurrently create and delete N node pools;
#     measures control-plane throughput under parallel ops.
#   overlapping_cluster_update: run a cluster update and a node-pool create
#     simultaneously; measures behaviour when a cluster-scoped op overlaps a
#     node-pool-scoped one.
#   large_scale_provisioning: create then delete a large number of node pools
#     (optionally swept via --k8s_mgmt_scale_sweep); measures scaling limits
#     and large-batch provisioning latency.
_VALID_SCENARIOS = frozenset({
    "concurrent_node_pool_ops",
    "overlapping_cluster_update",
    "large_scale_provisioning",
})

# ── Shared flags (apply across all scenarios) ──
_SCENARIOS = flags.DEFINE_list(
    "k8s_mgmt_scenarios",
    [
        "concurrent_node_pool_ops",
        "overlapping_cluster_update",
        "large_scale_provisioning",
    ],
    "Comma-separated subset of scenarios to run. Valid values: "
    + "concurrent_node_pool_ops, overlapping_cluster_update, "
    + "large_scale_provisioning.",
)
_NODES_PER_NODEPOOL = flags.DEFINE_integer(
    "k8s_mgmt_nodes_per_nodepool",
    2,
    "Number of nodes per node pool. Google spec: 2 nodes per pool.",
)
_MAX_CONCURRENT = flags.DEFINE_integer(
    "k8s_mgmt_max_concurrent",
    50,
    "Cap on concurrent provider API calls within a batch. "
    + "Higher = faster but more aggressive on connection pools.",
)

# ── concurrent_node_pool_ops flags ──
_CONCURRENT_NODEPOOLS = flags.DEFINE_integer(
    "k8s_mgmt_concurrent_nodepools",
    5,
    "Number of node pools to create and delete concurrently in the "
    + "concurrent_node_pool_ops scenario.",
)
_INITIAL_VERSION = flags.DEFINE_string(
    "k8s_mgmt_initial_version",
    None,
    "Kubernetes version for newly-created node pools (N-1). None = auto.",
)

# ── large_scale_provisioning flags ──
_LARGE_SCALE_NODEPOOLS = flags.DEFINE_integer(
    "k8s_mgmt_large_scale_nodepools",
    1000,
    "Number of node pools to provision in the large_scale_provisioning "
    + "scenario. Spec target is 1000; ensure VPC/quota is available before "
    + "running.",
)
_SCALE_SWEEP = flags.DEFINE_list(
    "k8s_mgmt_scale_sweep",
    [],
    "Comma-separated list of node-pool counts for the large_scale_provisioning "
    + "scale sweep. Each scale runs as a separate sub-run with full "
    + "create/delete cycle. Example:"
    " --k8s_mgmt_scale_sweep=10,50,100,500,1000. "
    + "If empty, uses --k8s_mgmt_large_scale_nodepools.",
)

# AKS caps node-pool names at 12 chars — keep all names within that limit.
_PREFIX = "pkbm"


def _ScenarioAName(i):
  return f"{_PREFIX}a{i:03d}"


_SCENARIO_B_NAME = f"{_PREFIX}b"


def _ScenarioCName(i):
  return f"{_PREFIX}c{i:04d}"


@dataclasses.dataclass
class _OpResult:
  """Holds timing and outcome for a single async management-plane operation."""

  name: str
  init_dur: float
  e2e_dur: float
  error: Exception | None = None

  def __iter__(self):
    yield self.name
    yield self.init_dur
    yield self.e2e_dur
    yield self.error


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(
    benchmark_config: benchmark_config_spec.BenchmarkConfigSpec,
):
  """Validates flag values and cluster type before any cloud calls."""
  invalid = [s for s in _SCENARIOS.value if s.strip() not in _VALID_SCENARIOS]
  if invalid:
    raise errors.Config.InvalidValue(
        f"Invalid value(s) for --k8s_mgmt_scenarios: {invalid}. "
        + f"Valid options: {sorted(_VALID_SCENARIOS)}."
    )
  selected = {s.strip() for s in _SCENARIOS.value}
  if _SCALE_SWEEP.value and "large_scale_provisioning" not in selected:
    raise errors.Config.InvalidValue(
        "--k8s_mgmt_scale_sweep applies only to the large_scale_provisioning "
        + "scenario, which is not selected."
    )
  for s in _SCALE_SWEEP.value:
    try:
      int(s.strip())
    except ValueError as e:
      raise errors.Config.InvalidValue(
          f"Non-integer value in --k8s_mgmt_scale_sweep: {s!r}"
      ) from e
  if benchmark_config.container_cluster.type != "Kubernetes":
    raise errors.Config.InvalidValue(
        "kubernetes_management benchmark requires a Kubernetes"
        + " container cluster."
    )


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Deploys a sleep pod to confirm data-plane reachability."""
  cluster = benchmark_spec.container_cluster
  # Type narrowing for pytype; reachability is confirmed by the sleep pod below.
  assert isinstance(cluster, kubernetes_cluster.KubernetesCluster)
  benchmark_spec.always_call_cleanup = True
  logging.info(
      "kubernetes_management Prepare: cluster=%s, version=%s",
      cluster.name,
      cluster.k8s_version,
  )
  # Spec workload: "a simple container that sleeps for a given time".
  # Confirms data-plane reachability; generates no data-plane load.
  kubectl.RunKubectlCommand(
      [
          "run",
          _SLEEP_POD_NAME,
          "--image=busybox",
          "--restart=Never",
          "--",
          "sleep",
          "86400",
      ],
  )


def _CleanStartSweep(cluster: kubernetes_cluster.KubernetesCluster) -> None:
  """Deletes any stale pkbm* node pools so each run starts clean (spec C.2)."""
  stale = [n for n in cluster.GetNodePoolNames() if n.startswith(_PREFIX)]
  if not stale:
    logging.info("CleanStart: no stale pools found — clean start confirmed.")
    return
  logging.info("CleanStart: deleting %d stale pools: %s", len(stale), stale)
  background_tasks.RunThreaded(cluster.DeleteNodePool, stale)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the selected scenarios and returns flat list of samples."""
  cluster = benchmark_spec.container_cluster
  assert isinstance(cluster, kubernetes_cluster.KubernetesCluster)

  # Spec C.2: start clean.
  _CleanStartSweep(cluster)

  # Resolve the initial node-pool version once; log clearly; tag every sample.
  flag_initial = _INITIAL_VERSION.value
  if not flag_initial:
    resolved_initial, _ = cluster.ResolveNodePoolVersions()
    flag_initial = resolved_initial
  initial = flag_initial
  source = "flag" if _INITIAL_VERSION.value else "auto-resolved"

  logging.info(
      "NodePool version (%s): initial=%s "
      + "(cluster k8s_version=%s) | nodes_per_pool=%d | machine_type=%s",
      source,
      initial,
      cluster.k8s_version,
      _NODES_PER_NODEPOOL.value,
      cluster.default_nodepool.machine_type
      if hasattr(cluster, "default_nodepool")
      else "unknown",
  )

  scenarios = {s.strip() for s in _SCENARIOS.value}
  samples: list[sample.Sample] = []

  if "concurrent_node_pool_ops" in scenarios:
    samples += _RunScenarioA(cluster, initial)
  if "overlapping_cluster_update" in scenarios:
    samples += _RunScenarioB(cluster, initial)
  if "large_scale_provisioning" in scenarios:
    # fix: Scenario A/B pools may still be in Deleting state and count
    # toward AKS's 100-pool cluster limit.  Sweep them out before Scenario C
    # so we don't hit MaxAgentPoolCountReached mid-run.
    _CleanStartSweep(cluster)
    scales = (
        [int(x.strip()) for x in _SCALE_SWEEP.value]
        if _SCALE_SWEEP.value
        else [_LARGE_SCALE_NODEPOOLS.value]
    )
    logging.info("Scenario C: scale sweep = %s", scales)
    for scale in scales:
      scenario_c_samples = _RunScenarioC(cluster, initial, scale)
      for s in scenario_c_samples:
        s.metadata["scenario_c_scale"] = str(scale)
      samples += scenario_c_samples

  # Tag all samples with version path and run config for published results.
  run_meta = {
      "initial_version": str(initial),
      "cluster_k8s_version": str(cluster.k8s_version),
      "nodes_per_nodepool": str(_NODES_PER_NODEPOOL.value),
      "concurrent_nodepools": str(_CONCURRENT_NODEPOOLS.value),
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
      ["delete", "pod", _SLEEP_POD_NAME, "--ignore-not-found"],
      raise_on_failure=False,
  )
  leftover = [n for n in cluster.GetNodePoolNames() if n.startswith(_PREFIX)]
  if not leftover:
    return
  logging.info("Cleanup: deleting %d leftover node pools", len(leftover))
  background_tasks.RunThreaded(cluster.DeleteNodePool, leftover)


# ---------------------------------------------------------------------------
# Scenario A
# ---------------------------------------------------------------------------


def _RunScenarioA(
    cluster: kubernetes_cluster.KubernetesCluster,
    initial: str,
) -> list[sample.Sample]:
  """Concurrent CreateNodePool then DeleteNodePool."""
  n = _CONCURRENT_NODEPOOLS.value
  logging.info("concurrent_node_pool_ops: %d pools, initial=%s", n, initial)
  pool_names = [_ScenarioAName(i) for i in range(n)]
  configs_ = [_MakeNodePoolConfig(cluster, name) for name in pool_names]
  samples: list[sample.Sample] = []

  # ── Phase 1: concurrent creates ─────────────────────────────────────────
  create_results = _RunAsync(
      kickoff=lambda cfg: cluster.CreateNodePoolAsync(
          cfg, node_version=initial
      ),
      wait_fn=cluster.WaitForOperation,
      items=configs_,
      get_name=lambda cfg: cfg.name,
  )
  samples += _OpSamples(
      "ScenarioA_Create", create_results, attempted_ops=len(pool_names)
  )

  # ── Phase 2: concurrent deletes (live-list to catch EKS rollbacks) ──────
  alive = [p for p in cluster.GetNodePoolNames() if p.startswith(f"{_PREFIX}a")]
  logging.info(
      "concurrent_node_pool_ops: %d live pools for delete (originally %d)",
      len(alive),
      n,
  )
  delete_results = _RunAsync(
      kickoff=cluster.DeleteNodePoolAsync,
      wait_fn=cluster.WaitForOperation,
      items=alive,
      get_name=str,
  )
  # attempted_ops=n: success rate reflects original request, not just live.
  # EKS rolls back timed-out pools silently — without this shows 100%.
  samples += _OpSamples("ScenarioA_Delete", delete_results, attempted_ops=n)
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
  recorded independently. Overlap window = ClusterUpdate E2E latency.
  """
  logging.info("Scenario B: overlapping cluster update + node-pool create")
  cfg = _MakeNodePoolConfig(cluster, _SCENARIO_B_NAME)
  results = _Results()

  def DoClusterUpdate():
    init, e2e, err = _TimedAsync(
        cluster.UpdateClusterAsync, cluster.WaitForOperation
    )
    results.add("ScenarioB_ClusterUpdate", init, e2e, err)
    logging.info(
        "Scenario B ClusterUpdate: init=%.2fs e2e=%.2fs ok=%s",
        init,
        e2e,
        err is None,
    )

  def DoCreate():
    init, e2e, err = _TimedAsync(
        lambda: cluster.CreateNodePoolAsync(cfg, node_version=initial),
        cluster.WaitForOperation,
    )
    results.add("ScenarioB_NodePoolCreate", init, e2e, err)
    logging.info(
        "Scenario B NodePoolCreate: init=%.2fs e2e=%.2fs ok=%s",
        init,
        e2e,
        err is None,
    )

  background_tasks.RunThreaded(lambda fn: fn(), [DoClusterUpdate, DoCreate])

  samples: list[sample.Sample] = []
  for entry in results.entries:
    samples += _OpSamples(entry.name, [entry], attempted_ops=1)

  # Remove test pool (best-effort).
  cluster.DeleteNodePool(_SCENARIO_B_NAME)
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
      "Scenario C: scale=%d, max_concurrent=%d, initial_version=%s",
      scale,
      _MAX_CONCURRENT.value,
      initial,
  )
  pool_names = [_ScenarioCName(i) for i in range(scale)]
  configs_ = [_MakeNodePoolConfig(cluster, name) for name in pool_names]
  samples: list[sample.Sample] = []

  # ── Creates ──────────────────────────────────────────────────────────────
  create_results = _RunAsync(
      kickoff=lambda cfg: cluster.CreateNodePoolAsync(
          cfg, node_version=initial
      ),
      wait_fn=cluster.WaitForOperation,
      items=configs_,
      get_name=lambda cfg: cfg.name,
  )
  created_ok = sum(1 for r in create_results if r.error is None)
  logging.info(
      "Scenario C scale=%d: %d/%d creates succeeded", scale, created_ok, scale
  )
  samples += _OpSamples("ScenarioC_Create", create_results, attempted_ops=scale)

  # ── Deletes (live-list) ──────────────────────────────────────────────────
  alive = [p for p in cluster.GetNodePoolNames() if p.startswith(f"{_PREFIX}c")]
  logging.info(
      "Scenario C scale=%d: %d live pools for delete (originally %d;"
      + " %d rolled back by cloud)",
      scale,
      len(alive),
      scale,
      scale - len(alive),
  )
  if not alive:
    logging.info("Scenario C scale=%d: all creates rolled back.", scale)
    samples += _OpSamples("ScenarioC_Delete", [], attempted_ops=scale)
    return samples

  delete_results = _RunAsync(
      kickoff=cluster.DeleteNodePoolAsync,
      wait_fn=cluster.WaitForOperation,
      items=alive,
      get_name=str,
  )
  # attempted_ops=scale: accurate rate against original request count.
  samples += _OpSamples("ScenarioC_Delete", delete_results, attempted_ops=scale)
  return samples


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Results:
  """Thread-safe collector for (name, init_latency, e2e_latency, error)."""

  def __init__(self):
    self._lock = threading.Lock()
    self.entries: list[_OpResult] = []

  def add(
      self, name: str, init_dur: float, e2e_dur: float, err: Exception | None
  ) -> None:
    result = _OpResult(name, init_dur, e2e_dur, err)
    with self._lock:
      self.entries.append(result)


def _TimedAsync(
    kickoff: Callable[[], str],
    wait_fn: Callable[[str], None],
) -> tuple[float, float, Exception | None]:
  """Runs kickoff() then wait_fn(handle); returns (init_lat, e2e_lat, err).

  init_lat = time for kickoff() to return (API accepted).
  e2e_lat  = total wall time including wait. On kickoff failure both are set
             to elapsed time at failure point.
  """
  init_start = time.monotonic()
  try:
    handle = kickoff()
  except Exception as exc:  # pylint: disable=broad-except
    elapsed = time.monotonic() - init_start
    return elapsed, elapsed, exc
  init_dur = time.monotonic() - init_start
  try:
    wait_fn(handle)
    return init_dur, time.monotonic() - init_start, None
  except Exception as exc:  # pylint: disable=broad-except
    return init_dur, time.monotonic() - init_start, exc


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

  def DoWrap(item):
    init_dur, e2e_dur, err = _TimedAsync(lambda: kickoff(item), wait_fn)
    name = get_name(item)
    results.add(name, init_dur, e2e_dur, err)
    logging.info(
        "%s ok=%s initiation=%.2fs end_to_end=%.2fs",
        name,
        err is None,
        init_dur,
        e2e_dur,
    )

  background_tasks.RunThreaded(DoWrap, items, max_concurrent_threads=cap)
  return results.entries


def _MakeNodePoolConfig(
    cluster: kubernetes_cluster.KubernetesCluster,
    name: str,
) -> container_lib.BaseNodePoolConfig:
  """Builds a node-pool config from the cluster's default pool."""
  cfg = copy.copy(cluster.default_nodepool)
  cfg.name = name
  cfg.num_nodes = _NODES_PER_NODEPOOL.value
  cfg.min_nodes = _NODES_PER_NODEPOOL.value
  cfg.max_nodes = _NODES_PER_NODEPOOL.value
  return cfg


def _OpSamples(
    metric_prefix: str,
    results: list[_OpResult],
    attempted_ops: int | None = None,
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
  e2e_latencies: list[float] = []
  success = 0

  for r in results:
    if isinstance(r, tuple):
      r = _OpResult(*r)
    meta = {"operation_name": r.name, "success": str(r.error is None)}
    if r.error is not None:
      meta["error"] = str(r.error)[:200]
    else:
      success += 1
      init_latencies.append(r.init_dur)
      e2e_latencies.append(r.e2e_dur)
    samples.append(
        sample.Sample(
            f"{metric_prefix}_InitiationLatency",
            r.init_dur,
            "seconds",
            dict(meta),
        )
    )
    samples.append(
        sample.Sample(
            f"{metric_prefix}_EndToEndLatency", r.e2e_dur, "seconds", dict(meta)
        )
    )

  # ── Success rate ─────────────────────────────────────────────────────────
  total = attempted_ops if attempted_ops is not None else len(results)
  executed = len(results)
  if total > 0:
    samples.append(
        sample.Sample(
            f"{metric_prefix}_SuccessRate",
            100.0 * success / total,
            "percent",
            {
                "total_ops": str(total),
                "executed_ops": str(executed),
                "successful_ops": str(success),
                "skipped_ops": str(total - executed),
            },
        )
    )

  # ── Aggregate stats (successful ops only) ────────────────────────────────
  for phase_label, latencies in (
      ("InitiationLatency", init_latencies),
      ("EndToEndLatency", e2e_latencies),
  ):
    if len(latencies) >= 2:
      samples += _AggregateSamples(metric_prefix, phase_label, latencies)
    if len(latencies) >= 4:
      samples += _OutlierSamples(metric_prefix, phase_label, latencies)

  return samples


def _AggregateSamples(
    metric_prefix: str, phase_label: str, latencies: list[float]
) -> list[sample.Sample]:
  """Emits Mean/StdDev/Min/Median/P90/P99/Max samples for a latency series."""
  n = len(latencies)
  meta = {"sample_count": str(n)}

  # statistics.quantiles with method='inclusive' matches linear interpolation
  # and returns n-1 cut points; index 89→P90, 98→P99.
  quantiles = statistics.quantiles(latencies, n=100, method="inclusive")

  stats = [
      ("Mean", statistics.mean(latencies)),
      ("StdDev", statistics.pstdev(latencies)),
      ("Min", min(latencies)),
      ("Median", statistics.median(latencies)),
      ("P90", quantiles[89]),
      ("P99", quantiles[98]),
      ("Max", max(latencies)),
  ]
  result = []
  for label, value in stats:
    result.append(
        sample.Sample(
            f"{metric_prefix}_{phase_label}_{label}",
            value,
            "seconds",
            dict(meta),
        )
    )
  return result


def _OutlierSamples(
    metric_prefix: str, phase_label: str, latencies: list[float]
) -> list[sample.Sample]:
  """Emits a single OutlierCount sample using IQR-fence outlier detection."""
  # statistics.quantiles(n=4) returns [Q1, Q2, Q3]; indices 0 and 2.
  quartiles = statistics.quantiles(latencies, n=4, method="inclusive")
  q1, q3 = quartiles[0], quartiles[2]
  iqr = q3 - q1
  lower_fence = q1 - 1.5 * iqr
  upper_fence = q3 + 1.5 * iqr
  outlier_count = sum(
      1 for v in latencies if v < lower_fence or v > upper_fence
  )
  meta = {
      "q1": str(q1),
      "q3": str(q3),
      "iqr": str(iqr),
      "upper_fence": str(upper_fence),
      "lower_fence": str(lower_fence),
      "sample_count": str(len(latencies)),
  }
  return [
      sample.Sample(
          f"{metric_prefix}_{phase_label}_OutlierCount",
          outlier_count,
          "count",
          meta,
      )
  ]
