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

Measures GKE/EKS/AKS control-plane API responsiveness via two scenarios:
  concurrent_node_pool_ops: concurrent node-pool create/delete.
  overlapping_cluster_update: node-pool create overlapping a cluster update.

Optimizations for minimum run time:
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
    create/delete, and overlapping cluster + node-pool ops. Focused on
    control-plane API responsiveness.
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
_VALID_SCENARIOS = frozenset({
    "concurrent_node_pool_ops",
    "overlapping_cluster_update",
})

# ── Shared flags (apply across all scenarios) ──
_SCENARIOS = flags.DEFINE_list(
    "k8s_mgmt_scenarios",
    [
        "concurrent_node_pool_ops",
        "overlapping_cluster_update",
    ],
    "Comma-separated subset of scenarios to run. Valid values: "
    + "concurrent_node_pool_ops, overlapping_cluster_update.",
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

# AKS caps node-pool names at 12 chars — keep all names within that limit.
_PREFIX = "pkbm"


def _ConcurrentPoolName(i: int) -> str:
  """Returns the i-th concurrent-ops pool name.

  Three-digit zero-padded so names stay within AKS's 12-char node-pool limit.

  Args:
    i: Zero-based index of the pool.

  Returns:
    Pool name string, e.g. 'pkbma001'.
  """
  return f"{_PREFIX}a{i:03d}"


_OVERLAPPING_POOL_NAME = f"{_PREFIX}b"


@dataclasses.dataclass
class OpTiming:
  """Latency of a single async management-plane operation.

  Pure timing data — the metric name is supplied by the sample builder, and
  failures abort the run rather than being recorded here (so there is no
  error field).

  Attributes:
    initiation_latency: Seconds from issuing the async API call until it is
      accepted and an operation handle is returned (time to *start*).
    end_to_end_latency: Seconds from issuing the call until the operation
      fully completes (initiation plus server-side execution).
  """

  initiation_latency: float
  end_to_end_latency: float


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
  if benchmark_config.container_cluster.type != "Kubernetes":
    raise errors.Config.InvalidValue(
        "kubernetes_management benchmark requires a Kubernetes"
        + " container cluster."
    )


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Deploys a sleep pod to confirm data-plane reachability."""
  cluster = benchmark_spec.container_cluster
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


def _ClearNodePools(cluster: kubernetes_cluster.KubernetesCluster) -> None:
  """Clears all pkbm* node pools, blocking until each delete completes.

  Called after each scenario so the next one starts from a clean cluster,
  and from Cleanup() as a final best-effort teardown.

  Args:
    cluster: The Kubernetes cluster whose pkbm* node pools will be deleted.
  """
  pools = [n for n in cluster.GetNodePoolNames() if n.startswith(_PREFIX)]
  if not pools:
    logging.info("Clear: no pkbm* pools present — cluster is clean.")
    return
  logging.info("Clear: deleting %d pkbm* pools: %s", len(pools), pools)
  background_tasks.RunThreaded(cluster.DeleteNodePool, pools)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the selected scenarios and returns flat list of samples."""
  cluster = benchmark_spec.container_cluster
  assert isinstance(cluster, kubernetes_cluster.KubernetesCluster)

  # Resolve the initial node-pool version once; log clearly; tag every sample.
  initial = _INITIAL_VERSION.value
  source = "flag" if initial else "auto-resolved"
  if not initial:
    initial, _ = cluster.ResolveNodePoolVersions()
  assert initial is not None

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
    samples += _RunConcurrentNodePoolOps(cluster, initial)
    # Each scenario leaves the cluster clean for the next one.
    _ClearNodePools(cluster)
  if "overlapping_cluster_update" in scenarios:
    samples += _RunOverlappingClusterUpdate(cluster, initial)
    _ClearNodePools(cluster)

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
  assert isinstance(cluster, kubernetes_cluster.KubernetesCluster)
  kubectl.RunKubectlCommand(
      ["delete", "pod", _SLEEP_POD_NAME, "--ignore-not-found"],
      raise_on_failure=False,
  )
  # Final teardown reuses the same sweep the scenarios use.
  _ClearNodePools(cluster)


def _RunConcurrentNodePoolOps(
    cluster: kubernetes_cluster.KubernetesCluster,
    initial: str,
) -> list[sample.Sample]:
  """Concurrent CreateNodePool then DeleteNodePool."""
  n = _CONCURRENT_NODEPOOLS.value
  logging.info("concurrent_node_pool_ops: %d pools, initial=%s", n, initial)
  pool_names = [_ConcurrentPoolName(i) for i in range(n)]
  configs_ = [_MakeNodePoolConfig(cluster, name) for name in pool_names]
  samples: list[sample.Sample] = []

  # ── Phase 1: concurrent creates (fail-hard — any failure aborts) ────────
  create_results = _RunAsync(
      kickoff=lambda cfg: cluster.CreateNodePoolAsync(
          cfg, node_version=initial
      ),
      wait_fn=cluster.WaitForOperation,
      items=configs_,
      get_name=lambda cfg: cfg.name,
  )
  samples += _OpSamples("ConcurrentOps_Create", create_results)

  # ── Phase 2: concurrent deletes (live-list; all creates succeeded) ──────
  alive = _LiveNodePoolNames(cluster, f"{_PREFIX}a")
  logging.info("concurrent_node_pool_ops: deleting %d pools", len(alive))
  delete_results = _RunAsync(
      kickoff=cluster.DeleteNodePoolAsync,
      wait_fn=cluster.WaitForOperation,
      items=alive,
      get_name=str,
  )
  samples += _OpSamples("ConcurrentOps_Delete", delete_results)
  return samples


def _RunOverlappingClusterUpdate(
    cluster: kubernetes_cluster.KubernetesCluster,
    initial: str,
) -> list[sample.Sample]:
  """CreateNodePool fired concurrently with a long-running cluster update.

  Both ops kick off async on separate threads; initiation + E2E latency
  recorded independently. Overlap window = ClusterUpdate E2E latency.

  Args:
    cluster: The Kubernetes cluster to run the scenario against.
    initial: The initial (N-1) Kubernetes version for the node pool.

  Returns:
    List of samples with initiation and end-to-end latency for both ops.
  """
  logging.info(
      "overlapping_cluster_update: cluster update + node-pool create"
  )
  cfg = _MakeNodePoolConfig(cluster, _OVERLAPPING_POOL_NAME)
  results = ThreadSafeResults()

  def DoClusterUpdate():
    timing = _TimedAsync(cluster.UpdateClusterAsync, cluster.WaitForOperation)
    results.Add("OverlappingUpdate_ClusterUpdate", timing)
    logging.info(
        "overlapping_cluster_update ClusterUpdate: init=%.2fs e2e=%.2fs",
        timing.initiation_latency,
        timing.end_to_end_latency,
    )

  def DoCreate():
    timing = _TimedAsync(
        lambda: cluster.CreateNodePoolAsync(cfg, node_version=initial),
        cluster.WaitForOperation,
    )
    results.Add("OverlappingUpdate_NodePoolCreate", timing)
    logging.info(
        "overlapping_cluster_update NodePoolCreate: init=%.2fs e2e=%.2fs",
        timing.initiation_latency,
        timing.end_to_end_latency,
    )

  background_tasks.RunThreaded(lambda fn: fn(), [DoClusterUpdate, DoCreate])

  samples: list[sample.Sample] = []
  for name, timing in results.entries:
    samples += _OpSamples(name, [(name, timing)])

  # Remove test pool (best-effort).
  cluster.DeleteNodePool(_OVERLAPPING_POOL_NAME)
  return samples


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class ThreadSafeResults:
  """Thread-safe collector of (name, OpTiming) pairs from concurrent ops."""

  def __init__(self):
    self._lock = threading.Lock()
    self.entries: list[tuple[str, OpTiming]] = []
    self.failed: list[str] = []

  def Add(self, name: str, timing: OpTiming) -> None:
    with self._lock:
      self.entries.append((name, timing))

  def AddFailure(self, name: str) -> None:
    with self._lock:
      self.failed.append(name)


def _TimedAsync(
    kickoff: Callable[[], str],
    wait_fn: Callable[[str], None],
) -> OpTiming:
  """Runs kickoff() then wait_fn(handle); returns the OpTiming.

  Lets exceptions propagate — a failed management-plane op aborts the
  benchmark rather than being silently absorbed. initiation_latency is the
  time for kickoff() to return (API accepted); end_to_end_latency is total
  wall time including the wait.

  Args:
    kickoff: Zero-arg callable that fires the async operation and returns
      an opaque handle.
    wait_fn: Callable that blocks until the operation identified by the
      handle reaches a terminal state.

  Returns:
    OpTiming with initiation_latency and end_to_end_latency in seconds.
  """
  init_start = time.monotonic()
  handle = kickoff()
  initiation_latency = time.monotonic() - init_start
  wait_fn(handle)
  end_to_end_latency = time.monotonic() - init_start
  return OpTiming(initiation_latency, end_to_end_latency)


def _RunAsync(
    kickoff: Callable[..., str],
    wait_fn: Callable[[str], None],
    items: list[object],
    get_name: Callable[[object], str],
) -> list[tuple[str, OpTiming]]:
  """Fires kickoff(item) concurrently; returns (name, OpTiming) per item.

  Fail-hard: any op that raises aborts the run (RunThreaded propagates the
  exception). Used by the create/upgrade/delete scenarios where a single
  failure is a benchmark failure. Uses a concurrency cap for streaming
  execution — completed ops free their slot immediately for the next one.

  Args:
    kickoff: Callable that accepts one item and fires the async operation,
      returning an opaque handle.
    wait_fn: Callable that blocks until the operation handle reaches a
      terminal state.
    items: List of items to process concurrently (one op per item).
    get_name: Callable that maps an item to its string name for tagging
      result samples.

  Returns:
    List of (name, OpTiming) pairs, one per item in the same order.
  """
  if not items:
    return []
  results = ThreadSafeResults()
  cap = min(len(items), _MAX_CONCURRENT.value)

  def DoWrap(item):
    timing = _TimedAsync(lambda: kickoff(item), wait_fn)
    name = get_name(item)
    results.Add(name, timing)
    logging.info(
        "%s initiation=%.2fs end_to_end=%.2fs",
        name,
        timing.initiation_latency,
        timing.end_to_end_latency,
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


def _LiveNodePoolNames(
    cluster: kubernetes_cluster.KubernetesCluster, prefix: str
) -> list[str]:
  """Returns current node-pool names matching the given prefix."""
  return [p for p in cluster.GetNodePoolNames() if p.startswith(prefix)]


def _OpSamples(
    metric_prefix: str,
    results: list[tuple[str, OpTiming]],
) -> list[sample.Sample]:
  """Per-op + aggregate latency samples for fail-hard scenarios.

  Every op in `results` succeeded (a failure would have aborted the run), so
  there is no success-rate or error accounting here — just initiation and
  end-to-end latency per op, plus aggregate stats.

  Args:
    metric_prefix: prefix for all metric names.
    results: (name, OpTiming) pairs from _RunAsync.

  Returns:
    List of Sample objects with per-op and aggregate latency metrics.
  """
  samples: list[sample.Sample] = []
  init_latencies: list[float] = []
  e2e_latencies: list[float] = []

  for name, timing in results:
    meta = {"operation_name": name}
    init_latencies.append(timing.initiation_latency)
    e2e_latencies.append(timing.end_to_end_latency)
    samples.append(
        sample.Sample(
            f"{metric_prefix}_InitiationLatency",
            timing.initiation_latency,
            "seconds",
            dict(meta),
        )
    )
    samples.append(
        sample.Sample(
            f"{metric_prefix}_EndToEndLatency",
            timing.end_to_end_latency,
            "seconds",
            dict(meta),
        )
    )

  samples += _AggregateAndOutlierSamples(
      metric_prefix, init_latencies, e2e_latencies
  )
  return samples


def _AggregateAndOutlierSamples(
    metric_prefix: str,
    init_latencies: list[float],
    e2e_latencies: list[float],
) -> list[sample.Sample]:
  """Emits aggregate stats (>=2 samples) and outlier counts (>=4 samples)."""
  samples: list[sample.Sample] = []
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
