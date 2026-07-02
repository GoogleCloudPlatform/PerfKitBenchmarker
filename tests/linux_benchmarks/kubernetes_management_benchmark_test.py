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
"""Tests for linux_benchmarks.kubernetes_management_benchmark."""

import threading
import time
import unittest

# pylint: disable=invalid-name,protected-access
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import kubernetes_management_benchmark
from perfkitbenchmarker.resources.container_service import kubernetes_cluster
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_CLUSTER_NAME = 'test-cluster'


def _make_sample(metric, value, unit='seconds', metadata=None):
  return sample.Sample(metric, value, unit, metadata or {})


def _make_mock_cluster(
    name=_CLUSTER_NAME,
    k8s_version='1.34',
    pool_names=None,
):
  """Creates a fully-stubbed KubernetesCluster mock for use in tests."""
  cluster = mock.create_autospec(
      kubernetes_cluster.KubernetesCluster, instance=True
  )
  cluster.name = name
  cluster.k8s_version = k8s_version
  cluster.cluster_version = k8s_version
  cluster.GetNodePoolNames.return_value = pool_names or []
  cluster.ResolveNodePoolVersions.return_value = ('1.33', '1.34')
  cluster.CreateNodePoolAsync.return_value = 'op-create-1'
  cluster.UpgradeNodePoolAsync.return_value = 'op-upgrade-1'
  cluster.DeleteNodePoolAsync.return_value = 'op-delete-1'
  cluster.UpdateClusterAsync.return_value = 'op-update-1'
  cluster.WaitForOperation.return_value = None
  default_np = mock.MagicMock()
  default_np.machine_type = 'e2-standard-2'
  default_np.num_nodes = 1
  default_np.min_nodes = 1
  default_np.max_nodes = 1
  default_np.zone = 'us-central1-a'
  default_np.disk_size = 100
  default_np.name = 'default-pool'
  cluster.default_nodepool = default_np
  return cluster


def _make_mock_benchmark_spec(cluster=None):
  spec = mock.MagicMock()
  spec.container_cluster = cluster or _make_mock_cluster()
  return spec


def _make_mock_config(cluster_type='Kubernetes'):
  cfg = mock.MagicMock()
  cfg.container_cluster.type = cluster_type
  return cfg


class NodePoolNameTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the node-pool name-generation helpers."""

  @parameterized.named_parameters(
      ('zero', 0, 'pkbma000'),
      ('two_digit', 42, 'pkbma042'),
      ('max_three_digit', 999, 'pkbma999'),
  )
  def testConcurrentPoolNameZeroPadsToThreeDigits(self, index, expected):
    self.assertEqual(
        expected, kubernetes_management_benchmark._ConcurrentPoolName(index)
    )

  def testOverlappingPoolNameIsConstant(self):
    self.assertEqual(
        'pkbmb', kubernetes_management_benchmark._OVERLAPPING_POOL_NAME
    )

  def testAllNamesWithinAksLimit(self):
    for i in range(1000):
      self.assertLessEqual(
          len(kubernetes_management_benchmark._ConcurrentPoolName(i)), 12
      )
    self.assertLessEqual(
        len(kubernetes_management_benchmark._OVERLAPPING_POOL_NAME), 12
    )


class CheckPrerequisitesTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the CheckPrerequisites validation function."""

  def testValidScenariosPass(self):
    with flagsaver.flagsaver(
        k8s_mgmt_scenarios=[
            'concurrent_node_pool_ops',
            'overlapping_cluster_update',
        ]
    ):
      kubernetes_management_benchmark.CheckPrerequisites(_make_mock_config())

  def testInvalidScenarioRaises(self):
    with flagsaver.flagsaver(k8s_mgmt_scenarios=['X']):
      with self.assertRaises(errors.Config.InvalidValue):
        kubernetes_management_benchmark.CheckPrerequisites(_make_mock_config())

  def testMixedValidInvalidRaises(self):
    with flagsaver.flagsaver(
        k8s_mgmt_scenarios=['concurrent_node_pool_ops', 'Z']
    ):
      with self.assertRaises(errors.Config.InvalidValue):
        kubernetes_management_benchmark.CheckPrerequisites(_make_mock_config())

  def testNonKubernetesClusterTypeRaises(self):
    with flagsaver.flagsaver(k8s_mgmt_scenarios=['concurrent_node_pool_ops']):
      with self.assertRaises(errors.Config.InvalidValue):
        kubernetes_management_benchmark.CheckPrerequisites(
            _make_mock_config(cluster_type='Mesos')
        )

  def testLowercaseScenarioRaises(self):
    with flagsaver.flagsaver(k8s_mgmt_scenarios=['a']):
      with self.assertRaises(errors.Config.InvalidValue):
        kubernetes_management_benchmark.CheckPrerequisites(_make_mock_config())


class PrepareTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the Prepare benchmark lifecycle function."""

  def _patch_kubectl(self, rc=0):
    return mock.patch(
        'perfkitbenchmarker.resources.container_service.kubectl'
        + '.RunKubectlCommand',
        return_value=('', '', rc),
    )

  def testPrepareRunsKubectlSleepPod(self):
    cluster = _make_mock_cluster()
    bm_spec = _make_mock_benchmark_spec(cluster)
    with self._patch_kubectl() as mock_kubectl:
      kubernetes_management_benchmark.Prepare(bm_spec)
      mock_kubectl.assert_called_once()
      args = mock_kubectl.call_args[0][0]
      self.assertIn('run', args)
      self.assertIn('pkb-mgmt-sleep', args)
      self.assertIn('sleep', args)

  def testPrepareSetsAlwaysCallCleanup(self):
    cluster = _make_mock_cluster()
    bm_spec = _make_mock_benchmark_spec(cluster)
    with self._patch_kubectl():
      kubernetes_management_benchmark.Prepare(bm_spec)
    self.assertTrue(bm_spec.always_call_cleanup)

  def testPrepareToleratesKubectlNonZeroReturn(self):
    cluster = _make_mock_cluster()
    bm_spec = _make_mock_benchmark_spec(cluster)
    with self._patch_kubectl(rc=1):
      kubernetes_management_benchmark.Prepare(bm_spec)


class CleanupTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the Cleanup benchmark lifecycle function."""

  def _patch_kubectl(self):
    return mock.patch(
        'perfkitbenchmarker.resources.container_service.kubectl'
        + '.RunKubectlCommand',
        return_value=('', '', 0),
    )

  def testCleanupDeletesSleepPod(self):
    cluster = _make_mock_cluster()
    bm_spec = _make_mock_benchmark_spec(cluster)
    with self._patch_kubectl() as mock_kubectl:
      kubernetes_management_benchmark.Cleanup(bm_spec)
      delete_calls = [
          str(c)
          for c in mock_kubectl.call_args_list
          if 'pkb-mgmt-sleep' in str(c)
      ]
      self.assertNotEmpty(delete_calls)

  def testCleanupDeletesAllPkbmPrefixedPools(self):
    cluster = _make_mock_cluster(
        pool_names=['pkbma000', 'default-pool', 'pkbmc0001']
    )
    bm_spec = _make_mock_benchmark_spec(cluster)
    with self._patch_kubectl():
      kubernetes_management_benchmark.Cleanup(bm_spec)
    deleted = {c.args[0] for c in cluster.DeleteNodePool.call_args_list}
    self.assertIn('pkbma000', deleted)
    self.assertIn('pkbmc0001', deleted)
    self.assertNotIn('default-pool', deleted)

  def testCleanupSkipsDeleteWhenNoLeftoverPools(self):
    cluster = _make_mock_cluster(pool_names=['default-pool'])
    bm_spec = _make_mock_benchmark_spec(cluster)
    with self._patch_kubectl():
      kubernetes_management_benchmark.Cleanup(bm_spec)
    cluster.DeleteNodePool.assert_not_called()

  def testCleanupHandlesNoneCluster(self):
    bm_spec = _make_mock_benchmark_spec()
    bm_spec.container_cluster = None
    kubernetes_management_benchmark.Cleanup(bm_spec)


class ClearNodePoolsTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _ClearNodePools helper function."""

  def testDeletesStalePkbmPools(self):
    cluster = _make_mock_cluster(
        pool_names=['pkbma000', 'pkbmc0001', 'user-pool']
    )
    kubernetes_management_benchmark._ClearNodePools(cluster)
    deleted = {c.args[0] for c in cluster.DeleteNodePool.call_args_list}
    self.assertIn('pkbma000', deleted)
    self.assertIn('pkbmc0001', deleted)
    self.assertNotIn('user-pool', deleted)

  def testDoesNothingWhenNoPkbmPools(self):
    cluster = _make_mock_cluster(pool_names=['user-pool', 'default-pool'])
    kubernetes_management_benchmark._ClearNodePools(cluster)
    cluster.DeleteNodePool.assert_not_called()

  def testClearRaisesOnGetNodePoolNamesException(self):
    cluster = _make_mock_cluster()
    cluster.GetNodePoolNames.side_effect = RuntimeError('API error')
    with self.assertRaises(RuntimeError):
      kubernetes_management_benchmark._ClearNodePools(cluster)


class ThreadSafeResultsTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the ThreadSafeResults collector."""

  def testAddSingleEntry(self):
    r = kubernetes_management_benchmark.ThreadSafeResults()
    r.add('op1', kubernetes_management_benchmark.OpTiming(0.1, 1.0))
    self.assertLen(r.entries, 1)
    name, timing = r.entries[0]
    self.assertEqual('op1', name)
    self.assertAlmostEqual(0.1, timing.initiation_latency, places=5)
    self.assertAlmostEqual(1.0, timing.end_to_end_latency, places=5)

  def testAddFailureRecordsName(self):
    r = kubernetes_management_benchmark.ThreadSafeResults()
    r.add_failure('bad-op')
    self.assertEqual(['bad-op'], r.failed)
    self.assertEmpty(r.entries)

  def testAddIsThreadSafe(self):
    """Tests that concurrent add() calls from multiple threads are safe."""
    r = kubernetes_management_benchmark.ThreadSafeResults()
    n = 100

    def _add(i):
      r.add(
          f'op{i}',
          kubernetes_management_benchmark.OpTiming(float(i), float(i) * 2),
      )

    threads = [threading.Thread(target=_add, args=(i,)) for i in range(n)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()
    self.assertLen(r.entries, n)


class TimedAsyncTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _TimedAsync timing helper."""

  def testSuccessfulKickoffAndWait(self):
    kickoff = mock.Mock(return_value='op-handle')
    wait_fn = mock.Mock(return_value=None)
    timing = kubernetes_management_benchmark._TimedAsync(kickoff, wait_fn)
    kickoff.assert_called_once()
    wait_fn.assert_called_once_with('op-handle')
    self.assertGreaterEqual(timing.initiation_latency, 0.0)
    self.assertGreaterEqual(
        timing.end_to_end_latency, timing.initiation_latency
    )

  def testKickoffFailurePropagates(self):
    exc = RuntimeError('kickoff failed')
    kickoff = mock.Mock(side_effect=exc)
    wait_fn = mock.Mock()
    with self.assertRaises(RuntimeError):
      kubernetes_management_benchmark._TimedAsync(kickoff, wait_fn)
    wait_fn.assert_not_called()

  def testWaitFailurePropagates(self):
    exc = RuntimeError('wait failed')
    kickoff = mock.Mock(return_value='op-handle')
    wait_fn = mock.Mock(side_effect=exc)
    with self.assertRaises(RuntimeError):
      kubernetes_management_benchmark._TimedAsync(kickoff, wait_fn)

  def testInitLatencyNotGreaterThanE2eLatency(self):
    kickoff = mock.Mock(return_value='handle')
    wait_fn = mock.Mock(side_effect=lambda _: time.sleep(0.01))
    timing = kubernetes_management_benchmark._TimedAsync(kickoff, wait_fn)
    self.assertLessEqual(timing.initiation_latency, timing.end_to_end_latency)

  def testHandlePassedToWaitFn(self):
    kickoff = mock.Mock(return_value='my-op-handle')
    wait_fn = mock.Mock()
    kubernetes_management_benchmark._TimedAsync(kickoff, wait_fn)
    wait_fn.assert_called_once_with('my-op-handle')


class RunAsyncTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _RunAsync fail-hard concurrent execution helper."""

  def testEmptyItemsReturnsEmptyList(self):
    results = kubernetes_management_benchmark._RunAsync(
        kickoff=mock.Mock(),
        wait_fn=mock.Mock(),
        items=[],
        get_name=str,
    )
    self.assertEmpty(results)

  @flagsaver.flagsaver(k8s_mgmt_max_concurrent=50)
  def testReturnsOneResultPerItem(self):
    kickoff = mock.Mock(return_value='op-handle')
    wait_fn = mock.Mock(return_value=None)
    results = kubernetes_management_benchmark._RunAsync(
        kickoff=kickoff, wait_fn=wait_fn, items=['a', 'b', 'c'], get_name=str
    )
    self.assertLen(results, 3)
    self.assertEqual({'a', 'b', 'c'}, {name for name, _ in results})

  @flagsaver.flagsaver(k8s_mgmt_max_concurrent=50)
  def testKickoffErrorPropagates(self):
    """Fail-hard: a failing op raises rather than being captured."""
    kickoff = mock.Mock(side_effect=RuntimeError('kaboom'))
    with self.assertRaises(Exception):
      kubernetes_management_benchmark._RunAsync(
          kickoff=kickoff, wait_fn=mock.Mock(), items=['x'], get_name=str
      )

  @flagsaver.flagsaver(k8s_mgmt_max_concurrent=2)
  def testConcurrencyCapDoesNotDropItems(self):
    results = kubernetes_management_benchmark._RunAsync(
        kickoff=mock.Mock(return_value='op'),
        wait_fn=mock.Mock(return_value=None),
        items=list(range(5)),
        get_name=str,
    )
    self.assertLen(results, 5)

  @flagsaver.flagsaver(k8s_mgmt_max_concurrent=50)
  def testGetNameCallableApplied(self):
    cfg = mock.MagicMock()
    cfg.name = 'poolname'
    results = kubernetes_management_benchmark._RunAsync(
        kickoff=mock.Mock(return_value='h'),
        wait_fn=mock.Mock(),
        items=[cfg],
        get_name=lambda c: c.name,
    )
    name, _ = results[0]
    self.assertEqual('poolname', name)


class MakeNodePoolConfigTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _MakeNodePoolConfig factory."""

  @flagsaver.flagsaver(k8s_mgmt_nodes_per_nodepool=3)
  def testNameIsSet(self):
    cluster = _make_mock_cluster()
    cfg = kubernetes_management_benchmark._MakeNodePoolConfig(cluster, 'mypool')
    self.assertEqual('mypool', cfg.name)

  @flagsaver.flagsaver(k8s_mgmt_nodes_per_nodepool=3)
  def testNumNodesComesFromFlag(self):
    cluster = _make_mock_cluster()
    cfg = kubernetes_management_benchmark._MakeNodePoolConfig(cluster, 'p')
    self.assertEqual(3, cfg.num_nodes)
    self.assertEqual(3, cfg.min_nodes)
    self.assertEqual(3, cfg.max_nodes)

  @flagsaver.flagsaver(k8s_mgmt_nodes_per_nodepool=1)
  def testDoesNotMutateDefaultNodepool(self):
    cluster = _make_mock_cluster()
    original_name = cluster.default_nodepool.name
    kubernetes_management_benchmark._MakeNodePoolConfig(cluster, 'newname')
    self.assertEqual(original_name, cluster.default_nodepool.name)


class OpSamplesTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _OpSamples latency-only sample helper (fail-hard path)."""

  def testEmptyResultsYieldsNoSamples(self):
    samples = kubernetes_management_benchmark._OpSamples('PrefixOp', [])
    self.assertEmpty(samples)

  def testPerOpInitiationAndE2eSamplesGenerated(self):
    results = [
        ('op1', kubernetes_management_benchmark.OpTiming(0.1, 1.0)),
        ('op2', kubernetes_management_benchmark.OpTiming(0.2, 2.0)),
    ]
    samples = kubernetes_management_benchmark._OpSamples('MyOp', results)
    metrics = [s.metric for s in samples]
    self.assertIn('MyOp_InitiationLatency', metrics)
    self.assertIn('MyOp_EndToEndLatency', metrics)

  def testNoSuccessRateOrCountMetrics(self):
    """Fail-hard path emits no SuccessRate/count metrics (B1)."""
    results = [
        ('op1', kubernetes_management_benchmark.OpTiming(1.0, 2.0)),
        ('op2', kubernetes_management_benchmark.OpTiming(0.5, 1.5)),
    ]
    samples = kubernetes_management_benchmark._OpSamples('Op', results)
    metrics = {s.metric for s in samples}
    self.assertNotIn('Op_SuccessRate', metrics)
    self.assertNotIn('Op_TotalOps', metrics)

  def testOperationNameInMetadata(self):
    results = [('mypool', kubernetes_management_benchmark.OpTiming(1.0, 2.0))]
    samples = kubernetes_management_benchmark._OpSamples('Op', results)
    init_s = next(s for s in samples if s.metric == 'Op_InitiationLatency')
    self.assertEqual('mypool', init_s.metadata['operation_name'])

  def testAggregatesGeneratedForTwoOrMore(self):
    results = [
        (
            f'op{i}',
            kubernetes_management_benchmark.OpTiming(float(i), float(i) * 2),
        )
        for i in range(1, 4)
    ]
    samples = kubernetes_management_benchmark._OpSamples('Op', results)
    metrics = [s.metric for s in samples]
    self.assertIn('Op_InitiationLatency_Mean', metrics)
    self.assertIn('Op_EndToEndLatency_Mean', metrics)

  def testAggregatesNotGeneratedForSingle(self):
    results = [('op1', kubernetes_management_benchmark.OpTiming(1.0, 2.0))]
    samples = kubernetes_management_benchmark._OpSamples('Op', results)
    self.assertNotIn('Op_InitiationLatency_Mean', [s.metric for s in samples])

  def testOutliersGeneratedForFourOrMore(self):
    results = [
        (
            f'op{i}',
            kubernetes_management_benchmark.OpTiming(float(i), float(i) * 2),
        )
        for i in range(1, 6)
    ]
    samples = kubernetes_management_benchmark._OpSamples('Op', results)
    metrics = [s.metric for s in samples]
    self.assertIn('Op_InitiationLatency_OutlierCount', metrics)

  def testOutliersNotGeneratedForThreeOrFewer(self):
    results = [
        (
            f'op{i}',
            kubernetes_management_benchmark.OpTiming(float(i), float(i) * 2),
        )
        for i in range(1, 4)
    ]
    samples = kubernetes_management_benchmark._OpSamples('Op', results)
    self.assertNotIn(
        'Op_InitiationLatency_OutlierCount', [s.metric for s in samples]
    )


class AggregateSamplesTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _AggregateSamples statistics helper."""

  def testProducesAllExpectedStatMetrics(self):
    samples = kubernetes_management_benchmark._AggregateSamples(
        'Pfx', 'InitiationLatency', [1.0, 2.0, 3.0, 4.0, 5.0]
    )
    metrics = {s.metric for s in samples}
    for label in ('Mean', 'StdDev', 'Min', 'Median', 'P90', 'P99', 'Max'):
      self.assertIn(f'Pfx_InitiationLatency_{label}', metrics)

  def testMeanValueCorrect(self):
    samples = kubernetes_management_benchmark._AggregateSamples(
        'Op', 'E2E', [1.0, 2.0, 3.0, 4.0, 5.0]
    )
    mean_s = next(s for s in samples if 'Mean' in s.metric)
    self.assertAlmostEqual(3.0, mean_s.value, places=3)

  def testMinValueCorrect(self):
    samples = kubernetes_management_benchmark._AggregateSamples(
        'Op', 'E2E', [10.0, 20.0, 30.0]
    )
    min_s = next(s for s in samples if 'Min' in s.metric)
    self.assertAlmostEqual(10.0, min_s.value, places=3)

  def testMaxValueCorrect(self):
    samples = kubernetes_management_benchmark._AggregateSamples(
        'Op', 'E2E', [10.0, 20.0, 30.0]
    )
    max_s = next(s for s in samples if 'Max' in s.metric)
    self.assertAlmostEqual(30.0, max_s.value, places=3)

  def testSampleCountInMetadata(self):
    samples = kubernetes_management_benchmark._AggregateSamples(
        'Op', 'E2E', [1.0, 2.0, 3.0]
    )
    for s in samples:
      self.assertEqual('3', s.metadata.get('sample_count'))

  def testUnitsAreSeconds(self):
    samples = kubernetes_management_benchmark._AggregateSamples(
        'Op', 'E2E', [1.0, 2.0]
    )
    for s in samples:
      self.assertEqual('seconds', s.unit)


class OutlierSamplesTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _OutlierSamples IQR-based outlier detection helper."""

  def testNoOutliersYieldsZeroCount(self):
    samples = kubernetes_management_benchmark._OutlierSamples(
        'Op', 'E2E', [1.0, 1.1, 1.2, 1.3, 1.4, 1.5]
    )
    self.assertLen(samples, 1)
    self.assertEqual(0, samples[0].value)

  def testClearOutlierDetected(self):
    samples = kubernetes_management_benchmark._OutlierSamples(
        'Op', 'E2E', [1.0, 1.0, 1.0, 1.0, 100.0]
    )
    self.assertEqual(1, samples[0].value)

  def testMetricNameFormatted(self):
    samples = kubernetes_management_benchmark._OutlierSamples(
        'MyPrefix', 'InitiationLatency', [1.0, 2.0, 3.0, 4.0]
    )
    self.assertEqual(
        'MyPrefix_InitiationLatency_OutlierCount', samples[0].metric
    )

  def testMetadataContainsFenceFields(self):
    """Tests that outlier samples contain fence metadata fields."""
    meta = kubernetes_management_benchmark._OutlierSamples(
        'Op', 'E2E', [1.0, 2.0, 3.0, 4.0, 5.0]
    )[0].metadata
    for field in (
        'q1',
        'q3',
        'iqr',
        'upper_fence',
        'lower_fence',
        'sample_count',
    ):
      self.assertIn(field, meta)

  def testSampleCountInMetadata(self):
    samples = kubernetes_management_benchmark._OutlierSamples(
        'Op', 'E2E', [1.0, 2.0, 3.0, 4.0, 5.0]
    )
    self.assertEqual('5', samples[0].metadata['sample_count'])

  def testUnitIsCount(self):
    samples = kubernetes_management_benchmark._OutlierSamples(
        'Op', 'E2E', [1.0, 2.0, 3.0, 4.0]
    )
    self.assertEqual('count', samples[0].unit)

  def testReturnsSingleSample(self):
    samples = kubernetes_management_benchmark._OutlierSamples(
        'Op', 'E2E', list(range(1, 11))
    )
    self.assertLen(samples, 1)


class RunTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the Run benchmark entry-point function."""

  @flagsaver.flagsaver(
      k8s_mgmt_scenarios=['concurrent_node_pool_ops'],
  )
  def testRunOnlyScenarioACallsOnlyA(self):
    """Run dispatches only to _RunConcurrentNodePoolOps for that scenario."""
    cluster = _make_mock_cluster()
    bm_spec = _make_mock_benchmark_spec(cluster)
    with mock.patch.object(
        kubernetes_management_benchmark, '_ClearNodePools'
    ), mock.patch.object(
        kubernetes_management_benchmark,
        '_RunConcurrentNodePoolOps',
        return_value=[],
    ) as mock_a, mock.patch.object(
        kubernetes_management_benchmark,
        '_RunOverlappingClusterUpdate',
        return_value=[],
    ) as mock_b:
      kubernetes_management_benchmark.Run(bm_spec)
    mock_a.assert_called_once()
    mock_b.assert_not_called()

  @flagsaver.flagsaver(
      k8s_mgmt_scenarios=['overlapping_cluster_update'],
  )
  def testRunOnlyScenarioBCallsOnlyB(self):
    """Run dispatches only to _RunOverlappingClusterUpdate for that scenario."""
    cluster = _make_mock_cluster()
    bm_spec = _make_mock_benchmark_spec(cluster)
    with mock.patch.object(
        kubernetes_management_benchmark, '_ClearNodePools'
    ), mock.patch.object(
        kubernetes_management_benchmark,
        '_RunConcurrentNodePoolOps',
        return_value=[],
    ) as mock_a, mock.patch.object(
        kubernetes_management_benchmark,
        '_RunOverlappingClusterUpdate',
        return_value=[],
    ) as mock_b:
      kubernetes_management_benchmark.Run(bm_spec)
    mock_a.assert_not_called()
    mock_b.assert_called_once()

  @flagsaver.flagsaver(
      k8s_mgmt_scenarios=['concurrent_node_pool_ops'],
  )
  def testRunTagsAllSamplesWithRunMetadata(self):
    """Tests that Run adds version and config keys to all sample metadata."""
    cluster = _make_mock_cluster()
    bm_spec = _make_mock_benchmark_spec(cluster)
    test_sample = _make_sample('m', 1.0)
    with mock.patch.object(
        kubernetes_management_benchmark, '_ClearNodePools'
    ), mock.patch.object(
        kubernetes_management_benchmark,
        '_RunConcurrentNodePoolOps',
        return_value=[test_sample],
    ), mock.patch.object(
        kubernetes_management_benchmark,
        '_RunOverlappingClusterUpdate',
        return_value=[],
    ):
      samples = kubernetes_management_benchmark.Run(bm_spec)
    meta = samples[0].metadata
    for key in (
        'initial_version',
        'cluster_k8s_version',
        'nodes_per_nodepool',
        'concurrent_nodepools',
    ):
      self.assertIn(key, meta)

  @flagsaver.flagsaver(
      k8s_mgmt_scenarios=['concurrent_node_pool_ops'],
      k8s_mgmt_initial_version='1.30',
  )
  def testRunUsesExplicitVersionFlags(self):
    """Tests that Run uses explicit version flags over auto-resolved ones."""
    cluster = _make_mock_cluster()
    bm_spec = _make_mock_benchmark_spec(cluster)
    with mock.patch.object(
        kubernetes_management_benchmark, '_ClearNodePools'
    ), mock.patch.object(
        kubernetes_management_benchmark,
        '_RunConcurrentNodePoolOps',
        return_value=[_make_sample('m', 1.0)],
    ), mock.patch.object(
        kubernetes_management_benchmark,
        '_RunOverlappingClusterUpdate',
        return_value=[],
    ):
      samples = kubernetes_management_benchmark.Run(bm_spec)
    cluster.ResolveNodePoolVersions.assert_not_called()
    self.assertEqual('1.30', samples[0].metadata['initial_version'])

  @flagsaver.flagsaver(
      k8s_mgmt_scenarios=['concurrent_node_pool_ops'],
  )
  def testRunAutoResolvesVersionsWhenFlagsAbsent(self):
    """Tests Run calls ResolveNodePoolVersions when version flags absent."""
    cluster = _make_mock_cluster()
    cluster.ResolveNodePoolVersions.return_value = ('1.33', '1.34')
    bm_spec = _make_mock_benchmark_spec(cluster)
    with mock.patch.object(
        kubernetes_management_benchmark, '_ClearNodePools'
    ), mock.patch.object(
        kubernetes_management_benchmark,
        '_RunConcurrentNodePoolOps',
        return_value=[_make_sample('m', 1.0)],
    ), mock.patch.object(
        kubernetes_management_benchmark,
        '_RunOverlappingClusterUpdate',
        return_value=[],
    ):
      samples = kubernetes_management_benchmark.Run(bm_spec)
    cluster.ResolveNodePoolVersions.assert_called_once()
    self.assertEqual('1.33', samples[0].metadata['initial_version'])


class RunScenarioATest(pkb_common_test_case.PkbCommonTestCase):
  """Tests the _RunConcurrentNodePoolOps create/delete path."""

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testProducesCreateAndDeleteSamples(self):
    """Tests Scenario A produces Create and Delete samples."""
    cluster = _make_mock_cluster(pool_names=['pkbma000', 'pkbma001'])
    samples = kubernetes_management_benchmark._RunConcurrentNodePoolOps(
        cluster, '1.33'
    )
    metrics = {s.metric for s in samples}
    self.assertTrue(any('ConcurrentOps_Create' in m for m in metrics))
    self.assertTrue(any('ConcurrentOps_Delete' in m for m in metrics))
    self.assertFalse(any('ConcurrentOps_Upgrade' in m for m in metrics))

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testPassesInitialVersionToCreate(self):
    """_RunConcurrentNodePoolOps passes initial_version to creates."""
    cluster = _make_mock_cluster(pool_names=['pkbma000', 'pkbma001'])
    kubernetes_management_benchmark._RunConcurrentNodePoolOps(cluster, '1.33')
    for call in cluster.CreateNodePoolAsync.call_args_list:
      kw = call.kwargs if call.kwargs else {}
      pos = call.args
      node_version = kw.get('node_version') or (
          pos[1] if len(pos) > 1 else None
      )
      self.assertEqual('1.33', node_version)

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testDeleteUsesLivePoolList(self):
    """_RunConcurrentNodePoolOps deletes only pools found at runtime."""
    cluster = _make_mock_cluster(pool_names=['pkbma000'])
    kubernetes_management_benchmark._RunConcurrentNodePoolOps(cluster, '1.33')
    self.assertEqual(1, cluster.DeleteNodePoolAsync.call_count)


class RunScenarioBTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests the _RunOverlappingClusterUpdate overlap scenario."""

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testProducesClusterUpdateAndNodePoolCreateSamples(self):
    """Overlap scenario emits both cluster-update and create samples."""
    cluster = _make_mock_cluster(pool_names=[])
    samples = kubernetes_management_benchmark._RunOverlappingClusterUpdate(
        cluster, '1.33'
    )
    metrics = {s.metric for s in samples}
    self.assertTrue(
        any('OverlappingUpdate_ClusterUpdate' in m for m in metrics)
    )
    self.assertTrue(
        any('OverlappingUpdate_NodePoolCreate' in m for m in metrics)
    )

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testDeletesTestPoolAfterRun(self):
    cluster = _make_mock_cluster(pool_names=[])
    kubernetes_management_benchmark._RunOverlappingClusterUpdate(
        cluster, '1.33'
    )
    cluster.DeleteNodePool.assert_called_once_with(
        kubernetes_management_benchmark._OVERLAPPING_POOL_NAME
    )

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testDeleteFailureRaisesInScenarioB(self):
    cluster = _make_mock_cluster(pool_names=[])
    cluster.DeleteNodePool.side_effect = RuntimeError('delete failed')
    with self.assertRaises(RuntimeError):
      kubernetes_management_benchmark._RunOverlappingClusterUpdate(
          cluster, '1.33'
      )

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testPassesInitialVersionToCreate(self):
    """_RunOverlappingClusterUpdate passes initial_version to the create."""
    cluster = _make_mock_cluster(pool_names=[])
    kubernetes_management_benchmark._RunOverlappingClusterUpdate(
        cluster, '1.33'
    )
    for call in cluster.CreateNodePoolAsync.call_args_list:
      kw = call.kwargs if call.kwargs else {}
      pos = call.args
      node_version = kw.get('node_version') or (
          pos[1] if len(pos) > 1 else None
      )
      self.assertEqual('1.33', node_version)


if __name__ == '__main__':
  unittest.main()
