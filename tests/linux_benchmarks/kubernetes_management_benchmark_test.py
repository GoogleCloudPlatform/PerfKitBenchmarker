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


class ScenarioNameTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _SCENARIO_A_NAME, _SCENARIO_B_NAME, _SCENARIO_C_NAME."""

  def testScenarioANameZeroPadsToThreeDigits(self):
    self.assertEqual(
        'pkbma000',
        kubernetes_management_benchmark._ScenarioAName(0),
    )

  def testScenarioANameTwoDigitIndex(self):
    self.assertEqual(
        'pkbma042',
        kubernetes_management_benchmark._ScenarioAName(42),
    )

  def testScenarioANameMaxThreeDigits(self):
    self.assertEqual(
        'pkbma999',
        kubernetes_management_benchmark._ScenarioAName(999),
    )

  def testScenarioBNameIsConstant(self):
    self.assertEqual(
        'pkbmb',
        kubernetes_management_benchmark._SCENARIO_B_NAME,
    )

  def testScenarioCNameZeroPadsToFourDigits(self):
    self.assertEqual(
        'pkbmc0000',
        kubernetes_management_benchmark._ScenarioCName(0),
    )

  def testScenarioCNameSingleDigitIndex(self):
    self.assertEqual(
        'pkbmc0007',
        kubernetes_management_benchmark._ScenarioCName(7),
    )

  def testScenarioCNameFourDigitIndex(self):
    self.assertEqual(
        'pkbmc1000',
        kubernetes_management_benchmark._ScenarioCName(1000),
    )

  def testAllNamesWithinAksLimit(self):
    for i in range(1000):
      self.assertLessEqual(
          len(kubernetes_management_benchmark._ScenarioAName(i)), 12
      )
    for i in range(10000):
      self.assertLessEqual(
          len(kubernetes_management_benchmark._ScenarioCName(i)), 12
      )
    self.assertLessEqual(
        len(kubernetes_management_benchmark._SCENARIO_B_NAME), 12
    )


class CheckPrerequisitesTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the CheckPrerequisites validation function."""

  def testValidScenariosPass(self):
    with flagsaver.flagsaver(k8s_mgmt_scenarios=['A', 'B', 'C']):
      kubernetes_management_benchmark.CheckPrerequisites(_make_mock_config())

  def testInvalidScenarioRaises(self):
    with flagsaver.flagsaver(k8s_mgmt_scenarios=['X']):
      with self.assertRaises(errors.Config.InvalidValue):
        kubernetes_management_benchmark.CheckPrerequisites(_make_mock_config())

  def testMixedValidInvalidRaises(self):
    with flagsaver.flagsaver(k8s_mgmt_scenarios=['A', 'Z']):
      with self.assertRaises(errors.Config.InvalidValue):
        kubernetes_management_benchmark.CheckPrerequisites(_make_mock_config())

  def testNonKubernetesClusterTypeRaises(self):
    with flagsaver.flagsaver(k8s_mgmt_scenarios=['A']):
      with self.assertRaises(errors.Config.InvalidValue):
        kubernetes_management_benchmark.CheckPrerequisites(
            _make_mock_config(cluster_type='Mesos')
        )

  def testInvalidScaleSweepRaises(self):
    with flagsaver.flagsaver(
        k8s_mgmt_scenarios=['C'], k8s_mgmt_scale_sweep=['10', 'abc']
    ):
      with self.assertRaises(errors.Config.InvalidValue):
        kubernetes_management_benchmark.CheckPrerequisites(_make_mock_config())

  def testValidScaleSweepPasses(self):
    with flagsaver.flagsaver(
        k8s_mgmt_scenarios=['C'], k8s_mgmt_scale_sweep=['10', '50', '100']
    ):
      kubernetes_management_benchmark.CheckPrerequisites(_make_mock_config())

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


class CleanStartSweepTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _CleanStartSweep helper function."""

  def testDeletesStalePkbmPools(self):
    cluster = _make_mock_cluster(
        pool_names=['pkbma000', 'pkbmc0001', 'user-pool']
    )
    kubernetes_management_benchmark._CleanStartSweep(cluster)
    deleted = {c.args[0] for c in cluster.DeleteNodePool.call_args_list}
    self.assertIn('pkbma000', deleted)
    self.assertIn('pkbmc0001', deleted)
    self.assertNotIn('user-pool', deleted)

  def testDoesNothingWhenNoPkbmPools(self):
    cluster = _make_mock_cluster(pool_names=['user-pool', 'default-pool'])
    kubernetes_management_benchmark._CleanStartSweep(cluster)
    cluster.DeleteNodePool.assert_not_called()

  def testCleanStartSweepRaisesOnGetNodePoolNamesException(self):
    cluster = _make_mock_cluster()
    cluster.GetNodePoolNames.side_effect = RuntimeError('API error')
    with self.assertRaises(RuntimeError):
      kubernetes_management_benchmark._CleanStartSweep(cluster)


class ResultsTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _Results result-accumulator helper."""

  def testAddSingleEntry(self):
    r = kubernetes_management_benchmark._Results()
    r.add('op1', 0.1, 1.0, None)
    self.assertLen(r.entries, 1)
    name, init, e2e, err = r.entries[0]
    self.assertEqual('op1', name)
    self.assertAlmostEqual(0.1, init, places=5)
    self.assertAlmostEqual(1.0, e2e, places=5)
    self.assertIsNone(err)

  def testAddMultipleEntries(self):
    r = kubernetes_management_benchmark._Results()
    r.add('op1', 0.1, 1.0, None)
    r.add('op2', 0.2, 2.0, ValueError('fail'))
    self.assertLen(r.entries, 2)

  def testAddIsThreadSafe(self):
    """Tests that concurrent add() calls from multiple threads are safe."""
    r = kubernetes_management_benchmark._Results()
    n = 100

    def _add(i):
      r.add(f'op{i}', float(i), float(i) * 2, None)

    threads = [threading.Thread(target=_add, args=(i,)) for i in range(n)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()
    self.assertLen(r.entries, n)

  def testAddPreservesError(self):
    r = kubernetes_management_benchmark._Results()
    exc = RuntimeError('test error')
    r.add('failing-op', 0.5, 0.5, exc)
    _, _, _, err = r.entries[0]
    self.assertIs(exc, err)


class TimedAsyncTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _TimedAsync timing helper."""

  def testSuccessfulKickoffAndWait(self):
    kickoff = mock.Mock(return_value='op-handle')
    wait_fn = mock.Mock(return_value=None)
    init_lat, e2e_lat, err = kubernetes_management_benchmark._TimedAsync(
        kickoff, wait_fn
    )
    kickoff.assert_called_once()
    wait_fn.assert_called_once_with('op-handle')
    self.assertIsNone(err)
    self.assertGreaterEqual(init_lat, 0.0)
    self.assertGreaterEqual(e2e_lat, init_lat)

  def testKickoffFailureReturnsError(self):
    exc = RuntimeError('kickoff failed')
    kickoff = mock.Mock(side_effect=exc)
    wait_fn = mock.Mock()
    init_lat, e2e_lat, err = kubernetes_management_benchmark._TimedAsync(
        kickoff, wait_fn
    )
    self.assertIs(exc, err)
    wait_fn.assert_not_called()
    self.assertAlmostEqual(init_lat, e2e_lat, places=2)

  def testWaitFailureReturnsError(self):
    exc = RuntimeError('wait failed')
    kickoff = mock.Mock(return_value='op-handle')
    wait_fn = mock.Mock(side_effect=exc)
    _, e2e_lat, err = kubernetes_management_benchmark._TimedAsync(
        kickoff, wait_fn
    )
    self.assertIs(exc, err)
    self.assertGreater(e2e_lat, 0.0)

  def testInitLatencyNotGreaterThanE2eLatency(self):
    kickoff = mock.Mock(return_value='handle')
    wait_fn = mock.Mock(side_effect=lambda _: time.sleep(0.01))
    init_lat, e2e_lat, err = kubernetes_management_benchmark._TimedAsync(
        kickoff, wait_fn
    )
    self.assertIsNone(err)
    self.assertLessEqual(init_lat, e2e_lat)

  def testHandlePassedToWaitFn(self):
    kickoff = mock.Mock(return_value='my-op-handle')
    wait_fn = mock.Mock()
    kubernetes_management_benchmark._TimedAsync(kickoff, wait_fn)
    wait_fn.assert_called_once_with('my-op-handle')


class RunAsyncTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _RunAsync concurrent execution helper."""

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
    self.assertEqual({'a', 'b', 'c'}, {name for name, _, _, _ in results})

  @flagsaver.flagsaver(k8s_mgmt_max_concurrent=50)
  def testKickoffErrorCapturedInResults(self):
    kickoff = mock.Mock(side_effect=RuntimeError('kaboom'))
    results = kubernetes_management_benchmark._RunAsync(
        kickoff=kickoff, wait_fn=mock.Mock(), items=['x'], get_name=str
    )
    self.assertLen(results, 1)
    _, _, _, err = results[0]
    self.assertIsNotNone(err)

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
    name, _, _, _ = results[0]
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
  """Tests for the _OpSamples sample-generation helper."""

  def testEmptyResultsYieldsSuccessRateOfZero(self):
    samples = kubernetes_management_benchmark._OpSamples(
        'PrefixOp', [], attempted_ops=5
    )
    rate = next(s for s in samples if s.metric == 'PrefixOp_SuccessRate')
    self.assertEqual(0.0, rate.value)

  def testPerOpInitiationAndE2eSamplesGenerated(self):
    results = [('op1', 0.1, 1.0, None), ('op2', 0.2, 2.0, None)]
    samples = kubernetes_management_benchmark._OpSamples(
        'MyOp', results, attempted_ops=2
    )
    metrics = [s.metric for s in samples]
    self.assertIn('MyOp_InitiationLatency', metrics)
    self.assertIn('MyOp_EndToEndLatency', metrics)

  def testSuccessRateHundredPercentWhenAllSucceed(self):
    results = [('op1', 1.0, 2.0, None), ('op2', 0.5, 1.5, None)]
    samples = kubernetes_management_benchmark._OpSamples(
        'Op', results, attempted_ops=2
    )
    rate = next(s for s in samples if s.metric == 'Op_SuccessRate')
    self.assertAlmostEqual(100.0, rate.value)

  def testSuccessRateFiftyPercentWhenHalfFail(self):
    results = [
        ('op1', 1.0, 2.0, None),
        ('op2', 0.5, 0.5, RuntimeError('fail')),
    ]
    samples = kubernetes_management_benchmark._OpSamples(
        'Op', results, attempted_ops=2
    )
    rate = next(s for s in samples if s.metric == 'Op_SuccessRate')
    self.assertAlmostEqual(50.0, rate.value)

  def testAttemptedOpsExceedingExecutedOpsLowersRate(self):
    results = [('op1', 1.0, 2.0, None)]
    samples = kubernetes_management_benchmark._OpSamples(
        'Op', results, attempted_ops=3
    )
    rate = next(s for s in samples if s.metric == 'Op_SuccessRate')
    self.assertAlmostEqual(100.0 / 3, rate.value, places=3)

  def testSuccessRateMetadataFields(self):
    results = [('op1', 1.0, 2.0, None), ('op2', 0.5, 0.5, Exception('err'))]
    samples = kubernetes_management_benchmark._OpSamples(
        'Op', results, attempted_ops=3
    )
    rate = next(s for s in samples if s.metric == 'Op_SuccessRate')
    self.assertEqual('3', rate.metadata['total_ops'])
    self.assertEqual('2', rate.metadata['executed_ops'])
    self.assertEqual('1', rate.metadata['successful_ops'])
    self.assertEqual('1', rate.metadata['skipped_ops'])

  def testFailedOpIncludesErrorMessage(self):
    results = [('fail-op', 0.5, 0.5, RuntimeError('oops'))]
    samples = kubernetes_management_benchmark._OpSamples(
        'Op', results, attempted_ops=1
    )
    init_s = next(s for s in samples if s.metric == 'Op_InitiationLatency')
    self.assertIn('error', init_s.metadata)
    self.assertIn('oops', init_s.metadata['error'])

  def testAggregatesGeneratedForTwoOrMoreSuccesses(self):
    results = [(f'op{i}', float(i), float(i) * 2, None) for i in range(1, 4)]
    samples = kubernetes_management_benchmark._OpSamples(
        'Op', results, attempted_ops=3
    )
    metrics = [s.metric for s in samples]
    self.assertIn('Op_InitiationLatency_Mean', metrics)
    self.assertIn('Op_EndToEndLatency_Mean', metrics)

  def testAggregatesNotGeneratedForSingleSuccess(self):
    results = [('op1', 1.0, 2.0, None)]
    samples = kubernetes_management_benchmark._OpSamples(
        'Op', results, attempted_ops=1
    )
    self.assertNotIn('Op_InitiationLatency_Mean', [s.metric for s in samples])

  def testOutliersGeneratedForFourOrMoreSuccesses(self):
    results = [(f'op{i}', float(i), float(i) * 2, None) for i in range(1, 6)]
    samples = kubernetes_management_benchmark._OpSamples(
        'Op', results, attempted_ops=5
    )
    metrics = [s.metric for s in samples]
    self.assertIn('Op_InitiationLatency_OutlierCount', metrics)
    self.assertIn('Op_EndToEndLatency_OutlierCount', metrics)

  def testOutliersNotGeneratedForThreeOrFewerSuccesses(self):
    results = [(f'op{i}', float(i), float(i) * 2, None) for i in range(1, 4)]
    samples = kubernetes_management_benchmark._OpSamples(
        'Op', results, attempted_ops=3
    )
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
      k8s_mgmt_scenarios=['A', 'B', 'C'],
      k8s_mgmt_scale_sweep=[],
      k8s_mgmt_large_scale_nodepools=10,
  )
  def testRunCallsCleanStartSweep(self):
    """Tests that Run invokes _CleanStartSweep before executing scenarios."""
    cluster = _make_mock_cluster()
    bm_spec = _make_mock_benchmark_spec(cluster)
    with mock.patch.object(
        kubernetes_management_benchmark, '_CleanStartSweep'
    ) as mock_clean, mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioA', return_value=[]
    ), mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioB', return_value=[]
    ), mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioC', return_value=[]
    ):
      kubernetes_management_benchmark.Run(bm_spec)
    self.assertEqual(mock_clean.call_count, 2)
    mock_clean.assert_called_with(cluster)

  @flagsaver.flagsaver(
      k8s_mgmt_scenarios=['A'],
      k8s_mgmt_scale_sweep=[],
      k8s_mgmt_large_scale_nodepools=10,
  )
  def testRunOnlyScenarioACallsOnlyA(self):
    """Tests that Run only calls _RunScenarioA when scenarios=['A']."""
    cluster = _make_mock_cluster()
    bm_spec = _make_mock_benchmark_spec(cluster)
    with mock.patch.object(
        kubernetes_management_benchmark, '_CleanStartSweep'
    ), mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioA', return_value=[]
    ) as mock_a, mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioB', return_value=[]
    ) as mock_b, mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioC', return_value=[]
    ) as mock_c:
      kubernetes_management_benchmark.Run(bm_spec)
    mock_a.assert_called_once()
    mock_b.assert_not_called()
    mock_c.assert_not_called()

  @flagsaver.flagsaver(
      k8s_mgmt_scenarios=['B'],
      k8s_mgmt_scale_sweep=[],
      k8s_mgmt_large_scale_nodepools=10,
  )
  def testRunOnlyScenarioBCallsOnlyB(self):
    """Tests that Run only calls _RunScenarioB when scenarios=['B']."""
    cluster = _make_mock_cluster()
    bm_spec = _make_mock_benchmark_spec(cluster)
    with mock.patch.object(
        kubernetes_management_benchmark, '_CleanStartSweep'
    ), mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioA', return_value=[]
    ) as mock_a, mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioB', return_value=[]
    ) as mock_b, mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioC', return_value=[]
    ) as mock_c:
      kubernetes_management_benchmark.Run(bm_spec)
    mock_a.assert_not_called()
    mock_b.assert_called_once()
    mock_c.assert_not_called()

  @flagsaver.flagsaver(
      k8s_mgmt_scenarios=['C'],
      k8s_mgmt_scale_sweep=[],
      k8s_mgmt_large_scale_nodepools=42,
  )
  def testRunScenarioCPassesLargeScaleFlag(self):
    """Tests that Run passes the large-scale-nodepools flag to _RunScenarioC."""
    cluster = _make_mock_cluster()
    bm_spec = _make_mock_benchmark_spec(cluster)
    with mock.patch.object(
        kubernetes_management_benchmark, '_CleanStartSweep'
    ), mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioA', return_value=[]
    ), mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioB', return_value=[]
    ), mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioC', return_value=[]
    ) as mock_c:
      kubernetes_management_benchmark.Run(bm_spec)
    mock_c.assert_called_once()
    _, _, scale = mock_c.call_args.args
    self.assertEqual(42, scale)

  @flagsaver.flagsaver(
      k8s_mgmt_scenarios=['C'],
      k8s_mgmt_scale_sweep=['10', '50'],
      k8s_mgmt_large_scale_nodepools=100,
  )
  def testRunScenarioCScaleSweepRunsTwice(self):
    """Tests that Run calls _RunScenarioC once per scale in the sweep."""
    cluster = _make_mock_cluster()
    bm_spec = _make_mock_benchmark_spec(cluster)
    with mock.patch.object(
        kubernetes_management_benchmark, '_CleanStartSweep'
    ), mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioA', return_value=[]
    ), mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioB', return_value=[]
    ), mock.patch.object(
        kubernetes_management_benchmark,
        '_RunScenarioC',
        return_value=[_make_sample('m', 1.0)],
    ) as mock_c:
      kubernetes_management_benchmark.Run(bm_spec)
    self.assertEqual(2, mock_c.call_count)
    scales = [call.args[2] for call in mock_c.call_args_list]
    self.assertIn(10, scales)
    self.assertIn(50, scales)

  @flagsaver.flagsaver(
      k8s_mgmt_scenarios=['C'],
      k8s_mgmt_scale_sweep=['10'],
      k8s_mgmt_large_scale_nodepools=10,
  )
  def testRunTagsScenarioCScaleInMetadata(self):
    """Tests that Run adds scenario_c_scale to each sample's metadata."""
    cluster = _make_mock_cluster()
    bm_spec = _make_mock_benchmark_spec(cluster)
    test_sample = _make_sample('metric', 1.0)
    with mock.patch.object(
        kubernetes_management_benchmark, '_CleanStartSweep'
    ), mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioA', return_value=[]
    ), mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioB', return_value=[]
    ), mock.patch.object(
        kubernetes_management_benchmark,
        '_RunScenarioC',
        return_value=[test_sample],
    ):
      samples = kubernetes_management_benchmark.Run(bm_spec)
    self.assertIn('scenario_c_scale', samples[0].metadata)
    self.assertEqual('10', samples[0].metadata['scenario_c_scale'])

  @flagsaver.flagsaver(
      k8s_mgmt_scenarios=['A'],
      k8s_mgmt_scale_sweep=[],
      k8s_mgmt_large_scale_nodepools=10,
  )
  def testRunTagsAllSamplesWithRunMetadata(self):
    """Tests that Run adds version and config keys to all sample metadata."""
    cluster = _make_mock_cluster()
    bm_spec = _make_mock_benchmark_spec(cluster)
    test_sample = _make_sample('m', 1.0)
    with mock.patch.object(
        kubernetes_management_benchmark, '_CleanStartSweep'
    ), mock.patch.object(
        kubernetes_management_benchmark,
        '_RunScenarioA',
        return_value=[test_sample],
    ), mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioB', return_value=[]
    ), mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioC', return_value=[]
    ):
      samples = kubernetes_management_benchmark.Run(bm_spec)
    meta = samples[0].metadata
    for key in (
        'initial_version',
        'target_version',
        'cluster_k8s_version',
        'nodes_per_nodepool',
        'concurrent_nodepools',
    ):
      self.assertIn(key, meta)

  @flagsaver.flagsaver(
      k8s_mgmt_scenarios=['A'],
      k8s_mgmt_initial_version='1.30',
      k8s_mgmt_target_version='1.31',
      k8s_mgmt_scale_sweep=[],
      k8s_mgmt_large_scale_nodepools=10,
  )
  def testRunUsesExplicitVersionFlags(self):
    """Tests that Run uses explicit version flags over auto-resolved ones."""
    cluster = _make_mock_cluster()
    bm_spec = _make_mock_benchmark_spec(cluster)
    with mock.patch.object(
        kubernetes_management_benchmark, '_CleanStartSweep'
    ), mock.patch.object(
        kubernetes_management_benchmark,
        '_RunScenarioA',
        return_value=[_make_sample('m', 1.0)],
    ), mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioB', return_value=[]
    ), mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioC', return_value=[]
    ):
      samples = kubernetes_management_benchmark.Run(bm_spec)
    cluster.ResolveNodePoolVersions.assert_not_called()
    self.assertEqual('1.30', samples[0].metadata['initial_version'])
    self.assertEqual('1.31', samples[0].metadata['target_version'])

  @flagsaver.flagsaver(
      k8s_mgmt_scenarios=['A'],
      k8s_mgmt_scale_sweep=[],
      k8s_mgmt_large_scale_nodepools=10,
  )
  def testRunAutoResolvesVersionsWhenFlagsAbsent(self):
    """Tests Run calls ResolveNodePoolVersions when version flags absent."""
    cluster = _make_mock_cluster()
    cluster.ResolveNodePoolVersions.return_value = ('1.33', '1.34')
    bm_spec = _make_mock_benchmark_spec(cluster)
    with mock.patch.object(
        kubernetes_management_benchmark, '_CleanStartSweep'
    ), mock.patch.object(
        kubernetes_management_benchmark,
        '_RunScenarioA',
        return_value=[_make_sample('m', 1.0)],
    ), mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioB', return_value=[]
    ), mock.patch.object(
        kubernetes_management_benchmark, '_RunScenarioC', return_value=[]
    ):
      samples = kubernetes_management_benchmark.Run(bm_spec)
    cluster.ResolveNodePoolVersions.assert_called_once()
    self.assertEqual('1.33', samples[0].metadata['initial_version'])
    self.assertEqual('1.34', samples[0].metadata['target_version'])


class RunScenarioATest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _RunScenarioA phase-by-phase and pipelined modes."""

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
      k8s_mgmt_pipeline_scenario_a=False,
  )
  def testPhaseByPhaseProducesCreateUpgradeDeleteSamples(self):
    """Tests Scenario A produces Create, Upgrade, and Delete samples."""
    cluster = _make_mock_cluster(pool_names=['pkbma000', 'pkbma001'])
    samples = kubernetes_management_benchmark._RunScenarioA(
        cluster, '1.33', '1.34'
    )
    metrics = {s.metric for s in samples}
    self.assertTrue(any('ScenarioA_Create' in m for m in metrics))
    self.assertTrue(any('ScenarioA_Upgrade' in m for m in metrics))
    self.assertTrue(any('ScenarioA_Delete' in m for m in metrics))

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
      k8s_mgmt_pipeline_scenario_a=False,
  )
  def testPhaseByPhasePassesInitialVersionToCreate(self):
    """Tests _RunScenarioA passes initial_version to CreateNodePoolAsync."""
    cluster = _make_mock_cluster(pool_names=['pkbma000', 'pkbma001'])
    kubernetes_management_benchmark._RunScenarioA(cluster, '1.33', '1.34')
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
      k8s_mgmt_pipeline_scenario_a=False,
  )
  def testPhaseByPhaseDeleteUsesLivePoolList(self):
    """Tests that _RunScenarioA deletes only the pools it finds at runtime."""
    cluster = _make_mock_cluster(pool_names=['pkbma000'])
    kubernetes_management_benchmark._RunScenarioA(cluster, '1.33', '1.34')
    self.assertEqual(1, cluster.DeleteNodePoolAsync.call_count)

  @flagsaver.flagsaver(
      k8s_mgmt_concurrent_nodepools=2,
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
      k8s_mgmt_pipeline_scenario_a=True,
  )
  def testPipelinedModeActivatedByFlag(self):
    """Tests pipelined mode is activated by the pipeline_scenario_a flag."""
    cluster = _make_mock_cluster(pool_names=[])
    samples = kubernetes_management_benchmark._RunScenarioA(
        cluster, '1.33', '1.34'
    )
    metrics = {s.metric for s in samples}
    self.assertTrue(any('ScenarioA_Create' in m for m in metrics))
    self.assertTrue(any('ScenarioA_Upgrade' in m for m in metrics))
    self.assertTrue(any('ScenarioA_Delete' in m for m in metrics))


class RunScenarioAPipelinedTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _RunScenarioAPipelined pipelined execution path."""

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testPipelinedProducesAllThreePhases(self):
    """Tests pipelined Scenario A produces Create/Upgrade/Delete samples."""
    cluster = _make_mock_cluster(pool_names=[])
    samples = kubernetes_management_benchmark._RunScenarioAPipelined(
        cluster, n=2, initial='1.33', target='1.34'
    )
    metrics = {s.metric for s in samples}
    self.assertTrue(any('ScenarioA_Create' in m for m in metrics))
    self.assertTrue(any('ScenarioA_Upgrade' in m for m in metrics))
    self.assertTrue(any('ScenarioA_Delete' in m for m in metrics))

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testPipelinedSkipsUpgradeAfterCreateFailure(self):
    """Tests pipelined mode skips upgrade when create fails."""
    cluster = _make_mock_cluster(pool_names=[])
    cluster.CreateNodePoolAsync.side_effect = RuntimeError('create failed')
    samples = kubernetes_management_benchmark._RunScenarioAPipelined(
        cluster, n=1, initial='1.33', target='1.34'
    )
    cluster.UpgradeNodePoolAsync.assert_not_called()
    upgrade_rate = next(
        (s for s in samples if s.metric == 'ScenarioA_Upgrade_SuccessRate'),
        None,
    )
    if upgrade_rate is not None:
      self.assertEqual(0.0, upgrade_rate.value)


class RunScenarioBTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _RunScenarioB cluster-update + nodepool-create scenario."""

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testProducesClusterUpdateAndNodePoolCreateSamples(self):
    cluster = _make_mock_cluster(pool_names=[])
    samples = kubernetes_management_benchmark._RunScenarioB(cluster, '1.33')
    metrics = {s.metric for s in samples}
    self.assertTrue(any('ScenarioB_ClusterUpdate' in m for m in metrics))
    self.assertTrue(any('ScenarioB_NodePoolCreate' in m for m in metrics))

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testDeletesTestPoolAfterRun(self):
    cluster = _make_mock_cluster(pool_names=[])
    kubernetes_management_benchmark._RunScenarioB(cluster, '1.33')
    cluster.DeleteNodePool.assert_called_once_with(
        kubernetes_management_benchmark._SCENARIO_B_NAME
    )

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testDeleteFailureRaisesInScenarioB(self):
    cluster = _make_mock_cluster(pool_names=[])
    cluster.DeleteNodePool.side_effect = RuntimeError('delete failed')
    with self.assertRaises(RuntimeError):
      kubernetes_management_benchmark._RunScenarioB(cluster, '1.33')

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testPassesInitialVersionToCreate(self):
    """Tests _RunScenarioB passes initial_version to CreateNodePoolAsync."""
    cluster = _make_mock_cluster(pool_names=[])
    kubernetes_management_benchmark._RunScenarioB(cluster, '1.33')
    for call in cluster.CreateNodePoolAsync.call_args_list:
      kw = call.kwargs if call.kwargs else {}
      pos = call.args
      node_version = kw.get('node_version') or (
          pos[1] if len(pos) > 1 else None
      )
      self.assertEqual('1.33', node_version)


class RunScenarioCTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the _RunScenarioC large-scale create-and-delete scenario."""

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testProducesCreateAndDeleteSamples(self):
    cluster = _make_mock_cluster(pool_names=['pkbmc0000', 'pkbmc0001'])
    samples = kubernetes_management_benchmark._RunScenarioC(
        cluster, '1.33', scale=2
    )
    metrics = {s.metric for s in samples}
    self.assertTrue(any('ScenarioC_Create' in m for m in metrics))
    self.assertTrue(any('ScenarioC_Delete' in m for m in metrics))

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testZeroLivePoolsRecordsZeroDeleteSuccessRate(self):
    """Tests Scenario C records 0% delete rate when no live pools exist."""
    cluster = _make_mock_cluster(pool_names=[])
    samples = kubernetes_management_benchmark._RunScenarioC(
        cluster, '1.33', scale=3
    )
    delete_rate = next(
        s for s in samples if s.metric == 'ScenarioC_Delete_SuccessRate'
    )
    self.assertEqual(0.0, delete_rate.value)
    cluster.DeleteNodePoolAsync.assert_not_called()

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testDeleteUsesLiveListNotOriginalCreateList(self):
    cluster = _make_mock_cluster(pool_names=['pkbmc0000', 'pkbmc0001'])
    kubernetes_management_benchmark._RunScenarioC(cluster, '1.33', scale=3)
    self.assertEqual(2, cluster.DeleteNodePoolAsync.call_count)

  @flagsaver.flagsaver(
      k8s_mgmt_nodes_per_nodepool=1,
      k8s_mgmt_max_concurrent=50,
  )
  def testCreateSuccessRateUsesScaleAsDenominator(self):
    """Tests Scenario C create success rate uses scale as total_ops."""
    cluster = _make_mock_cluster(pool_names=['pkbmc0000'])
    samples = kubernetes_management_benchmark._RunScenarioC(
        cluster, '1.33', scale=3
    )
    create_rate = next(
        s for s in samples if s.metric == 'ScenarioC_Create_SuccessRate'
    )
    self.assertLessEqual(create_rate.value, 100.0)
    self.assertEqual('3', create_rate.metadata['total_ops'])


if __name__ == '__main__':
  unittest.main()
