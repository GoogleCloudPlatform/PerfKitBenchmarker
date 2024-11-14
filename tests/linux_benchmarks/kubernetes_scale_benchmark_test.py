"""Tests for kubernetes_scale_benchmark, especially parsing events."""

import unittest

import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import container_service
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import kubernetes_scale_benchmark
from tests import pkb_common_test_case


def _SamplesByMetric(samples: list[sample.Sample]) -> dict[str, sample.Sample]:
  return {sample.metric: sample for sample in samples}


class KubernetesScaleBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.bm_spec = mock.create_autospec(benchmark_spec.BenchmarkSpec)
    self.cluster = mock.create_autospec(container_service.KubernetesCluster)
    self.bm_spec.container_cluster = self.cluster
    self.expected_num_samples_per_reason = 6

  def testTimestampConvert(self):
    epoch_time = kubernetes_scale_benchmark.ConvertToEpochTime(
        '1970-01-01T00:00:00Z'
    )
    self.assertEqual(epoch_time, 0)
    self.assertEqual(
        kubernetes_scale_benchmark.ConvertToEpochTime('1970-01-01T00:01:00Z'),
        60,
    )

  def testPodStatusConditions(self):
    self.enter_context(
        mock.patch.object(
            container_service,
            'RunKubectlCommand',
            return_value=(
                (
                    '{"lastProbeTime":null,"lastTransitionTime":"1970-01-01T00:01:19Z","status":"True","type":"PodReadyToStartContainers"}'
                    ' {"lastProbeTime":null,"lastTransitionTime":"1970-01-01T18:51:17Z","status":"True","type":"Initialized"}'
                    ' {"lastProbeTime":null,"lastTransitionTime":"1970-01-01T00:01:19Z","status":"True","type":"Ready"}'
                    ' {"lastProbeTime":null,"lastTransitionTime":"1970-01-01T00:01:19Z","status":"True","type":"ContainersReady"}'
                    ' {"lastProbeTime":null,"lastTransitionTime":"1970-01-01T18:51:17Z","status":"True","type":"PodScheduled"}'
                ),
                '',
                0,
            ),
        )
    )
    conditions = kubernetes_scale_benchmark._GetPodStatusConditions('pod123')
    self.assertLen(conditions, 5)

  def testOneStatForOnePod(self):
    self.cluster.GetAllPodNames.return_value = ['pod1']
    self.enter_context(
        mock.patch.object(
            container_service,
            'RunKubectlCommand',
            side_effect=[
                (
                    '{"lastProbeTime":null,"lastTransitionTime":"1970-01-01T00:01:00Z","status":"True","type":"Ready"}',
                    '',
                    0,
                ),
            ],
        )
    )
    samples = kubernetes_scale_benchmark.ParseEvents(self.cluster, 50)
    self.assertLen(samples, self.expected_num_samples_per_reason)
    for s in samples:
      self.assertStartsWith(s.metric, 'pod_Ready')
    samples_by_metric = _SamplesByMetric(samples)
    self.assertIn('pod_Ready_p50', samples_by_metric.keys())
    self.assertEqual(samples_by_metric['pod_Ready_p50'].value, 10.0)
    self.assertIn('pod_Ready_count', samples_by_metric.keys())
    self.assertEqual(samples_by_metric['pod_Ready_count'].value, 1)

  def testOneStatForMultiplePods(self):
    self.cluster.GetAllPodNames.return_value = ['pod1', 'pod2', 'pod3']
    self.enter_context(
        mock.patch.object(
            container_service,
            'RunKubectlCommand',
            side_effect=[
                (
                    '{"lastProbeTime":null,"lastTransitionTime":"1970-01-01T00:01:00Z","status":"True","type":"Ready"}',
                    '',
                    0,
                ),
                (
                    '{"lastProbeTime":null,"lastTransitionTime":"1970-01-01T00:00:40Z","status":"True","type":"Ready"}',
                    '',
                    0,
                ),
                (
                    '{"lastProbeTime":null,"lastTransitionTime":"1970-01-01T00:01:20Z","status":"True","type":"Ready"}',
                    '',
                    0,
                ),
            ],
        )
    )
    samples = kubernetes_scale_benchmark.ParseEvents(self.cluster, 40)
    self.assertLen(samples, self.expected_num_samples_per_reason)
    for s in samples:
      self.assertStartsWith(s.metric, 'pod_Ready')
    samples_by_metric = _SamplesByMetric(samples)
    self.assertIn('pod_Ready_p50', samples_by_metric.keys())
    self.assertEqual(samples_by_metric['pod_Ready_p50'].value, 20.0)
    self.assertIn('pod_Ready_p90', samples_by_metric.keys())
    # 90% of 40 seconds = 36 seconds
    self.assertEqual(samples_by_metric['pod_Ready_p90'].value, 36.0)
    self.assertIn('pod_Ready_count', samples_by_metric.keys())
    self.assertEqual(samples_by_metric['pod_Ready_count'].value, 3)

  def testMultipleStatForOnePod(self):
    self.cluster.GetAllPodNames.return_value = ['pod1']
    self.enter_context(
        mock.patch.object(
            container_service,
            'RunKubectlCommand',
            side_effect=[
                (
                    (
                        '{"lastProbeTime":null,"lastTransitionTime":"1970-01-01T00:01:00Z","status":"True","type":"Ready"} '
                        '{"lastProbeTime":null,"lastTransitionTime":"1970-01-01T00:01:00Z","status":"True","type":"ContainersReady"}'
                    ),
                    '',
                    0,
                ),
            ],
        )
    )
    samples = kubernetes_scale_benchmark.ParseEvents(self.cluster, 40)
    self.assertLen(samples, self.expected_num_samples_per_reason * 2)
    for s in samples:
      self.assertStartsWith(s.metric, 'pod_')
    samples_by_metric = _SamplesByMetric(samples)
    self.assertIn('pod_Ready_p50', samples_by_metric.keys())
    self.assertIn('pod_ContainersReady_p50', samples_by_metric.keys())


if __name__ == '__main__':
  unittest.main()
