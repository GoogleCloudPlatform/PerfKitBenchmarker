"""Tests for kubernetes_scale_benchmark, especially parsing events."""

import unittest

from absl.testing import flagsaver
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import container_service
from perfkitbenchmarker import errors
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
    self.expected_num_samples_per_reason = 9

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
                """
                "pod123": [
                  {
                    "lastProbeTime":null,
                    "lastTransitionTime":"1970-01-01T00:01:19Z",
                    "status":"True",
                    "type":"PodReadyToStartContainers"
                  }, {
                    "lastProbeTime":null,
                    "lastTransitionTime":"1970-01-01T18:51:17Z",
                    "status":"True",
                    "type":"Initialized"
                  }, {
                    "lastProbeTime":null,
                    "lastTransitionTime":"1970-01-01T00:01:19Z",
                    "status":"True",
                    "type":"Ready"
                  }, {
                    "lastProbeTime":null,
                    "lastTransitionTime":"1970-01-01T00:01:19Z",
                    "status":"True",
                    "type":"ContainersReady"
                  }
                ],
                "pod456": [
                  {
                    "lastProbeTime":null,
                    "lastTransitionTime":"1970-01-01T18:51:17Z",
                    "status":"True",
                    "type":"PodScheduled"
                  }
                ],
                """,
                '',
                0,
            ),
        )
    )
    conditions = kubernetes_scale_benchmark._GetStatusConditionsForResourceType(
        'pod',
        frozenset(),
    )
    self.assertLen(conditions, 5)

  def testPodStatusConditionsWithIgnoredResources(self):
    self.enter_context(
        mock.patch.object(
            container_service,
            'RunKubectlCommand',
            return_value=(
                """
                "pod123": [
                  {
                    "lastProbeTime":null,
                    "lastTransitionTime":"1970-01-01T00:01:19Z",
                    "status":"True",
                    "type":"PodReadyToStartContainers"
                  }, {
                    "lastProbeTime":null,
                    "lastTransitionTime":"1970-01-01T18:51:17Z",
                    "status":"True",
                    "type":"Initialized"
                  }
                ],
                "pod456": [
                  {
                    "lastProbeTime":null,
                    "lastTransitionTime":"1970-01-01T18:51:17Z",
                    "status":"True",
                    "type":"PodScheduled"
                  }
                ],
                """,
                '',
                0,
            ),
        )
    )
    conditions = kubernetes_scale_benchmark._GetStatusConditionsForResourceType(
        'pod',
        resources_to_ignore=frozenset(['pod456']),
    )
    self.assertLen(conditions, 2)

  def testOneStatForOnePod(self):
    self.enter_context(
        mock.patch.object(
            container_service,
            'RunKubectlCommand',
            side_effect=[
                (
                    """
                    "pod1": [{
                      "lastProbeTime":null,
                      "lastTransitionTime":"1970-01-01T00:01:00Z",
                      "status":"True",
                      "type":"Ready"
                    }],
                    """,
                    '',
                    0,
                ),
            ],
        )
    )
    samples = kubernetes_scale_benchmark.ParseStatusChanges('pod', 50)
    self.assertLen(samples, self.expected_num_samples_per_reason)
    for s in samples:
      self.assertStartsWith(s.metric, 'pod_Ready')
    samples_by_metric = _SamplesByMetric(samples)
    self.assertIn('pod_Ready_p50', samples_by_metric.keys())
    self.assertEqual(samples_by_metric['pod_Ready_p50'].value, 10.0)
    self.assertIn('pod_Ready_count', samples_by_metric.keys())
    self.assertEqual(samples_by_metric['pod_Ready_count'].value, 1)

  def testOneStatForMultiplePods(self):
    self.enter_context(
        mock.patch.object(
            container_service,
            'RunKubectlCommand',
            side_effect=[
                (
                    """
                    "pod1": [{
                      "lastProbeTime":null,
                      "lastTransitionTime":"1970-01-01T00:01:00Z",
                      "status":"True",
                      "type":"Ready"
                    }],
                    "pod2": [{
                      "lastProbeTime":null,
                      "lastTransitionTime":"1970-01-01T00:00:40Z",
                      "status":"True",
                      "type":"Ready"
                    }],
                    "pod3": [{
                      "lastProbeTime":null,
                      "lastTransitionTime":"1970-01-01T00:01:20Z",
                      "status":"True",
                      "type":"Ready"
                    }],
                    """,
                    '',
                    0,
                ),
            ],
        )
    )
    samples = kubernetes_scale_benchmark.ParseStatusChanges('pod', 40)
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
    self.enter_context(
        mock.patch.object(
            container_service,
            'RunKubectlCommand',
            side_effect=[
                (
                    """
                    "pod1": [
                      {
                        "lastProbeTime":null,
                        "lastTransitionTime":"1970-01-01T00:01:00Z",
                        "status":"True",
                        "type":"Ready"
                      }, {
                        "lastProbeTime":null,
                        "lastTransitionTime":"1970-01-01T00:01:00Z",
                        "status":"True",
                        "type":"ContainersReady"
                      }
                    ],
                    """,
                    '',
                    0,
                ),
            ],
        )
    )
    samples = kubernetes_scale_benchmark.ParseStatusChanges('pod', 40)
    self.assertLen(samples, self.expected_num_samples_per_reason * 2)
    for s in samples:
      self.assertStartsWith(s.metric, 'pod_')
    samples_by_metric = _SamplesByMetric(samples)
    self.assertIn('pod_Ready_p50', samples_by_metric.keys())
    self.assertIn('pod_ContainersReady_p50', samples_by_metric.keys())

  def testOneStatForOneNode(self):
    self.enter_context(
        mock.patch.object(
            container_service,
            'RunKubectlCommand',
            side_effect=[
                (
                    """
                    "node1": [{
                      "lastProbeTime":null,
                      "lastTransitionTime":"1970-01-01T00:01:00Z",
                      "status":"True",
                      "type":"Ready"
                    }],
                    """,
                    '',
                    0,
                ),
            ],
        )
    )
    samples = kubernetes_scale_benchmark.ParseStatusChanges('node', 50)
    self.assertLen(samples, self.expected_num_samples_per_reason)
    for s in samples:
      self.assertStartsWith(s.metric, 'node_Ready')
    samples_by_metric = _SamplesByMetric(samples)
    self.assertIn('node_Ready_p50', samples_by_metric.keys())
    self.assertEqual(samples_by_metric['node_Ready_p50'].value, 10.0)
    self.assertIn('node_Ready_count', samples_by_metric.keys())
    self.assertEqual(samples_by_metric['node_Ready_count'].value, 1)

  @flagsaver.flagsaver(kubernetes_scale_num_replicas=10)
  def testCheckFailuresPassesWithCorrectNumberOfPods(self):
    self.cluster.GetEvents.return_value = []
    kubernetes_scale_benchmark._CheckForFailures(
        self.cluster,
        [
            sample.Sample('pod_Ready_p90', 95.0, 'seconds'),
            sample.Sample('pod_Ready_count', 10, 'count'),
        ],
    )

  @flagsaver.flagsaver(kubernetes_scale_num_replicas=10)
  def testCheckFailuresThrowsRegularError(self):
    self.cluster.GetEvents.return_value = [
        container_service.KubernetesEvent(
            reason='PodReady',
            message='Pod is ready',
            resource=container_service.KubernetesEventResource(
                name='pod',
                kind='Pod',
            ),
            type='Normal',
            timestamp=100,
        )
    ]
    with self.assertRaises(errors.Benchmarks.RunError):
      kubernetes_scale_benchmark._CheckForFailures(
          self.cluster,
          [
              sample.Sample('pod_Ready_count', 5, 'count'),
          ],
      )

  @flagsaver.flagsaver(kubernetes_scale_num_replicas=10)
  def testCheckFailuresThrowsQuotaExceeded(self):
    self.cluster.GetEvents.return_value = [
        container_service.KubernetesEvent(
            reason='FailedScaleUp',
            message=(
                'Node scale up in zones us-west1-b associated with this pod'
                ' failed: GCE quota exceeded. Pod is at risk of not being'
                ' scheduled.'
            ),
            resource=container_service.KubernetesEventResource(
                name='pod',
                kind='Pod',
            ),
            type='Warning',
            timestamp=100,
        )
    ]
    with self.assertRaises(errors.Benchmarks.QuotaFailure):
      kubernetes_scale_benchmark._CheckForFailures(
          self.cluster,
          [
              sample.Sample('pod_Ready_count', 5, 'count'),
          ],
      )


if __name__ == '__main__':
  unittest.main()
