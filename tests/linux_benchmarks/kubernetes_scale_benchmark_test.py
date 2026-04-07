"""Tests for kubernetes_scale_benchmark, especially parsing events."""

import json
import unittest

from absl.testing import flagsaver
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import kubernetes_scale_benchmark
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_cluster
from perfkitbenchmarker.resources.container_service import kubernetes_events
from tests import pkb_common_test_case


def _SamplesByMetric(samples: list[sample.Sample]) -> dict[str, sample.Sample]:
  return {sample.metric: sample for sample in samples}


class KubernetesScaleBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.bm_spec = mock.create_autospec(benchmark_spec.BenchmarkSpec)
    self.cluster = mock.create_autospec(kubernetes_cluster.KubernetesCluster)
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
    stdout = json.dumps({
        'items': [
            {
                'metadata': {'name': 'pod123'},
                'status': {
                    'conditions': [
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T00:01:19Z',
                            'status': 'True',
                            'type': 'PodReadyToStartContainers',
                        },
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T18:51:17Z',
                            'status': 'True',
                            'type': 'Initialized',
                        },
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T00:01:19Z',
                            'status': 'True',
                            'type': 'Ready',
                        },
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T00:01:19Z',
                            'status': 'True',
                            'type': 'ContainersReady',
                        },
                    ]
                },
            },
            {
                'metadata': {'name': 'pod456'},
                'status': {
                    'conditions': [
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T18:51:17Z',
                            'status': 'True',
                            'type': 'PodScheduled',
                        },
                    ]
                },
            },
        ]
    })
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            return_value=(stdout, '', 0),
        )
    )
    conditions = kubernetes_scale_benchmark.GetStatusConditionsForResourceType(
        'pod',
        frozenset(),
    )
    self.assertLen(conditions, 5)

  def testPodStatusConditionsInvalid(self):
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            return_value=(
                """
                "pod123": [
                  {
                    "lastProbeTime":null,
                    "lastTransitionTime":null,
                    "status":"True",
                    "message":"Image docker.io is backed by image streaming.",
                    "type":"ImageStreaming"
                  }
                ],
                """,
                '',
                0,
            ),
        )
    )
    conditions = kubernetes_scale_benchmark.GetStatusConditionsForResourceType(
        'pod',
        frozenset(),
    )
    self.assertEmpty(conditions)

  def testPodStatusConditionsWithIgnoredResources(self):
    stdout = json.dumps({
        'items': [
            {
                'metadata': {'name': 'pod123'},
                'status': {
                    'conditions': [
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T00:01:19Z',
                            'status': 'True',
                            'type': 'PodReadyToStartContainers',
                        },
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T18:51:17Z',
                            'status': 'True',
                            'type': 'Initialized',
                        },
                    ]
                },
            },
            {
                'metadata': {'name': 'pod456'},
                'status': {
                    'conditions': [
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T18:51:17Z',
                            'status': 'True',
                            'type': 'PodScheduled',
                        },
                    ]
                },
            },
        ]
    })
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            return_value=(stdout, '', 0),
        )
    )
    conditions = kubernetes_scale_benchmark.GetStatusConditionsForResourceType(
        'pod',
        resources_to_ignore=frozenset(['pod456']),
    )
    self.assertLen(conditions, 2)

  def testOneStatForOnePod(self):
    stdout = json.dumps({
        'items': [
            {
                'metadata': {'name': 'pod1'},
                'status': {
                    'conditions': [
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T00:01:00Z',
                            'status': 'True',
                            'type': 'Ready',
                        },
                    ]
                },
            },
        ]
    })
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=[(stdout, '', 0)],
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
    stdout = json.dumps({
        'items': [
            {
                'metadata': {'name': 'pod1'},
                'status': {
                    'conditions': [{
                        'lastProbeTime': None,
                        'lastTransitionTime': '1970-01-01T00:01:00Z',
                        'status': 'True',
                        'type': 'Ready',
                    }]
                },
            },
            {
                'metadata': {'name': 'pod2'},
                'status': {
                    'conditions': [{
                        'lastProbeTime': None,
                        'lastTransitionTime': '1970-01-01T00:00:40Z',
                        'status': 'True',
                        'type': 'Ready',
                    }]
                },
            },
            {
                'metadata': {'name': 'pod3'},
                'status': {
                    'conditions': [{
                        'lastProbeTime': None,
                        'lastTransitionTime': '1970-01-01T00:01:20Z',
                        'status': 'True',
                        'type': 'Ready',
                    }]
                },
            },
        ]
    })
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=[(stdout, '', 0)],
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
    stdout = json.dumps({
        'items': [
            {
                'metadata': {'name': 'pod1'},
                'status': {
                    'conditions': [
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T00:01:00Z',
                            'status': 'True',
                            'type': 'Ready',
                        },
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T00:01:00Z',
                            'status': 'True',
                            'type': 'ContainersReady',
                        },
                    ]
                },
            },
        ]
    })
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=[(stdout, '', 0)],
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
    stdout = json.dumps({
        'items': [
            {
                'metadata': {'name': 'node1'},
                'status': {
                    'conditions': [
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T00:01:00Z',
                            'status': 'True',
                            'type': 'Ready',
                        },
                    ]
                },
            },
        ]
    })
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=[(stdout, '', 0)],
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

  @flagsaver.flagsaver(kubernetes_scale_report_latency_percentiles=False)
  @flagsaver.flagsaver(kubernetes_scale_report_individual_latencies=True)
  def testReportLatenciesMultipleStatsOnePod(self):
    stdout = json.dumps({
        'items': [
            {
                'metadata': {'name': 'pod1'},
                'status': {
                    'conditions': [
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T00:01:00Z',
                            'status': 'True',
                            'type': 'Ready',
                        },
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T00:01:00Z',
                            'status': 'True',
                            'type': 'ContainersReady',
                        },
                    ]
                },
            },
        ]
    })
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=[(stdout, '', 0)],
        )
    )
    samples = kubernetes_scale_benchmark.ParseStatusChanges('pod', 40)
    self.assertLen(samples, 4)
    # self.assertEqual(samples, [])
    samples_by_metric = _SamplesByMetric(samples)
    self.assertEqual(
        samples_by_metric.keys(),
        {
            'pod_Ready_count',
            'pod_ContainersReady_count',
            'pod_Ready',
            'pod_ContainersReady',
        },
    )

  @flagsaver.flagsaver(kubernetes_scale_num_replicas=10)
  def testCheckFailuresPassesWithCorrectNumberOfPods(self):
    self.cluster.event_poller = kubernetes_events.KubernetesEventPoller(set)
    kubernetes_scale_benchmark.CheckForFailures(
        self.cluster,
        [
            sample.Sample('pod_Ready_p90', 95.0, 'seconds'),
            sample.Sample('pod_Ready_count', 10, 'count'),
        ],
        9,
    )

  @flagsaver.flagsaver(kubernetes_scale_num_replicas=10)
  def testCheckFailuresThrowsRegularError(self):
    self.cluster.event_poller = kubernetes_events.KubernetesEventPoller(
        lambda: {
            kubernetes_events.KubernetesEvent(
                reason='PodReady',
                message='Pod is ready',
                resource=kubernetes_events.KubernetesEventResource(
                    name='pod',
                    kind='Pod',
                ),
                type='Normal',
                timestamp=100,
            )
        }
    )
    with self.assertRaises(errors.Benchmarks.RunError):
      kubernetes_scale_benchmark.CheckForFailures(
          self.cluster,
          [
              sample.Sample('pod_Ready_count', 5, 'count'),
          ],
          9,
      )

  @flagsaver.flagsaver(kubernetes_scale_num_replicas=10)
  def testCheckFailuresThrowsQuotaExceeded(self):
    self.cluster.event_poller = kubernetes_events.KubernetesEventPoller(
        lambda: {
            kubernetes_events.KubernetesEvent(
                reason='FailedScaleUp',
                message=(
                    'Node scale up in zones us-west1-b associated with this pod'
                    ' failed: GCE quota exceeded. Pod is at risk of not being'
                    ' scheduled.'
                ),
                resource=kubernetes_events.KubernetesEventResource(
                    name='pod',
                    kind='Pod',
                ),
                type='Warning',
                timestamp=100,
            )
        }
    )
    with self.assertRaises(errors.Benchmarks.QuotaFailure):
      kubernetes_scale_benchmark.CheckForFailures(
          self.cluster,
          [
              sample.Sample('pod_Ready_count', 5, 'count'),
          ],
          9,
      )


if __name__ == '__main__':
  unittest.main()
