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
"""Unit tests for kubernetes_node_provisioning_intervals parsing and metrics."""

import dataclasses
import unittest

from perfkitbenchmarker.linux_benchmarks.kubernetes import kubernetes_node_provisioning_intervals as pi
from tests import pkb_common_test_case


class ParseTest(pkb_common_test_case.PkbCommonTestCase):

  def testParseEpochSecondsSubSecond(self):
    self.assertAlmostEqual(
        pi.parse_epoch_seconds('1970-01-01T00:01:01.500Z'), 61.5, places=3
    )

  def testConditionTimeDefaultsToTrue(self):
    conds = [
        {
            'type': 'PodScheduled',
            'status': 'False',
            'lastTransitionTime': '1970-01-01T00:00:01Z',
        },
        {
            'type': 'PodScheduled',
            'status': 'True',
            'lastTransitionTime': '1970-01-01T00:00:46Z',
        },
    ]
    self.assertEqual(pi.condition_time(conds, 'PodScheduled'), 46.0)

  def testConditionTimeFalseStatus(self):
    conds = [{
        'type': 'PodScheduled',
        'status': 'False',
        'lastTransitionTime': '1970-01-01T00:00:01Z',
    }]
    self.assertEqual(
        pi.condition_time(conds, 'PodScheduled', status='False'), 1.0
    )

  def testConditionTimeMissingReturnsNone(self):
    self.assertIsNone(pi.condition_time([], 'Ready'))


class RecordTest(pkb_common_test_case.PkbCommonTestCase):

  def testPodRecordFullFields(self):
    item = {
        'metadata': {
            'name': 'pod1',
            'creationTimestamp': '1970-01-01T00:00:00Z',
        },
        'spec': {'nodeName': 'node1'},
        'status': {
            'conditions': [
                {
                    'type': 'PodScheduled',
                    'status': 'False',
                    'lastTransitionTime': '1970-01-01T00:00:01Z',
                },
                {
                    'type': 'PodScheduled',
                    'status': 'True',
                    'lastTransitionTime': '1970-01-01T00:00:46Z',
                },
                {
                    'type': 'ContainersReady',
                    'status': 'True',
                    'lastTransitionTime': '1970-01-01T00:01:10Z',
                },
                {
                    'type': 'Ready',
                    'status': 'True',
                    'lastTransitionTime': '1970-01-01T00:01:15Z',
                },
            ]
        },
    }
    rec = pi.PodRecord.from_item(item)
    self.assertEqual(rec.created, 0.0)
    self.assertEqual(rec.node_name, 'node1')
    self.assertEqual(rec.unschedulable, 1.0)
    self.assertEqual(rec.scheduled, 46.0)
    self.assertEqual(rec.containers_ready, 70.0)
    self.assertEqual(rec.ready, 75.0)

  def testNodeRecordFromItem(self):
    item = {
        'metadata': {
            'name': 'node1',
            'creationTimestamp': '1970-01-01T00:00:10Z',
        },
        'status': {
            'conditions': [{
                'type': 'Ready',
                'status': 'True',
                'lastTransitionTime': '1970-01-01T00:00:30Z',
            }]
        },
    }
    rec = pi.NodeRecord.from_item(item)
    self.assertEqual(rec.created, 10.0)
    self.assertEqual(rec.ready, 30.0)

  def testParseItemsMapsFactory(self):
    blob = {
        'items': [{
            'metadata': {
                'name': 'n1',
                'creationTimestamp': '1970-01-01T00:00:00Z',
            },
            'status': {'conditions': []},
        }]
    }
    recs = pi.parse_items(blob, pi.NodeRecord.from_item)
    self.assertEqual(recs[0].name, 'n1')

  def testNullFieldsHandledGracefully(self):
    pod_item = {
        'metadata': {
            'name': 'pod1',
            'creationTimestamp': '1970-01-01T00:00:00Z',
        },
        'spec': None,
        'status': None,
    }
    rec = pi.PodRecord.from_item(pod_item)
    self.assertIsNone(rec.node_name)
    self.assertIsNone(rec.ready)
    self.assertEqual(
        pi.parse_items({'items': None}, pi.PodRecord.from_item), []
    )


class IntervalsTest(pkb_common_test_case.PkbCommonTestCase):

  def _pod(self, **kw):
    base = dict(
        name='p1',
        created=10.0,
        node_name='n1',
        unschedulable=11.0,
        scheduled=56.0,
        containers_ready=80.0,
        ready=85.0,
    )
    base.update(kw)
    return pi.PodRecord(**base)

  def _node(self, **kw):
    base = dict(name='n1', created=15.0, ready=55.0)
    base.update(kw)
    return pi.NodeRecord(**base)

  def testSubReturnsNoneWhenEitherOperandIsNone(self):
    self.assertIsNone(pi._sub(None, 10.0))
    self.assertIsNone(pi._sub(10.0, None))
    self.assertEqual(pi._sub(15.0, 10.0), 5.0)

  def testIntervals(self):
    [iv] = pi.compute_intervals([self._pod()], [self._node()])
    self.assertEqual(iv.node_registered_s, 5.0)  # node.created(15) - pod(10)
    self.assertEqual(iv.node_ready_s, 45.0)  # node.ready(55) - pod(10)
    self.assertEqual(iv.pod_scheduled_s, 46.0)  # pod.scheduled(56) - pod(10)
    self.assertEqual(iv.pod_ready_s, 75.0)  # pod.ready(85) - pod(10)

  def testUnjoinablePodYieldsNones(self):
    [iv] = pi.compute_intervals([self._pod(node_name=None)], [self._node()])
    self.assertIsNone(iv.node_registered_s)
    self.assertIsNone(iv.node_ready_s)

  def testWarmNodeYieldsNoneForNodeProvisioningIntervals(self):
    # Pre-existing warm node created at t=0 before pod created at t=10.
    [iv] = pi.compute_intervals(
        [self._pod(created=10.0, scheduled=11.0, ready=12.0)],
        [self._node(created=0.0, ready=5.0)],
    )
    self.assertIsNone(iv.node_registered_s)
    self.assertIsNone(iv.node_ready_s)
    self.assertEqual(iv.pod_scheduled_s, 1.0)
    self.assertEqual(iv.pod_ready_s, 2.0)


class SamplesTest(pkb_common_test_case.PkbCommonTestCase):

  def _iv(self, ready):
    return pi.NodeProvisioningIntervals(
        pod_name='p',
        node_name='n',
        node_registered_s=5.0,
        node_ready_s=ready,
        pod_scheduled_s=ready + 1,
        pod_ready_s=ready + 5,
    )

  def testPercentilesAndCount(self):
    ivs = [self._iv(10.0), self._iv(20.0), self._iv(30.0)]
    samples = pi.to_samples(ivs, {'platform': 'gke_ccc'})
    by = {s.metric: s for s in samples if s.metric.endswith(('_p50', '_count'))}
    self.assertEqual(by['node_ready_s_p50'].value, 20.0)
    self.assertEqual(by['node_ready_s_count'].value, 3)
    self.assertEqual(by['node_ready_s_p50'].metadata['platform'], 'gke_ccc')
    per_pod = [s for s in samples if s.metric == 'node_ready_s']
    self.assertLen(per_pod, 3)
    self.assertEqual(per_pod[0].metadata['pod_name'], 'p')
    self.assertEqual(per_pod[0].metadata['node_name'], 'n')

  def testSkipsAllNoneField(self):
    iv = self._iv(10.0)
    iv = dataclasses.replace(iv, node_registered_s=None)
    metrics = {s.metric for s in pi.to_samples([iv], {})}
    self.assertNotIn('node_registered_s_p50', metrics)
    self.assertIn('node_ready_s_p50', metrics)


if __name__ == '__main__':
  unittest.main()
