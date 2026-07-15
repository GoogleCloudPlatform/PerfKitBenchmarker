import unittest
from perfkitbenchmarker.linux_benchmarks import provisioning_intervals as pi
from tests import pkb_common_test_case


class ParseTest(pkb_common_test_case.PkbCommonTestCase):

  def testParseEpochSecondsSubSecond(self):
    self.assertAlmostEqual(
        pi.parse_epoch_seconds('1970-01-01T00:01:01.500Z'), 61.5, places=3)

  def testConditionTimeDefaultsToTrue(self):
    conds = [
        {'type': 'PodScheduled', 'status': 'False',
         'lastTransitionTime': '1970-01-01T00:00:01Z'},
        {'type': 'PodScheduled', 'status': 'True',
         'lastTransitionTime': '1970-01-01T00:00:46Z'},
    ]
    self.assertEqual(pi.condition_time(conds, 'PodScheduled'), 46.0)

  def testConditionTimeFalseStatus(self):
    conds = [{'type': 'PodScheduled', 'status': 'False',
              'lastTransitionTime': '1970-01-01T00:00:01Z'}]
    self.assertEqual(
        pi.condition_time(conds, 'PodScheduled', status='False'), 1.0)

  def testConditionTimeMissingReturnsNone(self):
    self.assertIsNone(pi.condition_time([], 'Ready'))


class RecordTest(pkb_common_test_case.PkbCommonTestCase):

  def testPodRecordFullFields(self):
    item = {
        'metadata': {'name': 'pod1',
                     'creationTimestamp': '1970-01-01T00:00:00Z'},
        'spec': {'nodeName': 'node1'},
        'status': {'conditions': [
            {'type': 'PodScheduled', 'status': 'False',
             'lastTransitionTime': '1970-01-01T00:00:01Z'},
            {'type': 'PodScheduled', 'status': 'True',
             'lastTransitionTime': '1970-01-01T00:00:46Z'},
            {'type': 'ContainersReady', 'status': 'True',
             'lastTransitionTime': '1970-01-01T00:01:10Z'},
            {'type': 'Ready', 'status': 'True',
             'lastTransitionTime': '1970-01-01T00:01:15Z'},
        ]}}
    rec = pi.PodRecord.from_item(item)
    self.assertEqual(rec.created, 0.0)
    self.assertEqual(rec.node_name, 'node1')
    self.assertEqual(rec.unschedulable, 1.0)
    self.assertEqual(rec.scheduled, 46.0)
    self.assertEqual(rec.containers_ready, 70.0)
    self.assertEqual(rec.ready, 75.0)

  def testNodeRecordFromItem(self):
    item = {'metadata': {'name': 'node1',
                         'creationTimestamp': '1970-01-01T00:00:10Z'},
            'status': {'conditions': [
                {'type': 'Ready', 'status': 'True',
                 'lastTransitionTime': '1970-01-01T00:00:30Z'}]}}
    rec = pi.NodeRecord.from_item(item)
    self.assertEqual(rec.created, 10.0)
    self.assertEqual(rec.ready, 30.0)

  def testNodeClaimRecordFullConditions(self):
    item = {'metadata': {'name': 'nc1',
                         'creationTimestamp': '1970-01-01T00:00:02Z'},
            'status': {'nodeName': 'node1', 'conditions': [
                {'type': 'Launched', 'status': 'True',
                 'lastTransitionTime': '1970-01-01T00:00:03Z'},
                {'type': 'Registered', 'status': 'True',
                 'lastTransitionTime': '1970-01-01T00:00:40Z'},
                {'type': 'Initialized', 'status': 'True',
                 'lastTransitionTime': '1970-01-01T00:00:50Z'}]}}
    rec = pi.NodeClaimRecord.from_item(item)
    self.assertEqual(rec.node_name, 'node1')
    self.assertEqual(rec.launched, 3.0)
    self.assertEqual(rec.registered, 40.0)
    self.assertEqual(rec.initialized, 50.0)

  def testParseItemsMapsFactory(self):
    blob = {'items': [
        {'metadata': {'name': 'n1',
                      'creationTimestamp': '1970-01-01T00:00:00Z'},
         'status': {'conditions': []}}]}
    recs = pi.parse_items(blob, pi.NodeRecord.from_item)
    self.assertEqual(recs[0].name, 'n1')


class IntervalsTest(pkb_common_test_case.PkbCommonTestCase):

  def _pod(self, **kw):
    base = dict(name='p1', created=0.0, node_name='n1', unschedulable=1.0,
                scheduled=46.0, containers_ready=70.0, ready=75.0)
    base.update(kw)
    return pi.PodRecord(**base)

  def _node(self, **kw):
    base = dict(name='n1', created=5.0, ready=45.0)
    base.update(kw)
    return pi.NodeRecord(**base)

  def _claim(self, **kw):
    base = dict(name='nc1', node_name='n1', created=4.0, launched=6.0,
                registered=44.0, initialized=46.0)
    base.update(kw)
    return pi.NodeClaimRecord(**base)

  def testKarpenterFullSet(self):
    [iv] = pi.compute_intervals([self._pod()], [self._node()], [self._claim()])
    self.assertEqual(iv.unschedulable_detection_s, 1.0)    # 1 - 0
    # claim.created(4) - unsched(1)
    self.assertEqual(iv.provisioner_reaction_s, 3.0)
    # claim.created(4) - 0
    self.assertEqual(iv.provisioner_detection_s, 4.0)
    # launched(6) - claim.created(4)
    self.assertEqual(iv.karpenter_batch_s, 2.0)
    # registered(44) - launched(6)
    self.assertEqual(iv.nodeclaim_register_s, 38.0)
    # initialized(46) - registered(44)
    self.assertEqual(iv.nodeclaim_init_s, 2.0)
    # node.ready(45) - node.created(5)
    self.assertEqual(iv.node_vm_boot_s, 40.0)
    self.assertEqual(iv.total_provisioning_s, 45.0)        # node.ready(45) - 0
    # scheduled(46) - node.ready(45)
    self.assertEqual(iv.pod_bind_s, 1.0)
    # cReady(70) - scheduled(46)
    self.assertEqual(iv.container_ready_s, 24.0)
    # ready(75) - scheduled(46)
    self.assertEqual(iv.pod_ready_s, 29.0)
    self.assertEqual(iv.pod_to_containers_ready_s, 70.0)
    self.assertEqual(iv.pod_to_ready_s, 75.0)

  def testGkeFallbackNoClaims(self):
    [iv] = pi.compute_intervals([self._pod()], [self._node()], [])
    # node.created(5) - unsched(1)
    self.assertEqual(iv.provisioner_reaction_s, 4.0)
    self.assertIsNone(iv.karpenter_batch_s)
    self.assertIsNone(iv.nodeclaim_register_s)
    self.assertEqual(iv.total_provisioning_s, 45.0)

  def testUnjoinablePodYieldsNones(self):
    [iv] = pi.compute_intervals([self._pod(node_name=None)], [self._node()], [])
    self.assertIsNone(iv.total_provisioning_s)
    self.assertIsNone(iv.node_vm_boot_s)


class SamplesTest(pkb_common_test_case.PkbCommonTestCase):

  def _iv(self, total):
    return pi.PodIntervals(
        pod_name='p', node_name='n',
        unschedulable_detection_s=1.0, provisioner_reaction_s=None,
        provisioner_detection_s=None, karpenter_batch_s=None,
        nodeclaim_register_s=None, nodeclaim_init_s=None, node_vm_boot_s=None,
        total_provisioning_s=total, pod_bind_s=None, container_ready_s=None,
        pod_ready_s=None, pod_to_containers_ready_s=None, pod_to_ready_s=None)

  def testPercentilesAndCount(self):
    ivs = [self._iv(10.0), self._iv(20.0), self._iv(30.0)]
    samples = pi.to_samples(ivs, {'platform': 'gke_ccc'})
    by = {s.metric: s for s in samples if s.metric.endswith(('_p50', '_count'))}
    self.assertEqual(by['total_provisioning_s_p50'].value, 20.0)
    self.assertEqual(by['total_provisioning_s_count'].value, 3)
    self.assertEqual(
        by['total_provisioning_s_p50'].metadata['platform'], 'gke_ccc')

  def testSkipsAllNoneField(self):
    metrics = {s.metric for s in pi.to_samples([self._iv(10.0)], {})}
    self.assertNotIn('node_vm_boot_s_p50', metrics)
    self.assertIn('total_provisioning_s_p50', metrics)


class GpuIntervalTest(pkb_common_test_case.PkbCommonTestCase):

  def testDevicePluginInterval(self):
    pod = pi.PodRecord(name='p', created=0.0, node_name='n', unschedulable=1.0,
                       scheduled=46.0, containers_ready=80.0, ready=85.0)
    node = pi.NodeRecord(name='n', created=5.0, ready=45.0)
    base = pi.compute_intervals([pod], [node], [])
    enriched = pi.attach_gpu_interval(base, [node], {'n': 60.0})
    self.assertEqual(enriched[0].device_plugin_ready_s, 15.0)  # 60 - 45
    self.assertIn('device_plugin_ready_s_p50',
                  {s.metric for s in pi.to_samples(enriched, {})})


if __name__ == '__main__':
  unittest.main()
