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
"""Unit tests for kubernetes_node_autoprovisioning_benchmark cold resets and samples."""

import json
import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks.kubernetes import kubernetes_node_autoprovisioning_benchmark as npb
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from perfkitbenchmarker.resources.container_service import kubernetes_events
from tests import pkb_common_test_case


class DrainTest(pkb_common_test_case.PkbCommonTestCase):

  def testReturnsWhenZero(self):
    mock_cmd = self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=[
                ('node-a node-b', '', 0),
                ('node-a', '', 0),
                ('', '', 0),
            ],
        )
    )
    npb.WaitForZeroWorkerNodes('pkb_nodepool=provisioning', 600, poll_s=0)
    self.assertEqual(mock_cmd.call_count, 3)
    mock_cmd.assert_called_with(
        [
            'get',
            'nodes',
            '-l',
            'pkb_nodepool=provisioning',
            '-o',
            'jsonpath={.items[*].metadata.name}',
        ],
        suppress_logging=True,
    )

  def testTimeoutRaises(self):
    self.enter_context(
        mock.patch.object(
            kubectl, 'RunKubectlCommand', return_value=('node-a', '', 0)
        )
    )
    with self.assertRaises(vm_util.TimeoutExceededRetryError):
      npb.WaitForZeroWorkerNodes('pkb_nodepool=provisioning', 0, poll_s=0)


class ColdResetTest(pkb_common_test_case.PkbCommonTestCase):

  def testResetScalesDrainsThenResetsPool(self):
    calls = []
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=lambda args, **kw: (calls.append(args) or ('', '', 0)),
        )
    )
    reset_pool = mock.Mock()
    npb.ResetToColdState('pkb_nodepool=provisioning', reset_pool, 600, poll_s=0)
    self.assertLen(calls, 2)
    self.assertEqual(calls[0][:2], ['scale', '--replicas=0'])
    self.assertEqual(
        calls[1][:4], ['get', 'nodes', '-l', 'pkb_nodepool=provisioning']
    )
    reset_pool.assert_called_once()


class PoolResetTest(pkb_common_test_case.PkbCommonTestCase):

  def testKarpenterPoolResetDeletesThenApplies(self):
    calls = []
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=lambda args, **kw: (
                calls.append(args[0]) or ('', '', 0)
            ),
        )
    )
    reset = npb.MakeKarpenterPoolReset('provisioning', '/tmp/np.yaml')
    reset()
    # delete -> get (drain-wait: 0 nodes immediately) -> apply
    self.assertEqual(calls, ['delete', 'get', 'apply'])

  def testComputeClassPoolResetDeletesMatchedPoolsAndNapPools(self):
    cluster = mock.Mock()
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=[
                ('nap-n4std-abc\nnap-n4std-xyz\n', '', 0),  # CC pool names
                (
                    'nap-n4std-abc\nnap-n4std-xyz\nnap-default-zzz\n',
                    '',
                    0,
                ),  # all nodes
                ('', '', 0),  # drain wait: 0 immediately
            ],
        )
    )
    reset = npb.MakeGkeComputeClassPoolReset(cluster, class_name='provisioning')
    reset()
    cluster.DeleteNodePool.assert_any_call('nap-n4std-abc')
    cluster.DeleteNodePool.assert_any_call('nap-n4std-xyz')
    cluster.DeleteNodePool.assert_any_call('nap-default-zzz')
    self.assertEqual(cluster.DeleteNodePool.call_count, 3)

  def testComputeClassPoolResetNoDuplicateDeletion(self):
    cluster = mock.Mock()
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=[
                ('nap-prov-abc\n', '', 0),  # CC pool names
                ('nap-prov-abc\nnap-default-zzz\n', '', 0),  # all nodes
                ('', '', 0),  # drain wait
            ],
        )
    )
    reset = npb.MakeGkeComputeClassPoolReset(cluster, class_name='provisioning')
    reset()
    deleted = [c.args[0] for c in cluster.DeleteNodePool.call_args_list]
    self.assertEqual(deleted.count('nap-prov-abc'), 1)
    self.assertEqual(deleted.count('nap-default-zzz'), 1)

  def testComputeClassPoolResetWithSinglePool(self):
    cluster = mock.Mock()
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=[('nap-prov-1\n', '', 0), ('', '', 0), ('', '', 0)],
        )
    )
    reset = npb.MakeGkeComputeClassPoolReset(cluster, class_name='provisioning')
    reset()
    cluster.DeleteNodePool.assert_called_once_with('nap-prov-1')

  def testGetComputeClassPoolNamesDedup(self):
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            return_value=('nap-prov-abc\nnap-prov-abc\n', '', 0),
        )
    )
    result = npb._GetComputeClassPoolNames('provisioning')
    self.assertEqual(result, ['nap-prov-abc'])

  def testGetComputeClassPoolNamesEmpty(self):
    self.enter_context(
        mock.patch.object(
            kubectl, 'RunKubectlCommand', return_value=('', '', 0)
        )
    )
    result = npb._GetComputeClassPoolNames('provisioning')
    self.assertEqual(result, [])

  def testGetAllNapPoolNamesFiltersNapPrefix(self):
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            return_value=('default-pool\nnap-n4std-abc\nnap-gpu-xyz\n', '', 0),
        )
    )
    result = npb._GetAllNapPoolNames()
    self.assertIn('nap-n4std-abc', result)
    self.assertIn('nap-gpu-xyz', result)
    self.assertNotIn('default-pool', result)

  def testComputeClassPoolResetDrainUsesComputeClassLabel(self):
    cluster = mock.Mock()
    drain_calls = []
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=lambda args, **kw: drain_calls.append(args)
            or ('', '', 0),
        )
    )
    reset = npb.MakeGkeComputeClassPoolReset(cluster, class_name='custom')
    reset()
    first_call = drain_calls[0]
    label_idx = first_call.index('-l')
    self.assertIn('custom', first_call[label_idx + 1])

  def testComputeClassPoolResetWithZeroPools(self):
    cluster = mock.Mock()
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=[('', '', 0), ('', '', 0), ('', '', 0)],
        )
    )
    reset = npb.MakeGkeComputeClassPoolReset(cluster, class_name='provisioning')
    reset()
    cluster.DeleteNodePool.assert_not_called()


class CollectScaleSamplesTest(pkb_common_test_case.PkbCommonTestCase):

  def _make_cluster(self, events_fn=set):
    cluster = mock.Mock()
    cluster.event_poller = kubernetes_events.KubernetesEventPoller(events_fn)
    return cluster

  def testGetConfigEnablesPollForEvents(self):
    cfg = npb.GetConfig({})
    self.assertTrue(cfg['container_cluster']['poll_for_events'])

  @flagsaver.flagsaver(node_autoprovisioning_target_replicas=1)
  def testCollectScaleSamplesEndToEnd(self):
    pods_json = json.dumps({
        'items': [{
            'metadata': {'name': 'p1'},
            'status': {
                'conditions': [
                    {
                        'type': 'PodScheduled',
                        'status': 'True',
                        'lastTransitionTime': '1970-01-01T00:00:46Z',
                    },
                    {
                        'type': 'Ready',
                        'status': 'True',
                        'lastTransitionTime': '1970-01-01T00:01:10Z',
                    },
                ]
            },
        }]
    })
    nodes_json = json.dumps({
        'items': [{
            'metadata': {'name': 'n1'},
            'status': {
                'conditions': [{
                    'type': 'Ready',
                    'status': 'True',
                    'lastTransitionTime': '1970-01-01T00:00:45Z',
                }]
            },
        }]
    })
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=[
                (pods_json, '', 0),
                (nodes_json, '', 0),
            ],
        )
    )
    self.enter_context(
        mock.patch.object(
            kubernetes_commands, 'GetNodeNames', return_value={'sys-0', 'n1'}
        )
    )
    self.enter_context(
        mock.patch.object(
            kubernetes_commands, 'GetPodNames', return_value={'p1'}
        )
    )
    cluster = self._make_cluster()
    samples = npb.CollectScaleSamples(
        cluster,
        start_time=0,
        initial_nodes={'sys-0'},
        initial_pods=set(),
    )
    by = {s.metric: s for s in samples}
    self.assertEqual(by['node_Ready_p50'].value, 45.0)
    self.assertEqual(by['pod_PodScheduled_p50'].value, 46.0)
    self.assertEqual(by['pod_Ready_p50'].value, 70.0)
    self.assertEqual(by['initial_node_count'].value, 1)
    self.assertEqual(by['final_node_count'].value, 2)
    self.assertEqual(by['initial_pod_count'].value, 0)
    self.assertEqual(by['final_pod_count'].value, 1)
    self.assertEqual(by['node_Ready_p50'].metadata['platform'], 'gke_ccc')
    self.assertEqual(by['node_Ready_p50'].metadata['workload'], 'cpu')
    self.assertEqual(by['node_Ready_p50'].metadata['scenario'], '0to1')

  @flagsaver.flagsaver(
      node_autoprovisioning_start_replicas=1,
      node_autoprovisioning_target_replicas=2,
  )
  def testCollectScaleSamplesFiltersIgnoredInitialPodsAndNodes(self):
    pods_json = json.dumps({
        'items': [
            {
                'metadata': {'name': 'p-initial'},
                'status': {
                    'conditions': [{
                        'type': 'Ready',
                        'status': 'True',
                        'lastTransitionTime': '1970-01-01T00:00:10Z',
                    }]
                },
            },
            {
                'metadata': {'name': 'p-new'},
                'status': {
                    'conditions': [
                        {
                            'type': 'PodScheduled',
                            'status': 'True',
                            'lastTransitionTime': '1970-01-01T00:01:46Z',
                        },
                        {
                            'type': 'Ready',
                            'status': 'True',
                            'lastTransitionTime': '1970-01-01T00:01:55Z',
                        },
                    ]
                },
            },
        ]
    })
    nodes_json = json.dumps({
        'items': [
            {
                'metadata': {'name': 'n0'},
                'status': {
                    'conditions': [{
                        'type': 'Ready',
                        'status': 'True',
                        'lastTransitionTime': '1970-01-01T00:00:09Z',
                    }]
                },
            },
            {
                'metadata': {'name': 'n1'},
                'status': {
                    'conditions': [{
                        'type': 'Ready',
                        'status': 'True',
                        'lastTransitionTime': '1970-01-01T00:01:45Z',
                    }]
                },
            },
        ]
    })
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=[
                (pods_json, '', 0),
                (nodes_json, '', 0),
            ],
        )
    )
    self.enter_context(
        mock.patch.object(
            kubernetes_commands, 'GetNodeNames', return_value={'n0', 'n1'}
        )
    )
    self.enter_context(
        mock.patch.object(
            kubernetes_commands,
            'GetPodNames',
            return_value={'p-initial', 'p-new'},
        )
    )
    cluster = self._make_cluster()
    samples = npb.CollectScaleSamples(
        cluster,
        start_time=60,
        initial_nodes={'n0'},
        initial_pods={'p-initial'},
    )
    by = {s.metric: s for s in samples}
    self.assertEqual(by['node_Ready_count'].value, 1)
    self.assertEqual(by['pod_Ready_count'].value, 1)
    self.assertEqual(by['node_Ready_p50'].value, 45.0)
    self.assertEqual(by['pod_PodScheduled_p50'].value, 46.0)
    self.assertEqual(by['pod_Ready_p50'].value, 55.0)

  @flagsaver.flagsaver(
      node_autoprovisioning_workload='gpu',
      node_autoprovisioning_target_replicas=1,
  )
  def testCollectScaleSamplesGpuWorkloadIncludesGpuCountMetadata(self):
    pods_json = json.dumps({
        'items': [{
            'metadata': {'name': 'p1'},
            'status': {
                'conditions': [
                    {
                        'type': 'PodScheduled',
                        'status': 'True',
                        'lastTransitionTime': '1970-01-01T00:01:00Z',
                    },
                    {
                        'type': 'Ready',
                        'status': 'True',
                        'lastTransitionTime': '1970-01-01T00:01:20Z',
                    },
                ]
            },
        }]
    })
    nodes_json = json.dumps({
        'items': [{
            'metadata': {'name': 'n1'},
            'status': {
                'conditions': [{
                    'type': 'Ready',
                    'status': 'True',
                    'lastTransitionTime': '1970-01-01T00:00:45Z',
                }]
            },
        }]
    })
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=[
                (pods_json, '', 0),
                (nodes_json, '', 0),
            ],
        )
    )
    self.enter_context(
        mock.patch.object(
            kubernetes_commands, 'GetNodeNames', return_value={'n1'}
        )
    )
    self.enter_context(
        mock.patch.object(
            kubernetes_commands, 'GetPodNames', return_value={'p1'}
        )
    )
    cluster = self._make_cluster()
    by = {
        s.metric: s
        for s in npb.CollectScaleSamples(
            cluster, start_time=0, initial_nodes=set(), initial_pods=set()
        )
    }
    self.assertEqual(by['node_Ready_p50'].value, 45.0)
    self.assertEqual(by['pod_PodScheduled_p50'].value, 60.0)
    self.assertEqual(by['pod_Ready_p50'].value, 80.0)
    self.assertEqual(by['pod_Ready_p50'].metadata['workload'], 'gpu')
    self.assertEqual(by['pod_Ready_p50'].metadata['gpu_count'], 1)

  @flagsaver.flagsaver(node_autoprovisioning_target_replicas=5)
  def testCollectScaleSamplesRaisesQuotaFailureFromEvents(self):
    pods_json = json.dumps({'items': []})
    self.enter_context(
        mock.patch.object(
            kubectl, 'RunKubectlCommand', return_value=(pods_json, '', 0)
        )
    )
    cluster = self._make_cluster(
        lambda: {
            kubernetes_events.KubernetesEvent(
                reason='FailedScaleUp',
                message='Node scale up failed: GCE quota exceeded.',
                resource=kubernetes_events.KubernetesEventResource(
                    kind='Pod', name='p1'
                ),
                type='Warning',
                timestamp=100,
            )
        }
    )
    with self.assertRaises(errors.Benchmarks.QuotaFailure):
      npb.CollectScaleSamples(
          cluster, start_time=0, initial_nodes=set(), initial_pods=set()
      )

  @flagsaver.flagsaver(
      node_autoprovisioning_target_replicas=5,
      node_autoprovisioning_allow_partial_rollout=True,
  )
  def testCollectScaleSamplesPartialRolloutEmitsAvailableSamples(self):
    pods_json = json.dumps({
        'items': [{
            'metadata': {'name': 'p1'},
            'status': {
                'conditions': [{
                    'type': 'Ready',
                    'status': 'True',
                    'lastTransitionTime': '1970-01-01T00:00:30Z',
                }]
            },
        }]
    })
    nodes_json = json.dumps({'items': []})
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=[(pods_json, '', 0), (nodes_json, '', 0)],
        )
    )
    self.enter_context(
        mock.patch.object(
            kubernetes_commands, 'GetNodeNames', return_value=set()
        )
    )
    self.enter_context(
        mock.patch.object(
            kubernetes_commands, 'GetPodNames', return_value={'p1'}
        )
    )
    cluster = self._make_cluster()
    by = {
        s.metric: s
        for s in npb.CollectScaleSamples(
            cluster, start_time=0, initial_nodes=set(), initial_pods=set()
        )
    }
    self.assertEqual(by['pod_Ready_count'].value, 1)
    self.assertEqual(by['pod_Ready_p50'].value, 30.0)


class KarpenterNodePoolTaintTest(pkb_common_test_case.PkbCommonTestCase):

  def _render_nodepool(self, name):
    return kubernetes_commands.ConvertManifestToYamlDicts(
        'container/karpenter/nodepool.yaml.j2',
        NODEPOOL_NAME=name,
        CLUSTER_NAME='test-cluster',
        ALIAS_VERSION='v20240101',
    )

  def testProvisioningNodePoolHasNoScheduleTaintAndLabel(self):
    yaml_docs = self._render_nodepool('provisioning')
    taints = yaml_docs[0]['spec']['template']['spec'].get('taints', [])
    labels = yaml_docs[0]['spec']['template']['metadata'].get('labels', {})
    self.assertIn(
        {
            'key': 'pkb_nodepool',
            'value': 'provisioning',
            'effect': 'NoSchedule',
        },
        taints,
    )
    self.assertEqual(labels.get('pkb_nodepool'), 'provisioning')

  def testDefaultNodePoolHasNoTaint(self):
    yaml_docs = self._render_nodepool('default')
    self.assertNotIn('taints', yaml_docs[0]['spec']['template']['spec'])


class ComputeClassRenderingTest(pkb_common_test_case.PkbCommonTestCase):

  def _render_compute_class(self, priorities, **extra):
    return kubernetes_commands.ConvertManifestToYamlDicts(
        'container/kubernetes_node_autoprovisioning/compute_class.yaml.j2',
        class_name='provisioning',
        priorities=priorities,
        node_pool_auto_creation=True,
        **extra,
    )

  def testCpuComputeClassRendersMachineFamily(self):
    priorities = [{'machine_family': 'n4'}]
    docs = self._render_compute_class(priorities)
    spec = docs[0]['spec']
    self.assertLen(spec['priorities'], 1)
    self.assertEqual(spec['priorities'][0]['machineFamily'], 'n4')
    self.assertTrue(spec['nodePoolAutoCreation']['enabled'])

  def testGpuComputeClassRendersGpuRule(self):
    priorities = [{
        'machine_family': 'g2',
        'gpu_type': 'nvidia-l4',
        'gpu_count': 1,
    }]
    docs = self._render_compute_class(priorities)
    spec = docs[0]['spec']
    self.assertEqual(spec['priorities'][0]['machineFamily'], 'g2')
    self.assertEqual(
        spec['priorities'][0]['gpu'], {'type': 'nvidia-l4', 'count': 1}
    )

  def testMultiPriorityWithAdvancedFields(self):
    priorities = [
        {
            'machine_type': 'e2-standard-8',
            'min_cores': 8,
            'min_memory_gb': 32,
            'spot': True,
            'max_pods_per_node': 32,
            'nodepools': ['pool-1', 'pool-2'],
        },
        {
            'machine_family': 'n2',
            'spot': False,
        },
    ]
    docs = self._render_compute_class(
        priorities,
        active_migration=True,
        max_parallel_image_pulls=5,
    )
    spec = docs[0]['spec']
    self.assertLen(spec['priorities'], 2)
    p0 = spec['priorities'][0]
    self.assertEqual(p0['machineType'], 'e2-standard-8')
    self.assertEqual(p0['minCores'], 8)
    self.assertEqual(p0['minMemoryGb'], 32)
    self.assertTrue(p0['spot'])
    self.assertEqual(p0['maxPodsPerNode'], 32)
    self.assertEqual(p0['nodepools'], ['pool-1', 'pool-2'])
    p1 = spec['priorities'][1]
    self.assertEqual(p1['machineFamily'], 'n2')
    self.assertFalse(p1['spot'])
    self.assertTrue(spec['activeMigration']['optimizeRulePriority'])
    self.assertEqual(
        spec['nodePoolConfig']['nodeSystemConfig']['kubeletConfig'][
            'maxParallelImagePulls'
        ],
        5,
    )


class ManifestRenderingTest(pkb_common_test_case.PkbCommonTestCase):

  def _render_cpu_manifest(self, **kwargs):
    defaults = {
        'StartReplicas': 0,
        'Image': 'test-image',
        'CpuRequest': '2000m',
        'MemoryRequest': '8Gi',
    }
    defaults.update(kwargs)
    return kubernetes_commands.ConvertManifestToYamlDicts(
        'container/kubernetes_node_autoprovisioning/cpu.yaml.j2',
        **defaults,
    )

  def testCpuManifestSpreadModes(self):
    # anti_affinity
    docs = self._render_cpu_manifest(SpreadMode='anti_affinity')
    pod_spec = docs[0]['spec']['template']['spec']
    self.assertIn('affinity', pod_spec)
    self.assertIn('podAntiAffinity', pod_spec['affinity'])
    self.assertNotIn('topologySpreadConstraints', pod_spec)

    # tsc
    docs = self._render_cpu_manifest(SpreadMode='tsc')
    pod_spec = docs[0]['spec']['template']['spec']
    self.assertIn('topologySpreadConstraints', pod_spec)
    self.assertNotIn('affinity', pod_spec)

    # none
    docs = self._render_cpu_manifest(SpreadMode='none')
    pod_spec = docs[0]['spec']['template']['spec']
    self.assertNotIn('affinity', pod_spec)
    self.assertNotIn('topologySpreadConstraints', pod_spec)

  def testCpuManifestDoNotDisruptAlwaysSet(self):
    docs = self._render_cpu_manifest()
    annotations = (
        docs[0]['spec']['template']['metadata'].get('annotations') or {}
    )
    self.assertEqual(annotations.get('karpenter.sh/do-not-disrupt'), 'true')

  def testCpuManifestCpuMemoryLimitsAndRuntimeClass(self):
    docs = self._render_cpu_manifest(
        CpuRequest='50m',
        CpuLimit='500m',
        MemoryRequest='256Mi',
        MemoryLimit='1Gi',
        RuntimeClassName='gvisor',
    )
    pod_spec = docs[0]['spec']['template']['spec']
    self.assertEqual(pod_spec.get('runtimeClassName'), 'gvisor')
    container = pod_spec['containers'][0]
    self.assertEqual(container['resources']['requests']['cpu'], '50m')
    self.assertEqual(container['resources']['limits']['cpu'], '500m')
    self.assertEqual(container['resources']['requests']['memory'], '256Mi')
    self.assertEqual(container['resources']['limits']['memory'], '1Gi')


class BenchmarkRunTest(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(node_autoprovisioning_allow_partial_rollout=True)
  def testRunAllowsPartialRolloutOnTimeout(self):
    bm_spec = mock.Mock()
    bm_spec.container_cluster = mock.Mock()
    self.enter_context(
        mock.patch.object(
            npb, 'MakeGkeComputeClassPoolReset', return_value=mock.Mock()
        )
    )
    self.enter_context(
        mock.patch.object(
            kubernetes_commands,
            'WaitForRollout',
            side_effect=errors.VmUtil.IssueCommandTimeoutError('timeout'),
        )
    )
    mock_collect = self.enter_context(
        mock.patch.object(npb, 'CollectScaleSamples', return_value=[])
    )
    self.enter_context(
        mock.patch.object(
            kubectl, 'RunKubectlCommand', return_value=('', '', 0)
        )
    )
    result = npb.Run(bm_spec)
    self.assertEqual(result, [])
    mock_collect.assert_called_once()

  @flagsaver.flagsaver(node_autoprovisioning_allow_partial_rollout=False)
  def testRunRaisesOnRolloutTimeoutWhenPartialDisallowed(self):
    bm_spec = mock.Mock()
    bm_spec.container_cluster = mock.Mock()
    self.enter_context(
        mock.patch.object(
            npb, 'MakeGkeComputeClassPoolReset', return_value=mock.Mock()
        )
    )
    self.enter_context(
        mock.patch.object(
            kubernetes_commands,
            'WaitForRollout',
            side_effect=errors.VmUtil.IssueCommandTimeoutError('timeout'),
        )
    )
    mock_collect = self.enter_context(
        mock.patch.object(npb, 'CollectScaleSamples', return_value=[])
    )
    self.enter_context(
        mock.patch.object(
            kubectl, 'RunKubectlCommand', return_value=('', '', 0)
        )
    )
    with self.assertRaises(errors.VmUtil.IssueCommandTimeoutError):
      npb.Run(bm_spec)
    mock_collect.assert_called_once()

  @flagsaver.flagsaver(node_autoprovisioning_pool_state='zero')
  def testRunPoolStateZeroSkipsPoolReset(self):
    bm_spec = mock.Mock()
    bm_spec.container_cluster = mock.Mock()
    reset_mock = mock.Mock()
    self.enter_context(
        mock.patch.object(
            npb, 'MakeGkeComputeClassPoolReset', return_value=reset_mock
        )
    )
    mock_wait = self.enter_context(
        mock.patch.object(npb, 'WaitForZeroWorkerNodes')
    )
    self.enter_context(mock.patch.object(kubernetes_commands, 'WaitForRollout'))
    self.enter_context(
        mock.patch.object(npb, 'CollectScaleSamples', return_value=[])
    )
    self.enter_context(
        mock.patch.object(
            kubectl, 'RunKubectlCommand', return_value=('', '', 0)
        )
    )
    npb.Run(bm_spec)
    reset_mock.assert_not_called()
    mock_wait.assert_called_once()

  @flagsaver.flagsaver(node_autoprovisioning_platform='gke_ccc')
  def testPrepareAndCleanup(self):
    bm_spec = mock.Mock()
    bm_spec.container_cluster = mock.Mock()
    applied_docs = []
    self.enter_context(
        mock.patch.object(
            kubernetes_commands,
            'ApplyYaml',
            side_effect=lambda docs: applied_docs.extend(docs) or [],
        )
    )
    mock_kubectl = self.enter_context(
        mock.patch.object(
            kubectl, 'RunKubectlCommand', return_value=('', '', 0)
        )
    )
    npb.Prepare(bm_spec)
    self.assertLen(applied_docs, 2)
    pod_spec = applied_docs[1]['spec']['template']['spec']
    self.assertEqual(
        pod_spec['nodeSelector']['cloud.google.com/compute-class'],
        'provisioning',
    )
    bm_spec.container_cluster.SaveNodePoolManifest.assert_not_called()

    npb.Cleanup(bm_spec)
    self.assertEqual(mock_kubectl.call_count, 2)

  @flagsaver.flagsaver(node_autoprovisioning_platform='eks_karpenter')
  def testPrepareEksKarpenterSavesNodePoolManifest(self):
    bm_spec = mock.Mock()
    bm_spec.container_cluster = mock.Mock()
    self.enter_context(
        mock.patch.object(kubernetes_commands, 'ApplyYaml', return_value=[])
    )
    self.enter_context(
        mock.patch.object(
            npb, '_GetNodePoolManifestPath', return_value='/tmp/np.yaml'
        )
    )
    npb.Prepare(bm_spec)
    bm_spec.container_cluster.SaveNodePoolManifest.assert_called_once_with(
        'provisioning', '/tmp/np.yaml'
    )

  @flagsaver.flagsaver(node_autoprovisioning_platform='eks_auto')
  def testPrepareAndCleanupEksAuto(self):
    temp_np = self.create_tempdir().create_file('np.yaml').full_path
    bm_spec = mock.Mock()
    bm_spec.container_cluster = mock.Mock()
    applied_docs = []
    self.enter_context(
        mock.patch.object(
            kubernetes_commands,
            'ApplyYaml',
            side_effect=lambda docs: applied_docs.extend(docs) or [],
        )
    )
    self.enter_context(
        mock.patch.object(npb, '_GetNodePoolManifestPath', return_value=temp_np)
    )
    mock_kubectl = self.enter_context(
        mock.patch.object(
            kubectl, 'RunKubectlCommand', return_value=('', '', 0)
        )
    )
    npb.Prepare(bm_spec)
    self.assertLen(applied_docs, 1)
    pod_spec = applied_docs[0]['spec']['template']['spec']
    self.assertEqual(pod_spec['nodeSelector']['pkb_nodepool'], 'provisioning')
    bm_spec.container_cluster.SaveNodePoolManifest.assert_called_once_with(
        'provisioning', temp_np
    )
    mock_kubectl.assert_called_once_with(['apply', '-f', temp_np])

    mock_kubectl.reset_mock()
    npb.Cleanup(bm_spec)
    self.assertEqual(mock_kubectl.call_count, 2)

  @flagsaver.flagsaver(
      node_autoprovisioning_platform='eks_auto',
      node_autoprovisioning_workload='gpu',
  )
  def testRunEksAutoStandaloneRegeneratesMissingManifest(self):
    temp_dir = self.create_tempdir().full_path
    missing_np = f'{temp_dir}/missing-provisioning-nodepool.yaml'
    bm_spec = mock.Mock()
    bm_spec.container_cluster = mock.Mock()
    self.enter_context(
        mock.patch.object(
            npb, '_GetNodePoolManifestPath', return_value=missing_np
        )
    )
    self.enter_context(mock.patch.object(kubernetes_commands, 'WaitForRollout'))
    self.enter_context(
        mock.patch.object(npb, 'CollectScaleSamples', return_value=[])
    )
    mock_kubectl = self.enter_context(
        mock.patch.object(
            kubectl, 'RunKubectlCommand', return_value=('', '', 0)
        )
    )

    npb.Run(bm_spec)

    bm_spec.container_cluster.SaveNodePoolManifest.assert_called_once_with(
        'provisioning', missing_np
    )
    self.assertIn(
        'nodepool.karpenter.sh', mock_kubectl.call_args_list[1].args[0]
    )

  @flagsaver.flagsaver()
  def testBuildGkePrioritiesDerivesMachineFamilyFromCluster(self):
    cluster = mock.Mock()
    cluster.default_nodepool.machine_type = 'n4d-standard-4'
    self.assertEqual(
        npb._BuildGkePriorities(cluster), [{'machine_family': 'n4d'}]
    )
    cluster.default_nodepool.machine_type = 'c3d-standard-4'
    self.assertEqual(
        npb._BuildGkePriorities(cluster), [{'machine_family': 'c3d'}]
    )
    cluster.default_nodepool = None
    self.assertEqual(
        npb._BuildGkePriorities(cluster), [{'machine_family': 'n4'}]
    )


if __name__ == '__main__':
  unittest.main()
