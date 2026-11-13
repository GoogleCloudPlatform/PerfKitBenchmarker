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

import json
import os
from typing import Any
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.configs import container_spec
from perfkitbenchmarker.linux_benchmarks.kubernetes import kubernetes_alloydb_omni_sysbench_benchmark as omni_bench
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from tests import container_service_mock
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_ALLOYDB_OMNI_TEMPLATE = 'container/postgres_sysbench/alloydb_omni_all.yaml.j2'
_CLIENT_TEMPLATE = 'container/postgres_sysbench/client_pod.yaml.j2'


def _DefaultTemplateParams() -> dict[str, object]:
  return {
      'namespace': 'default',
      'omni_image': 'google/alloydbomni:16',
      'postgres_version': '16',
      'storage_class': 'postgres-storage-class',
      'create_storage_class': True,
      'storage_class_provisioner': 'pd.csi.storage.gke.io',
      'postgres_user': 'benchmark',
      'postgres_password': 'secret_password',
      'postgres_database': 'benchmark',
      'disk_size': '100Gi',
      'disk_type': 'hyperdisk-balanced',
      'pin_to_nodepool': True,
      'use_init_container': True,
      'host_network': False,
      'cpu_request': '11',
      'cpu_limit': '14',
      'memory_request': '39Gi',
      'memory_limit': '44Gi',
      'shm_size': '44Gi',
      'max_connections': 1000,
      'shared_buffers': '15GB',
      'effective_cache_size': '30GB',
      'work_mem': '64MB',
      'max_worker_processes': 20,
      'max_parallel_workers_per_gather': 8,
      'max_parallel_workers': 12,
      'wal_buffers': '64MB',
      'max_wal_size': '16GB',
      'checkpoint_timeout': '5min',
      'checkpoint_completion_target': '0.9',
      'effective_io_concurrency': 100,
      'random_page_cost': '1.1',
      'autovacuum_max_workers': 3,
      'enable_columnar_engine': False,
      'columnar_memory_size_mb': 0,
      'extra_postgres_flags': [],
  }


def _NodesJson(
    cpu: str,
    mem: str,
    cap_mem: str | None = None,
    instance_type: str | None = None,
) -> str:
  node: dict[str, Any] = {
      'status': {
          'allocatable': {'cpu': cpu, 'memory': mem},
          'capacity': {'cpu': cpu, 'memory': cap_mem or mem},
      }
  }
  if instance_type:
    node['metadata'] = {
        'labels': {'node.kubernetes.io/instance-type': instance_type}
    }
  return json.dumps({'items': [node]})


class KubernetesAlloydbOmniSysbenchBenchmarkTest(
    pkb_common_test_case.PkbCommonTestCase,
    parameterized.TestCase,
):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = '1234'
    self.bm_spec = mock.MagicMock()
    self.bm_spec.postgres_service_ip = None

    sysbench_output_path = os.path.join(
        os.path.dirname(__file__),
        '..',
        '..',
        'data',
        'sysbench-output-sample.txt',
    )
    with open(sysbench_output_path) as fp:
      self.sysbench_output = fp.read()

  def _SetupCluster(
      self,
      cloud: str,
      machine_type: str = 'c4-standard-16',
      user_managed: bool = False,
  ):
    spec = container_spec.ContainerClusterSpec(
        'test-cluster',
        **{
            'cloud': cloud,
            'vm_spec': {
                cloud: {
                    'machine_type': machine_type,
                    'zone': 'us-central1-a',
                },
            },
            'nodepools': {
                'postgres': {
                    'vm_spec': {
                        cloud: {
                            'machine_type': machine_type,
                            'zone': 'us-central1-a',
                        },
                    },
                },
                'clients': {
                    'vm_spec': {
                        cloud: {
                            'machine_type': machine_type,
                            'zone': 'us-central1-a',
                        },
                    },
                },
            },
        },
    )
    cluster = container_service_mock.CreateTestKubernetesCluster(spec)
    cluster.CLOUD = cloud
    cluster.user_managed = user_managed
    self.bm_spec.container_cluster = cluster

    mock_nodepool = mock.MagicMock()
    mock_nodepool.vm_spec = {cloud: {'machine_type': machine_type}}
    self.bm_spec.config = mock.MagicMock()
    self.bm_spec.config.container_cluster.nodepools = {
        'postgres': mock_nodepool,
    }
    self.bm_spec.config.container_cluster.cloud = cloud
    return cluster

  def _FindApplyManifestCall(self, mock_apply_manifest, template_name):
    for call in mock_apply_manifest.call_args_list:
      if call[0][0] == template_name:
        return call
    return None

  def _BasePrepareCommands(
      self,
      nodes_json: str = '',
      pod_ip: str = '10.244.0.5',
      include_load: bool = True,
  ) -> dict[str, list[tuple[str, str, int]]]:
    cmds = {
        'get nodes': [(nodes_json, '', 0)] if nodes_json else [('', '', 1)],
        'label namespace': [('', '', 0)],
        'pg_isready': [('accepting connections', '', 0)],
        'psql -h 127.0.0.1 -U benchmark -d benchmark -c "SELECT 1"': [
            ('1', '', 0)
        ],
        'get pod postgres-standalone-0': [(pod_ip, '', 0)],
        'delete secret sysbench-passwords': [('', '', 0)],
        'create secret generic sysbench-passwords': [('', '', 0)],
        'apt-get update': [('', '', 0)],
        'apt-get install': [('', '', 0)],
    }
    if include_load:
      cmds['sysbench /usr/share/sysbench/oltp_read_write.lua'] = [('', '', 0)]
    return cmds

  def testRenderManifest_Default(self):
    docs = kubernetes_commands.ConvertManifestToYamlDicts(
        _ALLOYDB_OMNI_TEMPLATE, **_DefaultTemplateParams()
    )
    kinds = [doc['kind'] for doc in docs]
    self.assertEqual(
        kinds,
        [
            'StorageClass',
            'Secret',
            'ConfigMap',
            'ConfigMap',
            'Service',
            'StatefulSet',
        ],
    )

    sc_doc = docs[0]
    self.assertEqual(sc_doc['provisioner'], 'pd.csi.storage.gke.io')
    self.assertEqual(sc_doc['parameters'], {'type': 'hyperdisk-balanced'})

    pg_config_doc = docs[2]
    self.assertNotIn('postgresql.conf', pg_config_doc['data'])
    self.assertIn('scram-sha-256', pg_config_doc['data']['pg_hba.conf'])

    sts_doc = docs[5]
    pod_spec = sts_doc['spec']['template']['spec']
    self.assertIn('affinity', pod_spec)
    container = pod_spec['containers'][0]
    self.assertEqual(container['image'], 'google/alloydbomni:16')
    env_map = {e['name']: e['value'] for e in container['env']}
    self.assertEqual(env_map['POSTGRES_HOST_AUTH_METHOD'], 'scram-sha-256')
    self.assertEqual(
        container['readinessProbe']['exec']['command'],
        [
            'pg_isready',
            '-h',
            '127.0.0.1',
            '-p',
            '5432',
            '-U',
            'benchmark',
            '-d',
            'benchmark',
        ],
    )
    volumes = {v['name']: v for v in pod_spec['volumes']}
    self.assertEqual(
        volumes['dshm']['emptyDir'],
        {'medium': 'Memory', 'sizeLimit': '44Gi'},
    )
    self.assertIn(
        'google_columnar_engine.enabled=off', container['command']
    )

  def testRenderManifest_CustomOptionsAndNoNodepoolPinning(self):
    params = _DefaultTemplateParams()
    params.update({
        'namespace': 'omni-ns',
        'omni_image': 'google/alloydbomni:16.9.0',
        'enable_columnar_engine': True,
        'columnar_memory_size_mb': 2048,
        'extra_postgres_flags': ['synchronous_commit=off'],
        'pin_to_nodepool': False,
        'create_storage_class': False,
        'storage_class': '',
    })
    docs = kubernetes_commands.ConvertManifestToYamlDicts(
        _ALLOYDB_OMNI_TEMPLATE, **params
    )
    self.assertEqual(docs[0]['kind'], 'Namespace')
    self.assertEqual(docs[0]['metadata']['name'], 'omni-ns')
    self.assertNotIn('StorageClass', [d['kind'] for d in docs])

    sts_doc = [d for d in docs if d['kind'] == 'StatefulSet'][0]
    self.assertNotIn(
        'storageClassName',
        sts_doc['spec']['volumeClaimTemplates'][0]['spec'],
    )
    pod_spec = sts_doc['spec']['template']['spec']
    self.assertNotIn('affinity', pod_spec)
    cmd = pod_spec['containers'][0]['command']
    self.assertEqual(
        pod_spec['containers'][0]['image'], 'google/alloydbomni:16.9.0'
    )
    self.assertIn('google_columnar_engine.enabled=on', cmd)
    self.assertIn('google_columnar_engine.memory_size_in_mb=2048', cmd)
    self.assertIn('synchronous_commit=off', cmd)

    client_docs = kubernetes_commands.ConvertManifestToYamlDicts(
        _CLIENT_TEMPLATE,
        namespace='omni-ns',
        client_image='ubuntu:22.04',
        client_cpu_request='4',
        client_cpu_limit='8',
        client_memory_request='10Gi',
        client_memory_limit='20Gi',
        pin_to_nodepool=False,
    )
    self.assertNotIn('affinity', client_docs[0]['spec'])

  def testDefaultBenchmarkConfigAliasExists(self):
    all_configs = configs.GetDefaultAndUserConfig()
    self.assertIn('gke_kubernetes_alloydb_omni_sysbench', all_configs)
    self.assertEqual(
        all_configs['gke_kubernetes_alloydb_omni_sysbench']['name'],
        'kubernetes_alloydb_omni_sysbench',
    )

  @parameterized.named_parameters(
      ('gcp', 'GCP', 'c4-standard-16'),
      ('aws', 'AWS', 'm7i.4xlarge'),
      ('azure', 'Azure', 'Standard_D16s_v5'),
  )
  def testGetConfig_LoadsValidSpec(self, cloud, expected_machine):
    loaded = omni_bench.GetConfig(
        {'container_cluster': {'cloud': cloud}}
    )
    spec = benchmark_config_spec.BenchmarkConfigSpec(
        omni_bench.BENCHMARK_NAME,
        flag_values=FLAGS,
        **loaded,
    )
    cluster_cfg: Any = getattr(spec, 'container_cluster')
    self.assertEqual(cluster_cfg.cloud, cloud)
    pg_vm_spec = cluster_cfg.nodepools['postgres'].vm_spec
    self.assertEqual(pg_vm_spec.machine_type, expected_machine)

  def testGetConfig_MachineTypeOverrides(self):
    with flagsaver.flagsaver(
        kubernetes_alloydb_omni_server_machine_type='n2-highmem-32',
        kubernetes_alloydb_omni_client_machine_type='c4-standard-8',
    ):
      loaded = omni_bench.GetConfig({})
      nodepools = loaded['container_cluster']['nodepools']
      self.assertEqual(
          nodepools['postgres']['vm_spec']['GCP']['machine_type'],
          'n2-highmem-32',
      )
      self.assertEqual(
          nodepools['clients']['vm_spec']['GCP']['machine_type'],
          'c4-standard-8',
      )

  def testCheckPrerequisites_MissingTemplateRaises(self):
    self._SetupCluster('GCP')
    with flagsaver.flagsaver(
        kubernetes_alloydb_omni_sysbench_template_path='/nonexistent/tmpl.j2'
    ):
      with self.assertRaisesRegex(
          errors.Setup.InvalidConfigurationError,
          'Could not find manifest template file',
      ):
        omni_bench.CheckPrerequisites(self.bm_spec.config)

  def testCheckPrerequisites_InvalidPostgresFlagRaises(self):
    self._SetupCluster('GCP')
    with flagsaver.flagsaver(
        kubernetes_alloydb_omni_postgres_flags=['bad_flag_without_equals']
    ):
      with self.assertRaisesRegex(
          errors.Setup.InvalidConfigurationError,
          'formatted as key=value',
      ):
        omni_bench.CheckPrerequisites(self.bm_spec.config)

  def testCheckPrerequisites_GkeNodeSystemConfigNonGcpRaises(self):
    self._SetupCluster('AWS', machine_type='m7i.4xlarge')
    with flagsaver.flagsaver(gke_node_system_config='node.yaml'):
      with self.assertRaisesRegex(
          errors.Setup.InvalidConfigurationError,
          'only supported on GCP',
      ):
        omni_bench.CheckPrerequisites(self.bm_spec.config)

  @parameterized.named_parameters(
      ('cores', '16', 16.0),
      ('millicores', '15890m', 15.89),
      ('nanocores', '500000000n', 0.5),
  )
  def testParseK8sCpu(self, raw, expected):
    self.assertAlmostEqual(omni_bench.ParseK8sCpu(raw), expected, places=4)

  @parameterized.named_parameters(
      ('gib', '48Gi', 48.0),
      ('mib', '2048Mi', 2.0),
      ('kib', '62914560Ki', 60.0),
      ('decimal_g', '1024M', 1024 * (1000.0**2) / (1024.0**3)),
      ('bytes', str(8 * 1024**3), 8.0),
  )
  def testParseK8sMemoryGiB(self, raw, expected):
    self.assertAlmostEqual(
        omni_bench.ParseK8sMemoryGiB(raw), expected, places=4
    )

  def testResolveServerResources_AutoSizesFrom16VcpuNode(self):
    res = omni_bench.ResolveServerResources((15.89, 52.0, 16.0, 60.0))
    self.assertEqual(res['cpu_request'], '11')
    self.assertEqual(res['cpu_limit'], '14')
    self.assertEqual(res['memory_request'], '39Gi')
    self.assertEqual(res['memory_limit'], '44Gi')
    self.assertEqual(res['shared_buffers'], '15GB')
    self.assertEqual(res['effective_cache_size'], '30GB')
    self.assertEqual(res['shm_size'], '44Gi')

  def testResolveClientResources_ClampsOnSmallNode(self):
    res = omni_bench.ResolveClientResources((3.9, 13.0, 4.0, 16.0))
    self.assertEqual(res['client_cpu_request'], '2')
    self.assertEqual(res['client_cpu_limit'], '3')
    self.assertEqual(res['client_memory_request'], '9Gi')
    self.assertEqual(res['client_memory_limit'], '11Gi')

  @mock.patch.object(kubernetes_commands, 'ApplyManifest')
  def testPrepare_GCP_C4AndN2DiskAutoSelection(self, mock_apply):
    cluster = self._SetupCluster('GCP', machine_type='c4-standard-16')
    c4_nodes = _NodesJson('15890m', '52Gi', '60Gi')
    self.MockIssueCommand(self._BasePrepareCommands(nodes_json=c4_nodes))
    cluster.WaitForResource = mock.MagicMock()

    omni_bench.Prepare(self.bm_spec)
    self.assertEqual(self.bm_spec.postgres_service_ip, '10.244.0.5')
    pg_call = self._FindApplyManifestCall(mock_apply, _ALLOYDB_OMNI_TEMPLATE)
    self.assertIsNotNone(pg_call)
    self.assertEqual(pg_call[1]['disk_type'], 'hyperdisk-balanced')
    self.assertEqual(
        pg_call[1]['storage_class_provisioner'], 'pd.csi.storage.gke.io'
    )
    self.assertEqual(pg_call[1]['shared_buffers'], '15GB')

    # Verify N2 automatically selects pd-ssd when data_disk_type is unset.
    mock_apply.reset_mock()
    cluster_n2 = self._SetupCluster('GCP', machine_type='n2-standard-16')
    self.MockIssueCommand(self._BasePrepareCommands(nodes_json=c4_nodes))
    cluster_n2.WaitForResource = mock.MagicMock()
    omni_bench.Prepare(self.bm_spec)
    n2_call = self._FindApplyManifestCall(mock_apply, _ALLOYDB_OMNI_TEMPLATE)
    self.assertEqual(n2_call[1]['disk_type'], 'pd-ssd')

  @parameterized.named_parameters(
      ('aws', 'AWS', 'm7i.4xlarge', 'ebs.csi.aws.com', 'gp3'),
      (
          'azure',
          'Azure',
          'Standard_D16s_v5',
          'disk.csi.azure.com',
          'Premium_LRS',
      ),
  )
  @mock.patch.object(kubernetes_commands, 'ApplyManifest')
  def testPrepare_AWSAndAzure(
      self, cloud, machine_type, expected_provisioner, expected_disk, mock_apply
  ):
    cluster = self._SetupCluster(cloud, machine_type=machine_type)
    self.MockIssueCommand(self._BasePrepareCommands())
    cluster.WaitForResource = mock.MagicMock()

    omni_bench.Prepare(self.bm_spec)
    pg_call = self._FindApplyManifestCall(mock_apply, _ALLOYDB_OMNI_TEMPLATE)
    self.assertEqual(
        pg_call[1]['storage_class_provisioner'], expected_provisioner
    )
    self.assertEqual(pg_call[1]['disk_type'], expected_disk)

  @flagsaver.flagsaver(sysbench_skip_load_stage=True)
  @mock.patch.object(kubernetes_commands, 'ApplyManifest')
  def testPrepare_SkipLoadStage(self, mock_apply):
    cluster = self._SetupCluster('GCP')
    self.MockIssueCommand(self._BasePrepareCommands(include_load=False))
    cluster.WaitForResource = mock.MagicMock()

    omni_bench.Prepare(self.bm_spec)
    self.assertEqual(self.bm_spec.postgres_service_ip, '10.244.0.5')
    self.assertEqual(mock_apply.call_count, 2)

  @mock.patch.object(kubernetes_commands, 'ApplyManifest')
  def testPrepare_PodFailureLogsDebugInfo(self, unused_mock_apply):
    cluster = self._SetupCluster('GCP')

    def _WaitSideEffect(resource, *unused_args, **unused_kwargs):
      if resource == 'pod/postgres-standalone-0':
        raise errors.Resource.RetryableGetError('Pod failed to become ready')

    cluster.WaitForResource = mock.MagicMock(side_effect=_WaitSideEffect)
    self.MockIssueCommand({
        'get nodes': [('', '', 1)],
        'label namespace': [('', '', 0)],
        'get statefulset': [('postgres-standalone', '', 0)],
        'describe pods': [('pods-desc', '', 0)],
        'describe statefulset postgres-standalone': [('sts-desc', '', 0)],
        'logs postgres-standalone-0': [('omni-logs', '', 0)],
        'delete secret sysbench-passwords': [('', '', 0)],
        'create secret generic sysbench-passwords': [('', '', 0)],
        'apt-get update': [('', '', 0)],
        'apt-get install': [('', '', 0)],
    })

    with self.assertRaisesRegex(
        errors.VmUtil.ThreadException, 'RetryableGetError'
    ):
      omni_bench.Prepare(self.bm_spec)

  @flagsaver.flagsaver(
      sysbench_tables=2,
      sysbench_run_threads=[16, 32],
      sysbench_report_interval=2,
      sysbench_run_seconds=10,
      sysbench_testname='oltp_read_write',
  )
  def testRun_MultipleThreadsAndMetadata(self):
    self._SetupCluster('GCP')
    self.bm_spec.postgres_service_ip = '10.244.0.5'
    self.bm_spec.resolved_disk_type = 'hyperdisk-balanced'
    self.bm_spec.resolved_shm_size = '44Gi'

    expected_cmds = {
        'cat /VERSION.txt': [('AlloyDB Omni version: 16.9.0\n', '', 0)],
        "current_setting('server_version')": [('16.9\n', '', 0)],
        "current_setting('shared_buffers')": [('15GB\n', '', 0)],
        "current_setting('effective_cache_size')": [('30GB\n', '', 0)],
        'google_columnar_engine.enabled': [('off\n', '', 0)],
        'ANALYZE sbtest1': [('', '', 0), ('', '', 0)],
        'ANALYZE sbtest2': [('', '', 0), ('', '', 0)],
        'CHECKPOINT': [('', '', 0)] * 6,
        'pg_stat_activity': [('0', '', 0), ('0', '', 0)],
        'sysbench /usr/share/sysbench/oltp_read_write.lua': [
            (self.sysbench_output, '', 0),
            (self.sysbench_output, '', 0),
        ],
    }
    mock_cmds = self.MockIssueCommand(expected_cmds)

    samples = omni_bench.Run(self.bm_spec)
    self.assertNotIn('--thread-init-timeout', mock_cmds.all_commands)
    tps_samples = [s for s in samples if s.metric == 'tps']
    self.assertLen(tps_samples, 2)
    self.assertEqual(
        [s.metadata['sysbench_threads'] for s in tps_samples], ['16', '32']
    )
    first_meta = tps_samples[0].metadata
    self.assertEqual(first_meta['alloydb_omni_version'], '16.9.0')
    self.assertEqual(first_meta['postgres_shared_buffers'], '15GB')
    self.assertEqual(first_meta['google_columnar_engine_enabled'], 'off')
    self.assertEqual(first_meta['disk_type'], 'hyperdisk-balanced')
    self.assertEqual(first_meta['shm_size'], '44Gi')
    self.assertTrue(any(s.metric == 'tps_array' for s in samples))
    self.assertTrue(any(s.metric == 'average_latency' for s in samples))

  @flagsaver.flagsaver(
      kubernetes_alloydb_omni_namespace='omni-ns',
      kubeconfig='/nonexistent/static-kubeconfig',
  )
  @mock.patch.object(kubernetes_commands, 'ApplyManifest')
  def testPrepare_StaticClusterCustomNamespaceAndNodeLabelDetection(
      self, mock_apply
  ):
    cluster = self._SetupCluster(
        'GCP', machine_type='c4-standard-16', user_managed=True
    )
    cluster.WaitForResource = mock.MagicMock()
    cluster._GetKubeconfig = mock.MagicMock()
    n2_nodes = _NodesJson(
        '15890m', '52Gi', '60Gi', instance_type='n2-standard-16'
    )
    cmds = self._BasePrepareCommands(nodes_json=n2_nodes)
    cmds['get namespace omni-ns'] = [('', 'not found', 1)]
    cmds['create namespace omni-ns'] = [('', '', 0)]
    cmds['delete storageclass postgres-storage-class-omni-ns'] = [('', '', 0)]
    mock_cmds = self.MockIssueCommand(cmds)

    omni_bench.Prepare(self.bm_spec)
    cluster._GetKubeconfig.assert_called_once_with()
    self.assertTrue(self.bm_spec.always_call_cleanup)
    self.assertIn('create namespace omni-ns', mock_cmds.all_commands)
    self.assertIn('label namespace omni-ns', mock_cmds.all_commands)
    self.assertIn(
        'delete storageclass postgres-storage-class-omni-ns',
        mock_cmds.all_commands,
    )
    pg_call = self._FindApplyManifestCall(mock_apply, _ALLOYDB_OMNI_TEMPLATE)
    self.assertIsNotNone(pg_call)
    self.assertEqual(pg_call[1]['disk_type'], 'pd-ssd')
    self.assertTrue(pg_call[1]['create_storage_class'])
    self.assertEqual(
        pg_call[1]['storage_class'], 'postgres-storage-class-omni-ns'
    )
    self.assertFalse(pg_call[1]['pin_to_nodepool'])
    self.assertEqual(self.bm_spec.resolved_machine_type, 'n2-standard-16')

  @mock.patch.object(kubernetes_commands, 'ApplyManifest')
  def testPrepare_GenericKubernetesCloudUsesDefaultStorageClass(
      self, mock_apply
  ):
    cluster = self._SetupCluster('GCP', user_managed=True)
    cluster.CLOUD = 'Kubernetes'
    cluster.WaitForResource = mock.MagicMock()
    cluster._GetKubeconfig = mock.MagicMock()
    self.MockIssueCommand(self._BasePrepareCommands())

    omni_bench.Prepare(self.bm_spec)
    pg_call = self._FindApplyManifestCall(mock_apply, _ALLOYDB_OMNI_TEMPLATE)
    self.assertIsNotNone(pg_call)
    self.assertFalse(pg_call[1]['create_storage_class'])
    self.assertEqual(pg_call[1]['storage_class'], '')
    self.assertEqual(pg_call[1]['storage_class_provisioner'], '')
    docs = kubernetes_commands.ConvertManifestToYamlDicts(
        _ALLOYDB_OMNI_TEMPLATE, **pg_call[1]
    )
    sts_doc = [d for d in docs if d['kind'] == 'StatefulSet'][0]
    self.assertNotIn(
        'storageClassName',
        sts_doc['spec']['volumeClaimTemplates'][0]['spec'],
    )

  def testCleanup_StaticVsManaged(self):
    self._SetupCluster('GCP', user_managed=False)
    mock_cmds = self.MockIssueCommand({})
    omni_bench.Cleanup(self.bm_spec)
    self.assertEqual(mock_cmds.all_commands, '')

    self._SetupCluster('GCP', user_managed=True)
    static_cmds = self.MockIssueCommand({'delete ': [('', '', 0)]})
    omni_bench.Cleanup(self.bm_spec)
    self.assertEqual(static_cmds.progress_through_calls['delete '], 9)


if __name__ == '__main__':
  unittest.main()
