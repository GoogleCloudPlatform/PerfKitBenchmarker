# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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
"""Unit tests for kubernetes_postgres_sysbench_benchmark execution and preparation."""

import os
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker import errors
from perfkitbenchmarker.configs import container_spec
from perfkitbenchmarker.linux_benchmarks import kubernetes_postgres_sysbench_benchmark
from tests import container_service_mock
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class KubernetesPostgresSysbenchBenchmarkTest(
    pkb_common_test_case.PkbCommonTestCase
):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = '1234'
    self.bm_spec = mock.MagicMock()
    self.bm_spec.postgres_password = 'password'
    self.bm_spec.postgres_service_ip = None

    # Load sample sysbench output for mocking Run
    sysbench_output_path = os.path.join(
        os.path.dirname(__file__), '..', 'data', 'sysbench-output-sample.txt'
    )
    with open(sysbench_output_path) as fp:
      self.sysbench_output = fp.read()

  def _SetupCluster(self, cloud, machine_type='c4-standard-16'):
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
        },
    )
    cluster = container_service_mock.CreateTestKubernetesCluster(spec)
    cluster.CLOUD = cloud
    self.bm_spec.container_cluster = cluster

    mock_nodepool = mock.MagicMock()
    mock_nodepool.vm_spec = {cloud: {'machine_type': machine_type}}
    self.bm_spec.config = mock.MagicMock()
    self.bm_spec.config.container_cluster.nodepools = {
        'postgres': mock_nodepool
    }
    self.bm_spec.config.container_cluster.cloud = cloud
    return cluster

  def _FindCallArgs(self, mock_apply_manifest, template_name):
    for call in mock_apply_manifest.call_args_list:
      if call[0][0] == template_name:
        return call
    return None

  @flagsaver.flagsaver(
      data_disk_type='pd-ssd',
      data_disk_size=100,
  )
  @mock.patch(
      'perfkitbenchmarker.linux_benchmarks.kubernetes_postgres_sysbench_benchmark.kubernetes_commands.ApplyManifest'
  )
  def testPrepare_GCP(self, mock_apply_manifest):
    cluster = self._SetupCluster('GCP')
    kubernetes_postgres_sysbench_benchmark.CheckPrerequisites(
        self.bm_spec.config
    )

    expected_cmds = {
        'delete secret': [('', '', 0)],
        'create secret': [('', '', 0)],
        'pg_isready': [('accepting connections', '', 0)],
        'psql -U benchmark -d benchmark -c "SELECT 1"': [('1', '', 0)],
        'get pod postgres-standalone-0': [('10.244.0.5', '', 0)],
        'apt-get update': [('', '', 0)],
        'apt-get install': [('', '', 0), ('', '', 0), ('', '', 0)],
        'sysbench prepare': [('', '', 0)],
    }
    self.MockIssueCommand(expected_cmds)
    cluster.WaitForResource = mock.MagicMock()

    kubernetes_postgres_sysbench_benchmark.Prepare(self.bm_spec)

    self.assertEqual(self.bm_spec.postgres_service_ip, '10.244.0.5')
    self.assertEqual(mock_apply_manifest.call_count, 2)
    postgres_call = self._FindCallArgs(
        mock_apply_manifest, 'container/postgres_sysbench/postgres_all.yaml.j2'
    )
    self.assertIsNotNone(postgres_call)
    self.assertEqual(postgres_call[1]['disk_type'], 'pd-ssd')
    self.assertEqual(postgres_call[1]['disk_size'], '100Gi')
    self.assertEqual(
        postgres_call[1]['storage_class_provisioner'], 'kubernetes.io/gce-pd'
    )

    client_call = self._FindCallArgs(
        mock_apply_manifest, 'container/postgres_sysbench/client_pod.yaml.j2'
    )
    self.assertIsNotNone(client_call)

  @flagsaver.flagsaver(
      data_disk_type='gp3',
      data_disk_size=100,
  )
  @mock.patch(
      'perfkitbenchmarker.linux_benchmarks.kubernetes_postgres_sysbench_benchmark.kubernetes_commands.ApplyManifest'
  )
  def testPrepare_AWS(self, mock_apply_manifest):
    cluster = self._SetupCluster('AWS', machine_type='m7i.4xlarge')
    expected_cmds = {
        'delete secret': [('', '', 0)],
        'create secret': [('', '', 0)],
        'pg_isready': [('accepting connections', '', 0)],
        'psql -U benchmark -d benchmark -c "SELECT 1"': [('1', '', 0)],
        'get pod postgres-standalone-0': [('10.244.0.5', '', 0)],
        'apt-get update': [('', '', 0)],
        'apt-get install': [('', '', 0), ('', '', 0), ('', '', 0)],
        'sysbench prepare': [('', '', 0)],
    }
    self.MockIssueCommand(expected_cmds)
    cluster.WaitForResource = mock.MagicMock()

    kubernetes_postgres_sysbench_benchmark.Prepare(self.bm_spec)
    self.assertEqual(self.bm_spec.postgres_service_ip, '10.244.0.5')

    self.assertEqual(mock_apply_manifest.call_count, 2)
    postgres_call = self._FindCallArgs(
        mock_apply_manifest, 'container/postgres_sysbench/postgres_all.yaml.j2'
    )
    self.assertIsNotNone(postgres_call)
    self.assertEqual(
        postgres_call[1]['storage_class_provisioner'], 'kubernetes.io/aws-ebs'
    )

  @flagsaver.flagsaver(
      data_disk_type='Premium_LRS',
      data_disk_size=100,
  )
  @mock.patch(
      'perfkitbenchmarker.linux_benchmarks.kubernetes_postgres_sysbench_benchmark.kubernetes_commands.ApplyManifest'
  )
  def testPrepare_Azure(self, mock_apply_manifest):
    cluster = self._SetupCluster('Azure', machine_type='Standard_D16s_v5')
    expected_cmds = {
        'delete secret': [('', '', 0)],
        'create secret': [('', '', 0)],
        'pg_isready': [('accepting connections', '', 0)],
        'psql -U benchmark -d benchmark -c "SELECT 1"': [('1', '', 0)],
        'get pod postgres-standalone-0': [('10.244.0.5', '', 0)],
        'apt-get update': [('', '', 0)],
        'apt-get install': [('', '', 0), ('', '', 0), ('', '', 0)],
        'sysbench prepare': [('', '', 0)],
    }
    self.MockIssueCommand(expected_cmds)
    cluster.WaitForResource = mock.MagicMock()

    kubernetes_postgres_sysbench_benchmark.Prepare(self.bm_spec)
    self.assertEqual(self.bm_spec.postgres_service_ip, '10.244.0.5')

    self.assertEqual(mock_apply_manifest.call_count, 2)
    postgres_call = self._FindCallArgs(
        mock_apply_manifest, 'container/postgres_sysbench/postgres_all.yaml.j2'
    )
    self.assertIsNotNone(postgres_call)
    self.assertEqual(
        postgres_call[1]['storage_class_provisioner'],
        'kubernetes.io/azure-disk',
    )

  @flagsaver.flagsaver(
      kubernetes_postgres_sysbench_template_path=(
          'container/postgres_sysbench/custom_template.yaml.j2'
      ),
      data_disk_type='pd-ssd',
  )
  @mock.patch(
      'perfkitbenchmarker.linux_benchmarks.kubernetes_postgres_sysbench_benchmark.kubernetes_commands.ApplyManifest'
  )
  def testPrepare_CustomTemplatePathForwarded(self, mock_apply_manifest):
    cluster = self._SetupCluster('GCP')
    expected_cmds = {
        'delete secret': [('', '', 0)],
        'create secret': [('', '', 0)],
        'pg_isready': [('accepting connections', '', 0)],
        'psql -U benchmark -d benchmark -c "SELECT 1"': [('1', '', 0)],
        'get pod postgres-standalone-0': [('10.244.0.5', '', 0)],
        'apt-get update': [('', '', 0)],
        'apt-get install': [('', '', 0), ('', '', 0), ('', '', 0)],
        'sysbench prepare': [('', '', 0)],
    }
    self.MockIssueCommand(expected_cmds)
    cluster.WaitForResource = mock.MagicMock()

    kubernetes_postgres_sysbench_benchmark.Prepare(self.bm_spec)
    self.assertEqual(mock_apply_manifest.call_count, 2)
    custom_call = self._FindCallArgs(
        mock_apply_manifest,
        'container/postgres_sysbench/custom_template.yaml.j2',
    )
    self.assertIsNotNone(custom_call)

  def testCheckPrerequisites_GCP_GkeNodeSystemConfig_Passes(self):
    with flagsaver.flagsaver(gke_node_system_config='node_config.yaml'):
      self._SetupCluster('GCP')
      kubernetes_postgres_sysbench_benchmark.CheckPrerequisites(
          self.bm_spec.config
      )

  def testCheckPrerequisites_AWS_GkeNodeSystemConfig_RaisesError(self):
    with flagsaver.flagsaver(gke_node_system_config='node_config.yaml'):
      self._SetupCluster('AWS')
      with self.assertRaises(errors.Setup.InvalidConfigurationError):
        kubernetes_postgres_sysbench_benchmark.CheckPrerequisites(
            self.bm_spec.config
        )

  @flagsaver.flagsaver(
      sysbench_tables=2,
      sysbench_run_threads=[4],
      sysbench_report_interval=10,
      sysbench_run_seconds=10,
      sysbench_testname='oltp_read_write',
  )
  def testRun(self):
    self.bm_spec.postgres_service_ip = '10.244.0.5'

    expected_cmds = {
        'ANALYZE sbtest1': [('', '', 0)],
        'ANALYZE sbtest2': [('', '', 0)],
        'CHECKPOINT': [('', '', 0), ('', '', 0), ('', '', 0)],
        'SELECT count(*) FROM pg_stat_activity': [('0', '', 0)],
        'SHOW shared_buffers;': [('15GB', '', 0)],
        'SHOW effective_cache_size;': [('30GB', '', 0)],
        'sysbench': [(self.sysbench_output, '', 0)],
    }
    self.MockIssueCommand(expected_cmds)

    samples = kubernetes_postgres_sysbench_benchmark.Run(self.bm_spec)

    self.assertTrue(any(s.metric == 'average_latency' for s in samples))
    self.assertTrue(any(s.metric == 'tps' for s in samples))
    self.assertEqual(samples[0].metadata['postgres_shared_buffers'], '15GB')
    self.assertEqual(
        samples[0].metadata['postgres_effective_cache_size'], '30GB'
    )


if __name__ == '__main__':
  unittest.main()
