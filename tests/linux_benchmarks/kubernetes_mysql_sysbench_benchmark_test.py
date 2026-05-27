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

"""Unit tests for running sysbench against MySQL on a Kubernetes cluster."""

import unittest
from unittest import mock
from absl import flags
from perfkitbenchmarker.linux_benchmarks import kubernetes_mysql_sysbench_benchmark
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class KubernetesMysqlSysbenchBenchmarkTest(
    pkb_common_test_case.PkbCommonTestCase
):

  def testGetConfig(self):
    config = kubernetes_mysql_sysbench_benchmark.GetConfig({})
    self.assertIn('container_cluster', config)

  def testCheckPrerequisites(self):
    # Verifies that it doesn't raise an error
    kubernetes_mysql_sysbench_benchmark.CheckPrerequisites({})

  @mock.patch(
      'perfkitbenchmarker.resources.container_service.'
      'kubernetes_commands.ApplyManifest'
  )
  @mock.patch(
      'perfkitbenchmarker.resources.container_service.'
      'kubernetes_commands.WaitForRollout'
  )
  def testPrepare(self, mock_wait_rollout, mock_apply_manifest):
    mock_bm_spec = mock.MagicMock()
    mock_bm_spec.container_cluster.CLOUD = 'GCP'
    mock_nodepool = mock.MagicMock()
    mock_nodepool.disk_type = 'hyperdisk-balanced'
    mock_bm_spec.container_cluster.nodepools = {'servers': mock_nodepool}
    mock_bm_spec.container_cluster.default_nodepool = mock_nodepool

    kubernetes_mysql_sysbench_benchmark.Prepare(mock_bm_spec)

    mock_apply_manifest.assert_called_once()
    self.assertEqual(
        mock_apply_manifest.call_args.args[0],
        'container/kubernetes_mysql_sysbench/kubernetes_mysql_sysbench.yaml.j2',
    )
    kwargs = mock_apply_manifest.call_args.kwargs
    self.assertEqual(kwargs['storage_provisioner'], 'pd.csi.storage.gke.io')
    self.assertEqual(kwargs['storage_type'], 'hyperdisk-balanced')

  @mock.patch(
      'perfkitbenchmarker.resources.container_service.'
      'kubernetes_commands.ApplyManifest'
  )
  @mock.patch(
      'perfkitbenchmarker.resources.container_service.'
      'kubernetes_commands.WaitForRollout'
  )
  def testPrepare_CustomTemplatePath(
      self, mock_wait_rollout, mock_apply_manifest
  ):
    FLAGS.kubernetes_mysql_sysbench_template_path = (
        'mysql-pd/kubernetes_mysql_sysbench.yaml.j2'
    )

    mock_bm_spec = mock.MagicMock()
    mock_bm_spec.container_cluster.CLOUD = 'GCP'
    mock_nodepool = mock.MagicMock()
    mock_nodepool.disk_type = 'hyperdisk-balanced'
    mock_bm_spec.container_cluster.nodepools = {'servers': mock_nodepool}
    mock_bm_spec.container_cluster.default_nodepool = mock_nodepool

    kubernetes_mysql_sysbench_benchmark.Prepare(mock_bm_spec)

    mock_apply_manifest.assert_called_once()
    self.assertEqual(
        mock_apply_manifest.call_args.args[0],
        'mysql-pd/kubernetes_mysql_sysbench.yaml.j2',
    )

  @mock.patch(
      'perfkitbenchmarker.resources.container_service.'
      'kubernetes_commands._WriteAndApplyManifest'
  )
  @mock.patch(
      'perfkitbenchmarker.resources.container_service.'
      'kubernetes_commands.WaitForRollout'
  )
  def testPrepare_DefaultTemplateRendering(
      self, mock_wait_rollout, mock_write_apply_manifest
  ):
    mock_bm_spec = mock.MagicMock()
    mock_bm_spec.container_cluster.CLOUD = 'GCP'
    mock_nodepool = mock.MagicMock()
    mock_nodepool.disk_type = 'hyperdisk-balanced'
    mock_bm_spec.container_cluster.nodepools = {'servers': mock_nodepool}
    mock_bm_spec.container_cluster.default_nodepool = mock_nodepool

    mock_write_apply_manifest.return_value = ['statefulset/mysql']

    kubernetes_mysql_sysbench_benchmark.Prepare(mock_bm_spec)

    mock_write_apply_manifest.assert_called_once()
    rendered_manifest = mock_write_apply_manifest.call_args.args[0]

    self.assertIn('performance_schema                  = 1', rendered_manifest)
    self.assertIn(
        'innodb_buffer_pool_size             = 100G',
        rendered_manifest,
    )
    self.assertIn(
        'innodb_redo_log_capacity            = 120G',
        rendered_manifest,
    )
    self.assertIn(
        'innodb_io_capacity                  = 80000',
        rendered_manifest,
    )
    self.assertIn(
        'innodb_io_capacity_max              = 1600000',
        rendered_manifest,
    )
    self.assertIn('innodb_flush_log_at_trx_commit      = 1', rendered_manifest)
    self.assertNotIn('skip_log_bin', rendered_manifest)
    self.assertNotIn('innodb_log_buffer_size', rendered_manifest)

  @mock.patch(
      'perfkitbenchmarker.resources.container_service.kubectl.RunKubectlCommand'
  )
  def testRun(self, mock_run_kubectl):
    FLAGS.kubernetes_mysql_sysbench_image = 'dummy_image'
    FLAGS.sysbench_tables = 1
    FLAGS.sysbench_table_size = 10000
    FLAGS.sysbench_run_threads = [1]
    FLAGS.sysbench_run_seconds = 10

    # Setup mock logs
    mock_logs = """
Starting Sysbench Prepare
...
Starting Sysbench Run
SQL statistics:
    queries performed:
        read:                            100
        write:                           50
        other:                           10
        total:                           160
    transactions:                        10 (1.0 per sec.)
    queries:                             160 (16.0 per sec.)
    ignored errors:                      0 (0.00 per sec.)
    reconnects:                          0 (0.00 per sec.)

General statistics:
    total time:                          10.0s
    total number of events:              10

Latency (ms):
         min:                                  1.00
         avg:                                  5.00
         max:                                 10.00
         95th percentile:                      9.00
         sum:                                 50.00

Threads fairness:
    events (avg/stddev):           10.00/0.00
    execution time (avg/stddev):   0.05/0.00
"""
    mock_run_kubectl.return_value = (mock_logs, '', 0)

    # Mock benchmark spec
    mock_bm_spec = mock.MagicMock()

    # Mock ApplyManifest and WaitForResourceForMultiConditions
    with (
        mock.patch(
            'perfkitbenchmarker.resources.container_service.kubernetes_commands.ApplyManifest'
        ) as mock_apply_manifest,
        mock.patch(
            'perfkitbenchmarker.resources.container_service.kubernetes_commands.WaitForResourceForMultiConditions'
        ) as mock_wait_condition,
    ):

      mock_apply_manifest.return_value = {'job/sysbench'}
      mock_wait_condition.return_value = 'condition=Complete'

      samples = kubernetes_mysql_sysbench_benchmark.Run(mock_bm_spec)

      # Verify samples
      self.assertLen(samples, 5)  # 2 from transactions, 3 from latency

      # Check specific samples
      tps_sample = next(s for s in samples if s.metric == 'tps')
      self.assertEqual(tps_sample.value, 1.0)
      self.assertEqual(
          tps_sample.metadata['kubernetes_mysql_sysbench_template_path'],
          'container/kubernetes_mysql_sysbench/kubernetes_mysql_sysbench.yaml.j2',
      )
      self.assertEqual(
          tps_sample.metadata['sysbench_testname'], 'oltp_read_write'
      )
      self.assertEqual(tps_sample.metadata['sysbench_threads'], 1)
      self.assertEqual(
          tps_sample.metadata['kubernetes_mysql_sysbench_image'],
          'dummy_image',
      )
      self.assertEqual(tps_sample.metadata['kubernetes_mysql_version'], '8.0')

      avg_lat_sample = next(s for s in samples if s.metric == 'average_latency')
      self.assertEqual(avg_lat_sample.value, 5.0)

  _MOCK_LOGS = """
Starting Sysbench Prepare
...
Starting Sysbench Run
SQL statistics:
    queries performed:
        read:                            100
        write:                           50
        other:                           10
        total:                           160
    transactions:                        10 (1.0 per sec.)
    queries:                             160 (16.0 per sec.)
    ignored errors:                      0 (0.00 per sec.)
    reconnects:                          0 (0.00 per sec.)

General statistics:
    total time:                          10.0s
    total number of events:              10

Latency (ms):
         min:                                  1.00
         avg:                                  5.00
         max:                                 10.00
         95th percentile:                      9.00
         sum:                                 50.00

Threads fairness:
    events (avg/stddev):           10.00/0.00
    execution time (avg/stddev):   0.05/0.00
"""

  @mock.patch(
      'perfkitbenchmarker.resources.container_service.kubectl.RunKubectlCommand'
  )
  @mock.patch(
      'perfkitbenchmarker.resources.container_service.kubernetes_commands.ApplyManifest'
  )
  @mock.patch(
      'perfkitbenchmarker.resources.container_service.kubernetes_commands.WaitForResourceForMultiConditions'
  )
  def testRun_DefaultSSL(
      self, mock_wait_condition, mock_apply_manifest, mock_run_kubectl
  ):
    FLAGS.kubernetes_mysql_sysbench_image = 'dummy_image'
    FLAGS.sysbench_tables = 1
    FLAGS.sysbench_table_size = 10000
    FLAGS.sysbench_run_threads = [1]
    FLAGS.sysbench_run_seconds = 10
    FLAGS.sysbench_version = None  # Default

    mock_run_kubectl.return_value = (self._MOCK_LOGS, '', 0)
    mock_apply_manifest.return_value = {'job/sysbench'}
    mock_wait_condition.return_value = 'condition=Complete'

    mock_bm_spec = mock.MagicMock()
    kubernetes_mysql_sysbench_benchmark.Run(mock_bm_spec)

    mock_apply_manifest.assert_called_once()
    kwargs = mock_apply_manifest.call_args.kwargs
    self.assertTrue(kwargs['use_legacy_ssl'])

  @mock.patch(
      'perfkitbenchmarker.resources.container_service.kubectl.RunKubectlCommand'
  )
  @mock.patch(
      'perfkitbenchmarker.resources.container_service.kubernetes_commands.ApplyManifest'
  )
  @mock.patch(
      'perfkitbenchmarker.resources.container_service.kubernetes_commands.WaitForResourceForMultiConditions'
  )
  def testRun_LegacySSL(
      self, mock_wait_condition, mock_apply_manifest, mock_run_kubectl
  ):
    FLAGS.kubernetes_mysql_sysbench_image = 'dummy_image'
    FLAGS.sysbench_tables = 1
    FLAGS.sysbench_table_size = 10000
    FLAGS.sysbench_run_threads = [1]
    FLAGS.sysbench_run_seconds = 10
    FLAGS.sysbench_version = '1.0.20'  # Explicit legacy

    mock_run_kubectl.return_value = (self._MOCK_LOGS, '', 0)
    mock_apply_manifest.return_value = {'job/sysbench'}
    mock_wait_condition.return_value = 'condition=Complete'

    mock_bm_spec = mock.MagicMock()
    kubernetes_mysql_sysbench_benchmark.Run(mock_bm_spec)

    mock_apply_manifest.assert_called_once()
    kwargs = mock_apply_manifest.call_args.kwargs
    self.assertTrue(kwargs['use_legacy_ssl'])


if __name__ == '__main__':
  unittest.main()
