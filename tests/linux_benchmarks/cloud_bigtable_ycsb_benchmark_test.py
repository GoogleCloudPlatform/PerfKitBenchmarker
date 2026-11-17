# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for cloud_bigtable_ycsb_benchmark."""
import textwrap
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker.linux_benchmarks import cloud_bigtable_ycsb_benchmark
from perfkitbenchmarker.linux_packages import google_cloud_cbt
from perfkitbenchmarker.linux_packages import ycsb
from perfkitbenchmarker.providers.gcp import gcp_bigtable
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class CloudBigtableYcsbBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def _CreateMockBigtable(self, node_count, load_node_count):
    mock_spec = textwrap.dedent(f"""
    cloud_bigtable_ycsb:
      non_relational_db:
        service_type: bigtable
        node_count: {node_count}
        load_node_count: {load_node_count}
    """)
    self.mock_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        mock_spec, 'cloud_bigtable_ycsb'
    )
    self.mock_bm_spec.ConstructNonRelationalDb()
    self.mock_update_nodes = self.enter_context(
        mock.patch.object(self.mock_bm_spec.non_relational_db, '_UpdateNodes')
    )
    return self.mock_bm_spec.non_relational_db

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = 'test_uri'
    self.executor = ycsb.YCSBExecutor('cloudbigtable')
    self.mock_load = self.enter_context(
        mock.patch.object(self.executor, 'Load')
    )

  def testLoadDatabaseIncreasedCapacity(self):
    test_instance: gcp_bigtable.GcpBigtableInstance = self._CreateMockBigtable(
        node_count=3, load_node_count=6
    )
    cloud_bigtable_ycsb_benchmark._LoadDatabase(
        self.executor, test_instance, [], {}
    )

    self.mock_update_nodes.assert_has_calls([mock.call(6), mock.call(3)])
    self.mock_load.assert_called_once()

  def testLoadDatabaseNotCalledRestored(self):
    test_instance: gcp_bigtable.GcpBigtableInstance = self._CreateMockBigtable(
        node_count=3, load_node_count=6
    )
    test_instance.restored = True

    cloud_bigtable_ycsb_benchmark._LoadDatabase(
        self.executor, test_instance, [], {}
    )

    self.mock_update_nodes.assert_not_called()
    self.mock_load.assert_not_called()

  @flagsaver.flagsaver(ycsb_skip_load_stage=True)
  def testLoadDatabaseNotCalledSkipFlag(self):
    test_instance: gcp_bigtable.GcpBigtableInstance = self._CreateMockBigtable(
        node_count=3, load_node_count=6
    )

    cloud_bigtable_ycsb_benchmark._LoadDatabase(
        self.executor, test_instance, [], {}
    )

    self.mock_update_nodes.assert_not_called()
    self.mock_load.assert_not_called()

  @flagsaver.flagsaver(
      project='test-project',
      google_bigtable_admin_endpoint='test-bigtableadmin.googleapis.com',
      google_bigtable_static_table_name='ycsb table (test)',
  )
  def testCreateTableQuotesCommandArguments(self):
    test_instance: gcp_bigtable.GcpBigtableInstance = self._CreateMockBigtable(
        node_count=3, load_node_count=6
    )
    test_instance.name = 'instance with spaces'
    mock_vm = mock.Mock()
    self.mock_bm_spec.unmanaged_vm_groups = {'default': [mock_vm]}

    cloud_bigtable_ycsb_benchmark._CreateTable(self.mock_bm_spec)

    mock_vm.RemoteCommand.assert_called_once()
    cmd = mock_vm.RemoteCommand.call_args[0][0]
    expected_prefix = (
        f'{google_cloud_cbt.CBT_BIN} -project=test-project '
        "'-instance=instance with spaces' "
        '-admin-endpoint=test-bigtableadmin.googleapis.com:443 '
        "createtable 'ycsb table (test)' "
        f'families={cloud_bigtable_ycsb_benchmark.COLUMN_FAMILY}:maxversions=1 '
        'splits=user1000,'
    )
    self.assertStartsWith(cmd, expected_prefix)

  @flagsaver.flagsaver(
      project='test-project',
      google_bigtable_admin_endpoint='test-bigtableadmin.googleapis.com',
      google_bigtable_static_table_name='ycsb table (test)',
      google_bigtable_delete_static_table=True,
  )
  def testCleanupTableQuotesCommandArguments(self):
    test_instance: gcp_bigtable.GcpBigtableInstance = self._CreateMockBigtable(
        node_count=3, load_node_count=6
    )
    test_instance.name = 'instance with spaces'
    test_instance.user_managed = True
    mock_vm = mock.Mock()
    self.mock_bm_spec.unmanaged_vm_groups = {'default': [mock_vm]}

    cloud_bigtable_ycsb_benchmark.Cleanup(self.mock_bm_spec)

    expected_cmd = (
        f'{google_cloud_cbt.CBT_BIN} -project=test-project '
        "'-instance=instance with spaces' "
        '-admin-endpoint=test-bigtableadmin.googleapis.com:443 '
        "deletetable 'ycsb table (test)'"
    )
    mock_vm.RemoteCommand.assert_called_once_with(expected_cmd)


if __name__ == '__main__':
  unittest.main()
