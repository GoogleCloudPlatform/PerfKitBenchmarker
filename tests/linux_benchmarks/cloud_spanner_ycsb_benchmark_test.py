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
"""Tests for cloud_spanner_ycsb_benchmark."""
import textwrap
import unittest
from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker.linux_benchmarks import cloud_spanner_ycsb_benchmark
from perfkitbenchmarker.linux_packages import ycsb
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class CloudSpannerYcsbBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def _CreateMockSpec(self, node_count, load_node_count):
    mock_spec = textwrap.dedent(f"""
    cloud_spanner_ycsb:
      relational_db:
        engine: spanner-googlesql
        spanner_nodes: {node_count}
        spanner_load_nodes: {load_node_count}
    """)
    self.mock_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        mock_spec, 'cloud_spanner_ycsb'
    )
    self.mock_bm_spec.ConstructRelationalDb()
    self.mock_set_nodes = self.enter_context(
        mock.patch.object(self.mock_bm_spec.relational_db, '_SetNodes')
    )

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = 'test_uri'
    self.executor = ycsb.YCSBExecutor('cloudspanner')
    self.mock_load = self.enter_context(
        mock.patch.object(self.executor, 'Load')
    )

  def testLoadDatabaseIncreasedCapacity(self):
    self._CreateMockSpec(3, 6)

    cloud_spanner_ycsb_benchmark._LoadDatabase(
        self.executor, self.mock_bm_spec.relational_db, [], {}
    )

    self.mock_set_nodes.assert_has_calls([mock.call(6), mock.call(3)])
    self.mock_load.assert_called_once()

  def testLoadDatabaseNotCalledRestored(self):
    self._CreateMockSpec(3, 6)
    self.mock_bm_spec.relational_db.restored = True

    cloud_spanner_ycsb_benchmark._LoadDatabase(
        self.executor, self.mock_bm_spec.relational_db, [], {}
    )

    self.mock_set_nodes.assert_not_called()
    self.mock_load.assert_not_called()

  @flagsaver.flagsaver(ycsb_skip_load_stage=True)
  def testLoadDatabaseNotCalledSkipFlag(self):
    self._CreateMockSpec(3, 6)
    self.mock_bm_spec.relational_db.restored = True

    cloud_spanner_ycsb_benchmark._LoadDatabase(
        self.executor, self.mock_bm_spec.relational_db, [], {}
    )

    self.mock_set_nodes.assert_not_called()
    self.mock_load.assert_not_called()


if __name__ == '__main__':
  unittest.main()
