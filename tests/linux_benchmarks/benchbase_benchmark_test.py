# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for Benchbase benchmark."""

import unittest

from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker.linux_benchmarks import benchbase_benchmark
from perfkitbenchmarker.linux_packages import benchbase
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class BenchbaseBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.mock_vm = mock.Mock()
    self.mock_benchmark_spec = mock.create_autospec(
        benchmark_spec.BenchmarkSpec, instance=True
    )
    self.mock_benchmark_spec.vms = [self.mock_vm]
    self.mock_benchmark_spec.relational_db = mock.Mock()
    self.mock_benchmark_spec.relational_db.GetResourceMetadata.return_value = {}
    self.mock_load_config = self.enter_context(
        mock.patch.object(configs, 'LoadConfig', autospec=True)
    )

  def test_get_config(self):
    user_config = {'key': 'value'}
    benchbase_benchmark.GetConfig(user_config)
    self.mock_load_config.assert_called_once_with(
        benchbase_benchmark.BENCHMARK_CONFIG,
        user_config,
        benchbase_benchmark.BENCHMARK_NAME,
    )

  @flagsaver.flagsaver(db_engine='spanner-postgres')
  @mock.patch.object(benchbase, 'CreateConfigFile', autospec=True)
  def test_prepare(self, mock_create_config):
    benchbase_benchmark.Prepare(self.mock_benchmark_spec)

    self.mock_vm.Install.assert_called_once_with('benchbase')
    mock_create_config.assert_called_once_with(self.mock_vm)

  @mock.patch('time.sleep')
  @mock.patch.object(benchbase, 'ParseResults', autospec=True)
  def test_run(self, mock_parse_results, mock_sleep):
    mock_parse_results.return_value = []
    results = benchbase_benchmark.Run(self.mock_benchmark_spec)
    self.assertEqual(results, [])
    self.mock_benchmark_spec.relational_db.GetResourceMetadata.assert_called_once()
    mock_parse_results.assert_called_once_with(self.mock_vm, {})
    mock_sleep.assert_called_once_with(3600)
    self.assertEqual(self.mock_vm.RemoteCommand.call_count, 4)
    update_time_cmd = (
        "sed -i 's|<time>.*</time>|<time>1800</time>|' "
        f'{benchbase.CONFIG_FILE_PATH}'
    )
    self.assertEqual(
        self.mock_vm.RemoteCommand.call_args_list.count(
            mock.call(update_time_cmd)
        ),
        2,
    )


if __name__ == '__main__':
  unittest.main()
