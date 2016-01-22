# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for ping_benchmark."""

import unittest
import mock
from perfkitbenchmarker.linux_benchmarks import object_storage_service_benchmark
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import providers
from tests import mock_flags

PKB = 'perfkitbenchmarker'
MOD_PATH = PKB + '.linux_benchmarks.object_storage_service_benchmark'


class TestGenerateJobFileString(unittest.TestCase):


  def testRunCountTest(self):
    with mock.patch('os.path.isfile', return_value=True),\
        mock.patch(PKB + '.data.ResourcePath', return_value=['a', 'b']),\
        mock.patch(MOD_PATH + '.S3StorageBenchmark.Prepare') as Prepare,\
        mock.patch(MOD_PATH
                   + '.S3StorageBenchmark.Run') as Run, mock.patch(
                       MOD_PATH + '.S3StorageBenchmark.Cleanup') as Cleanup:
      flags = mock_flags.PatchTestCaseFlags(self)
      flags.storage = providers.AWS
      flags.object_storage_scenario = 'all'
      vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
      vm0 = mock.MagicMock()
      vm_spec.vms = [vm0]
      object_storage_service_benchmark.Prepare(vm_spec)
      self.assertEqual(Prepare.call_count, 1)
      object_storage_service_benchmark.Run(vm_spec)
      self.assertEqual(Run.call_count, 1)
      object_storage_service_benchmark.Cleanup(vm_spec)
      self.assertEqual(Cleanup.call_count, 1)

if __name__ == '__main__':
  unittest.main()
