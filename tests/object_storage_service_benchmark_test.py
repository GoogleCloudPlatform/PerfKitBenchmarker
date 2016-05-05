# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for the object_storage_service benchmark."""

import mock
import unittest
import time

from perfkitbenchmarker.linux_benchmarks import object_storage_service_benchmark
from tests import mock_flags


class TestBuildCommands(unittest.TestCase):
  def setUp(self):
    mocked_flags = mock_flags.PatchTestCaseFlags(self)

    mocked_flags.object_storage_scenario = 'api_multistream'
    mocked_flags.object_storage_multistream_objects_per_stream = 100
    mocked_flags.object_storage_object_sizes = {'1KB': '100%'}
    mocked_flags.object_storage_multistream_num_streams = 10

  def testBuildCommands(self):
    vm = mock.MagicMock()
    vm.RobustRemoteCommand = mock.MagicMock(return_value=('', ''))

    with mock.patch(time.__name__ + '.time', return_value=1.0):
      with mock.patch(object_storage_service_benchmark.__name__ +
                      '._ProcessMultiStreamResults'):
        object_storage_service_benchmark.ApiBasedBenchmarks(
            [], {}, vm, 'GCP', 'test_script.py', 'bucket')

    self.assertEqual(
        vm.RobustRemoteCommand.call_args_list[0],
        mock.call('test_script.py --storage_provider=GCP --bucket=bucket '
                  '--objects_per_stream=100 --object_sizes="{1000: 100.0}" '
                  '--num_streams=10 --start_time=7.0 '
                  '--objects_written_file=/tmp/pkb/pkb-objects-written '
                  '--scenario=MultiStreamWrite',
                  should_log=True))

    self.assertEqual(
        vm.RobustRemoteCommand.call_args_list[1],
        mock.call('test_script.py --storage_provider=GCP --bucket=bucket '
                  '--objects_per_stream=100 '
                  '--num_streams=10 --start_time=7.0 '
                  '--objects_written_file=/tmp/pkb/pkb-objects-written '
                  '--scenario=MultiStreamRead',
                  should_log=True))


class TestDistributionToBackendFormat(unittest.TestCase):
  def testPointDistribution(self):
    dist = {'100KB': '100%'}

    self.assertEqual(
        object_storage_service_benchmark._DistributionToBackendFormat(dist),
        {100000: 100.0})

  def testMultiplePointsDistribution(self):
    dist = {'1B': '10%',
            '2B': '20%',
            '4B': '70%'}

    self.assertEqual(
        object_storage_service_benchmark._DistributionToBackendFormat(dist),
        {1: 10.0,
         2: 20.0,
         4: 70.0})

  def testAbbreviatedPointDistribution(self):
    dist = '100KB'

    self.assertEqual(
        object_storage_service_benchmark._DistributionToBackendFormat(dist),
        {100000: 100.0})

  def testBadPercentages(self):
    dist = {'1B': '50%'}

    with self.assertRaises(ValueError):
      object_storage_service_benchmark._DistributionToBackendFormat(dist)


if __name__ == '__main__':
  unittest.main()
