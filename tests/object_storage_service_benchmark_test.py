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


class TestProcessMultiStreamResults(unittest.TestCase):
  def setUp(self):
    self.raw_result = """
[{"latency": 0.1, "operation": "upload", "stream_num": 1, "start_time": 5.0, "size": 1},
 {"latency": 0.1, "operation": "upload", "stream_num": 1, "start_time": 10.0, "size": 1}]"""  # noqa: line too long

    mocked_flags = mock_flags.PatchTestCaseFlags(self)
    mocked_flags.object_storage_scenario = 'api_multistream'
    mocked_flags.object_storage_multistream_objects_per_stream = 100
    mocked_flags.object_storage_object_sizes = '1B'
    mocked_flags.object_storage_multistream_num_streams = 10


  def testProcessResults(self):
    with mock.patch(object_storage_service_benchmark.__name__ +
                    '._AppendPercentilesToResults',
                    return_value=[3]) as append_percentiles:
      object_storage_service_benchmark._ProcessMultiStreamResults(
          self.raw_result, 'upload', [1], [])

      # Can't use append_percentiles.assert_called_once_with when one
      # of the arguments is a generator, because it compares
      # generators by object identity, not by the objects they
      # generate.
      positional_args = append_percentiles.call_args[0]
      self.assertEqual(positional_args[0], [])
      self.assertEqual(list(positional_args[1]), [0.1, 0.1])
      self.assertEqual(positional_args[2],
                       'Multi-stream upload latency')
      self.assertEqual(positional_args[3], 'sec')
      self.assertEqual(positional_args[4], {'object_size_B': 1,
                                            'num_streams': 10,
                                            'objects_per_stream': 100})

  def testPreservesBaseMetadata(self):
    results = []
    object_storage_service_benchmark._ProcessMultiStreamResults(
        self.raw_result, 'upload', [1], results, metadata={'foo': 'bar'})

    self.assertEqual(results[0].metadata['foo'],
                     'bar')

  def testObjectSizeMetadata(self):
    results = []
    object_storage_service_benchmark._ProcessMultiStreamResults(
        self.raw_result, 'upload', [1], results)

    # Testing with [0] and [-1] makes this test independent of exactly
    # how many percentiles _AppendPercentilesToResults adds to
    # results for each object size.
    self.assertEqual(results[0].metadata['object_size_B'], 'distribution')
    self.assertEqual(results[-1].metadata['object_size_B'], 1)


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
