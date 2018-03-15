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

"""Tests for object storage service benchmark."""

import datetime
import time
import unittest
import mock

from perfkitbenchmarker.linux_benchmarks import object_storage_service_benchmark
from tests import mock_flags


class TestBuildCommands(unittest.TestCase):
  def setUp(self):
    mocked_flags = mock_flags.PatchTestCaseFlags(self)

    mocked_flags.object_storage_multistream_objects_per_stream = 100
    mocked_flags.object_storage_object_sizes = {'1KB': '100%'}
    mocked_flags.object_storage_streams_per_vm = 1
    mocked_flags.num_vms = 1
    mocked_flags.object_storage_object_naming_scheme = 'sequential_by_stream'

  def testBuildCommands(self):
    vm = mock.MagicMock()
    vm.RobustRemoteCommand = mock.MagicMock(return_value=('', ''))

    command_builder = mock.MagicMock()
    service = mock.MagicMock()

    with mock.patch(time.__name__ + '.time', return_value=1.0):
      with mock.patch(object_storage_service_benchmark.__name__ +
                      '._ProcessMultiStreamResults'):
        with mock.patch(object_storage_service_benchmark.__name__ +
                        '.LoadWorkerOutput', return_value=(None, None, None)):
          object_storage_service_benchmark.MultiStreamRWBenchmark(
              [], {}, [vm], command_builder, service, 'bucket')

    self.assertEqual(
        command_builder.BuildCommand.call_args_list[0],
        mock.call(['--bucket=bucket',
                   '--objects_per_stream=100',
                   '--num_streams=1',
                   '--start_time=16.1',
                   '--objects_written_file=/tmp/pkb/pkb-objects-written',
                   '--object_sizes="{1000: 100.0}"',
                   '--object_naming_scheme=sequential_by_stream',
                   '--scenario=MultiStreamWrite',
                   '--stream_num_start=0']))

    self.assertEqual(
        command_builder.BuildCommand.call_args_list[1],
        mock.call(['--bucket=bucket',
                   '--objects_per_stream=100',
                   '--num_streams=1',
                   '--start_time=16.1',
                   '--objects_written_file=/tmp/pkb/pkb-objects-written',
                   '--scenario=MultiStreamRead',
                   '--stream_num_start=0']))


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


class TestColdObjectsWrittenFiles(unittest.TestCase):

  def testFilename(self):
    """Tests the objects written filename can be parsed for an age."""
    with mock_flags.PatchFlags() as mocked_flags:
      mocked_flags.object_storage_region = 'us-central1-a'
      mocked_flags.object_storage_objects_written_file_prefix = 'prefix'
      write_time = datetime.datetime.now() - datetime.timedelta(hours=72)
      with mock.patch.object(object_storage_service_benchmark, '_DatetimeNow',
                             return_value=write_time):
        filename = (
            object_storage_service_benchmark._ColdObjectsWrittenFilename())
      read_time = datetime.datetime.now()
      with mock.patch.object(object_storage_service_benchmark, '_DatetimeNow',
                             return_value=read_time):
        age = object_storage_service_benchmark._ColdObjectsWrittenFileAgeHours(
            filename)
      # Verify that the age is between 72 and 73 hours.
      self.assertLessEqual(72, age)
      self.assertLessEqual(age, 73)


if __name__ == '__main__':
  unittest.main()
