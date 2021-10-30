# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for fio_benchmark."""

import unittest
from absl import flags
import mock

from perfkitbenchmarker import temp_dir
from perfkitbenchmarker import units
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import fio_benchmark
from tests import pkb_common_test_case
from six.moves import builtins

FLAGS = flags.FLAGS


class TestGenerateJobFileString(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(TestGenerateJobFileString, self).setUp()
    self.filename = '/test/filename'

  def testBasicGeneration(self):
    expected_jobfile = """
[global]
ioengine=libaio
invalidate=1
direct=1
runtime=600
time_based
filename=/test/filename
do_verify=0
verify_fatal=0
group_reporting=1
randrepeat=0
offset_increment=1k

[sequential_read-io-depth-1-num-jobs-1]
stonewall
rw=read
blocksize=512k
iodepth=1
size=100%
numjobs=1

[sequential_read-io-depth-2-num-jobs-1]
stonewall
rw=read
blocksize=512k
iodepth=2
size=100%
numjobs=1"""

    self.assertEqual(
        fio_benchmark.GenerateJobFileString(
            self.filename,
            ['sequential_read'],
            [1, 2], [1],
            None, None, 600, True, ['randrepeat=0', 'offset_increment=1k']),
        expected_jobfile)

  def testMultipleScenarios(self):
    expected_jobfile = """
[global]
ioengine=libaio
invalidate=1
direct=1
runtime=600
time_based
filename=/test/filename
do_verify=0
verify_fatal=0
group_reporting=1
randrepeat=0

[sequential_read-io-depth-1-num-jobs-1]
stonewall
rw=read
blocksize=512k
iodepth=1
size=100%
numjobs=1

[sequential_write-io-depth-1-num-jobs-1]
stonewall
rw=write
blocksize=512k
iodepth=1
size=100%
numjobs=1"""

    self.assertEqual(
        fio_benchmark.GenerateJobFileString(
            self.filename,
            ['sequential_read', 'sequential_write'],
            [1], [1],
            None, None, 600, True, ['randrepeat=0']),
        expected_jobfile)

  def testCustomBlocksize(self):
    orig_blocksize = fio_benchmark.SCENARIOS['sequential_write']['blocksize']

    job_file = fio_benchmark.GenerateJobFileString(
        self.filename,
        ['sequential_read'],
        [1], [1], None, units.Unit('megabyte') * 2, 600, True, {})

    self.assertIn('blocksize=2000000B', job_file)

    # Test that generating a job file doesn't modify the global
    # SCENARIOS variable.
    self.assertEqual(fio_benchmark.SCENARIOS['sequential_write']['blocksize'],
                     orig_blocksize)

  def testIndirectIO(self):
    job_file = fio_benchmark.GenerateJobFileString(
        self.filename,
        ['sequential_read'],
        [1], [1], None, units.Unit('megabyte') * 2, 600, False, {})
    self.assertIn('direct=0', job_file)

  def testParseGenerateScenario(self):
    expected_jobfile = """
[global]
ioengine=libaio
invalidate=1
direct=1
runtime=600
time_based
filename=/test/filename
do_verify=0
verify_fatal=0
group_reporting=1
randrepeat=0

[seq_64M_read_10TB-io-depth-1-num-jobs-1]
stonewall
rw=read
blocksize=64M
iodepth=1
size=10TB
numjobs=1

[rand_16k_readwrite_5TB_rwmixread-65-io-depth-1-num-jobs-1]
stonewall
rw=randrw
rwmixread=65
blocksize=16k
iodepth=1
size=5TB
numjobs=1"""

    self.assertEqual(
        fio_benchmark.GenerateJobFileString(
            self.filename,
            ['seq_64M_read_10TB', 'rand_16k_readwrite_5TB_rwmixread-65'],
            [1], [1],
            None, None, 600, True, ['randrepeat=0']),
        expected_jobfile)


class TestProcessedJobFileString(pkb_common_test_case.PkbCommonTestCase):

  def testReplaceFilenames(self):
    file_contents = """
[global]
blocksize = 4k
filename = zanzibar
ioengine=libaio

[job1]
filename = asdf
blocksize = 8k
"""

    jobfile = fio_benchmark.ProcessedJobFileString(file_contents, True)
    self.assertNotIn('filename', jobfile)
    self.assertNotIn('zanzibar', jobfile)
    self.assertNotIn('asdf', jobfile)

  def doTargetModeTest(self, mode,
                       expect_fill_device=None,
                       expect_against_device=None,
                       expect_format_disk=None):
    fio_name = fio_benchmark.__name__
    vm_name = vm_util.__name__
    dir_name = temp_dir.__name__

    with mock.patch(fio_name + '.FillDevice') as mock_fill_device, \
        mock.patch(fio_name + '.GetOrGenerateJobFileString') as mock_get_job_string, \
        mock.patch(builtins.__name__ + '.open'), \
        mock.patch(vm_name + '.GetTempDir', return_value='/tmp/dir'), \
        mock.patch(vm_name + '.PrependTempDir', return_value='/tmp/prepend_dir'), \
        mock.patch(dir_name + '.GetRunDirPath', return_value='/tmp/run_dir'), \
        mock.patch(fio_name + '.fio.ParseResults'), \
        mock.patch(fio_name + '.FLAGS') as mock_fio_flags:
      mock_fio_flags.fio_target_mode = mode
      benchmark_spec = mock.MagicMock()
      benchmark_spec.vms = [mock.MagicMock()]
      benchmark_spec.vms[0].RobustRemoteCommand = (
          mock.MagicMock(return_value=('"stdout"', '"stderr"')))
      fio_benchmark.Prepare(benchmark_spec)
      fio_benchmark.Run(benchmark_spec)

      if expect_fill_device is True:
        self.assertEqual(mock_fill_device.call_count, 1)
      elif expect_fill_device is False:
        self.assertEqual(mock_fill_device.call_count, 0)
      # get_job_string.call_args[0][2] is a boolean saying whether or
      # not we are testing against a device.
      against_device_arg = mock_get_job_string.call_args[0][2]
      if expect_against_device is True:
        self.assertEqual(against_device_arg, True)
      elif expect_against_device is False:
        self.assertEqual(against_device_arg, False)

      if expect_format_disk is True:
        self.assertEqual(benchmark_spec.vms[0].FormatDisk.call_count, 1)
      elif expect_format_disk is False:
        self.assertEqual(benchmark_spec.vms[0].FormatDisk.call_count, 0)

  def testAgainstFileWithFill(self):
    self.doTargetModeTest('against_file_with_fill',
                          expect_fill_device=True,
                          expect_against_device=False,
                          expect_format_disk=True)

  def testAgainstFileWithoutFill(self):
    self.doTargetModeTest('against_file_without_fill',
                          expect_fill_device=False,
                          expect_against_device=False,
                          expect_format_disk=False)

  def testAgainstDeviceWithFill(self):
    self.doTargetModeTest('against_device_with_fill',
                          expect_fill_device=True,
                          expect_against_device=True,
                          expect_format_disk=False)

  def testAgainstDeviceWithoutFill(self):
    self.doTargetModeTest('against_device_without_fill',
                          expect_fill_device=False,
                          expect_against_device=True,
                          expect_format_disk=False)


if __name__ == '__main__':
  unittest.main()
