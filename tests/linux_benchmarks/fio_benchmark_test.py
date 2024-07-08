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
from perfkitbenchmarker.linux_packages import numactl
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
ramp_time=10
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
iodepth_batch_submit=1
iodepth_batch_complete_max=1

[sequential_read-io-depth-2-num-jobs-1]
stonewall
rw=read
blocksize=512k
iodepth=2
size=100%
numjobs=1
iodepth_batch_submit=2
iodepth_batch_complete_max=2"""

    self.assertEqual(
        fio_benchmark.GenerateJobFileString(
            self.filename,
            ['sequential_read'],
            [1, 2],
            [1],
            None,
            None,
            600,
            10,
            True,
            ['randrepeat=0', 'offset_increment=1k'],
            [0],
            ['/dev/sdb'],
            False,
        ),
        expected_jobfile,
    )

  def testAllScenarios(self):
    expected_jobfile = """
[global]
ioengine=libaio
invalidate=1
direct=1
runtime=600
ramp_time=10
time_based
filename=/test/filename
do_verify=0
verify_fatal=0
group_reporting=1

[sequential_write-io-depth-1-num-jobs-1]
stonewall
rw=write
blocksize=512k
iodepth=1
size=100%
numjobs=1
iodepth_batch_submit=1
iodepth_batch_complete_max=1

[sequential_read-io-depth-1-num-jobs-1]
stonewall
rw=read
blocksize=512k
iodepth=1
size=100%
numjobs=1
iodepth_batch_submit=1
iodepth_batch_complete_max=1

[random_write-io-depth-1-num-jobs-1]
stonewall
rw=randwrite
blocksize=4k
iodepth=1
size=100%
numjobs=1
iodepth_batch_submit=1
iodepth_batch_complete_max=1

[random_read-io-depth-1-num-jobs-1]
stonewall
rw=randread
blocksize=4k
iodepth=1
size=100%
numjobs=1
iodepth_batch_submit=1
iodepth_batch_complete_max=1

[random_read_write-io-depth-1-num-jobs-1]
stonewall
rw=randrw
blocksize=4k
iodepth=1
size=100%
numjobs=1
iodepth_batch_submit=1
iodepth_batch_complete_max=1

[sequential_trim-io-depth-1-num-jobs-1]
stonewall
rw=trim
blocksize=512k
iodepth=1
size=100%
numjobs=1
iodepth_batch_submit=1
iodepth_batch_complete_max=1

[rand_trim-io-depth-1-num-jobs-1]
stonewall
rw=randtrim
blocksize=4k
iodepth=1
size=100%
numjobs=1
iodepth_batch_submit=1
iodepth_batch_complete_max=1"""

    self.assertEqual(
        fio_benchmark.GenerateJobFileString(
            self.filename,
            ['all'],
            [1],
            [1],
            None,
            None,
            600,
            10,
            True,
            [],
            [0],
            ['/dev/sdb'],
            False,
        ),
        expected_jobfile,
    )

  def testMultipleScenarios(self):
    expected_jobfile = """
[global]
ioengine=libaio
invalidate=1
direct=1
runtime=600
ramp_time=10
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
iodepth_batch_submit=1
iodepth_batch_complete_max=1

[sequential_write-io-depth-1-num-jobs-1]
stonewall
rw=write
blocksize=512k
iodepth=1
size=100%
numjobs=1
iodepth_batch_submit=1
iodepth_batch_complete_max=1"""

    self.assertEqual(
        fio_benchmark.GenerateJobFileString(
            self.filename,
            ['sequential_read', 'sequential_write'],
            [1],
            [1],
            None,
            None,
            600,
            10,
            True,
            ['randrepeat=0'],
            [0],
            ['/dev/sdb'],
            False,
        ),
        expected_jobfile,
    )

  def testCustomBlocksize(self):
    orig_blocksize = fio_benchmark.SCENARIOS['sequential_write']['blocksize']

    job_file = fio_benchmark.GenerateJobFileString(
        self.filename,
        ['sequential_read'],
        [1],
        [1],
        None,
        units.Unit('megabyte') * 2,
        600,
        10,
        True,
        [],
        [0],
        ['/dev/sdb'],
        False,
    )

    self.assertIn('blocksize=2000000B', job_file)

    # Test that generating a job file doesn't modify the global
    # SCENARIOS variable.
    self.assertEqual(
        fio_benchmark.SCENARIOS['sequential_write']['blocksize'], orig_blocksize
    )

  def testIndirectIO(self):
    job_file = fio_benchmark.GenerateJobFileString(
        self.filename,
        ['sequential_read'],
        [1],
        [1],
        None,
        units.Unit('megabyte') * 2,
        600,
        10,
        False,
        [],
        [0],
        ['/dev/sdb'],
        False,
    )
    self.assertIn('direct=0', job_file)

  def testParseGenerateScenario(self):
    expected_jobfile = """
[global]
ioengine=libaio
invalidate=1
direct=1
runtime=600
ramp_time=10
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
iodepth_batch_submit=1
iodepth_batch_complete_max=1

[seq_64M_read_10TB-io-depth-2-num-jobs-1]
stonewall
rw=read
blocksize=64M
iodepth=2
size=10TB
numjobs=1
iodepth_batch_submit=2
iodepth_batch_complete_max=2

[seq_64M_read_10TB-io-depth-1-num-jobs-3]
stonewall
rw=read
blocksize=64M
iodepth=1
size=10TB
numjobs=3
iodepth_batch_submit=1
iodepth_batch_complete_max=1

[seq_64M_read_10TB-io-depth-2-num-jobs-3]
stonewall
rw=read
blocksize=64M
iodepth=2
size=10TB
numjobs=3
iodepth_batch_submit=2
iodepth_batch_complete_max=2

[rand_16k_readwrite_5TB_rwmixread-65-io-depth-1-num-jobs-1]
stonewall
rw=randrw
rwmixread=65
blocksize=16k
iodepth=1
size=5TB
numjobs=1
iodepth_batch_submit=1
iodepth_batch_complete_max=1

[rand_16k_readwrite_5TB_rwmixread-65-io-depth-2-num-jobs-1]
stonewall
rw=randrw
rwmixread=65
blocksize=16k
iodepth=2
size=5TB
numjobs=1
iodepth_batch_submit=2
iodepth_batch_complete_max=2

[rand_16k_readwrite_5TB_rwmixread-65-io-depth-1-num-jobs-3]
stonewall
rw=randrw
rwmixread=65
blocksize=16k
iodepth=1
size=5TB
numjobs=3
iodepth_batch_submit=1
iodepth_batch_complete_max=1

[rand_16k_readwrite_5TB_rwmixread-65-io-depth-2-num-jobs-3]
stonewall
rw=randrw
rwmixread=65
blocksize=16k
iodepth=2
size=5TB
numjobs=3
iodepth_batch_submit=2
iodepth_batch_complete_max=2"""

    self.assertEqual(
        fio_benchmark.GenerateJobFileString(
            self.filename,
            ['seq_64M_read_10TB', 'rand_16k_readwrite_5TB_rwmixread-65'],
            [1, 2],
            [1, 3],
            None,
            None,
            600,
            10,
            True,
            ['randrepeat=0'],
            [0],
            ['/dev/sdb'],
            False,
        ),
        expected_jobfile,
    )

  def testParseGenerateScenarioWithIodepthNumjobs(self):
    expected_jobfile = """
[global]
ioengine=libaio
invalidate=1
direct=1
runtime=600
ramp_time=10
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
iodepth_batch_submit=1
iodepth_batch_complete_max=1

[rand_16k_readwrite_5TB_rwmixread-65-io-depth-4-num-jobs-4]
stonewall
rw=randrw
rwmixread=65
blocksize=16k
iodepth=4
size=5TB
numjobs=4
iodepth_batch_submit=4
iodepth_batch_complete_max=4"""

    self.assertEqual(
        fio_benchmark.GenerateJobFileString(
            self.filename,
            [
                'seq_64M_read_10TB_iodepth-1_numjobs-1',
                'rand_16k_readwrite_5TB_iodepth-4_numjobs-4_rwmixread-65',
            ],
            [1, 2],
            [1, 3],
            None,
            None,
            600,
            10,
            True,
            ['randrepeat=0'],
            [0],
            ['/dev/sdb'],
            False,
        ),
        expected_jobfile,
    )

  def testGenerateJobFileStringWithSplitAcrossRawDisksAndNuma(self):
    FLAGS.fio_pinning = True
    expected_jobfile = """
[global]
ioengine=libaio
invalidate=1
direct=1
runtime=600
ramp_time=10
time_based
filename=/test/filename
do_verify=0
verify_fatal=0
group_reporting=1
randrepeat=0

[rand_8k_read_100%-io-depth-1-num-jobs-1.0]
stonewall
rw=randread
blocksize=8k
iodepth=1
size=100%
numjobs=1
iodepth_batch_submit=1
iodepth_batch_complete_max=1
numa_cpu_nodes=0
filename=/dev/disk/by-id/google-pkb-46dd2ae9-0-data-0-0

[rand_8k_read_100%-io-depth-1-num-jobs-1.1]
rw=randread
blocksize=8k
iodepth=1
size=100%
numjobs=1
iodepth_batch_submit=1
iodepth_batch_complete_max=1
numa_cpu_nodes=1
filename=/dev/disk/by-id/google-pkb-46dd2ae9-0-data-0-1

[rand_8k_write_100%-io-depth-1-num-jobs-1.0]
stonewall
rw=randwrite
blocksize=8k
iodepth=1
size=100%
numjobs=1
iodepth_batch_submit=1
iodepth_batch_complete_max=1
numa_cpu_nodes=0
filename=/dev/disk/by-id/google-pkb-46dd2ae9-0-data-0-0

[rand_8k_write_100%-io-depth-1-num-jobs-1.1]
rw=randwrite
blocksize=8k
iodepth=1
size=100%
numjobs=1
iodepth_batch_submit=1
iodepth_batch_complete_max=1
numa_cpu_nodes=1
filename=/dev/disk/by-id/google-pkb-46dd2ae9-0-data-0-1"""

    self.assertEqual(
        fio_benchmark.GenerateJobFileString(
            self.filename,
            [
                'rand_8k_read_100%',
                'rand_8k_write_100%',
            ],
            [1],
            [1],
            None,
            None,
            600,
            10,
            True,
            ['randrepeat=0'],
            [0, 1, 2, 3],
            ['/dev/disk/by-id/google-pkb-46dd2ae9-0-data-0-0',
             '/dev/disk/by-id/google-pkb-46dd2ae9-0-data-0-1'],
            True,
        ),
        expected_jobfile,
    )

  def testGenerateJobFileStringMultipleDisks(self):
    expected_jobfile = """
[global]
ioengine=libaio
invalidate=1
direct=1
runtime=600
ramp_time=10
time_based
filename=/test/filename
do_verify=0
verify_fatal=0
group_reporting=1
randrepeat=0

[rand_8k_read_100%-io-depth-1-num-jobs-1.0]
stonewall
rw=randread
blocksize=8k
iodepth=1
size=100%
numjobs=1
iodepth_batch_submit=1
iodepth_batch_complete_max=1
filename=/dev/disk/by-id/google-pkb-46dd2ae9-0-data-0-0

[rand_8k_read_100%-io-depth-1-num-jobs-1.1]
rw=randread
blocksize=8k
iodepth=1
size=100%
numjobs=1
iodepth_batch_submit=1
iodepth_batch_complete_max=1
filename=/dev/disk/by-id/google-pkb-46dd2ae9-0-data-0-1

[rand_8k_write_100%-io-depth-1-num-jobs-1.0]
stonewall
rw=randwrite
blocksize=8k
iodepth=1
size=100%
numjobs=1
iodepth_batch_submit=1
iodepth_batch_complete_max=1
filename=/dev/disk/by-id/google-pkb-46dd2ae9-0-data-0-0

[rand_8k_write_100%-io-depth-1-num-jobs-1.1]
rw=randwrite
blocksize=8k
iodepth=1
size=100%
numjobs=1
iodepth_batch_submit=1
iodepth_batch_complete_max=1
filename=/dev/disk/by-id/google-pkb-46dd2ae9-0-data-0-1"""

    self.assertEqual(
        fio_benchmark.GenerateJobFileString(
            self.filename,
            [
                'rand_8k_read_100%',
                'rand_8k_write_100%',
            ],
            [1],
            [1],
            None,
            None,
            600,
            10,
            True,
            ['randrepeat=0'],
            [0, 1, 2, 3],
            ['/dev/disk/by-id/google-pkb-46dd2ae9-0-data-0-0',
             '/dev/disk/by-id/google-pkb-46dd2ae9-0-data-0-1'],
            True,
        ),
        expected_jobfile,
    )


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

  def doTargetModeTest(
      self,
      mode,
      expect_fill_device=None,
      expect_against_device=None,
      expect_format_disk=None,
  ):
    fio_name = fio_benchmark.__name__
    vm_name = vm_util.__name__
    dir_name = temp_dir.__name__

    with mock.patch(fio_name + '.FillDevice') as mock_fill_device, mock.patch(
        fio_name + '.GetOrGenerateJobFileString'
    ) as mock_get_job_string, mock.patch(
        builtins.__name__ + '.open'
    ), mock.patch(
        vm_name + '.GetTempDir', return_value='/tmp/dir'
    ), mock.patch(
        vm_name + '.PrependTempDir', return_value='/tmp/prepend_dir'
    ), mock.patch(
        dir_name + '.GetRunDirPath', return_value='/tmp/run_dir'
    ), mock.patch(
        fio_name + '.fio.ParseResults'
    ), mock.patch(
        fio_name + '.FLAGS'
    ) as mock_fio_flags, mock.patch.object(
        numactl, 'GetNuma', new=lambda vm: {'0': '0'}
    ):
      mock_fio_flags.fio_target_mode = mode
      benchmark_spec = mock.MagicMock()
      benchmark_spec.vms = [mock.MagicMock()]
      benchmark_spec.vms[0].RobustRemoteCommand = mock.MagicMock(
          return_value=('"stdout"', '"stderr"')
      )
      scratch_disk = mock.MagicMock()
      scratch_disk.GetDevicePath = mock.MagicMock(return_value='/dev/sdb')
      benchmark_spec.vms[0].scratch_disks = [scratch_disk]
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
    self.doTargetModeTest(
        'against_file_with_fill',
        expect_fill_device=True,
        expect_against_device=False,
        expect_format_disk=True,
    )

  def testAgainstFileWithoutFill(self):
    self.doTargetModeTest(
        'against_file_without_fill',
        expect_fill_device=False,
        expect_against_device=False,
        expect_format_disk=False,
    )

  def testAgainstDeviceWithFill(self):
    self.doTargetModeTest(
        'against_device_with_fill',
        expect_fill_device=True,
        expect_against_device=True,
        expect_format_disk=False,
    )

  def testAgainstDeviceWithoutFill(self):
    self.doTargetModeTest(
        'against_device_without_fill',
        expect_fill_device=False,
        expect_against_device=True,
        expect_format_disk=False,
    )


if __name__ == '__main__':
  unittest.main()
