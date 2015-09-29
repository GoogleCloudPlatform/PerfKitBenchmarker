# Copyright 2015 Google Inc. All rights reserved.
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

import mock

from perfkitbenchmarker.benchmarks import fio_benchmark
from perfkitbenchmarker.packages import fio


class TestGetIODepths(unittest.TestCase):
  def testOneInteger(self):
    self.assertEqual(list(fio_benchmark.GetIODepths('3')), [3])

  def testIntegerRange(self):
    self.assertEqual(list(fio_benchmark.GetIODepths('3-5')), [3, 4, 5])

  def testIntegerList(self):
    self.assertEqual(list(fio_benchmark.GetIODepths('3-5,8,10-12')),
                     [3, 4, 5, 8, 10, 11, 12])

  def testNoInteger(self):
    with self.assertRaises(ValueError):
      fio_benchmark.GetIODepths('a')

  def testBadRange(self):
    with self.assertRaises(ValueError):
      fio_benchmark.GetIODepths('3-a')

  def testBadList(self):
    with self.assertRaises(ValueError):
      fio_benchmark.GetIODepths('3-5,8a')

  def testTrailingComma(self):
    with self.assertRaises(ValueError):
      fio_benchmark.GetIODepths('3-5,')


class TestGenerateJobFileString(unittest.TestCase):
  def setUp(self):
    self.filename = '/test/filename'

  def testBasicGeneration(self):
    expected_jobfile = """
[global]
ioengine=libaio
invalidate=1
direct=1
runtime=10m
time_based
filename=/test/filename
do_verify=0
verify_fatal=0
randrepeat=0



[sequential_read-io-depth-1]
stonewall
rw=read
blocksize=512k
iodepth=1
size=100%

[sequential_read-io-depth-2]
stonewall
rw=read
blocksize=512k
iodepth=2
size=100%

"""

    self.assertEqual(
        fio_benchmark.GenerateJobFileString(
            self.filename,
            [fio_benchmark.SCENARIOS['sequential_read']],
            [1, 2],
            None),
        expected_jobfile)

  def testMultipleScenarios(self):
    expected_jobfile = """
[global]
ioengine=libaio
invalidate=1
direct=1
runtime=10m
time_based
filename=/test/filename
do_verify=0
verify_fatal=0
randrepeat=0



[sequential_read-io-depth-1]
stonewall
rw=read
blocksize=512k
iodepth=1
size=100%



[sequential_write-io-depth-1]
stonewall
rw=write
blocksize=512k
iodepth=1
size=100%

"""

    self.assertEqual(
        fio_benchmark.GenerateJobFileString(
            self.filename,
            [fio_benchmark.SCENARIOS['sequential_read'],
             fio_benchmark.SCENARIOS['sequential_write']],
            [1],
            None),
        expected_jobfile)


class TestGetOrGenerateJobFileString(unittest.TestCase):
  def testFilenameSubstitution(self):
    filename = '1234567890'
    jobfile = str(fio_benchmark.GetOrGenerateJobFileString(
        None, filename, None, '1', None))

    self.assertIn(filename, jobfile)


class TestRunForMinutes(unittest.TestCase):
  def testRunForMinutes(self):
    vm = mock.Mock()
    vm.RemoteCommand = mock.MagicMock()
    vm.scratch_disks = [mock.Mock()]
    benchmark_spec = mock.Mock()
    benchmark_spec.vms = [vm]

    bench_module = fio_benchmark.__name__
    with mock.patch(bench_module + '.FLAGS') as fb_flags, \
            mock.patch(bench_module + '.GetOrGenerateJobFileString'), \
            mock.patch(fio.__name__ + '.ParseResults') as parse_results:
      # Pick a length of time that forces rounding
      fb_flags.run_for_minutes = int(round(fio_benchmark.MINUTES_PER_JOB * 2.5))
      fb_flags.io_depths = '1'
      fb_flags.generate_scenarios = ['random_read']
      parse_results.return_value = ['spam']
      vm.RemoteCommand.return_value = '{"foo": "bar"}', ''
      self.assertEquals(fio_benchmark.Run(benchmark_spec),
                        ['spam', 'spam', 'spam'])
      self.assertEquals(vm.RemoteCommand.call_count, 3)


if __name__ == '__main__':
  unittest.main()
