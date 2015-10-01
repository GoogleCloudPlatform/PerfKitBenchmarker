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
            ['sequential_read'],
            '1,2',
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
            ['sequential_read', 'sequential_write'],
            '1',
            None),
        expected_jobfile)


class TestProcessedJobFileString(unittest.TestCase):
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

    open_mock = mock.MagicMock()
    manager = open_mock.return_value.__enter__.return_value
    manager.read.return_value = file_contents
    manager.__exit__.return_value = mock.Mock()

    with mock.patch('__builtin__.open', open_mock):
      jobfile = fio_benchmark.ProcessedJobFileString('filename', True)
      self.assertNotIn('filename', jobfile)
      self.assertNotIn('zanzibar', jobfile)
      self.assertNotIn('asdf', jobfile)


class TestRunForMinutes(unittest.TestCase):
  def testBasicRun(self):
    proc = mock.Mock()
    fio_benchmark.RunForMinutes(proc, 20, 10)
    self.assertEquals(proc.call_count, 2)

  def TestRounding(self):
    proc = mock.Mock()
    fio_benchmark.RunForMinutes(proc, 18, 10)
    self.assertEquals(proc.call_count, 2)

  def TestRoundsUp(self):
    proc = mock.Mock()
    fio_benchmark.RunForMinutes(proc, 12, 10)
    self.assertEquals(proc.call_count, 2)

  def TestZeroMinutes(self):
    proc = mock.Mock()
    fio_benchmark.RunForMinutes(proc, 0, 10)
    self.assertEquals(proc.call_count, 0)


if __name__ == '__main__':
  unittest.main()
