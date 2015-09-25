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

from perfkitbenchmarker import disk
from perfkitbenchmarker.gcp import gce_disk
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
    self.disk_spec = disk.BaseDiskSpec(100, 'remote_ssd', '/scratch0')
    self.disk = gce_disk.GceDisk(self.disk_spec, 'foo', 'us-central1-a', 'proj')

  def testAgainstDevice(self):
    expected_jobfile = """
[global]
ioengine=libaio
invalidate=1
direct=1
runtime=10m
time_based
filename=/dev/disk/by-id/google-foo
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
            self.disk,
            True,
            [fio_benchmark.SCENARIOS['sequential_read']],
            [1, 2],
            None),
        expected_jobfile)

  def testAgainstFile(self):
    expected_jobfile = """
[global]
ioengine=libaio
invalidate=1
direct=1
runtime=10m
time_based
filename=/scratch0/fio-temp-file
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
            self.disk,
            False,
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
filename=/scratch0/fio-temp-file
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
            self.disk,
            False,
            [fio_benchmark.SCENARIOS['sequential_read'],
             fio_benchmark.SCENARIOS['sequential_write']],
            [1],
            None),
        expected_jobfile)


if __name__ == '__main__':
  unittest.main()
