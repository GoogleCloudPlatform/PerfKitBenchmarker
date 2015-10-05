# Copyright 2014 Google Inc. All rights reserved.
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
"""Tests for perfkitbenchmarker.packages.fio."""

import json
import os
import unittest

import mock

from perfkitbenchmarker import sample
from perfkitbenchmarker.packages import fio


class FioTestCase(unittest.TestCase):

  maxDiff = None

  def setUp(self):
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    result_path = os.path.join(data_dir, 'fio-parser-sample-result.json')
    with open(result_path) as result_file:
      self.result_contents = json.loads(result_file.read())
    job_file_path = os.path.join(data_dir, 'fio.job')
    with open(job_file_path) as job_file:
      self.job_contents = job_file.read()

  def tearDown(self):
    pass

  def testParseFioJobFile(self):
    parameter_dict = fio.ParseJobFile(self.job_contents)
    expected_result = {
        'random_write_test': {
            'rw': 'randwrite', 'blocksize': '4k', 'direct': '1',
            'filename': 'fio_test_file', 'filesize': '10*10*1000*$mb_memory',
            'ioengine': 'libaio', 'iodepth': '1', 'directory': '/scratch0',
            'overwrite': '1', 'size': '10*1000*$mb_memory'},
        'sequential_read': {
            'invalidate': '1', 'rw': 'read', 'blocksize': '512k',
            'direct': '1', 'filename': 'fio_test_file',
            'filesize': '10*10*1000*$mb_memory', 'ioengine': 'libaio',
            'iodepth': '64', 'overwrite': '0', 'directory': '/scratch0',
            'size': '10*10*1000*$mb_memory'},
        'sequential_write': {
            'rw': 'write', 'end_fsync': '1', 'blocksize': '512k', 'direct': '1',
            'filename': 'fio_test_file', 'filesize': '10*10*1000*$mb_memory',
            'ioengine': 'libaio', 'iodepth': '64', 'overwrite': '0',
            'size': '10*10*1000*$mb_memory', 'directory': '/scratch0'},
        'random_read_test': {
            'invalidate': '1', 'rw': 'randread', 'blocksize': '4k',
            'direct': '1', 'filename': 'fio_test_file',
            'filesize': '10*10*1000*$mb_memory', 'directory': '/scratch0',
            'ioengine': 'libaio', 'iodepth': '1', 'size': '10*1000*$mb_memory'},
        'random_read_test_parallel': {
            'invalidate': '1', 'rw': 'randread', 'blocksize': '4k',
            'direct': '1', 'filename': 'fio_test_file',
            'directory': '/scratch0', 'filesize': '10*10*1000*$mb_memory',
            'ioengine': 'libaio', 'iodepth': '64',
            'size': '10*1000*$mb_memory'}}
    self.assertDictEqual(parameter_dict, expected_result)

  def testParseFioResults(self):
    with mock.patch(
        fio.__name__ + '.ParseJobFile',
        return_value={
            'sequential_write': {},
            'sequential_read': {},
            'random_write_test': {},
            'random_read_test': {},
            'random_read_test_parallel': {}}):
      result = fio.ParseResults('', self.result_contents)
      expected_result = [
          ['sequential_write:write:bandwidth', 68118, 'KB/s',
           {'bw_max': 74454, 'bw_agg': 63936.8,
            'bw_min': 19225, 'bw_dev': 20346.28,
            'bw_mean': 63936.8}],
          ['sequential_write:write:latency', 477734.84, 'usec',
           {'max': 869891, 'stddev': 92609.34, 'min': 189263,
            'p1': 387072, 'p5': 440320, 'p50': 444416,
            'p95': 724992, 'p99': 823296, 'p99.99': 872448}],
          ['sequential_write:write:iops', 133, '', {}],
          ['sequential_read:read:bandwidth', 129836, 'KB/s',
           {'bw_max': 162491, 'bw_agg': 130255.2,
            'bw_min': 115250, 'bw_dev': 18551.37,
            'bw_mean': 130255.2}],
          ['sequential_read:read:latency', 250667.06, 'usec',
           {'max': 528542, 'stddev': 70403.40, 'min': 24198,
            'p1': 59136, 'p5': 116224, 'p50': 264192,
            'p95': 366592, 'p99': 473088, 'p99.99': 528384}],
          ['sequential_read:read:iops', 253, '', {}],
          ['random_write_test:write:bandwidth', 6443, 'KB/s',
           {'bw_max': 7104, 'bw_agg': 6446.55,
            'bw_min': 5896, 'bw_dev': 336.21,
            'bw_mean': 6446.55}],
          ['random_write_test:write:latency', 587.02, 'usec',
           {'max': 81806, 'stddev': 897.93, 'min': 1,
            'p1': 446, 'p5': 462, 'p50': 510,
            'p95': 1064, 'p99': 1688, 'p99.99': 81408}],
          ['random_write_test:write:iops', 1610, '', {}],
          ['random_read_test:read:bandwidth', 1269, 'KB/s',
           {'bw_max': 1745, 'bw_agg': 1275.52,
            'bw_min': 330, 'bw_dev': 201.59,
            'bw_mean': 1275.52}],
          ['random_read_test:read:latency', 3117.62, 'usec',
           {'max': 352736, 'stddev': 5114.37, 'min': 0,
            'p1': 524, 'p5': 588, 'p50': 3280,
            'p95': 3440, 'p99': 3728, 'p99.99': 354304}],
          ['random_read_test:read:iops', 317, '', {}],
          ['random_read_test_parallel:read:bandwidth', 1292, 'KB/s',
           {'bw_max': 1693, 'bw_agg': 1284.71,
            'bw_min': 795, 'bw_dev': 88.67,
            'bw_mean': 1284.71}],
          ['random_read_test_parallel:read:latency', 198030.44, 'usec',
           {'max': 400078, 'stddev': 21709.40, 'min': 0,
            'p1': 65280, 'p5': 189440, 'p50': 199680,
            'p95': 209920, 'p99': 209920, 'p99.99': 399360}],
          ['random_read_test_parallel:read:iops', 323, '', {}]]
      expected_result = [sample.Sample(*sample_tuple)
                         for sample_tuple in expected_result]
      self.assertEqual(result, expected_result)

  def testFioCommandToJob(self):
    fio_parameters = (
        '--filesize=10g --directory=/scratch0 --ioengine=libaio '
        '--filename=fio_test_file --invalidate=1 --randrepeat=0 '
        '--direct=0 --size=3790088k --iodepth=8 '
        '--name=sequential_write --overwrite=0 --rw=write --end_fsync=1 '
        '--name=random_read --size=379008k --stonewall --rw=randread '
        '--name=sequential_read --stonewall --rw=read ')
    expected_result = (
        '[global]\n'
        'filesize=10g\n'
        'directory=/scratch0\n'
        'ioengine=libaio\n'
        'filename=fio_test_file\n'
        'invalidate=1\n'
        'randrepeat=0\n'
        'direct=0\n'
        'size=3790088k\n'
        'iodepth=8\n'
        '[sequential_write]\n'
        'overwrite=0\n'
        'rw=write\n'
        'end_fsync=1\n'
        '[random_read]\n'
        'size=379008k\n'
        'stonewall\n'
        'rw=randread\n'
        '[sequential_read]\n'
        'stonewall\n'
        'rw=read\n')
    result = fio.FioParametersToJob(fio_parameters)
    self.assertEqual(expected_result, result)

  def testDeleteParameterFromJobFile(self):
    original_job_file = (
        '[global]\n'
        'directory=/dev/sdb\n'
        'filename=foo12_3\n'
        '...')
    expected_job_file = (
        '[global]\n'
        '...')
    self.assertEqual(
        expected_job_file,
        fio.DeleteParameterFromJobFile(
            fio.DeleteParameterFromJobFile(original_job_file, 'directory'),
            'filename'))


if __name__ == '__main__':
  unittest.main()
