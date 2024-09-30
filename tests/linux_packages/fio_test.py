# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

import copy
import json
import os
import unittest

from absl import flags
import mock
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import fio
from tests import pkb_common_test_case

BASE_METADATA = {'foo': 'bar'}
FLAGS = flags.FLAGS


def _ReadFileToString(filename):
  """Helper function to read a file into a string."""
  with open(filename) as f:
    return f.read()


def _ExtractHistogramFromMetric(results, sample_name):
  """Search results for given metric and extract the embedded histogram."""
  for r in results:
    if r.metric == sample_name:
      return json.loads(r.metadata['histogram'])
  return None


class FioTestCase(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):
  maxDiff = None

  def setUp(self):
    """Set up the test."""
    super().setUp()
    self.data_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'fio')
    result_path = os.path.join(self.data_dir, 'fio-parser-sample-result.json')
    self.result_contents = json.loads(_ReadFileToString(result_path))
    job_file_path = os.path.join(self.data_dir, 'fio.job')
    self.job_contents = _ReadFileToString(job_file_path)

    get_tmp_dir_mock = mock.patch(
        vm_util.__name__ + '.GetTempDir', return_value='tmp'
    )
    get_tmp_dir_mock.start()
    self.addCleanup(get_tmp_dir_mock.stop)

  def testParseFioJobFile(self):
    parameter_dict = fio.ParseJobFile(self.job_contents)
    expected_result = {
        'random_write_test': {
            'rw': 'randwrite',
            'blocksize': '4k',
            'direct': '1',
            'filename': 'fio_test_file',
            'filesize': '10*10*1000*$mb_memory',
            'ioengine': 'libaio',
            'iodepth': '1',
            'directory': '/scratch0',
            'overwrite': '1',
            'size': '10*1000*$mb_memory',
        },
        'sequential_read': {
            'invalidate': '1',
            'rw': 'read',
            'blocksize': '512k',
            'direct': '1',
            'filename': 'fio_test_file',
            'filesize': '10*10*1000*$mb_memory',
            'ioengine': 'libaio',
            'iodepth': '64',
            'overwrite': '0',
            'directory': '/scratch0',
            'size': '10*10*1000*$mb_memory',
        },
        'sequential_write': {
            'rw': 'write',
            'end_fsync': '1',
            'blocksize': '512k',
            'direct': '1',
            'filename': 'fio_test_file',
            'filesize': '10*10*1000*$mb_memory',
            'ioengine': 'libaio',
            'iodepth': '64',
            'overwrite': '0',
            'size': '10*10*1000*$mb_memory',
            'directory': '/scratch0',
        },
        'random_read_test': {
            'invalidate': '1',
            'rw': 'randread',
            'blocksize': '4k',
            'direct': '1',
            'filename': 'fio_test_file',
            'filesize': '10*10*1000*$mb_memory',
            'directory': '/scratch0',
            'ioengine': 'libaio',
            'iodepth': '1',
            'size': '10*1000*$mb_memory',
        },
        'random_read_test_parallel': {
            'invalidate': '1',
            'rw': 'randread',
            'blocksize': '4k',
            'direct': '1',
            'filename': 'fio_test_file',
            'directory': '/scratch0',
            'filesize': '10*10*1000*$mb_memory',
            'ioengine': 'libaio',
            'iodepth': '64',
            'size': '10*1000*$mb_memory',
        },
    }
    self.assertDictEqual(parameter_dict, expected_result)

  def testParsePinnedFioJobFile(self):
    file_str = """
[global]
ioengine=libaio
invalidate=1

[rand_8k_write_100%-io-depth-256-num-jobs-16.0]
stonewall
rw=randwrite
iodepth=2
blocksize=8k
numa_cpu_nodes=0
filename=/dev/disk/by-id/google-pkb-67d581a7-0-data-0-0

[rand_8k_write_100%-io-depth-256-num-jobs-16.1]
rw=randwrite
iodepth=1
blocksize=8k
numa_cpu_nodes=1
filename=/dev/disk/by-id/google-pkb-67d581a7-0-data-0-1
    """
    parameter_dict = fio.ParseJobFile(file_str, True)

    expected_result = {
        'rand_8k_write_100%-io-depth-256-num-jobs-16': {
            'ioengine': 'libaio',
            'invalidate': '1',
            'rw.0': 'randwrite',
            'iodepth.0': '2',
            'blocksize.0': '8k',
            'numa_cpu_nodes.0': '0',
            'filename.0': '/dev/disk/by-id/google-pkb-67d581a7-0-data-0-0',
            'rw.1': 'randwrite',
            'iodepth.1': '1',
            'blocksize.1': '8k',
            'numa_cpu_nodes.1': '1',
            'filename.1': '/dev/disk/by-id/google-pkb-67d581a7-0-data-0-1',
        }
    }
    self.assertDictEqual(parameter_dict, expected_result)

  def merge_two_dicts(self, x, y):
    z = copy.deepcopy(x)
    z.update(y)
    return z

  def testParseFioResults(self):
    with mock.patch(
        fio.__name__ + '.ParseJobFile',
        return_value={
            'sequential_write': {},
            'sequential_read': {},
            'random_write_test': {},
            'random_read_test': {},
            'random_read_test_parallel': {},
        },
    ):
      result = fio.ParseResults('', self.result_contents)
      sequential_write_metadata = {
          'fio_job': 'sequential_write',
          'fio_version': 'fio-3.27',
      }
      sequential_read_metadata = {
          'fio_job': 'sequential_read',
          'fio_version': 'fio-3.27',
      }
      random_write_metadata = {
          'fio_job': 'random_write_test',
          'fio_version': 'fio-3.27',
      }
      random_read_metadata = {
          'fio_job': 'random_read_test',
          'fio_version': 'fio-3.27',
      }
      random_read_parallel_metadata = {
          'fio_job': 'random_read_test_parallel',
          'fio_version': 'fio-3.27',
      }
      expected_result = [
          [
              'sequential_write:write:bandwidth',
              68118,
              'KB/s',
              self.merge_two_dicts(
                  {
                      'bw_max': 74454,
                      'bw_agg': 63936.8,
                      'bw_min': 19225,
                      'bw_dev': 20346.28,
                      'bw_mean': 63936.8,
                  },
                  sequential_write_metadata,
              ),
          ],
          [
              'sequential_write:write:latency',
              477734.84,
              'usec',
              self.merge_two_dicts(
                  {
                      'max': 869891,
                      'stddev': 92609.34,
                      'min': 189263,
                      'mean': 477734.84,
                      'p60': 444416,
                      'p1': 387072,
                      'p99.9': 872448,
                      'p70': 448512,
                      'p5': 440320,
                      'p90': 610304,
                      'p99.95': 872448,
                      'p80': 452608,
                      'p95': 724992,
                      'p10': 440320,
                      'p99.5': 847872,
                      'p99': 823296,
                      'p20': 440320,
                      'p99.99': 872448,
                      'p30': 444416,
                      'p50': 444416,
                      'p40': 444416,
                  },
                  sequential_write_metadata,
              ),
          ],
          [
              'sequential_write:write:latency:min',
              189263,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:max',
              869891,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:mean',
              477734.84,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:stddev',
              92609.34,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:p1',
              387072,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:p5',
              440320,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:p10',
              440320,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:p20',
              440320,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:p30',
              444416,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:p40',
              444416,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:p50',
              444416,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:p60',
              444416,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:p70',
              448512,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:p80',
              452608,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:p90',
              610304,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:p95',
              724992,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:p99',
              823296,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:p99.5',
              847872,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:p99.9',
              872448,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:p99.95',
              872448,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:latency:p99.99',
              872448,
              'usec',
              sequential_write_metadata,
          ],
          [
              'sequential_write:write:iops',
              133,
              '',
              sequential_write_metadata,
          ],
          [
              'sequential_read:read:bandwidth',
              129836,
              'KB/s',
              self.merge_two_dicts(
                  {
                      'bw_max': 162491,
                      'bw_agg': 130255.2,
                      'bw_min': 115250,
                      'bw_dev': 18551.37,
                      'bw_mean': 130255.2,
                  },
                  sequential_read_metadata,
              ),
          ],
          [
              'sequential_read:read:latency',
              250667.06,
              'usec',
              self.merge_two_dicts(
                  {
                      'max': 528542,
                      'stddev': 70403.40,
                      'min': 24198,
                      'mean': 250667.06,
                      'p60': 268288,
                      'p1': 59136,
                      'p99.9': 528384,
                      'p70': 272384,
                      'p5': 116224,
                      'p90': 292864,
                      'p99.95': 528384,
                      'p80': 280576,
                      'p95': 366592,
                      'p10': 164864,
                      'p99.5': 489472,
                      'p99': 473088,
                      'p20': 199680,
                      'p99.99': 528384,
                      'p30': 246784,
                      'p50': 264192,
                      'p40': 257024,
                  },
                  sequential_read_metadata,
              ),
          ],
          [
              'sequential_read:read:latency:min',
              24198,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:max',
              528542,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:mean',
              250667.06,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:stddev',
              70403.40,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:p1',
              59136,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:p5',
              116224,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:p10',
              164864,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:p20',
              199680,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:p30',
              246784,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:p40',
              257024,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:p50',
              264192,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:p60',
              268288,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:p70',
              272384,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:p80',
              280576,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:p90',
              292864,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:p95',
              366592,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:p99',
              473088,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:p99.5',
              489472,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:p99.9',
              528384,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:p99.95',
              528384,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:latency:p99.99',
              528384,
              'usec',
              sequential_read_metadata,
          ],
          [
              'sequential_read:read:iops',
              253,
              '',
              sequential_read_metadata,
          ],
          [
              'random_write_test:write:bandwidth',
              6443,
              'KB/s',
              self.merge_two_dicts(
                  {
                      'bw_max': 7104,
                      'bw_agg': 6446.55,
                      'bw_min': 5896,
                      'bw_dev': 336.21,
                      'bw_mean': 6446.55,
                  },
                  random_write_metadata,
              ),
          ],
          [
              'random_write_test:write:latency',
              587.02,
              'usec',
              self.merge_two_dicts(
                  {
                      'max': 81806,
                      'stddev': 897.93,
                      'min': 1,
                      'mean': 587.02,
                      'p60': 524,
                      'p1': 446,
                      'p99.9': 3216,
                      'p70': 532,
                      'p5': 462,
                      'p90': 636,
                      'p99.95': 4128,
                      'p80': 564,
                      'p95': 1064,
                      'p10': 470,
                      'p99.5': 1736,
                      'p99': 1688,
                      'p20': 482,
                      'p99.99': 81408,
                      'p30': 494,
                      'p50': 510,
                      'p40': 502,
                  },
                  random_write_metadata,
              ),
          ],
          [
              'random_write_test:write:latency:min',
              1,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:max',
              81806,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:mean',
              587.02,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:stddev',
              897.93,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:p1',
              446,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:p5',
              462,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:p10',
              470,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:p20',
              482,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:p30',
              494,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:p40',
              502,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:p50',
              510,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:p60',
              524,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:p70',
              532,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:p80',
              564,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:p90',
              636,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:p95',
              1064,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:p99',
              1688,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:p99.5',
              1736,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:p99.9',
              3216,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:p99.95',
              4128,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:latency:p99.99',
              81408,
              'usec',
              random_write_metadata,
          ],
          [
              'random_write_test:write:iops',
              1610,
              '',
              random_write_metadata,
          ],
          [
              'random_read_test:read:bandwidth',
              1269,
              'KB/s',
              self.merge_two_dicts(
                  {
                      'bw_max': 1745,
                      'bw_agg': 1275.52,
                      'bw_min': 330,
                      'bw_dev': 201.59,
                      'bw_mean': 1275.52,
                  },
                  random_read_metadata,
              ),
          ],
          [
              'random_read_test:read:latency',
              3117.62,
              'usec',
              self.merge_two_dicts(
                  {
                      'max': 352736,
                      'stddev': 5114.37,
                      'min': 0,
                      'mean': 3117.62,
                      'p60': 3312,
                      'p1': 524,
                      'p99.9': 6880,
                      'p70': 3344,
                      'p5': 588,
                      'p90': 3408,
                      'p99.95': 11840,
                      'p80': 3376,
                      'p95': 3440,
                      'p10': 2544,
                      'p99.5': 4128,
                      'p99': 3728,
                      'p20': 3152,
                      'p99.99': 354304,
                      'p30': 3216,
                      'p50': 3280,
                      'p40': 3248,
                  },
                  random_read_metadata,
              ),
          ],
          [
              'random_read_test:read:latency:min',
              0,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:max',
              352736,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:mean',
              3117.62,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:stddev',
              5114.37,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:p1',
              524,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:p5',
              588,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:p10',
              2544,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:p20',
              3152,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:p30',
              3216,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:p40',
              3248,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:p50',
              3280,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:p60',
              3312,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:p70',
              3344,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:p80',
              3376,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:p90',
              3408,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:p95',
              3440,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:p99',
              3728,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:p99.5',
              4128,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:p99.9',
              6880,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:p99.95',
              11840,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:latency:p99.99',
              354304,
              'usec',
              random_read_metadata,
          ],
          [
              'random_read_test:read:iops',
              317,
              '',
              random_read_metadata,
          ],
          [
              'random_read_test_parallel:read:bandwidth',
              1292,
              'KB/s',
              self.merge_two_dicts(
                  {
                      'bw_max': 1693,
                      'bw_agg': 1284.71,
                      'bw_min': 795,
                      'bw_dev': 88.67,
                      'bw_mean': 1284.71,
                  },
                  random_read_parallel_metadata,
              ),
          ],
          [
              'random_read_test_parallel:read:latency',
              198030.44,
              'usec',
              self.merge_two_dicts(
                  {
                      'max': 400078,
                      'stddev': 21709.40,
                      'min': 0,
                      'mean': 198030.44,
                      'p60': 199680,
                      'p1': 65280,
                      'p99.9': 370688,
                      'p70': 203776,
                      'p5': 189440,
                      'p90': 205824,
                      'p99.95': 387072,
                      'p80': 203776,
                      'p95': 209920,
                      'p10': 189440,
                      'p99.5': 257024,
                      'p99': 209920,
                      'p20': 193536,
                      'p99.99': 399360,
                      'p30': 197632,
                      'p50': 199680,
                      'p40': 197632,
                  },
                  random_read_parallel_metadata,
              ),
          ],
          [
              'random_read_test_parallel:read:latency:min',
              0,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:max',
              400078,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:mean',
              198030.44,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:stddev',
              21709.40,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:p1',
              65280,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:p5',
              189440,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:p10',
              189440,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:p20',
              193536,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:p30',
              197632,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:p40',
              197632,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:p50',
              199680,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:p60',
              199680,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:p70',
              203776,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:p80',
              203776,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:p90',
              205824,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:p95',
              209920,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:p99',
              209920,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:p99.5',
              257024,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:p99.9',
              370688,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:p99.95',
              387072,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:latency:p99.99',
              399360,
              'usec',
              random_read_parallel_metadata,
          ],
          [
              'random_read_test_parallel:read:iops',
              323,
              '',
              random_read_parallel_metadata,
          ],
      ]
      expected_result = [
          sample.Sample(*sample_tuple) for sample_tuple in expected_result
      ]
      self.assertSampleListsEqualUpToTimestamp(result, expected_result)

  def testParseResultsBaseMetadata(self):
    with mock.patch(
        fio.__name__ + '.ParseJobFile',
        return_value={
            'sequential_write': {},
            'sequential_read': {},
            'random_write_test': {},
            'random_read_test': {},
            'random_read_test_parallel': {},
        },
    ):
      results = fio.ParseResults(
          '', self.result_contents, base_metadata=BASE_METADATA
      )

      for result in results:
        self.assertDictContainsSubset(BASE_METADATA, result.metadata)

  def testFioCommandToJob(self):
    fio_parameters = (
        '--filesize=10g --directory=/scratch0 --ioengine=libaio '
        '--filename=fio_test_file --invalidate=1 --randrepeat=0 '
        '--direct=0 --size=3790088k --iodepth=8 '
        '--name=sequential_write --overwrite=0 --rw=write --end_fsync=1 '
        '--name=random_read --size=379008k --stonewall --rw=randread '
        '--name=sequential_read --stonewall --rw=read '
    )
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
        'rw=read\n'
    )
    result = fio.FioParametersToJob(fio_parameters)
    self.assertEqual(expected_result, result)

  def testDeleteParameterFromJobFile(self):
    original_job_file = '[global]\ndirectory=/dev/sdb\nfilename=foo12_3\n...'
    expected_job_file = '[global]\n...'
    self.assertEqual(
        expected_job_file,
        fio.DeleteParameterFromJobFile(
            fio.DeleteParameterFromJobFile(original_job_file, 'directory'),
            'filename',
        ),
    )

  def testParseHistogramMultipleJobs(self):
    hist_dir = os.path.join(self.data_dir, 'hist')
    job_file = _ReadFileToString(
        os.path.join(hist_dir, 'pkb-7fb0c9d8-0_fio.job')
    )
    fio_json_result = json.loads(
        _ReadFileToString(os.path.join(hist_dir, 'pkb-7fb0c9d8-0_fio.json'))
    )
    log_file_base = 'pkb_fio_avg_1506559526.49'

    single_bin_vals = [
        float(f)
        for f in _ReadFileToString(os.path.join(hist_dir, 'bin_vals')).split()
    ]
    # each hist file has its own bin_vals, but they're all the same
    bin_vals = [
        single_bin_vals,
        single_bin_vals,
        single_bin_vals,
        single_bin_vals,
    ]

    # redirect open to the hist subdirectory
    def OpenTestFile(filename):
      return open(os.path.join(hist_dir, os.path.basename(filename)))

    with mock.patch(
        fio.__name__ + '.open',
        new=mock.MagicMock(side_effect=OpenTestFile),
        create=True,
    ):
      results = fio.ParseResults(
          job_file, fio_json_result, None, log_file_base, bin_vals
      )

    actual_read_hist = _ExtractHistogramFromMetric(
        results, 'rand_16k_read_100%-io-depth-1-num-jobs-2:16384:read:histogram'
    )
    expected_read_hist = json.loads(
        _ReadFileToString(os.path.join(hist_dir, 'expected_read.json'))
    )
    self.assertEqual(expected_read_hist, actual_read_hist)

    actual_write_hist = _ExtractHistogramFromMetric(
        results,
        'rand_16k_write_100%-io-depth-1-num-jobs-2:16384:write:histogram',
    )
    expected_write_hist = json.loads(
        _ReadFileToString(os.path.join(hist_dir, 'expected_write.json'))
    )
    self.assertEqual(expected_write_hist, actual_write_hist)


if __name__ == '__main__':
  unittest.main()
