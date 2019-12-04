# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for nccl_benchmark."""

import os
import unittest
import mock

from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import nccl_benchmark
from perfkitbenchmarker.sample import Sample


class NcclBenchmarkTest(unittest.TestCase, test_util.SamplesTestMixin):

  def setUp(self):
    super(NcclBenchmarkTest, self).setUp()
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'nccl_output.txt')
    with open(path) as fp:
      self.contents = fp.read()

  @mock.patch('time.time', mock.MagicMock(return_value=1550279509.59))
  def testNcclResults(self):
    samples, bandwidth = nccl_benchmark.MakeSamplesFromOutput({}, self.contents)
    metadata = {
        'nThread': '1',
        'nGpus': '1',
        'minBytes': '8',
        'maxBytes': '8589934592',
        'step': '2(factor)',
        'warmup_iters': '5',
        'iters': '100',
        'validation': '1',
        'Rank  0': 'Pid  40529 on ip-172-31-20-44 device  0 [0x00] Tesla '
                   'V100-SXM2-32GB',
        'Rank  1': 'Pid  40530 on ip-172-31-20-44 device  1 [0x00] Tesla '
                   'V100-SXM2-32GB',
        'Rank  2': 'Pid  40531 on ip-172-31-20-44 device  2 [0x00] Tesla '
                   'V100-SXM2-32GB',
        'Rank  3': 'Pid  40532 on ip-172-31-20-44 device  3 [0x00] Tesla '
                   'V100-SXM2-32GB',
        'Rank  4': 'Pid  40533 on ip-172-31-20-44 device  4 [0x00] Tesla '
                   'V100-SXM2-32GB',
        'Rank  5': 'Pid  40534 on ip-172-31-20-44 device  5 [0x00] Tesla '
                   'V100-SXM2-32GB',
        'Rank  6': 'Pid  40535 on ip-172-31-20-44 device  6 [0x00] Tesla '
                   'V100-SXM2-32GB',
        'Rank  7': 'Pid  40536 on ip-172-31-20-44 device  7 [0x00] Tesla '
                   'V100-SXM2-32GB',
        'Rank  8': 'Pid  25655 on ip-172-31-26-32 device  0 [0x00] Tesla '
                   'V100-SXM2-32GB',
        'Rank  9': 'Pid  25656 on ip-172-31-26-32 device  1 [0x00] Tesla '
                   'V100-SXM2-32GB',
        'Rank 10': 'Pid  25657 on ip-172-31-26-32 device  2 [0x00] Tesla '
                   'V100-SXM2-32GB',
        'Rank 11': 'Pid  25658 on ip-172-31-26-32 device  3 [0x00] Tesla '
                   'V100-SXM2-32GB',
        'Rank 12': 'Pid  25659 on ip-172-31-26-32 device  4 [0x00] Tesla '
                   'V100-SXM2-32GB',
        'Rank 13': 'Pid  25660 on ip-172-31-26-32 device  5 [0x00] Tesla '
                   'V100-SXM2-32GB',
        'Rank 14': 'Pid  25661 on ip-172-31-26-32 device  6 [0x00] Tesla '
                   'V100-SXM2-32GB',
        'Rank 15': 'Pid  25664 on ip-172-31-26-32 device  7 [0x00] Tesla '
                   'V100-SXM2-32GB',
        'size': '8',
        'count': '2',
        'nccl_type': 'float',
        'redop': 'sum',
        'out_of_place_time': '58.10',
        'out_of_place_algbw': '0.00',
        'out_of_place_busbw': '0.00',
        'out_of_place_error': '4e-07',
        'in_place_time': '57.87',
        'in_place_algbw': '0.00',
        'in_place_busbw': '0.00',
        'in_place_error': '4e-07'}
    golden = Sample(metric='In place algorithm bandwidth', value=0.0,
                    unit='GB/s', metadata=metadata)
    self.assertIn(golden, samples)
    self.assertAlmostEqual(bandwidth, 8.43)


if __name__ == '__main__':
  unittest.main()
