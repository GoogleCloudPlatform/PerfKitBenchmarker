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
    path = os.path.join(
        os.path.dirname(__file__), '..', 'data', 'nccl_output.txt'
    )
    with open(path) as fp:
      self.contents = fp.read()

  @mock.patch('time.time', mock.MagicMock(return_value=1550279509.59))
  def testNcclResults(self):
    samples, bandwidth = nccl_benchmark.MakeSamplesFromOutput({}, self.contents)
    metadata = {
        'size': '1048576',
        'count': '262144',
        'nccl_type': 'float',
        'out_of_place_time': '2410.0',
        'out_of_place_algbw': '0.44',
        'out_of_place_busbw': '0.44',
        'out_of_place_error': 'N/A',
        'in_place_time': '2709.7',
        'in_place_algbw': '0.39',
        'in_place_busbw': '0.39',
        'in_place_error': 'N/A',
        'nThread': '1',
        'nGpus': '1',
        'minBytes': '1048576',
        'maxBytes': '1073741824',
        'step': '2(factor)',
        'warmup_iters': '5',
        'iters': '1',
        'agg_iters': '1',
        'validation': '0',
        'graph': '0',
        'Rank  0': 'Pid   9192 on pkb-2b6a393d-2 device  0 [0x00] Tesla K80',
        'Rank  1': 'Pid   9178 on pkb-2b6a393d-3 device  0 [0x00] Tesla K80',
    }
    golden = Sample(
        metric='In place algorithm bandwidth',
        value=0.39,
        unit='GB/s',
        metadata=metadata,
    )
    self.assertIn(golden, samples)
    self.assertAlmostEqual(bandwidth, 0.63)


if __name__ == '__main__':
  unittest.main()
