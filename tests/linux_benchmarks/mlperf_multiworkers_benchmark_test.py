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
"""Tests for multiworker mlperf_benchmark."""
import os
import unittest

from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import mlperf_multiworkers_benchmark
from perfkitbenchmarker.sample import Sample
from tests import pkb_common_test_case


class MlperfMultiworkersBenchmarkTestCase(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):

  def setUp(self):
    super().setUp()
    path = os.path.join(
        os.path.dirname(__file__),
        '..',
        'data',
        'mlperf_multiworkers_output.txt',
    )
    with open(path) as fp:
      self.contents = fp.read()

  def testTrainResults(self):
    samples = mlperf_multiworkers_benchmark.MakeSamplesFromOutput(
        {'version': 'v2.0'}, self.contents, model='bert'
    )
    self.assertLen(samples, 458)
    self.assertSamplesEqualUpToTimestamp(
        Sample(
            metric='speed',
            value=2427.4356684047016,
            unit='samples/sec',
            metadata={'version': 'v2.0'},
        ),
        samples[0],
    )
    self.assertSamplesEqualUpToTimestamp(
        Sample(
            metric='run_stop',
            value=0.0,
            unit='',
            metadata={
                'file': '/workspace/bert/run_pretraining.py',
                'lineno': 1874,
                'status': 'success',
                'version': 'v2.0',
                'namespace': '',
                'event_type': 'INTERVAL_END',
                'value': None,
            },
        ),
        samples[-1],
    )


if __name__ == '__main__':
  unittest.main()
