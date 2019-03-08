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
"""Tests for the Horovod benchmark."""
import os
import unittest
from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_benchmarks import horovod_benchmark
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class HorovodBenchmarkTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(HorovodBenchmarkTestCase, self).setUp()
    path = os.path.join(os.path.dirname(__file__), '../data',
                        'horovod_results.txt')
    with open(path) as fp:
      self.test_output = fp.read()

  def testExtractThroughputAndRuntime(self):
    throughput, runtime = horovod_benchmark._ExtractThroughputAndRuntime(
        self.test_output)
    self.assertEqual(789.5, throughput)
    self.assertEqual(1650.5, runtime)


if __name__ == '__main__':
  unittest.main()
