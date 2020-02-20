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
    filenames = [
        'horovod_output_resnet.txt',
        'horovod_output_bert.txt',
    ]
    self.test_output = dict()
    for fn in filenames:
      path = os.path.join(os.path.dirname(__file__), '../data', fn)
      self.test_output[fn] = open(path).read()

  def testExtractResNetThroughput(self):
    throughput, _ = horovod_benchmark._ExtractResNetThroughput(
        self.test_output['horovod_output_resnet.txt'])
    self.assertEqual(36517.1, throughput)

  def testExtractBertThroughput(self):
    throughput, _ = horovod_benchmark._ExtractBertThroughput(
        self.test_output['horovod_output_bert.txt'])
    self.assertEqual(52.3, throughput)


if __name__ == '__main__':
  unittest.main()
