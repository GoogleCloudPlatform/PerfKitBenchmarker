# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Unit tests for the GKE Scale-to-Zero vs KEDA benchmark framework."""

import unittest
from perfkitbenchmarker.linux_benchmarks.kubernetes import k8s_stz_benchmark
from tests import pkb_common_test_case


class GkeStzBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def testParseOutput(self):
    output_string = 'some logs\nsfz_latency_ms=123.45\nmore logs'
    base_metadata = {'deploy_keda': False}

    samples = k8s_stz_benchmark._ParseOutput(output_string, base_metadata)

    self.assertLen(samples, 1)
    self.assertEqual(samples[0].metric, 'SFZ_Latency')
    self.assertEqual(samples[0].value, 123.45)
    self.assertEqual(samples[0].unit, 'ms')
    self.assertEqual(samples[0].metadata, {'deploy_keda': False})


if __name__ == '__main__':
  unittest.main()
