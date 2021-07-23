# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for the specjbb2015_benchmark."""

import unittest
from perfkitbenchmarker.linux_benchmarks import specjbb2015_benchmark


SAMPLE_OUTPUT = (
    'Random Text\n\nRUN RESULT: hbIR (max attempted) = 20138, hbIR (settled) ='
    ' 19989, max-jOPS = 19131, critical-jOPS = 6237\nOther Random Text'
)


class Specjbb2015BenchmarkTest(unittest.TestCase):

  def testParseJbbOutput(self):
    metadata = {'OpenJDK_version': '8'}

    samples = specjbb2015_benchmark.ParseJbbOutput(SAMPLE_OUTPUT, metadata)
    expected = {
        'max_jOPS': 19131,
        'critical_jOPS': 6237,
    }

    for sample in samples:
      self.assertEqual(expected[sample.metric], sample.value)
      self.assertEqual('8', sample.metadata['OpenJDK_version'])


if __name__ == '__main__':
  unittest.main()
