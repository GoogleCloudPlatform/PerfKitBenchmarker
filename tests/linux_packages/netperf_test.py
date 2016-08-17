# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.packages.netperf."""

import os
import unittest


from perfkitbenchmarker.linux_packages import netperf


class NetperfParseHistogramTestCase(unittest.TestCase):

  def setUp(self):
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    result_path = os.path.join(data_dir, 'netperf_results.txt')
    with open(result_path) as results_file:
      self.netperf_output = results_file.read()

  def testParsesHistogram(self):
    expected = {
        300: 5771, 400: 118948, 500: 7121, 600: 639, 700: 199, 800: 90,
        900: 53, 1000: 149, 2000: 31, 3000: 11, 4000: 8, 5000: 1, 6000: 1,
        7000: 1, 9000: 1
    }

    hist = netperf.ParseHistogram(self.netperf_output)
    self.assertEqual(hist, expected)
