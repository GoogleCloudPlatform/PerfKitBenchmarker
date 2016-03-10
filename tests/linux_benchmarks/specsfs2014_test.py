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

"""Tests for specsfs2014_benchmark."""

import os
import unittest

from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import specsfs2014_benchmark


class SpecSfs2014BenchmarkTestCase(unittest.TestCase,
                                   test_util.SamplesTestMixin):

  def setUp(self):
    self.maxDiff = None
    # Load data
    path = os.path.join(os.path.dirname(__file__),
                        '..', 'data',
                        'specsfs2014_results.xml')
    with open(path) as fp:
      self.specsfs2014_xml_results = fp.read()

  def testSpecSfs2014Parsing(self):
    samples = specsfs2014_benchmark._ParseSpecSfsOutput(
        self.specsfs2014_xml_results)

    expected_metadata = {
        'client data set size (MiB)': '4296',
        'starting data set size (MiB)': '4296',
        'maximum file space (MiB)': '4687', 'file size (KB)': '16',
        'run time (seconds)': '300', 'benchmark': 'SWBUILD',
        'business_metric': '1', 'op rate (ops/s)': '500.00',
        'processes per client': '5', 'initial file space (MiB)': '4296'
    }

    expected_samples = [
        sample.Sample(
            'achieved rate', 500.05, 'ops/s', expected_metadata),
        sample.Sample(
            'average latency', 1.48, 'milliseconds', expected_metadata),
        sample.Sample(
            'overall throughput', 6463.95, 'KB/s', expected_metadata),
        sample.Sample(
            'read throughput', 3204.78, 'KB/s', expected_metadata),
        sample.Sample(
            'write throughput', 3259.17, 'KB/s', expected_metadata)
    ]

    self.assertSampleListsEqualUpToTimestamp(expected_samples, samples)
