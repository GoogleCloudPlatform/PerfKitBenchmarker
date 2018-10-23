# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for diskspd_benchmark."""

import os
import unittest

from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.windows_packages import diskspd


class DiskspdBenchmarkTestCase(unittest.TestCase, test_util.SamplesTestMixin):

  def getDataContents(self, file_name):
    path = os.path.join(os.path.dirname(__file__), '..', 'data', file_name)
    with open(path) as fp:
      contents = fp.read()
    return contents

  def setUp(self):
    self.result_xml = self.getDataContents('diskspd_result.xml')

  def testNtttcpTcpParsing(self):
    samples = diskspd.ParseDiskSpdResults(self.result_xml, {})
    expected_metadata = {'DisableAffinity': 'false',
                         'MaxFileSize': '0',
                         'BlockSize': '65536',
                         'IdlePercent': '96.80',
                         'CompletionRoutines': 'false',
                         'StrideSize': '65536',
                         'RandomAccess': 'false',
                         'Weight': '1',
                         'UserPercent': '0.25',
                         'WriteBytes': 0,
                         'Warmup': '5',
                         'Pattern': 'sequential',
                         'IOPriority': '3',
                         'ThreadsPerFile': '4',
                         'KernelPercent': '2.95',
                         'ReadIops': 3030,
                         'BytesCount': 5961809920,
                         'InterlockedSequential': 'false',
                         'MeasureLatency': 'false',
                         'WriteRatio': '0',
                         'FileSize': '838860800',
                         'BaseFileOffset': '0',
                         'Cooldown': '0',
                         'IOCount': 90970,
                         'UseLargePages': 'false',
                         'UsagePercent': '3.20',
                         'SequentialScan': 'true',
                         'TotalIops': 3030,
                         'WriteSpeed': 0,
                         'ProcCount': '4',
                         'WriteCount': 0,
                         'ReadSpeed': 189,
                         'TestTimeSeconds': '30.02',
                         'ThreadCount': '4',
                         'ReadBytes': 5961809920,
                         'TemporaryFile': 'false',
                         'ReadCount': 90970,
                         'Duration': '30',
                         'ThreadStride': '0',
                         'TotalSpeed': 189,
                         'RandSeed': '0',
                         'RequestCount': '0',
                         'Path': 'C:\\scratch\\testfile.dat',
                         'WriteThrough': 'true',
                         'CalculateIopsStdDev': 'false',
                         'IoBucketDuration': '1000',
                         'ParallelAsyncIO': 'false',
                         'Throughput': '0',
                         'DisableOSCache': 'true',
                         'WriteIops': 0}
    expected_samples = [
        sample.Sample('ReadSpeed', 189, 'MB/s', expected_metadata),
    ]
    self.assertSampleListsEqualUpToTimestamp(expected_samples, samples)
