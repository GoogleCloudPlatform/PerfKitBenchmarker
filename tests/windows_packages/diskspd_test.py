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

  def testDiskSpdParsing(self):
    samples = diskspd.ParseDiskSpdResults(self.result_xml, {})
    expected_metadata = {
        'CompletionRoutines': 'false',
        'MeasureLatency': 'false',
        'CalculateIopsStdDev': 'false',
        'DisableAffinity': 'false',
        'Duration': '30',
        'Warmup': '5',
        'Cooldown': '0',
        'ThreadCount': '4',
        'RequestCount': '0',
        'IoBucketDuration': '1000',
        'RandSeed': '0',
        'Path': 'C:\\scratch\\testfile.dat',
        'BlockSize': '65536',
        'BaseFileOffset': '0',
        'SequentialScan': 'true',
        'RandomAccess': 'false',
        'TemporaryFile': 'false',
        'UseLargePages': 'false',
        'DisableOSCache': 'true',
        'WriteThrough': 'true',
        'Pattern': 'sequential',
        'ParallelAsyncIO': 'false',
        'FileSize': '838860800',
        'StrideSize': '65536',
        'InterlockedSequential': 'false',
        'ThreadStride': '0',
        'MaxFileSize': '0',
        'WriteRatio': '0',
        'Throughput': '0',
        'ThreadsPerFile': '4',
        'IOPriority': '3',
        'Weight': '1',
        'TestTimeSeconds': '30.02',
        'ProcCount': '4',
        'UsagePercent': '3.20',
        'UserPercent': '0.25',
        'KernelPercent': '2.95',
        'IdlePercent': '96.80',
        'BytesCount': 5961809920,
        'IOCount': 90970,
        'ReadBytes': 5961809920,
        'ReadCount': 90970,
        'WriteBytes': 0,
        'WriteCount': 0,
    }
    sample_list = [
        sample.Sample('read_speed', 189, 'MB/s', expected_metadata),
        sample.Sample('read_iops', 3030, '', expected_metadata),
        sample.Sample('total_speed', 189, 'MB/s', expected_metadata),
        sample.Sample('total_iops', 3030, '', expected_metadata),
    ]
    self.assertSampleListsEqualUpToTimestamp(
        sorted(samples), sorted(sample_list)
    )


if __name__ == '__main__':
  unittest.main()
