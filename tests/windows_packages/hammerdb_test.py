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

"""Tests for hammerdb."""

import os
import unittest

from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.windows_packages import hammerdb


class HammerDBBenchmarkTestCase(unittest.TestCase, test_util.SamplesTestMixin):

  def getDataContents(self, file_name):
    path = os.path.join(os.path.dirname(__file__), '..', 'data', file_name)
    with open(path) as fp:
      contents = fp.read()
    return contents

  def setUp(self):
    self.result_xml = self.getDataContents('hammerdb_output_log.txt')

  def testHammerDBTpccParsing(self):
    samples = hammerdb.ParseHammerDBResultTPCC(self.result_xml, {}, [1])
    expected_metadata = {
        'hammerdb_tpcc_virtual_user': 1
    }
    expected_samples = [
        sample.Sample('TPM', 60301, 'times/minutes', expected_metadata)
    ]
    self.assertSampleListsEqualUpToTimestamp(expected_samples, samples)

  def testHammerDBTpchParsing(self):
    samples = hammerdb.ParseHammerDBResultTPCH(self.result_xml, {}, 1)
    query_time_list = [68.5, 10, 6.9, 6.3, 20.5, 5.1, 28.1,
                       19.3, 75.9, 17.2, 22.8, 29, 34.2, 2.4, 15.4,
                       12.2, 33.3, 94.1, 34, 15.5, 124, 15.8]
    rf_time_list = [61.4, 26.8]
    power = hammerdb._CalculateTPCHPower(query_time_list, rf_time_list, 300)
    assert int(power) == 49755
    expected_metadata = {}
    expected_samples = [
        sample.Sample('qphh', 1421.1785988465313, 'N/A', expected_metadata)
    ]
    self.assertSampleListsEqualUpToTimestamp(expected_samples, samples)


if __name__ == '__main__':
  unittest.main()

