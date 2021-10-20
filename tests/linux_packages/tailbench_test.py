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
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for tailbench."""

import os
from typing import List
import unittest

from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import tailbench
from tests import pkb_common_test_case


class TailbenchTest(pkb_common_test_case.PkbCommonTestCase):

  def validate_results(self, results: List[tailbench._TestResult]):
    for result in results:
      histogram: sample._Histogram = result.histogram
      if not (histogram[i] <= histogram[i + 1]
              for i in range(len(histogram) - 1)):
        return False
    return True

  def testParseResultsIsReadable(self):
    test_results = List[tailbench._TestResult]
    path = os.path.join(
        os.path.dirname(__file__), '..', 'data', 'tailbench-img-dnnResult.txt')
    test_results = tailbench._ParseResultsFile(path,
                                               'name')
    self.assertTrue(self.validate_results(test_results))

  def testDataConforms(self):
    path = os.path.join(
        os.path.dirname(__file__), '..', 'data', 'tailbench-img-dnnResult.txt')
    samp = tailbench.BuildHistogramSamples(path)
    dictionary = samp[0].asdict()
    self.assertGreater(len(dictionary), 0)


if __name__ == '__main__':
  unittest.main()
