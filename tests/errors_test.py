# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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

import sys
import unittest

from absl import flags
from absl.testing import parameterized
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_status
from perfkitbenchmarker import errors


FLAGS = flags.FLAGS


class ErrorsTestCase(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    try:
      FLAGS.mark_as_parsed()
    except flags.UnparsedFlagAccessError:
      FLAGS(sys.argv)

  @parameterized.parameters(
      (
          errors.Benchmarks.QuotaFailure('quota exceeded'),
          benchmark_status.FailedSubstatus.QUOTA,
      ),
      (
          KeyboardInterrupt(),
          benchmark_status.FailedSubstatus.PROCESS_KILLED,
      ),
      (
          ValueError('some random error'),
          benchmark_status.FailedSubstatus.UNCATEGORIZED,
      ),
  )
  def testGetBenchmarkStatusFromException(self, exception, expected_status):
    self.assertEqual(
        errors.GetBenchmarkStatusFromException(exception),
        expected_status,
    )

  def testGetBenchmarkStatusFromException_ThreadException(self):
    def RaiseQuotaFailure(unused):
      raise errors.Benchmarks.QuotaFailure('internal quota failure')

    with self.assertRaises(errors.VmUtil.ThreadException) as cm:
      background_tasks.RunThreaded(RaiseQuotaFailure, [None])

    self.assertEqual(
        errors.GetBenchmarkStatusFromException(cm.exception),
        benchmark_status.FailedSubstatus.QUOTA,
    )


if __name__ == '__main__':
  unittest.main()
