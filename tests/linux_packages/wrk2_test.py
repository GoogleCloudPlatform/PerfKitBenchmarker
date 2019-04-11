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

import os
import unittest

from perfkitbenchmarker.linux_packages import wrk2


def _ReadOutputFile(file_name):
  data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
  result_path = os.path.join(data_dir, file_name)
  with open(result_path) as results_file:
    return results_file.read()


class Wrk2Test(unittest.TestCase):

  def testParseSuccessfulRun(self):
    wrk_output = _ReadOutputFile('wrk2_output.txt')
    result = list(wrk2._ParseOutput(wrk_output))
    self.assertEqual(
        [('p50 latency', 53.31, 'ms'),
         ('p75 latency', 56.99, 'ms'),
         ('p90 latency', 62.62, 'ms'),
         ('p99 latency', 223.23, 'ms'),
         ('p99.9 latency', 244.22, 'ms'),
         ('p99.99 latency', 244.22, 'ms'),
         ('p99.999 latency', 244.22, 'ms'),
         ('p100 latency', 244.22, 'ms'),
         ('requests', 600, ''),
         ('error_rate', 0, ''),
         ('errors', 0, '')], result)

  def testParseAllRequestsFailed(self):
    wrk_output = _ReadOutputFile('wrk2_output_all_error.txt')
    with self.assertRaisesRegexp(ValueError, 'More than 10%'):
      list(wrk2._ParseOutput(wrk_output))

  def testParseWithErrors(self):
    wrk_output = _ReadOutputFile('wrk2_output_errors.txt')
    res = list(wrk2._ParseOutput(wrk_output))
    self.assertIn(('errors', 14, ''), res)
    self.assertIn(('error_rate', 14. / 600, ''), res)


if __name__ == '__main__':
  unittest.main()
