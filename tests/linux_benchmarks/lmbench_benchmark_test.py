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
"""Tests for LMBENCH benchmark."""
import os
import unittest

import mock

from perfkitbenchmarker.linux_benchmarks import lmbench_benchmark


class LmbenchTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch(lmbench_benchmark.__name__)
    p.start()
    self.addCleanup(p.stop)

    path = os.path.join(
        os.path.dirname(__file__), '../data', 'lmbench_output.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def testParseLmbench(self):
    samples = lmbench_benchmark._ParseOutput(self.contents)

    self.assertEqual(62, len(samples))

    # Test metadata
    metadata = samples[0].metadata
    self.assertEqual('8', metadata['MB'])
    self.assertEqual('NO', metadata['BENCHMARK_HARDWARE'])
    self.assertEqual('YES', metadata['BENCHMARK_OS'])

    # Test metric and value
    processor_results = {i.metric: i.value for i in samples}

    self.assertAlmostEqual(0.2345, processor_results['syscall'])
    self.assertAlmostEqual(0.3515, processor_results['read'])
    self.assertAlmostEqual(0.3082, processor_results['write'])
    self.assertAlmostEqual(0.6888, processor_results['stat'])
    self.assertAlmostEqual(0.3669, processor_results['fstat'])
    self.assertAlmostEqual(1.5541, processor_results['open/close'])
    self.assertAlmostEqual(0.3226,
                           processor_results['Signal handler installation'])
    self.assertAlmostEqual(1.1736, processor_results['Signal handler overhead'])
    self.assertAlmostEqual(0.7491, processor_results['Protection fault'])
    self.assertAlmostEqual(25.5437, processor_results['Pipe latency'])
    self.assertAlmostEqual(121.7399, processor_results['Process fork+exit'])
    self.assertAlmostEqual(318.6445, processor_results['Process fork+execve'])
    self.assertAlmostEqual(800.2188,
                           processor_results['Process fork+/bin/sh -c'])
    self.assertAlmostEqual(0.1639,
                           processor_results['Pagefaults on /var/tmp/XXX'])

    sample = next(x for x in samples if x.metric == 'context_switching_time' and
                  x.metadata['num_of_processes'] == 96 and
                  x.metadata['memory_size'] == '64k')
    self.assertAlmostEqual(15.45, sample.value)

    sample = next(x for x in samples if x.metric == 'context_switching_time' and
                  x.metadata['num_of_processes'] == 4 and
                  x.metadata['memory_size'] == '32k')
    self.assertAlmostEqual(13.96, sample.value)

    sample = next(x for x in samples if x.metric == 'context_switching_time' and
                  x.metadata['num_of_processes'] == 2 and
                  x.metadata['memory_size'] == '16k')
    self.assertAlmostEqual(14.21, sample.value)

    sample = next(x for x in samples if x.metric == 'context_switching_time' and
                  x.metadata['num_of_processes'] == 16 and
                  x.metadata['memory_size'] == '8k')
    self.assertAlmostEqual(13.02, sample.value)

    sample = next(x for x in samples if x.metric == 'context_switching_time' and
                  x.metadata['num_of_processes'] == 8 and
                  x.metadata['memory_size'] == '4k')
    self.assertAlmostEqual(12.40, sample.value)

    sample = next(x for x in samples if x.metric == 'context_switching_time' and
                  x.metadata['num_of_processes'] == 32 and
                  x.metadata['memory_size'] == '0k')
    self.assertAlmostEqual(12.63, sample.value)


if __name__ == '__main__':
  unittest.main()
