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

"""Tests for Glibc benchmark."""

import os
import unittest

import mock

from perfkitbenchmarker.linux_benchmarks import glibc_benchmark

_TEST_GCC_VERSION = '7.4.0'


class GlibcTestCase(unittest.TestCase):

  def setUp(self):
    super(GlibcTestCase, self).setUp()
    p = mock.patch(glibc_benchmark.__name__ + '.FLAGS')
    p.start()
    self.addCleanup(p.stop)
    glibc_benchmark._GetGccVersion = mock.Mock(return_value=_TEST_GCC_VERSION)

  def CallParseOutput(self, filename, benchmark_spec, upper_key, results):
    """Read sample outputs of glibc_benchmark and call ParseOutput function.

    Args:
      filename: The name of the sample output file
      benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
      upper_key: The first dimension key of the glibc_output dict.
      results:
        A list to which the ParseOutput function will append new samples based
        on the glibc output.
    """
    path = os.path.join(os.path.dirname(__file__), '../data',
                        filename)
    with open(path) as fp:
      self.contents = fp.read()
    glibc_benchmark.ParseOutput(self.contents, benchmark_spec,
                                upper_key, results)

  def testParseGlibc(self):
    benchmark_spec = mock.MagicMock()
    results = []
    upper_key = 'functions'

    self.CallParseOutput(
        'glibc_bench_output.txt', benchmark_spec, upper_key, results)

    result = {i.metric: i.metadata for i in results}
    metadata = result['pthread_once:']

    self.assertEqual(63, len(results))
    self.assertAlmostEqual(1.72198e+10, metadata['duration'])
    self.assertAlmostEqual(3.20756e+09, metadata['iterations'])
    self.assertAlmostEqual(9626.89, metadata['max'])
    self.assertAlmostEqual(5.198, metadata['min'])
    self.assertAlmostEqual(5.3685, metadata['mean'])
    self.assertEqual(_TEST_GCC_VERSION, metadata['gcc'])

  def testParseGlibc2(self):
    benchmark_spec = mock.MagicMock()
    results = []
    upper_key = 'math-inlines'

    self.CallParseOutput(
        'glibc_benchset_output.txt', benchmark_spec, upper_key, results)

    result = {i.metric: i.metadata for i in results}
    metadata = result['__isnan:inf/nan']

    self.assertEqual(42, len(results))
    self.assertAlmostEqual(8.42329e+06, metadata['duration'])
    self.assertAlmostEqual(500, metadata['iterations'])
    self.assertAlmostEqual(16846, metadata['mean'])
    self.assertEqual(_TEST_GCC_VERSION, metadata['gcc'])

  def testParseGlibc3(self):
    benchmark_spec = mock.MagicMock()
    results = []
    upper_key = 'functions'

    self.CallParseOutput(
        'glibc_malloc_output.txt', benchmark_spec, upper_key, results)

    metadata = results[0].metadata
    metric = results[0].metric

    self.assertEqual(1, len(results))
    self.assertEqual('malloc:', metric)
    self.assertAlmostEqual(1.2e+11, metadata['duration'])
    self.assertAlmostEqual(2.82979e+09, metadata['iterations'])
    self.assertAlmostEqual(42.406, metadata['time_per_iteration'])
    self.assertAlmostEqual(1800, metadata['max_rss'])
    self.assertAlmostEqual(1, metadata['threads'])
    self.assertAlmostEqual(4, metadata['min_size'])
    self.assertAlmostEqual(32768, metadata['max_size'])
    self.assertAlmostEqual(88, metadata['random_seed'])
    self.assertEqual(_TEST_GCC_VERSION, metadata['gcc'])


if __name__ == '__main__':
  unittest.main()
