# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for perfkitbenchmarker.benchmark_sets."""

import unittest
from mock import patch

# This import to ensure required FLAGS are defined.
from perfkitbenchmarker import pkb  # NOQA
from perfkitbenchmarker import linux_benchmarks
from perfkitbenchmarker import benchmark_sets


class BenchmarkSetsTestCase(unittest.TestCase):

  def setUp(self):
    # create set of valid benchmark names from the benchmark directory
    self.valid_benchmark_names = set()
    for benchmark_module in linux_benchmarks.BENCHMARKS:
        self.valid_benchmark_names.add(benchmark_module.BENCHMARK_NAME)

    self.valid_benchmark_set_names = set()
    # include the benchmark_set names since these can also appear
    # as a valid name.  At runtime they get expanded.
    for benchmark_set_name in benchmark_sets.BENCHMARK_SETS:
      self.valid_benchmark_set_names.add(benchmark_set_name)

    # Mock flags to simulate setting --benchmarks.
    p = patch(benchmark_sets.__name__ + '.FLAGS')
    self.mock_flags = p.start()
    self.addCleanup(p.stop)

  def testStandardSet(self):
    self.assertIn(benchmark_sets.STANDARD_SET, benchmark_sets.BENCHMARK_SETS)
    standard_set = (benchmark_sets.BENCHMARK_SETS[
                    benchmark_sets.STANDARD_SET])[benchmark_sets.BENCHMARK_LIST]
    self.assertIn('iperf', standard_set)
    self.assertIn('fio', standard_set)

  def testBenchmarkSetsHaveValidNames(self):
    # check all the benchmark sets to make sure they contain valid names
    valid_benchmark_and_set_names = (self.valid_benchmark_names |
                                     self.valid_benchmark_set_names)
    benchmark_set_items = benchmark_sets.BENCHMARK_SETS.items()
    for key_name, key_value in benchmark_set_items:
      benchmark_def_list = key_value[benchmark_sets.BENCHMARK_LIST]
      for benchmark_name in benchmark_def_list:
        self.assertIn(benchmark_name, valid_benchmark_and_set_names)

  def testBenchmarkDerivedSets(self):
    # make sure that sets which are derived from the standard_set
    # expands into a valid set of benchmarks
    with patch.dict(benchmark_sets.BENCHMARK_SETS, {
            'test_derived_set': {
            benchmark_sets.MESSAGE: 'test derived benchmark set.',
            benchmark_sets.BENCHMARK_LIST: [benchmark_sets.STANDARD_SET]}}):
      self.mock_flags.benchmarks = ['test_derived_set']
      benchmark_tuple_list = benchmark_sets.GetBenchmarksFromFlags()
      self.assertIsNotNone(benchmark_tuple_list)
      self.assertGreater(len(benchmark_tuple_list), 0)
      for benchmark_tuple in benchmark_tuple_list:
        self.assertIn(benchmark_tuple[0].BENCHMARK_NAME,
                      self.valid_benchmark_names)

  def testBenchmarkNestedDerivedSets(self):
    # make sure that sets which are derived from the standard_set
    # expands into a valid set of benchmarks
    self.mock_flags.benchmarks = [benchmark_sets.STANDARD_SET]
    standard_module_list = benchmark_sets.GetBenchmarksFromFlags()
    with patch.dict(benchmark_sets.BENCHMARK_SETS, {
            'test_derived_set': {
            benchmark_sets.MESSAGE: 'test derived benchmark set.',
            benchmark_sets.BENCHMARK_LIST: [benchmark_sets.STANDARD_SET]},
            'test_nested_derived_set': {
            benchmark_sets.MESSAGE: 'test nested derived benchmark set.',
            benchmark_sets.BENCHMARK_LIST: ['test_derived_set']}}):
      # TODO(voellm): better check would be to make sure both lists are the same
      benchmark_tuple_list = benchmark_sets.GetBenchmarksFromFlags()
      self.assertIsNotNone(benchmark_tuple_list)
      self.assertIsNotNone(standard_module_list)
      self.assertEqual(len(benchmark_tuple_list), len(standard_module_list))
      for benchmark_tuple in benchmark_tuple_list:
        self.assertIn(benchmark_tuple[0].BENCHMARK_NAME,
                      self.valid_benchmark_names)

  def testBenchmarkValidCommandLine1(self):
    # make sure the standard_set expands to a valid set of benchmarks
    self.mock_flags.benchmarks = ['standard_set']
    benchmark_tuple_list = benchmark_sets.GetBenchmarksFromFlags()
    self.assertIsNotNone(benchmark_tuple_list)
    self.assertGreater(len(benchmark_tuple_list), 0)
    for benchmark_tuple in benchmark_tuple_list:
      self.assertIn(benchmark_tuple[0].BENCHMARK_NAME,
                    self.valid_benchmark_names)

  @staticmethod
  def _ContainsModule(module_name, module_list):
    for module_tuple in module_list:
      if module_tuple[0].BENCHMARK_NAME == module_name:
        return True
    return False

  def testBenchmarkValidCommandLine2(self):
    # make sure the standard_set plus a listed benchmark expands
    # to a valid set of benchmarks
    self.mock_flags.benchmarks = ['standard_set', 'bonnie++']
    benchmark_tuple_list = benchmark_sets.GetBenchmarksFromFlags()
    self.assertIsNotNone(benchmark_tuple_list)
    self.assertGreater(len(benchmark_tuple_list), 0)
    for benchmark_tuple in benchmark_tuple_list:
      self.assertIn(benchmark_tuple[0].BENCHMARK_NAME,
                    self.valid_benchmark_names)
    # make sure bonnie++ is a listed benchmark
    self.assertTrue(self._ContainsModule('bonnie++', benchmark_tuple_list))

  def testBenchmarkValidCommandLine3(self):
    # make sure the command with two benchmarks is processed correctly
    self.mock_flags.benchmarks = ['iperf', 'fio']
    benchmark_tuple_list = benchmark_sets.GetBenchmarksFromFlags()
    self.assertIsNotNone(benchmark_tuple_list)
    self.assertEqual(len(benchmark_tuple_list), 2)
    for benchmark_tuple in benchmark_tuple_list:
      self.assertIn(benchmark_tuple[0].BENCHMARK_NAME,
                    self.valid_benchmark_names)
    # make sure listed benchmarks are present
    self.assertTrue(self._ContainsModule('iperf', benchmark_tuple_list))
    self.assertTrue(self._ContainsModule('fio', benchmark_tuple_list))

  def testBenchmarkInvalidCommandLine1(self):
    # make sure invalid benchmark names and sets cause a failure
    self.mock_flags.benchmarks = ['standard_set_invalid_name']
    self.assertRaises(ValueError, benchmark_sets.GetBenchmarksFromFlags)

  def testBenchmarkInvalidCommandLine2(self):
    # make sure invalid benchmark names and sets cause a failure
    self.mock_flags.benchmarks = ['standard_set', 'iperf_invalid_name']
    self.assertRaises(ValueError, benchmark_sets.GetBenchmarksFromFlags)
