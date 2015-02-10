# Copyright 2014 Google Inc. All rights reserved.
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

from perfkitbenchmarker import benchmarks
from perfkitbenchmarker import benchmark_sets
from perfkitbenchmarker import flags

FLAGS = flags.FLAGS


class BenchmarkSetsTestCase(unittest.TestCase):
  def setUp(self):
    # create set of valid benchmark names from the benchmark directory
    self.valid_benchmark_names = set()
    for benchmark_module in benchmarks.BENCHMARKS:
        self.valid_benchmark_names.add(benchmark_module.GetInfo()['name'])

    self.valid_benchmark_set_names = set()
    # include the benchmark_set names since these can also appear
    # as a valid name.  At runtime they get expanded.
    for benchmark_set_name in benchmark_sets.BENCHMARK_SETS:
      self.valid_benchmark_set_names.add(benchmark_set_name)

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
    for key_name, benchmark_description in \
            benchmark_sets.BENCHMARK_SETS.items():
      for benchmark_name in \
              benchmark_description[benchmark_sets.BENCHMARK_LIST]:
        self.assertIn(benchmark_name, valid_benchmark_and_set_names)

  def testBenchmarkDerivedSets(self):
    # make sure that sets which are derived from the standard_set
    # expands into a valid set of benchmarks
    temp = benchmark_sets.BENCHMARK_SETS.copy()
    benchmark_sets.BENCHMARK_SETS['test_derived_set'] = {
        benchmark_sets.MESSAGE: ('test derived benchmark set.'),
        benchmark_sets.BENCHMARK_LIST: [benchmark_sets.STANDARD_SET]}
    FLAGS.benchmarks = ['test_derived_set']
    benchmark_module_list = benchmark_sets.GetBenchmarksFromFlags()
    self.assertTrue((benchmark_module_list is not None) and
                    (len(benchmark_module_list) > 0))
    for benchmark_module in benchmark_module_list:
      self.assertIn(benchmark_module.GetInfo()['name'],
                    self.valid_benchmark_names)
    benchmark_sets.BENCHMARK_SETS = temp

  def testBenchmarkNestedDerivedSets(self):
    # make sure that sets which are derived from the standard_set
    # expands into a valid set of benchmarks
    FLAGS.benchmarks = [benchmark_sets.STANDARD_SET]
    standard_module_list = benchmark_sets.GetBenchmarksFromFlags()
    temp = benchmark_sets.BENCHMARK_SETS.copy()
    benchmark_sets.BENCHMARK_SETS['test_derived_set'] = {
        benchmark_sets.MESSAGE: ('test derived benchmark set.'),
        benchmark_sets.BENCHMARK_LIST: [benchmark_sets.STANDARD_SET]}
    benchmark_sets.BENCHMARK_SETS['test_nested_derived_set'] = {
        benchmark_sets.MESSAGE: ('test nested derived benchmark set.'),
        benchmark_sets.BENCHMARK_LIST: ['test_derived_set']}
    FLAGS.benchmarks = ['test_nested_derived_set']
    benchmark_module_list = benchmark_sets.GetBenchmarksFromFlags()
    # TODO(voellm): better check would be to make sure both lists are the same
    self.assertTrue((benchmark_module_list is not None) and
                    (standard_module_list is not None) and
                    (len(benchmark_module_list) == len(standard_module_list)))
    for benchmark_module in benchmark_module_list:
      self.assertIn(benchmark_module.GetInfo()['name'],
                    self.valid_benchmark_names)
    benchmark_sets.BENCHMARK_SETS = temp

  def testBenchmarkValidCommandLine1(self):
    # make sure the standard_set expands to a valid set of benchmarks
    FLAGS.benchmarks = ['standard_set']
    benchmark_module_list = benchmark_sets.GetBenchmarksFromFlags()
    self.assertTrue((benchmark_module_list is not None) and
                    (len(benchmark_module_list) > 0))
    for benchmark_module in benchmark_module_list:
      self.assertIn(benchmark_module.GetInfo()['name'],
                    self.valid_benchmark_names)

  @staticmethod
  def _ContainsModule(module_name, module_list):
    has_module = False
    for module in module_list:
      print module.GetInfo()['name']
      if module.GetInfo()['name'] == module_name:
        has_module = True
    return has_module

  def testBenchmarkValidCommandLine2(self):
    # make sure the standard_set plus a listed benchmark expands
    # to a valid set of benchmarks
    FLAGS.benchmarks = ['standard_set', 'bonnie++']
    benchmark_module_list = benchmark_sets.GetBenchmarksFromFlags()
    self.assertTrue((benchmark_module_list is not None) and
                    (len(benchmark_module_list) > 0))
    for benchmark_module in benchmark_module_list:
      self.assertIn(benchmark_module.GetInfo()['name'],
                    self.valid_benchmark_names)
    # make sure bonnie++ is a listed benchmark
    self.assertTrue(self._ContainsModule('bonnie++', benchmark_module_list))

  def testBenchmarkValidCommandLine3(self):
    # make sure the command with two benchmarks is processed correctly
    FLAGS.benchmarks = ['iperf', 'fio']
    benchmark_module_list = benchmark_sets.GetBenchmarksFromFlags()
    self.assertTrue((benchmark_module_list is not None) and
                    (len(benchmark_module_list) == 2))
    for benchmark_module in benchmark_module_list:
      self.assertIn(benchmark_module.GetInfo()['name'],
                    self.valid_benchmark_names)
    # make sure listed benchmarks are present
    self.assertTrue(self._ContainsModule('iperf', benchmark_module_list))
    self.assertTrue(self._ContainsModule('fio', benchmark_module_list))

  def testBenchmarkInvalidCommandLine1(self):
    # make sure invalid benchmark names and sets cause a failure
    FLAGS.benchmarks = ['standard_set_invalid_name']
    self.assertRaises(ValueError, benchmark_sets.GetBenchmarksFromFlags)

  def testBenchmarkInvalidCommandLine2(self):
    # make sure invalid benchmark names and sets cause a failure
    FLAGS.benchmarks = ['standard_set', 'iperf_invalid_name']
    self.assertRaises(ValueError, benchmark_sets.GetBenchmarksFromFlags)

if __name__ == '__main__':
  unittest.main()
