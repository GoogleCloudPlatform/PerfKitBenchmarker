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

"""Tests for bonnie_benchmark."""

import os
import unittest

from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import bonnie_benchmark


class BonnieBenchmarkTestCase(unittest.TestCase, test_util.SamplesTestMixin):

  def setUp(self):
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'bonnie-plus-plus-sample.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def testParseCSVResults(self):
    result = bonnie_benchmark.ParseCSVResults(self.contents)
    expected_result = [
        ['put_block', 72853.0, 'K/sec',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['put_block_cpu', 15.0, '%s',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['rewrite', 47358.0, 'K/sec',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['rewrite_cpu', 5.0, '%s',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['get_block', 156821.0, 'K/sec',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['get_block_cpu', 7.0, '%s',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['seeks', 537.7, 'K/sec',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['seeks_cpu', 10.0, '%s',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['seq_create', 49223.0, 'K/sec',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['seq_create_cpu', 58.0, '%s',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['seq_del', 54405.0, 'K/sec',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['seq_del_cpu', 53.0, '%s',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['ran_create', 2898.0, 'K/sec',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['ran_create_cpu', 97.0, '%s',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['ran_del', 59089.0, 'K/sec',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['ran_del_cpu', 60.0, '%s',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['put_block_latency', 512.0, 'ms',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['rewrite_latency', 670.0, 'ms',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['get_block_latency', 44660.0, 'us',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['seeks_latency', 200.0, 'ms',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['seq_create_latency', 3747.0, 'us',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['seq_stat_latency', 1759.0, 'us',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['seq_del_latency', 1643.0, 'us',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['ran_create_latency', 33518.0, 'us',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['ran_stat_latency', 192.0, 'us',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}],
        ['ran_del_latency', 839.0, 'us',
         {'name': 'perfkit-7b22f510-0', 'format_version': '1.96',
          'num_files': '100', 'seed': '1421800799', 'concurrency': '1',
          'file_size': '7423M', 'bonnie_version': '1.96'}]]
    expected_result = [sample.Sample(*sample_tuple)
                       for sample_tuple in expected_result]
    self.assertSampleListsEqualUpToTimestamp(result, expected_result)


if __name__ == '__main__':
  unittest.main()
