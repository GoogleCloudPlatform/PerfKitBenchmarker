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

"""Tests for bonnie_benchmark."""

import os
import unittest

from perfkitbenchmarker.benchmarks import bonnie_benchmark


class BonnieBenchmarkTestCase(unittest.TestCase):

  def setUp(self):
    path = os.path.join('tests/data',
                        'bonnie-plus-plus-sample.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def tearDown(self):
    pass

  def testParseCSVResults(self):
    result = bonnie_benchmark.ParseCSVResults(self.contents)
    expected_result = [['Sequential Output:Block:Throughput', 72853.0, 'K/sec',
                        {'test_size': '7423M', 'version': '1.96',
                         'chunk_size': '1'}],
                       ['Sequential Output:Block:Cpu Percentage', 15.0, '%',
                        {'test_size': '7423M', 'version': '1.96',
                         'chunk_size': '1'}],
                       ['Sequential Output:Block:Latency', 512.0, 'ms',
                        {'test_size': '7423M', 'version': '1.96',
                         'chunk_size': '1'}],
                       ['Sequential Output:Rewrite:Throughput', 47358.0,
                        'K/sec', {'test_size': '7423M', 'version': '1.96',
                                  'chunk_size': '1'}],
                       ['Sequential Output:Rewrite:Cpu Percentage', 5.0, '%',
                        {'test_size': '7423M', 'version': '1.96',
                         'chunk_size': '1'}],
                       ['Sequential Output:Rewrite:Latency', 670.0, 'ms',
                        {'test_size': '7423M', 'version': '1.96',
                         'chunk_size': '1'}],
                       ['Sequential Input:Block:Throughput', 156821.0, 'K/sec',
                        {'test_size': '7423M', 'version': '1.96',
                         'chunk_size': '1'}],
                       ['Sequential Input:Block:Cpu Percentage', 7.0, '%',
                        {'test_size': '7423M', 'version': '1.96',
                         'chunk_size': '1'}],
                       ['Sequential Iutput:Block:Latency', 44660.0, 'us',
                        {'test_size': '7423M', 'version': '1.96',
                         'chunk_size': '1'}],
                       ['Random Seeks:Throughput', 537.7, 'K/sec',
                        {'test_size': '7423M', 'version': '1.96',
                         'chunk_size': '1'}],
                       ['Random Seeks:Cpu Percentage', 10.0, '%',
                        {'test_size': '7423M', 'version': '1.96',
                         'chunk_size': '1'}],
                       ['Random Seeks:Latency', 200.0, 'ms',
                        {'test_size': '7423M', 'version': '1.96',
                         'chunk_size': '1'}],
                       ['Sequential Create:Create:Throughput', 49223.0, 'K/sec',
                        {'num_files': '100', 'test_size': '7423M',
                         'version': '1.96', 'chunk_size': '1'}],
                       ['Sequential Create:Create:Cpu Percentage', 58.0, '%',
                        {'num_files': '100', 'test_size': '7423M',
                         'version': '1.96', 'chunk_size': '1'}],
                       ['Sequential Create:Create:Latency', 3747.0, 'us',
                        {'num_files': '100', 'test_size': '7423M',
                         'version': '1.96', 'chunk_size': '1'}],
                       ['Sequential Create:Read:Latency', 1759.0, 'us',
                        {'num_files': '100', 'test_size': '7423M',
                         'version': '1.96', 'chunk_size': '1'}],
                       ['Sequential Create:Delete:Throughput', 54405.0, 'K/sec',
                        {'num_files': '100', 'test_size': '7423M',
                         'version': '1.96', 'chunk_size': '1'}],
                       ['Sequential Create:Delete:Cpu Percentage', 53.0, '%',
                        {'num_files': '100', 'test_size': '7423M',
                         'version': '1.96', 'chunk_size': '1'}],
                       ['Sequential Create:Delete:Latency', 1643.0, 'us',
                        {'num_files': '100', 'test_size': '7423M',
                         'version': '1.96', 'chunk_size': '1'}],
                       ['Random Create:Create:Throughput', 2898.0, 'K/sec',
                        {'num_files': '100', 'test_size': '7423M',
                         'version': '1.96', 'chunk_size': '1'}],
                       ['Random Create:Create:Cpu Percentage', 97.0, '%',
                        {'num_files': '100', 'test_size': '7423M',
                         'version': '1.96', 'chunk_size': '1'}],
                       ['Random Create:Create:Latency', 33518.0, 'us',
                        {'num_files': '100', 'test_size': '7423M',
                         'version': '1.96', 'chunk_size': '1'}],
                       ['Random Create:Read:Latency', 192.0, 'us',
                        {'num_files': '100', 'test_size': '7423M',
                         'version': '1.96', 'chunk_size': '1'}],
                       ['Random Create:Delete:Throughput', 59089.0, 'K/sec',
                        {'num_files': '100', 'test_size': '7423M',
                         'version': '1.96', 'chunk_size': '1'}],
                       ['Random Create:Delete:Cpu Percentage', 60.0, '%',
                        {'num_files': '100', 'test_size': '7423M',
                         'version': '1.96', 'chunk_size': '1'}],
                       ['Random Create:Delete:Latency', 839.0, 'us',
                        {'num_files': '100', 'test_size': '7423M',
                         'version': '1.96', 'chunk_size': '1'}]]
    self.assertEqual(result, expected_result)


if __name__ == '__main__':
  unittest.main()
