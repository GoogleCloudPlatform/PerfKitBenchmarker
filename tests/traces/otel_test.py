# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for otel trace utility."""
import json
import os
import unittest

from absl import flags
from perfkitbenchmarker.traces import otel

FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()

TEST_VM = 'test_vm0'


class OtelTestCase(unittest.TestCase):

  def setUp(self):
    super(OtelTestCase, self).setUp()
    directory = os.path.join(os.path.dirname(__file__), '..', 'data')

    self.collector = otel._OTELCollector(output_directory=directory)
    self.collector._role_mapping[TEST_VM] = 'otel_output.txt'

    path = os.path.join(directory, 'otel_samples.json')
    with open(path, 'r') as file_content:
      self.contents = json.load(file_content)

  def testOtelAnalyze(self):
    samples = []
    self.collector.Analyze('test_sender', None, samples)
    self.assertEqual(len(samples), 30)

    metric_names = self.contents.keys()
    for sample in samples:
      self.assertIn(sample.metric, metric_names)
      self.assertEqual(sample.metadata['values'],
                       self.contents[sample.metric]['values'])
      self.assertEqual(sample.metadata['values'],
                       self.contents[sample.metric]['values'])
      self.assertEqual(sample.metadata['timestamps'],
                       self.contents[sample.metric]['timestamps'])
      self.assertEqual(sample.metadata['unit'],
                       self.contents[sample.metric]['unit'])
      self.assertEqual(sample.metadata['vm_role'], TEST_VM)


if __name__ == '__main__':
  unittest.main()
