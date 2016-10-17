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
"""Tests for perfkitbenchmarker.traces.dstat"""

import os
import unittest

from perfkitbenchmarker import events
from perfkitbenchmarker.sample import Sample
from perfkitbenchmarker.traces import dstat


class DstatTestCase(unittest.TestCase):
  maxDiff = None

  def setUp(self):
    directory = os.path.join(os.path.dirname(__file__), '..', 'data')
    path = os.path.join(directory, 'dstat-result.csv')
    self.collector = dstat._DStatCollector(output_directory=directory)
    self.collector._role_mapping['test_vm0'] = path
    events.TracingEvent.events = []
    self.samples = []

  def testAnalyzeEmptyEvents(self):
    self.collector.Analyze('testSender', None, self.samples)
    self.assertEqual(self.samples, [])

  def testAnalyzeInvalidEventTimestamps(self):
    events.AddEvent('sender', 'event', -1, -2, {})
    self.collector.Analyze('testSender', None, self.samples)
    self.assertEqual(self.samples, [])

  def testAnalyzeValidEventSingleRow(self):
    events.AddEvent('sender', 'event', 1475708693, 1475708694,
                    {'label1': 123})
    self.collector.Analyze('testSender', None, self.samples)
    # 61 metrics
    self.assertTrue(len(self.samples), 61)
    expected = Sample(metric='usr__total cpu usage',
                      value=6.4000000000000004,
                      unit='',
                      metadata={'vm_role': 'test_vm0',
                                'label1': 123, 'event': 'event',
                                'sender': 'sender'},
                      timestamp=0.0)
    self.assertEqual(
        expected.metric, self.samples[0].metric)
    self.assertEqual(
        expected.value, self.samples[0].value)
    self.assertEqual(
        expected.metadata, self.samples[0].metadata)

  def testAnalyzeValidEventTwoRows(self):
    events.AddEvent('sender', 'event', 1475708693, 1475708695,
                    {'label1': 123})
    self.collector.Analyze('testSender', None, self.samples)
    # 61 metrics
    self.assertTrue(len(self.samples), 61)
    expected = Sample(metric='usr__total cpu usage',
                      value=3.200000000000000,
                      unit='',
                      metadata={'vm_role': 'test_vm0',
                                'label1': 123, 'event': 'event',
                                'sender': 'sender'},
                      timestamp=0.0)
    self.assertEqual(
        expected.metric, self.samples[0].metric)
    self.assertEqual(
        expected.value, self.samples[0].value)
    self.assertEqual(
        expected.metadata, self.samples[0].metadata)

  def testAnalyzeValidEventEntireFile(self):
    events.AddEvent('sender', 'event', 1475708693, 1475709076,
                    {'label1': 123})
    self.collector.Analyze('testSender', None, self.samples)
    # 61 metrics
    self.assertTrue(len(self.samples), 61)
    expected = Sample(metric='usr__total cpu usage',
                      value=10.063689295039159,
                      unit='',
                      metadata={'vm_role': 'test_vm0',
                                'label1': 123, 'event': 'event',
                                'sender': 'sender'},
                      timestamp=0.0)
    self.assertEqual(
        expected.metric, self.samples[0].metric)
    self.assertEqual(
        expected.value, self.samples[0].value)
    self.assertEqual(
        expected.metadata, self.samples[0].metadata)


if __name__ == '__main__':
  unittest.main()
