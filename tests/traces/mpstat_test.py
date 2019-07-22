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
"""Tests for mpstat utility."""
import os
import unittest


from perfkitbenchmarker.traces import mpstat


class MpstatTestCase(unittest.TestCase):

  def setUp(self):
    super(MpstatTestCase, self).setUp()
    path = os.path.join(
        os.path.dirname(__file__), '../data', 'mpstat_output.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def testMpstatParse(self):
    metadata = {
        'event': 'mpstat',
        'sender': 'run',
    }

    samples = mpstat._MpstatResults(metadata, self.contents)

    for sample in samples:
      if sample.metric == 'mpstat_avg_intr':
        self.assertEqual(4452.74, sample.value)
        self.assertEqual('mpstat', sample.metadata['event'])
        self.assertEqual('run', sample.metadata['sender'])
      elif sample.metric == 'mpstat_avg_irq':
        self.assertEqual(0.0, sample.value)
        self.assertEqual('mpstat', sample.metadata['event'])
        self.assertEqual('run', sample.metadata['sender'])
      elif sample.metric == 'mpstat_avg_soft':
        self.assertEqual(1.20, sample.value)
        self.assertEqual('mpstat', sample.metadata['event'])
        self.assertEqual('run', sample.metadata['sender'])
      # spot test a couple of per cpu samples
      elif (sample.metric == 'mpstat_intr' and
            sample.metadata['mpstat_cpu_id'] == 2):
        self.assertEqual(248.26, sample.value)
      elif (sample.metric == 'mpstat_soft' and
            sample.metadata['mpstat_cpu_id'] == 0):
        self.assertEqual(11.21, sample.value)

if __name__ == '__main__':
  unittest.main()
