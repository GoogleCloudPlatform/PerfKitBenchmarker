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

"""Tests for diskspd_benchmark."""

import os
import unittest

from perfkitbenchmarker import test_util
from perfkitbenchmarker.windows_packages import diskspd


class DiskspdBenchmarkTestCase(unittest.TestCase, test_util.SamplesTestMixin):

  def getDataContents(self, file_name):
    path = os.path.join(os.path.dirname(__file__), '..', 'data', file_name)
    with open(path) as fp:
      contents = fp.read()
    return contents

  def setUp(self):
    self.result_xml = self.getDataContents('diskspd_result.xml')

  def testDiskSpdParsing(self):
    samples = diskspd.ParseDiskSpdResults(self.result_xml, {})
    metric_names = [
        'read_speed',
        'read_iops',
        'total_speed',
        'total_iops',
        'cpu_total_utilization',
    ]
    cpu_utilization_sample = list(
        filter(lambda x: x.metric == 'cpu_total_utilization', samples)
    )
    per_cpu_usage = list(cpu_utilization_sample)[0].metadata['per_cpu_usage']
    per_cpu_usage_expected = {
        'usage_cpu_0': {
            'cpu_total_utilization': 2.08,
            'cpu_user_percent': 0.21,
            'cpu_kernel_percent': 1.87,
            'cpu_idle_percent': 97.92,
        },
        'usage_cpu_1': {
            'cpu_total_utilization': 2.03,
            'cpu_user_percent': 0.21,
            'cpu_kernel_percent': 1.82,
            'cpu_idle_percent': 97.97,
        },
        'usage_cpu_2': {
            'cpu_total_utilization': 1.67,
            'cpu_user_percent': 0.16,
            'cpu_kernel_percent': 1.51,
            'cpu_idle_percent': 98.33,
        },
        'usage_cpu_3': {
            'cpu_total_utilization': 7.03,
            'cpu_user_percent': 0.42,
            'cpu_kernel_percent': 6.61,
            'cpu_idle_percent': 92.97,
        },
    }
    self.assertEqual(per_cpu_usage, per_cpu_usage_expected)
    self.assertCountEqual(
        metric_names, map(lambda x: x.metric, samples)
    )


if __name__ == '__main__':
  unittest.main()
