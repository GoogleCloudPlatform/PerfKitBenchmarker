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
import xml.etree.ElementTree as ET

from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker import test_util
from tests import pkb_common_test_case
from perfkitbenchmarker.windows_packages import diskspd

FLAGS = flags.FLAGS


class DiskspdBenchmarkTestCase(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):

  def getDataContents(self, file_name):
    path = os.path.join(os.path.dirname(__file__), '..', 'data', file_name)
    with open(path) as fp:
      contents = fp.read()
    return contents

  def setUp(self):
    super(DiskspdBenchmarkTestCase, self).setUp()
    self.result_xml = self.getDataContents('diskspd_result.xml')

  def testDiskSpdParsing(self):
    samples = diskspd.ParseDiskSpdResults(self.result_xml, {})
    metric_names = [
        'read_bandwidth',
        'read_iops',
        'total_bandwidth',
        'total_iops',
        'cpu_total_utilization',
    ]
    cpu_utilization_sample = list(
        filter(lambda x: x.metric == 'cpu_total_utilization', samples)
    )
    per_cpu_usage = list(cpu_utilization_sample)[0].metadata['per_cpu_usage']
    per_cpu_usage_expected = {
        'usage_cpu_0_0_0_0_0': {
            'cpu_total_utilization': 2.08,
            'cpu_user_percent': 0.21,
            'cpu_kernel_percent': 1.87,
            'cpu_idle_percent': 97.92,
        },
        'usage_cpu_0_0_0_0_1': {
            'cpu_total_utilization': 2.03,
            'cpu_user_percent': 0.21,
            'cpu_kernel_percent': 1.82,
            'cpu_idle_percent': 97.97,
        },
        'usage_cpu_0_0_0_2_2': {
            'cpu_total_utilization': 1.67,
            'cpu_user_percent': 0.16,
            'cpu_kernel_percent': 1.51,
            'cpu_idle_percent': 98.33,
        },
        'usage_cpu_0_0_0_3_3': {
            'cpu_total_utilization': 7.03,
            'cpu_user_percent': 0.42,
            'cpu_kernel_percent': 6.61,
            'cpu_idle_percent': 92.97,
        },
    }
    self.assertEqual(per_cpu_usage, per_cpu_usage_expected)
    self.assertCountEqual(metric_names, [s.metric for s in samples])

  def testConvertTotalBytesToMBPerSecond(self):
    self.assertAlmostEqual(
        diskspd.ConvertTotalBytesToMBPerSecond(1024 * 1024 * 10, 10), 1.0
    )

  @flagsaver.flagsaver(diskspd_file_size='10G')
  def testGetDiskspdFileSizeInPrefillSizeUnit_GB(self):
    size, unit = diskspd.GetDiskspdFileSizeInPrefillSizeUnit(1024, 10)
    self.assertEqual(unit, 'G')
    self.assertEqual(size, 10.0)

  @flagsaver.flagsaver(diskspd_file_size='10M')
  def testGetDiskspdFileSizeInPrefillSizeUnit_MB(self):
    size, unit = diskspd.GetDiskspdFileSizeInPrefillSizeUnit(1, 10)
    self.assertEqual(unit, 'M')
    self.assertEqual(size, 10.0)

  @flagsaver.flagsaver(diskspd_file_size='10K')
  def testGetDiskspdFileSizeInPrefillSizeUnit_KB(self):
    size, unit = diskspd.GetDiskspdFileSizeInPrefillSizeUnit(1 / 1024, 10)
    self.assertEqual(unit, 'K')
    self.assertEqual(size, 10.0)

  def testFormatLatencyMetricName(self):
    self.assertEqual(
        diskspd.FormatLatencyMetricName('ReadMilliseconds'), 'read_latency'
    )
    self.assertEqual(diskspd.FormatLatencyMetricName('Total'), 'total_latency')

  def testParseLatencyBucket(self):
    bucket_xml = """
    <Bucket>
      <Percentile>99.9</Percentile>
      <ReadMilliseconds>1.2</ReadMilliseconds>
      <WriteMilliseconds>3.4</WriteMilliseconds>
      <TotalMilliseconds>5.6</TotalMilliseconds>
    </Bucket>
    """
    element = ET.fromstring(bucket_xml)
    samples = diskspd.ParseLatencyBucket(element)
    self.assertLen(samples, 3)
    self.assertEqual(samples[0].metric, 'total_latency_p99.9')
    self.assertEqual(samples[0].value, 5.6)
    self.assertEqual(samples[1].metric, 'read_latency_p99.9')
    self.assertEqual(samples[1].value, 1.2)
    self.assertEqual(samples[2].metric, 'write_latency_p99.9')
    self.assertEqual(samples[2].value, 3.4)

  def testParseLatencyBucketInvalidPercentile(self):
    bucket_xml = """
    <Bucket>
      <Percentile>99.1</Percentile>
      <TotalMilliseconds>1.23</TotalMilliseconds>
    </Bucket>
    """
    element = ET.fromstring(bucket_xml)
    samples = diskspd.ParseLatencyBucket(element)
    self.assertEmpty(samples)

  @flagsaver.flagsaver(
      diskspd_file_size='10G',
      diskspd_duration=60,
      diskspd_warmup=10,
      diskspd_cooldown=5,
      diskspd_write_read_ratio=70,
      diskspd_block_size='16K',
      diskspd_access_pattern='r',
      diskspd_stride=None)
  def testGenerateDiskspdConfig(self):
    config = diskspd._GenerateDiskspdConfig(outstanding_io=8, threads=4)
    expected_config = (
        '-c10G -d60 -t4 -o8 -L -W10 -C5 -Rxml -w70   -Su -Sw  -b16K -r -fr'
        f' F:\\{diskspd.DISKSPD_TMPFILE} > {diskspd.DISKSPD_XMLFILE}'
    )
    self.assertMultiLineEqual(config, expected_config)

  @flagsaver.flagsaver(
      diskspd_file_size='1G',
      diskspd_duration=30,
      diskspd_write_read_ratio=100,
      diskspd_block_size='4K',
      diskspd_access_pattern='s',
      diskspd_stride='64K')
  def testGenerateDiskspdConfigWithStride(self):
    config = diskspd._GenerateDiskspdConfig(outstanding_io=1, threads=1)
    self.assertIn('-s64K', config)
    self.assertIn('-fs', config)

  @flagsaver.flagsaver(
      diskspd_latency_stats=False,
      diskspd_software_cache=False,
      diskspd_write_through=False,
      diskspd_large_page=True,
      diskspd_disable_affinity=True)
  def testGenerateDiskspdConfigBooleans(self):
    config = diskspd._GenerateDiskspdConfig(outstanding_io=1, threads=1)
    self.assertNotIn('-L', config)
    self.assertNotIn('-Su', config)
    self.assertNotIn('-Sw', config)
    self.assertIn('-l', config)
    self.assertIn('-n', config)


if __name__ == '__main__':
  unittest.main()
