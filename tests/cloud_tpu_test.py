# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for _CloudTpuSpec."""

import unittest

from perfkitbenchmarker import cloud_tpu
from perfkitbenchmarker.configs import benchmark_config_spec
from tests import mock_flags

_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'benchmark_uid'
_COMPONENT = 'test_component'
_FLAGS = None


def MergeDicts(dict1, dict2):
  result = dict1.copy()
  result.update(dict2)
  return result


class FakeCloudTpu(cloud_tpu.BaseCloudTpu):

  def _Create(self):
    pass

  def _Delete(self):
    pass

  def GetCloudTpuIp(self):
    pass


class CloudTpuSpecTestCase(unittest.TestCase):

  def setUp(self):
    self.flags = mock_flags.MockFlags()
    self.flags['run_uri'].parse('123')

    self.minimal_spec = {
        'cloud': 'GCP',
    }

    cloud_tpu._CLOUD_TPU_REGISTRY = {
        'GCP': FakeCloudTpu(None)
    }

  def tearDown(self):
    cloud_tpu._CLOUD_TPU_REGISTRY = {}

  def testMinimalConfig(self):
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **self.minimal_spec)
    self.assertEqual(result.cloud, 'GCP')

  def testDefaultCloudTpuName(self):
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **self.minimal_spec)
    self.assertEqual(result.tpu_name, 'pkb-tpu-123')

  def testCustomCloudTpuName(self):
    spec = MergeDicts(self.minimal_spec, {'tpu_name': 'pkb-tpu'})
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **spec)
    self.assertEqual(result.tpu_name, 'pkb-tpu')

  def testDefaultCloudTpuCidrRange(self):
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **self.minimal_spec)
    self.assertEqual(result.tpu_cidr_range, None)

  def testCustomCloudTpuCidrRange(self):
    spec = MergeDicts(self.minimal_spec, {'tpu_cidr_range': '192.168.0.0/29'})
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **spec)
    self.assertEqual(result.tpu_cidr_range, '192.168.0.0/29')

  def testDefaultCloudTpuAcceleratorType(self):
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **self.minimal_spec)
    self.assertEqual(result.tpu_accelerator_type, None)

  def testCustomCloudTpuAcceleratorType(self):
    spec = MergeDicts(self.minimal_spec, {'tpu_accelerator_type': 'tpu-v2'})
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **spec)
    self.assertEqual(result.tpu_accelerator_type, 'tpu-v2')

  def testDefaultCloudTpuDescription(self):
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **self.minimal_spec)
    self.assertEqual(result.tpu_description, None)

  def testCustomCloudTpuDescription(self):
    spec = MergeDicts(self.minimal_spec, {'tpu_description': 'My TF Node'})
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **spec)
    self.assertEqual(result.tpu_description, 'My TF Node')

  def testDefaultCloudTpuNetwork(self):
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **self.minimal_spec)
    self.assertEqual(result.tpu_network, None)

  def testCustomCloudTpuNetwork(self):
    spec = MergeDicts(self.minimal_spec, {'tpu_network': 'default'})
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **spec)
    self.assertEqual(result.tpu_network, 'default')

  def testDefaultCloudTpuZone(self):
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **self.minimal_spec)
    self.assertEqual(result.tpu_zone, None)

  def testCustomCloudTpuZone(self):
    spec = MergeDicts(self.minimal_spec, {'tpu_zone': 'us-central1-a'})
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **spec)
    self.assertEqual(result.tpu_zone, 'us-central1-a')

  def testDefaultCloudTpuVersion(self):
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **self.minimal_spec)
    self.assertEqual(result.tpu_tf_version, None)

  def testCustomCloudTpuVersion(self):
    spec = MergeDicts(self.minimal_spec, {'tpu_tf_version': 'nightly'})
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **spec)
    self.assertEqual(result.tpu_tf_version, 'nightly')


class CloudTpuSpecFlagsTestCase(unittest.TestCase):

  def setUp(self):
    self.flags = mock_flags.MockFlags()
    self.flags['run_uri'].parse('123')

    self.full_spec = {
        'cloud': 'GCP',
        'tpu_name': 'pkb-tpu-123',
        'tpu_cidr_range': '192.168.0.0/29',
        'tpu_accelerator_type': 'tpu-v2',
        'tpu_description': 'My TF Node',
        'tpu_network': 'default',
        'tpu_tf_version': 'nightly',
        'tpu_zone': 'us-central1-a'
    }

    cloud_tpu._CLOUD_TPU_REGISTRY = {
        'GCP': FakeCloudTpu(None)
    }

  def tearDown(self):
    cloud_tpu._CLOUD_TPU_REGISTRY = {}

  def testCloudFlag(self):
    pass

  def testCloudTpuNameFlag(self):
    self.flags['tpu_name'].parse('pkb-tpu')
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **self.full_spec)
    self.assertEqual(result.tpu_name, 'pkb-tpu')

  def testTpuCidrRangeFlag(self):
    self.flags['tpu_cidr_range'].parse('10.240.0.0/29')
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **self.full_spec)
    self.assertEqual(result.tpu_cidr_range, '10.240.0.0/29')

  def testTpuAcceleratorTypeFlag(self):
    self.flags['tpu_accelerator_type'].parse('tpu-v1')
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **self.full_spec)
    self.assertEqual(result.tpu_accelerator_type, 'tpu-v1')

  def testTpuDescriptionFlag(self):
    self.flags['tpu_description'].parse('MyTfNode')
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **self.full_spec)
    self.assertEqual(result.tpu_description, 'MyTfNode')

  def testTpuNetworkFlag(self):
    self.flags['tpu_network'].parse('my-tf-network')
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **self.full_spec)
    self.assertEqual(result.tpu_network, 'my-tf-network')

  def testTpuTfVersion(self):
    self.flags['tpu_tf_version'].parse('1.2')
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **self.full_spec)
    self.assertEqual(result.tpu_tf_version, '1.2')

  def testTpuZone(self):
    self.flags['tpu_zone'].parse('us-central1-c')
    result = benchmark_config_spec._CloudTpuSpec(
        _COMPONENT, flag_values=self.flags, **self.full_spec)
    self.assertEqual(result.tpu_zone, 'us-central1-c')


if __name__ == '__main__':
  unittest.main()
