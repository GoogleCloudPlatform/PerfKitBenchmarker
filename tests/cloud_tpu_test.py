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
"""Tests for _TpuGroupSpec."""

import unittest
from absl import flags
from perfkitbenchmarker import cloud_tpu
from perfkitbenchmarker.configs import benchmark_config_spec
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'benchmark_uid'
_COMPONENT = 'test_component'
_FLAGS = None
_GROUP_NAME = 'train'


def MergeDicts(dict1, dict2):
  result = dict1.copy()
  result.update(dict2)
  return result


class FakeTpu(cloud_tpu.BaseTpu):

  def _Create(self):
    pass

  def _Delete(self):
    pass


class TpuSpecTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS['run_uri'].parse('123')

    self.minimal_spec = {
        'cloud': 'GCP',
    }

    cloud_tpu._CLOUD_TPU_REGISTRY = {'GCP': FakeTpu(None)}

  def tearDown(self):
    super().tearDown()
    cloud_tpu._CLOUD_TPU_REGISTRY = {}

  def testMinimalConfig(self):
    result = benchmark_config_spec._TpuGroupSpec(
        _COMPONENT, _GROUP_NAME, flag_values=FLAGS, **self.minimal_spec
    )
    self.assertEqual(result.cloud, 'GCP')

  def testDefaultTpuName(self):
    result = benchmark_config_spec._TpuGroupSpec(
        _COMPONENT, _GROUP_NAME, flag_values=FLAGS, **self.minimal_spec
    )
    self.assertEqual(result.tpu_name, 'pkb-tpu-train-123')

  def testCustomTpuName(self):
    spec = MergeDicts(self.minimal_spec, {'tpu_name': 'pkb-tpu'})
    result = benchmark_config_spec._TpuGroupSpec(
        _COMPONENT, _GROUP_NAME, flag_values=FLAGS, **spec
    )
    self.assertEqual(result.tpu_name, 'pkb-tpu')

  def testDefaultTpuType(self):
    result = benchmark_config_spec._TpuGroupSpec(
        _COMPONENT, _GROUP_NAME, flag_values=FLAGS, **self.minimal_spec
    )
    self.assertIsNone(result.tpu_type)

  def testCustomTpuType(self):
    spec = MergeDicts(self.minimal_spec, {'tpu_type': 'v6e'})
    result = benchmark_config_spec._TpuGroupSpec(
        _COMPONENT, _GROUP_NAME, flag_values=FLAGS, **spec
    )
    self.assertEqual(result.tpu_type, 'v6e')

  def testDefaultTpuTopology(self):
    result = benchmark_config_spec._TpuGroupSpec(
        _COMPONENT, _GROUP_NAME, flag_values=FLAGS, **self.minimal_spec
    )
    self.assertIsNone(result.tpu_topology)

  def testCustomTpuTopology(self):
    spec = MergeDicts(self.minimal_spec, {'tpu_topology': '1x1'})
    result = benchmark_config_spec._TpuGroupSpec(
        _COMPONENT, _GROUP_NAME, flag_values=FLAGS, **spec
    )
    self.assertEqual(result.tpu_topology, '1x1')

  def testDefaultTpuZone(self):
    result = benchmark_config_spec._TpuGroupSpec(
        _COMPONENT, _GROUP_NAME, flag_values=FLAGS, **self.minimal_spec
    )
    self.assertEqual(result.tpu_zone, None)

  def testCustomTpuZone(self):
    spec = MergeDicts(self.minimal_spec, {'tpu_zone': 'us-central1-a'})
    result = benchmark_config_spec._TpuGroupSpec(
        _COMPONENT, _GROUP_NAME, flag_values=FLAGS, **spec
    )
    self.assertEqual(result.tpu_zone, 'us-central1-a')

  def testDefaultTpuVersion(self):
    result = benchmark_config_spec._TpuGroupSpec(
        _COMPONENT, _GROUP_NAME, flag_values=FLAGS, **self.minimal_spec
    )
    self.assertEqual(result.tpu_tf_version, None)

  def testCustomTpuVersion(self):
    spec = MergeDicts(self.minimal_spec, {'tpu_tf_version': 'nightly'})
    result = benchmark_config_spec._TpuGroupSpec(
        _COMPONENT, _GROUP_NAME, flag_values=FLAGS, **spec
    )
    self.assertEqual(result.tpu_tf_version, 'nightly')


class TpuSpecFlagsTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS['run_uri'].parse('123')

    self.full_spec = {
        'cloud': 'GCP',
        'tpu_name': 'pkb-tpu-123',
        'tpu_type': 'v5',
        'tpu_topology': '2x2',
        'tpu_tf_version': 'nightly',
        'tpu_zone': 'us-central1-a',
    }

    cloud_tpu._CLOUD_TPU_REGISTRY = {'GCP': FakeTpu(None)}

  def tearDown(self):
    super().tearDown()
    cloud_tpu._CLOUD_TPU_REGISTRY = {}

  def testCloudFlag(self):
    pass

  def testTpuNameFlag(self):
    FLAGS['tpu_name'].parse('pkb-tpu')
    result = benchmark_config_spec._TpuGroupSpec(
        _COMPONENT, _GROUP_NAME, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(result.tpu_name, 'pkb-tpu')

  def testTpuTypeFlag(self):
    FLAGS['tpu_type'].parse('v6e')
    result = benchmark_config_spec._TpuGroupSpec(
        _COMPONENT, _GROUP_NAME, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(result.tpu_type, 'v6e')

  def testTpuTopologyFlag(self):
    FLAGS['tpu_topology'].parse('1x1')
    result = benchmark_config_spec._TpuGroupSpec(
        _COMPONENT, _GROUP_NAME, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(result.tpu_topology, '1x1')

  def testTpuTfVersion(self):
    FLAGS['tpu_tf_version'].parse('1.2')
    result = benchmark_config_spec._TpuGroupSpec(
        _COMPONENT, _GROUP_NAME, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(result.tpu_tf_version, '1.2')

  def testTpuZone(self):
    FLAGS['tpu_zone'].parse('us-central1-c')
    result = benchmark_config_spec._TpuGroupSpec(
        _COMPONENT, _GROUP_NAME, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(result.tpu_zone, 'us-central1-c')


if __name__ == '__main__':
  unittest.main()
