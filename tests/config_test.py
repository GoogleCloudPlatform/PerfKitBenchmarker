# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.configs."""

import unittest
import yaml

import mock

from perfkitbenchmarker import linux_benchmarks
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import windows_benchmarks

CONFIG_NAME = 'a'
INVALID_NAME = 'b'
INVALID_YAML_CONFIG = """
a:
  vm_groups:
    default:
    :
"""
VALID_CONFIG = """
a:
  vm_groups:
    default:
      vm_spec: null
"""
CONFIG_A = """
a:
  flags:
    flag1: old_value
    flag2: not_overwritten
  vm_groups: {}
"""
CONFIG_B = """
a:
  flags:
    flag1: new_value
    flag3: new_flag

"""
REF_CONFIG = """
a:
  vm_groups:
    default:
      vm_spec: *default_single_core
"""
BAD_REF_CONFIG = """
a:
  vm_groups:
    default:
      vm_spec: *anchor_does_not_exist
"""


class ConfigsTestCase(unittest.TestCase):

  def testLoadAllDefaultConfigs(self):
    all_benchmarks = (linux_benchmarks.BENCHMARKS +
                      windows_benchmarks.BENCHMARKS)
    for benchmark_module in all_benchmarks:
      self.assertIsInstance(benchmark_module.GetConfig({}), dict)

  def testLoadValidConfig(self):
    self.assertIsInstance(
        configs.LoadMinimalConfig(VALID_CONFIG, CONFIG_NAME), dict)

  def testWrongName(self):
    with self.assertRaises(KeyError):
      configs.LoadMinimalConfig(VALID_CONFIG, INVALID_NAME)

  def testLoadInvalidYaml(self):
    with self.assertRaises(errors.Config.ParseError):
      configs.LoadMinimalConfig(INVALID_YAML_CONFIG, CONFIG_NAME)

  def testMergeBasicConfigs(self):
    old_config = yaml.load(CONFIG_A)
    new_config = yaml.load(CONFIG_B)
    config = configs.MergeConfigs(old_config, new_config)
    # Key is present in both configs.
    self.assertEqual(config['a']['flags']['flag1'], 'new_value')
    # Key is only present in default config.
    self.assertEqual(config['a']['flags']['flag2'], 'not_overwritten')
    # Key is only present in the override config.
    self.assertEqual(config['a']['flags']['flag3'], 'new_flag')

  def testLoadConfigDoesMerge(self):
    default = yaml.load(CONFIG_A)
    overrides = yaml.load(CONFIG_B)
    merged_config = configs.MergeConfigs(default, overrides)
    config = configs.LoadConfig(CONFIG_A, overrides['a'], CONFIG_NAME)
    self.assertEqual(merged_config['a'], config)

  def testMergeConfigWithNoOverrides(self):
    old_config = yaml.load(CONFIG_A)
    config = configs.MergeConfigs(old_config, None)
    self.assertEqual(config, old_config)

  def testLoadConfigWithExternalReference(self):
    self.assertIsInstance(
        configs.LoadMinimalConfig(REF_CONFIG, CONFIG_NAME), dict)

  def testLoadConfigWithBadReference(self):
    with self.assertRaises(errors.Config.ParseError):
      configs.LoadMinimalConfig(BAD_REF_CONFIG, CONFIG_NAME)


  def testConfigOverrideFlag(self):
    p = mock.patch(configs.__name__ + '.FLAGS')
    self.addCleanup(p.stop)
    mock_flags = p.start()
    config_override = [
        'a.vm_groups.default.vm_count=5',
        'a.flags.flag=value']
    mock_flags.configure_mock(config_override=config_override,
                              benchmark_config_file=None)
    config = configs.GetUserConfig()
    self.assertEqual(config['a']['vm_groups']['default']['vm_count'], 5)
    self.assertEqual(config['a']['flags']['flag'], 'value')

  def testConfigImport(self):
    p = mock.patch(configs.__name__ + '.FLAGS')
    self.addCleanup(p.stop)
    mock_flags = p.start()
    mock_flags.configure_mock(benchmark_config_file="test_import.yml")
    config = configs.GetUserConfig()
    self.assertEqual(config['flags']['num_vms'], 3)
