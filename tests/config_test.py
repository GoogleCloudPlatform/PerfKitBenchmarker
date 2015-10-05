# Copyright 2015 Google Inc. All rights reserved.
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

from perfkitbenchmarker import benchmarks
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
      vm_spec: *default_single_core
"""


class ConfigsTestCase(unittest.TestCase):

  def testLoadAllDefaultConfigs(self):
    all_benchmarks = benchmarks.BENCHMARKS + windows_benchmarks.BENCHMARKS
    for benchmark_module in all_benchmarks:
      self.assertIsInstance(benchmark_module.GetConfig(), dict)

  def testLoadValidConfig(self):
    self.assertIsInstance(configs.LoadConfig(VALID_CONFIG, CONFIG_NAME), dict)

  def testWrongName(self):
    with self.assertRaises(KeyError):
      configs.LoadConfig(VALID_CONFIG, INVALID_NAME)

  def testLoadInvalidYaml(self):
    with self.assertRaises(errors.Config.ParseError):
      configs.LoadConfig(INVALID_YAML_CONFIG, CONFIG_NAME)
