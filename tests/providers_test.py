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

"""Tests for perfkitbenchmarker.providers"""

import unittest

import mock

from perfkitbenchmarker import providers
from perfkitbenchmarker import benchmark_spec


class LoadProvidersTestCase(unittest.TestCase):

  def testLoadAllProviders(self):
    for cloud in providers.VALID_CLOUDS:
      providers.LoadProvider(cloud.lower())

  def testLoadInvalidProvider(self):
    with self.assertRaises(ImportError):
      providers.LoadProvider('InvalidCloud')

  def testBenchmarkSpecLoadsProvider(self):
    p = mock.patch(providers.__name__ + '.LoadProvider')
    p.start()
    self.addCleanup(p.stop)
    config = {
        'vm_groups': {
            'group1': {
                'cloud': 'AWS',
                'vm_count': 0,
                'vm_spec': {'AWS': {}}
            }
        }
    }
    spec = benchmark_spec.BenchmarkSpec(config, 'name', 'uid')
    spec.ConstructVirtualMachines()
    providers.LoadProvider.assert_called_with('aws')
