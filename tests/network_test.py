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
"""Tests for perfkitbenchmarker.network."""

import unittest
from absl import flags

from perfkitbenchmarker import network
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class TestNetwork(network.BaseNetwork):
  CLOUD = 'test_cloud'


class BaseNetworkTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testGetKeyFromNetworkSpec(self):
    spec = network.BaseNetworkSpec(
        zone='us-central1-a', cidr=None, machine_type=None)
    spec.subnet_name = 'test'
    actual_key = TestNetwork._GetKeyFromNetworkSpec(spec)
    expected_key = ('test_cloud', 'us-central1-a', 'test')
    self.assertEqual(actual_key, expected_key)


if __name__ == '__main__':
  unittest.main()
