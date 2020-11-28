# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.tests.providers.ibmcloud.ibmcloud_disk."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
from absl import flags
import mock

from perfkitbenchmarker.providers.ibmcloud import ibmcloud_network
from perfkitbenchmarker.providers.ibmcloud import ibm_api as ibm
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class IbmcloudNetworkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(IbmcloudNetworkTest, self).setUp()
    # Patch the __init__ method for simplicity.
    with mock.patch.object(ibmcloud_network.IbmCloudNetwork, '__init__', lambda self: None):
      self.network = ibmcloud_network.IbmCloudNetwork()

  def get_vpc(self):
    return 'vpc-id'

  def get_subnet(self):
    return 'subnet-id'

  def testCreateVpc(self):
    self.network = mock.Mock()
    self.network._Create.side_effect = self.get_vpc
    self.assertEqual('vpc-id', self.network._Create())

  def testCreateSubnet(self):
    self.network = mock.Mock()
    self.network.CreateSubnet.side_effect = self.get_subnet
    self.assertEqual('subnet-id', self.network.CreateSubnet())


if __name__ == '__main__':
  unittest.main()
