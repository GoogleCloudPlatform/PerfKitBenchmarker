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


import unittest

from absl import flags
import mock
from perfkitbenchmarker.providers.ibmcloud import ibmcloud_network
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

VPC_ID = 'vcpid'
SUBNET_ID = 'subnetid'
FIP_ID = 'fipid'


class IbmcloudNetworkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(IbmcloudNetworkTest, self).setUp()
    # Patch the __init__ method for simplicity.
    with mock.patch.object(ibmcloud_network.IbmCloudNetwork, '__init__',
                           lambda self: None):
      self.network = ibmcloud_network.IbmCloudNetwork()

  def get_vpc(self):
    return VPC_ID

  def get_subnet(self):
    return SUBNET_ID

  def get_fip(self):
    return FIP_ID

  def testGetSubnetIndex(self):
    subnetx = '10.103.20.0/24'
    expected_subet_index = 2
    self.assertEqual(expected_subet_index,
                     ibmcloud_network.GetSubnetIndex(subnetx))

  def testCreateVpc(self):
    self.network = mock.Mock()
    self.network._Create.side_effect = self.get_vpc
    self.assertEqual(VPC_ID, self.network._Create())

  def testCreateSubnet(self):
    self.network = mock.Mock()
    self.network.CreateSubnet.side_effect = self.get_subnet
    self.assertEqual(SUBNET_ID, self.network.CreateSubnet())

  def testCreateFip(self):
    self.network = mock.Mock()
    self.network.CreateFip.side_effect = self.get_fip
    self.assertEqual(FIP_ID, self.network.CreateFip())


if __name__ == '__main__':
  unittest.main()
