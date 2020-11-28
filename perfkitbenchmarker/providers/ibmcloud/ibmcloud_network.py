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
""""Module containing classes related to IBM Cloud Networking."""

import time
import threading
import logging

from absl import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import errors

from perfkitbenchmarker.providers.ibmcloud import util
from perfkitbenchmarker.providers.ibmcloud import ibm_api as ibm
from perfkitbenchmarker.providers.ibmcloud.ibmcloud_virtual_machine \
import IbmCloudVirtualMachine

FLAGS = flags.FLAGS

VPC_NAME = 'vpc'
VPC_PREFIX_RANGES = ['10.101.0.0/16', '10.102.0.0/16', '10.103.0.0/16',
                     '10.104.0.0/16', '10.105.0.0/16']
VPC_SUBNETS = ['10.101.0.0/20', '10.102.0.0/20', '10.103.0.0/20',
               '10.104.0.0/20', '10.105.0.0/20']

_DEFAULT_TIMEOUT = 300


class IbmCloudNetwork(resource.BaseResource):
  """Object holding the information needed to create an IbmCloudNetwork.

  Attributes:
    prefix: prefix string.
    zone: zone name.
    vpcid: vpc id.
  """
  _lock = threading.Lock()
  _lock_vpc = threading.Lock()

  def __init__(self, prefix, zone, vpcid=None):
    super(IbmCloudNetwork, self).__init__()
    self.prefix = prefix
    self.vpcid = vpcid
    self.subnet = None
    self.zone = zone
    self.name = prefix + FLAGS.run_uri + 'vpc'

  def _Create(self):
    """Creates a IBM Cloud VPC with address prefixes for all zones."""
    cmd = ibm.IbmAPICommand(self)
    cmd.flags['name'] = self.name
    logging.info('Creating vpc: %s', cmd.flags)
    self.vpcid = cmd.CreateVpc()
    if self.vpcid:
      for zone in FLAGS.zones:
        cmd.flags.update({
          'vpcid': self.vpcid,
          'zone': zone
          })
        index = int(zone[len(zone) - 1])  # get the ending -1
        cmd.flags['cidr'] = VPC_PREFIX_RANGES[index - 1]
        cmd.flags['name'] = self.prefix + VPC_NAME + util.DELIMITER + zone
        resp = cmd.CreatePrefix()
        if resp:
          logging.info('Created vpc prefix range: %s', resp.get('id'))
        else:
          raise errors.Error('IBM Cloud ERROR: Failed to create '
                             'vpc address prefix')
    else:
      raise errors.Error('IBM Cloud ERROR: Failed to create vpc')

  def CreateSubnet(self, vpcid):
    """Creates a IBM Cloud subnet on the given vpc."""
    self.vpcid = vpcid
    subnet_index = int(self.zone[len(self.zone) - 1])  # get the ending -1
    cmd = ibm.IbmAPICommand(self)
    cmd.flags.update({
      'vpcid': self.vpcid,
      'zone': self.zone,
      'name': self.prefix + VPC_NAME + util.SUBNET_SUFFIX + \
      str(subnet_index) + util.DELIMITER + self.zone,
      'cidr': VPC_SUBNETS[subnet_index - 1]
      })
    logging.info('Creating subnet: %s', cmd.flags)
    resp = cmd.CreateSubnet()
    if resp:
      self.subnet = resp.get('id')
      IbmCloudVirtualMachine.validated_subnets += 1
      logging.info('Created subnet: %s, zone %s', self.subnet, self.zone)
    else:
      raise errors.Error('IBM Cloud ERROR: Failed to create subnet')

  def _Exists(self):
    """Returns true if the VPC exists."""
    cmd = ibm.IbmAPICommand(self)
    cmd.flags.update({
      'prefix': self.prefix,
      'zone': self.zone,
      'items': 'vpcs'
      })
    self.vpcid = cmd.ListResources()
    if self.vpcid:
      return True
    return False

  def _Delete(self):
    """Deletes the vpc and subnets."""
    with self._lock:
      if IbmCloudVirtualMachine.validated_subnets > 0:
        cmd = ibm.IbmAPICommand(self)
        cmd.flags['items'] = 'subnets'
        cmd.flags['id'] = self.subnet
        logging.info('Waiting to delete subnet')
        time.sleep(30)
        cmd.DeleteResource()
        IbmCloudVirtualMachine.validated_subnets -= 1

    # different lock so all threads get a chance to delete its subnet first
    with self._lock_vpc:
      if self.vpcid not in IbmCloudVirtualMachine.validated_resources_set:
        cmd = ibm.IbmAPICommand(self)
        # check till all subnets are gone
        time_to_end = time.time() + _DEFAULT_TIMEOUT
        while IbmCloudVirtualMachine.validated_subnets > 0:
          logging.info('Subnets not empty yet')
          if time_to_end < time.time():
            break
          time.sleep(10)
        cmd.flags['items'] = 'vpcs'
        cmd.flags['id'] = self.vpcid
        logging.info('Waiting to delete vpc')
        time.sleep(10)
        IbmCloudVirtualMachine.validated_resources_set.add(self.vpcid)
        cmd.DeleteResource()
        self.vpcid = None
