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
import json
import logging

from typing import List

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

# these are used to create extra subnets for multi vnic support
SUBNET_SUFFIX_EXTRA = 'sxs'
SUBNETX1 = SUBNET_SUFFIX_EXTRA + '1'
SUBNETX2 = SUBNET_SUFFIX_EXTRA + '2'
SUBNETX3 = SUBNET_SUFFIX_EXTRA + '3'
SUBNETX4 = SUBNET_SUFFIX_EXTRA + '4'
SUBNETXS = [SUBNETX1, SUBNETX2, SUBNETX3, SUBNETX4]

SUBNETS_EXTRA = {
    SUBNETX1: ['10.101.20.0/24', '10.102.20.0/24', '10.103.20.0/24', '10.104.20.0/24', '10.105.20.0/24'],
    SUBNETX2: ['10.101.30.0/24', '10.102.30.0/24', '10.103.30.0/24', '10.104.30.0/24', '10.105.30.0/24'],
    SUBNETX3: ['10.101.40.0/24', '10.102.40.0/24', '10.103.40.0/24', '10.104.40.0/24', '10.105.40.0/24'],
    SUBNETX4: ['10.101.50.0/24', '10.102.50.0/24', '10.103.50.0/24', '10.104.50.0/24', '10.105.50.0/24']
}

SUBNETS_EXTRA_GATEWAY = {
    SUBNETX1: ['10.101.20.1', '10.102.20.1', '10.103.20.1', '10.104.20.1', '10.105.20.1'],
    SUBNETX2: ['10.101.30.1', '10.102.30.1', '10.103.30.1', '10.104.30.1', '10.105.30.1'],
    SUBNETX3: ['10.101.40.1', '10.102.40.1', '10.103.40.1', '10.104.40.1', '10.105.40.1'],
    SUBNETX4: ['10.101.50.1', '10.102.50.1', '10.103.50.1', '10.104.50.1', '10.105.50.1']
}

_DEFAULT_TIMEOUT = 300


def GetSubnetIndex(ipv4_cidr_block: str) -> int:
  """Finds the index for the given cidr

  Args:
    ipv4_cidr_block: cidr to find.

  Returns:
    The index number of the found cidr as in the predefined list.
    -1 is returned if the cidr is not known and not found 
    in the predefined list of subnets
  """
  for name in SUBNETXS:
    ip_list = SUBNETS_EXTRA[name]
    for i in range(len(ip_list)):
      if ip_list[i] == ipv4_cidr_block:
        return i
  return -1


def GetRouteCommands(data: str, index: int, target_index: int) -> List[str]:
  """Creates a list of ip route commands in text format to run on vm,
    not used on normal perfkit runs.

  Args:
    data: output from route command on vm.
    index: subnet index.
    target_index: target subnet index.

  Returns:
    The index number of the found cidr as in the predefined list
  """
  route_cmds = []
  if data:
    for subnet_name in SUBNETXS:
      subnet_cidr_block = SUBNETS_EXTRA[subnet_name][index]
      target_cidr_block = SUBNETS_EXTRA[subnet_name][target_index]
      subnet_gateway = SUBNETS_EXTRA_GATEWAY[subnet_name][index]
      route_entry = subnet_cidr_block.split('/24')
      interface = None
      for line in data.splitlines():
        items = line.split()
        if len(items) > 6 and items[0] == route_entry[0]:
          interface = items[7]
          route_cmds.append('ip route add ' + target_cidr_block + ' via ' + \
                            subnet_gateway + ' dev ' + interface)
  return route_cmds


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

  def CreateFip(self, name: str, vmid: str):
    """Creates a VNIC in a IBM Cloud VM instance."""
    cmd = ibm.IbmAPICommand(self)
    cmd.flags['instanceid'] = vmid
    vnicid = cmd.InstanceGetPrimaryVnic()
    cmd.flags['name'] = name
    cmd.flags['target'] = vnicid
    logging.info('Creating FIP for instanceid: %s', vmid)
    resp = json.loads(cmd.InstanceFipCreate())
    if resp:
      logging.info('FIP create resp: %s', resp)
      assert resp['address'] != None
      return resp['address'], resp['id']
    logging.error('FAILED to create FIP for instance %s', vmid)
    return None, None

  def DeleteFip(self, vmid: str, fip_address: str, fipid: str):
    """Deletes fip in a IBM Cloud VM instance."""
    logging.info('Deleting FIP, instanceid: %s, fip address: %s, fip id: %s',
                 vmid, fip_address, fipid)
    cmd = ibm.IbmAPICommand(self)
    cmd.flags['fipid'] = fipid
    cmd.InstanceFipDelete()

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
        logging.info('Waiting to delete subnet: %s', self.subnet)
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
