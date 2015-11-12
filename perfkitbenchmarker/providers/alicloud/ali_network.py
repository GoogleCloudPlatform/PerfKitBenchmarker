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
"""Module containing classes related to AliCloud VM networking.

The Firewall class provides a way of opening VM ports. The Network class allows
VMs to communicate via internal ips and isolates PerfKitBenchmarker VMs from
others in the
same project. See https://developers.google.com/compute/docs/networking for
more information about AliCloud VM networking.
"""
import threading
import json
import uuid

from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.alicloud import util
from perfkitbenchmarker import resource

FLAGS = flags.FLAGS
MAX_NAME_LENGTH = 128


class AliSecurityGroup(resource.BaseResource):
  """Object representing an Azure Affinity Group."""

  def __init__(self, name, region):
    super(AliSecurityGroup, self).__init__()
    self.name = name
    self.region = region

  def _Create(self):
    """Creates the affinity group."""
    create_cmd = util.ALI_PREFIX + [
        'ecs',
        'CreateSecurityGroup',
        '--SecurityGroupName %s' % self.name,
        '--RegionId %s' % self.region]
    create_cmd = util.GetEncodedCmd(create_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(create_cmd)
    self.group_id = json.loads(stdout)['SecurityGroupId']

    auth_sg_cmd = util.ALI_PREFIX + [
        'ecs',
        'AuthorizeSecurityGroup',
        '--SecurityGroupId %s' % self.group_id,
        '--RegionId %s' % self.region,
        '--IpProtocol tcp',
        '--PortRange 22/22',
        '--SourceCidrIp 0.0.0.0/0']
    auth_sg_cmd = util.GetEncodedCmd(auth_sg_cmd)
    vm_util.IssueRetryableCommand(auth_sg_cmd)

  def _Delete(self):
    """Deletes the affinity group."""
    delete_cmd = util.ALI_PREFIX + [
        'ecs',
        'DeleteSecurityGroup',
        '--RegionId %s' % self.region,
        '--SecurityGroupId %s' % self.group_id]
    delete_cmd = util.GetEncodedCmd(delete_cmd)
    vm_util.IssueRetryableCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the affinity group exists."""
    show_cmd = util.ALI_PREFIX + [
        'ecs',
        'DescribeSecurityGroupAttribute',
        '--RegionId %s' % self.region,
        '--SecurityGroupId %s' % self.group_id]
    show_cmd = util.GetEncodedCmd(show_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(show_cmd)
    return 'SecurityGroupId' in json.loads(stdout)

  def AllowPort(self, port):
    for proto in ('tcp', 'udp'):
      allow_cmd = util.ALI_PREFIX + [
          'ecs',
          'AuthorizeSecurityGroup',
          '--IpProtocol %s' % proto,
          '--PortRange %s/%s' % (port, port),
          '--SourceCidrIp 0.0.0.0/0',
          '--RegionId %s' % self.region,
          '--SecurityGroupId %s' % self.group_id]
      allow_cmd = util.GetEncodedCmd(allow_cmd)
      vm_util.IssueRetryableCommand(allow_cmd)


class AliFirewall(network.BaseFirewall):
  """An object representing the AliCloud Firewall."""

  def __init__(self):
    """Initialize AliCloud firewall class.

    Args:
      project: The Ali project name under which firewall is created.
    """
    self._lock = threading.Lock()
    self.firewall_names = []

  def __getstate__(self):
    """Implements getstate to allow pickling (since locks can't be pickled)."""
    d = self.__dict__.copy()
    del d['_lock']
    return d

  def __setstate__(self, state):
    """Restores the lock after the object is unpickled."""
    self.__dict__ = state
    self._lock = threading.Lock()

  def AllowPort(self, vm, port):
    """Opens a port on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the port for.
      port: The local port to open.
    """
    vm.network.security_group.AllowPort(port)

  def DisallowAllPorts(self):
    """Closes all ports on the firewall."""
    pass


class AliNetwork(network.BaseNetwork):
  """Object representing a AliCloud Network."""

  def __init__(self, zone):
    super(AliNetwork, self).__init__(zone)
    name = ('perfkit%s%s' %
            (FLAGS.run_uri, str(uuid.uuid4())[-12:])).lower()[:MAX_NAME_LENGTH]
    region = util.GetRegionByZone(zone)
    self.security_group = AliSecurityGroup(name, region)

  @vm_util.Retry()
  def Create(self):
    """Creates the actual network."""
    self.security_group.Create()

  def Delete(self):
    """Deletes the actual network."""
    self.security_group.Delete()
