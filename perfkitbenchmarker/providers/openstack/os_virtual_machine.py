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

"""Class to represent an OpenStack Virtual Machine.

Regions:
  User defined
Machine types, or flavors:
  run 'openstack flavor list'
Images:
  run 'openstack image list'
"""

import json
import logging
import threading

from perfkitbenchmarker import virtual_machine, linux_virtual_machine
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import providers
from perfkitbenchmarker.providers.openstack import os_disk
from perfkitbenchmarker.providers.openstack import os_network
from perfkitbenchmarker.providers.openstack import utils as os_utils

RHEL_IMAGE = 'rhel-7.2'
UBUNTU_IMAGE = 'ubuntu-14.04'
NONE = 'None'

VALIDATION_ERROR_MESSAGE = '{0} {1} could not be found.'

FLAGS = flags.FLAGS


class OpenStackVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing an OpenStack Virtual Machine"""

  CLOUD = providers.OPENSTACK
  DEFAULT_IMAGE = None

  _lock = threading.Lock()  # _lock guards the following:
  command_works = False
  validated_resources_set = set()
  uploaded_keypair_set = set()
  deleted_keypair_set = set()
  created_server_group_dict = {}
  deleted_server_group_set = set()
  floating_network_id = None

  def __init__(self, vm_spec):
    """Initialize an OpenStack virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(OpenStackVirtualMachine, self).__init__(vm_spec)
    self.key_name = 'perfkit_key_%s' % FLAGS.run_uri
    self.user_name = FLAGS.openstack_image_username
    self.image = self.image or self.DEFAULT_IMAGE
    # FIXME(meteorfox): Remove --openstack_public_network and
    # --openstack_private_network once depreciation time has expired
    self.network_name = (FLAGS.openstack_network or
                         FLAGS.openstack_private_network)
    self.floating_ip_pool_name = (FLAGS.openstack_floating_ip_pool or
                                  FLAGS.openstack_public_network)
    self.id = None
    self.boot_volume_id = None
    self.server_group_id = None
    self.floating_ip = None
    self.firewall = None
    self.public_network = None
    self.subnet_id = None

  @property
  def group_id(self):
    """Returns the security group ID of this VM."""
    return 'perfkit_sc_group'

  def _CreateDependencies(self):
    """Validate and Create dependencies prior creating the VM."""
    self._CheckPrerequisites()
    self.firewall = os_network.OpenStackFirewall.GetFirewall()
    self.public_network = os_network.OpenStackFloatingIPPool(
        OpenStackVirtualMachine.floating_network_id)
    self._UploadSSHPublicKey()
    source_range = self._GetInternalNetworkCIDR()
    self.firewall.AllowPort(self, os_network.MIN_PORT, os_network.MAX_PORT,
                            source_range)
    self.firewall.AllowICMP(self)  # Allowing ICMP traffic (i.e. ping)
    self.AllowRemoteAccessPorts()

  def _Create(self):
    """Creates an OpenStack VM instance and waits until it is ACTIVE."""
    if FLAGS.openstack_boot_from_volume:
      vol_name = '%s_volume' % self.name
      disk_resp = os_disk.CreateBootVolume(self, vol_name, self.image)
      self.boot_volume_id = disk_resp['id']
      os_disk.WaitForVolumeCreation(self, self.boot_volume_id)
    self._CreateInstance()

  @vm_util.Retry(max_retries=4, poll_interval=2)
  def _PostCreate(self):
    self._SetIPAddresses()

  def _Delete(self):
    if self.id is None:
      return
    self._DeleteInstance()
    if self.floating_ip:
      self.public_network.release(self, self.floating_ip)
    if self.server_group_id:
      self._DeleteServerGroup()
    if self.boot_volume_id:
      os_disk.DeleteVolume(self, self.boot_volume_id)
      self.boot_volume_id = None

  def _DeleteDependencies(self):
    """Delete dependencies that were needed for the VM after the VM has been
    deleted."""
    self._DeleteSSHPublicKey()

  def _Exists(self):
    if self.id is None:
      return False

    show_cmd = os_utils.OpenStackCLICommand(self, 'server', 'show', self.id)
    stdout, _, _ = show_cmd.Issue(suppress_warning=True)
    try:
      resp = json.loads(stdout)
      return resp
    except ValueError:
      return False

  def _CheckCanaryCommand(self):
    if OpenStackVirtualMachine.command_works:  # fast path
      return
    with self._lock:
      if OpenStackVirtualMachine.command_works:
        return
      logging.info('Testing OpenStack CLI command is installed and working')
      cmd = os_utils.OpenStackCLICommand(self, 'image', 'list')
      stdout, stderr, _ = cmd.Issue()
      if stderr:
        raise errors.Config.InvalidValue(
            'OpenStack CLI test command failed. Please make sure the OpenStack '
            'CLI client is installed and properly configured')
      OpenStackVirtualMachine.command_works = True

  def _CheckPrerequisites(self):
    """Checks prerequisites are met otherwise aborts execution."""
    self._CheckCanaryCommand()
    if self.zone in self.validated_resources_set:
      return  # No need to check again
    with self._lock:
      if self.zone in self.validated_resources_set:
        return
      logging.info('Validating prerequisites.')
      self._CheckImage()
      self._CheckFlavor()
      self._CheckNetworks()
      self.validated_resources_set.add(self.zone)
      logging.info('Prerequisites validated.')

  def _CheckImage(self):
    """Tries to get image, if found continues execution otherwise aborts."""
    cmd = os_utils.OpenStackCLICommand(self, 'image', 'show', self.image)
    err_msg = VALIDATION_ERROR_MESSAGE.format('Image', self.image)
    self._IssueCommandCheck(cmd, err_msg)

  def _CheckFlavor(self):
    """Tries to get flavor, if found continues execution otherwise aborts."""
    cmd = os_utils.OpenStackCLICommand(self, 'flavor', 'show',
                                       self.machine_type)
    err_msg = VALIDATION_ERROR_MESSAGE.format('Machine type', self.machine_type)
    self._IssueCommandCheck(cmd, err_msg)

  def _CheckNetworks(self):
    """Tries to get network, if found continues execution otherwise aborts."""
    if not self.network_name:
      if self.floating_ip_pool_name:
        msg = ('Cannot associate floating-ip address from pool %s without '
               'an internally routable network. Make sure '
               '--openstack_network flag is set.')
      else:
        msg = ('Cannot build instance without a network. Make sure to set '
               'either just --openstack_network or both '
               '--openstack_network and --openstack_floating_ip_pool flags.')
      raise errors.Error(msg)

    self._CheckNetworkExists(self.network_name)

    if self.floating_ip_pool_name:
      floating_network_dict = self._CheckFloatingIPNetworkExists(
          self.floating_ip_pool_name)
      OpenStackVirtualMachine.floating_network_id = floating_network_dict['id']

  def _CheckFloatingIPNetworkExists(self, floating_network_name_or_id):
    network = self._CheckNetworkExists(floating_network_name_or_id)
    if network['router:external'] != 'External':
      raise errors.Config.InvalidValue('Network "%s" is not External'
                                       % self.floating_ip_pool_name)
    return network

  def _CheckNetworkExists(self, network_name_or_id):
    cmd = os_utils.OpenStackCLICommand(self, 'network', 'show',
                                       network_name_or_id)
    err_msg = VALIDATION_ERROR_MESSAGE.format('Network', network_name_or_id)
    stdout = self._IssueCommandCheck(cmd, err_msg)
    network = json.loads(stdout)
    return network

  def _IssueCommandCheck(self, cmd, err_msg=None):
    """Issues command and, if stderr is non-empty, raises an error message
    Args:
        cmd: The command to be issued.
        err_msg: string. Error message if command fails.
    """
    if err_msg is None:
      err_msg = ""
    stdout, stderr, _ = cmd.Issue()
    if stderr:
      raise errors.Config.InvalidValue(err_msg)
    return stdout

  def _UploadSSHPublicKey(self):
    """Uploads SSH public key to the VM's region."""
    with self._lock:
      if self.zone in self.uploaded_keypair_set:
        return
      cmd = os_utils.OpenStackCLICommand(self, 'keypair', 'create',
                                         self.key_name)
      cmd.flags['public-key'] = self.ssh_public_key
      cmd.IssueRetryable()
      self.uploaded_keypair_set.add(self.zone)
      if self.zone in self.deleted_keypair_set:
        self.deleted_keypair_set.remove(self.zone)

  def _DeleteSSHPublicKey(self):
    """Deletes SSH public key used for the VM."""
    with self._lock:
      if self.zone in self.deleted_keypair_set:
        return
      cmd = os_utils.OpenStackCLICommand(self, 'keypair', 'delete',
                                         self.key_name)
      del cmd.flags['format']  # keypair delete does not support json output
      cmd.Issue()
      self.deleted_keypair_set.add(self.zone)
      if self.zone in self.uploaded_keypair_set:
        self.uploaded_keypair_set.remove(self.zone)

  def _CreateInstance(self):
    """Execute command for creating an OpenStack VM instance."""
    create_cmd = self._GetCreateCommand()
    stdout, stderr, _ = create_cmd.Issue()
    if stderr:
      raise errors.Error(stderr)
    resp = json.loads(stdout)
    self.id = resp['id']

  def _GetCreateCommand(self):
    cmd = os_utils.OpenStackCLICommand(self, 'server', 'create', self.name)
    cmd.flags['flavor'] = self.machine_type
    cmd.flags['security-group'] = self.group_id
    cmd.flags['key-name'] = self.key_name
    cmd.flags['availability-zone'] = self.zone
    cmd.flags['nic'] = 'net-id=%s' % self.network_name
    cmd.flags['wait'] = True
    if FLAGS.openstack_config_drive:
      cmd.flags['config-drive'] = 'True'

    hints = self._GetSchedulerHints()
    if hints:
      cmd.flags['hint'] = hints

    if FLAGS.openstack_boot_from_volume:
      cmd.flags['volume'] = self.boot_volume_id
    else:
      cmd.flags['image'] = self.image

    return cmd

  def _GetSchedulerHints(self):
    if FLAGS.openstack_scheduler_policy == NONE:
      return None

    with self._lock:
      group_name = 'perfkit_server_group_%s' % FLAGS.run_uri
      hint_temp = 'group=%s'
      if self.zone in self.created_server_group_dict:
        hint = hint_temp % self.created_server_group_dict[self.zone]['id']
        return hint
      server_group = self._CreateServerGroup(group_name)
      self.server_group_id = server_group['id']
      self.created_server_group_dict[self.zone] = server_group
      if self.zone in self.deleted_server_group_set:
        self.deleted_server_group_set.remove(self.zone)

      return hint_temp % server_group['id']

  def _CreateServerGroup(self, group_name):
    cmd = os_utils.OpenStackCLICommand(self, 'server group', 'create',
                                       group_name)
    cmd.flags['policy'] = FLAGS.openstack_scheduler_policy
    stdout, stderr, _ = cmd.Issue()
    if stderr:
      raise errors.Error(stderr)
    server_group = json.loads(stdout)
    return server_group

  def _DeleteServerGroup(self):
    with self._lock:
      if self.zone in self.deleted_server_group_set:
        return
      cmd = os_utils.OpenStackCLICommand(self, 'server group', 'delete',
                                         self.server_group_id)
      del cmd.flags['format']  # delete does not support json output
      cmd.Issue()
      self.deleted_server_group_set.add(self.zone)
      if self.zone in self.created_server_group_dict:
        del self.created_server_group_dict[self.zone]

  def _DeleteInstance(self):
    cmd = os_utils.OpenStackCLICommand(self, 'server', 'delete', self.id)
    del cmd.flags['format']  # delete does not support json output
    cmd.flags['wait'] = True
    cmd.Issue(suppress_warning=True)

  def _SetIPAddresses(self):
    show_cmd = os_utils.OpenStackCLICommand(self, 'server', 'show', self.name)
    stdout, _, _ = show_cmd.Issue()
    server_dict = json.loads(stdout)
    self.ip_address = self._GetNetworkIPAddress(server_dict, self.network_name)
    if self.floating_ip_pool_name:
      self.floating_ip = self._AllocateFloatingIP()
      self.internal_ip = self.ip_address
      self.ip_address = self.floating_ip.floating_ip_address

  def _GetNetworkIPAddress(self, server_dict, network_name):
    addresses = server_dict['addresses'].split(',')
    for address in addresses:
      if network_name in address:
        _, ip = address.split('=')
        return ip

  def _GetInternalNetworkCIDR(self):
    """Returns IP addresses source range of internal network."""
    net_cmd = os_utils.OpenStackCLICommand(self, 'network', 'show',
                                           self.network_name)
    net_stdout, _, _ = net_cmd.Issue()
    network = json.loads(net_stdout)
    self.subnet_id = network['subnets']
    subnet_cmd = os_utils.OpenStackCLICommand(self, 'subnet', 'show',
                                              self.subnet_id)
    stdout, _, _ = subnet_cmd.Issue()
    subnet_dict = json.loads(stdout)
    return subnet_dict['cidr']

  def _AllocateFloatingIP(self):
    floating_ip = self.public_network.associate(self)
    logging.info('floating-ip associated: {}'.format(
        floating_ip.floating_ip_address))
    return floating_ip

  def CreateScratchDisk(self, disk_spec):
    disks_names = ('%s_data_%d_%d'
                   % (self.name, len(self.scratch_disks), i)
                   for i in range(disk_spec.num_striped_disks))
    disks = [os_disk.OpenStackDisk(disk_spec, name, self.zone)
             for name in disks_names]

    self._CreateScratchDiskFromDisks(disk_spec, disks)


class DebianBasedOpenStackVirtualMachine(OpenStackVirtualMachine,
                                         linux_virtual_machine.DebianMixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE


class RhelBasedOpenStackVirtualMachine(OpenStackVirtualMachine,
                                       linux_virtual_machine.RhelMixin):
  DEFAULT_IMAGE = RHEL_IMAGE
