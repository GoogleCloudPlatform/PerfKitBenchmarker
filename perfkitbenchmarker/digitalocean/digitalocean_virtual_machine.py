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
"""Class to represent a DigitalOcean Virtual Machine object (Droplet).
"""

import json
import time

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import package_managers
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.digitalocean import digitalocean_disk
from perfkitbenchmarker.digitalocean import util

FLAGS = flags.FLAGS

CLOUD_CONFIG_TEMPLATE = '''#cloud-config
users:
  - name: {0}
    ssh-authorized-keys:
      - {1}
    sudo: ['ALL=(ALL) NOPASSWD:ALL']
    groups: sudo
    shell: /bin/bash
'''

class DigitaloceanVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a DigitalOcean Virtual Machine (Droplet)."""

  def __init__(self, vm_spec):
    """Initialize a DigitalOcean virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(DigitaloceanVirtualMachine, self).__init__(vm_spec)
    self.droplet_id = None
    self.max_local_disks = 1
    self.local_disk_counter = 0

  def _CreateDependencies(self):
    """Create VM dependencies."""
    pass

  def _DeleteDependencies(self):
    """Delete VM dependencies."""
    pass

  def _Create(self):
    """Create a DigitalOcean VM instance (droplet)."""
    super(DigitaloceanVirtualMachine, self)._Create()
    with open(self.ssh_public_key) as f:
      public_key = f.read().rstrip('\n')

    stdout, ret = util.RunCurlCommand(
        'POST', 'droplets', {
            'name': self.name,
            'region': self.zone,
            'size': self.machine_type,
            'image': self.image,
            'backups': False,
            'ipv6': False,
            'ssh_keys': [],
            'user_data': CLOUD_CONFIG_TEMPLATE.format(
                self.user_name, public_key)
            })
    if ret != 0:
      raise errors.Error('Creation failed: %s' % stdout)
    response = json.loads(stdout)
    self.droplet_id = response['droplet']['id']

    # The freshly created droplet will be in a locked and unusable
    # state for a while, and it cannot be deleted or modified in
    # this state. Wait for the action to finish and check the
    # reported result.
    if not self._GetActionResult(response['links']['actions'][0]['id']):
      raise errors.Resource.RetryableCreationError('Creation failed.')

  def _GetActionResult(self, action_id, wait_seconds=10, max_tries=30):
    """Wait until a VM action completes."""
    for _ in xrange(max_tries):
      time.sleep(wait_seconds)
      stdout, ret = util.RunCurlCommand('GET', 'actions/%s' % action_id)
      if ret != 0:
        raise errors.Error('Unexpected action failure: %s' % stdout)
      response = json.loads(stdout)
      status = response['action']['status']
      if status == 'completed':
        return True
      elif status == 'errored':
        return False
    return False  # Timeout, treat as failure.

  @vm_util.Retry()
  def _PostCreate(self):
    """Get the instance's data."""
    stdout, _ = util.RunCurlCommand(
        'GET', 'droplets/%s' % self.droplet_id)
    response = json.loads(stdout)['droplet']
    network_interface = response['networks']['v4'][0]
    # Not currently using private addresses. Do we need to?
    self.internal_ip = network_interface['ip_address']
    self.ip_address = network_interface['ip_address']

  @vm_util.Retry()
  def _Delete(self):
    """Delete a DigitalOcean VM instance."""
    stdout, ret = util.RunCurlCommand(
        'DELETE', 'droplets/%s' % self.droplet_id)
    if ret != 0:
      raise errors.Error('Deletion failed: %s' % stdout)
    # TODO(klausw): It would be nice to check the action progress
    # via _GetActionResult, but this is not easy. The DELETE action
    # does not provide any JSON output, so the action ID is not
    # directly available. The actions endpoint doesn't seem to offer
    # a way to query actions by droplet ID, and the full list can
    # be very large.

  def _Exists(self):
    """Returns true if the VM exists in a configured state."""
    stdout, ret = util.RunCurlCommand(
        'GET', 'droplets/%s' % self.droplet_id,
        suppress_warning=True)
    if ret != 0:
      return False
    try:
      response = json.loads(stdout)['droplet']
      if response['locked']:
        # A VM is locked if it's being created, resized, or deleted.
        return False
      # Check that configuration appears complete. The 'networks'
      # section only appears with some delay after creation.
      network = response['networks']['v4'][0]
    except (ValueError, IndexError):
      return False
    return True

  def GetScratchDir(self, disk_num=0):
    """Gets the path to the scratch directory.

    DigitalOcean does not support separate disk devices at this time.
    Use a directory on the main disk as a single scratch dir, but fail
    if a test assumes multiple distinct disks, i.e. for copy benchmark.

    Args:
      disk_num: The number of the disk to mount.
    Returns:
      The mounted disk directory.
    """

    if disk_num > 0:
      raise errors.Error('DigitalOcean only supports one scratch disk.')

    return self.scratch_disks[disk_num].mount_point

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    if disk_spec.disk_type == disk.LOCAL:
      self.local_disk_counter += disk_spec.num_striped_disks
      if self.local_disk_counter > self.max_local_disks:
        raise errors.Error('Not enough local disks.')

    # Just create a local directory at the specified path, don't mount
    # anything.
    self.RemoteCommand('sudo mkdir -p {0} && sudo chown -R $USER:$USER {0}'
                       .format(disk_spec.mount_point))
    self.scratch_disks.append(digitalocean_disk.DigitaloceanDisk(disk_spec))

  def GetName(self):
    """Get a DigitalOcean VM's unique name."""
    return self.name

  def GetLocalDisks(self):
    """Returns a list of local disks on the VM.

    Returns:
      A list of strings, where each string is the absolute path to the local
          disks on the VM (e.g. '/dev/sdb').
    """
    return []

  def SetupLocalDisks(self):
    pass


class DebianBasedDigitaloceanVirtualMachine(DigitaloceanVirtualMachine,
                                   package_managers.AptMixin):
  pass


class RhelBasedDigitaloceanVirtualMachine(DigitaloceanVirtualMachine,
                                 package_managers.YumMixin):
  pass
