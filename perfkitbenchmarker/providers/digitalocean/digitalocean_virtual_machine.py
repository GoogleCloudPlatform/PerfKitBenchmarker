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
"""Class to represent a DigitalOcean Virtual Machine object (Droplet).
"""

import json
import logging
import time

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.digitalocean import digitalocean_disk
from perfkitbenchmarker.providers.digitalocean import util
from perfkitbenchmarker import providers

FLAGS = flags.FLAGS

UBUNTU_IMAGE = 'ubuntu-14-04-x64'

# DigitalOcean sets up the root account with a temporary
# password that's set as expired, requiring it to be changed
# immediately. This breaks dpkg postinst scripts, for example
# running adduser will produce errors:
#
#   # chfn -f 'RabbitMQ messaging server' rabbitmq
#   You are required to change your password immediately (root enforced)
#   chfn: PAM: Authentication token is no longer valid; new one required
#
# To avoid this, just disable the root password (we don't need it),
# and remove the forced expiration.
CLOUD_CONFIG_TEMPLATE = '''#cloud-config
users:
  - name: {0}
    ssh-authorized-keys:
      - {1}
    sudo: ['ALL=(ALL) NOPASSWD:ALL']
    groups: sudo
    shell: /bin/bash
runcmd:
  - [ passwd, -l, root ]
  - [ chage, -d, -1, -I, -1, -E, -1, -M, 999999, root ]
'''

# HTTP status codes for creation that should not be retried.
FATAL_CREATION_ERRORS = set([
    422,  # 'unprocessable_entity' such as invalid size or region.
])

# Default configuration for action status polling.
DEFAULT_ACTION_WAIT_SECONDS = 10
DEFAULT_ACTION_MAX_TRIES = 90


def GetErrorMessage(stdout):
  """Extract a message field from JSON output if present."""
  try:
    return json.loads(stdout)['message']
  except (ValueError, KeyError):
    return stdout


class DigitalOceanVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a DigitalOcean Virtual Machine (Droplet)."""

  CLOUD = providers.DIGITALOCEAN
  # Subclasses should override the default image.
  DEFAULT_IMAGE = None

  def __init__(self, vm_spec):
    """Initialize a DigitalOcean virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(DigitalOceanVirtualMachine, self).__init__(vm_spec)
    self.droplet_id = None
    self.max_local_disks = 1
    self.local_disk_counter = 0
    self.image = self.image or self.DEFAULT_IMAGE

  def _Create(self):
    """Create a DigitalOcean VM instance (droplet)."""
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
            'private_networking': True,
            'ssh_keys': [],
            'user_data': CLOUD_CONFIG_TEMPLATE.format(
                self.user_name, public_key)
        })
    if ret != 0:
      msg = GetErrorMessage(stdout)
      if ret in FATAL_CREATION_ERRORS:
        raise errors.Error('Creation request invalid, not retrying: %s' % msg)
      raise errors.Resource.RetryableCreationError('Creation failed: %s' % msg)
    response = json.loads(stdout)
    self.droplet_id = response['droplet']['id']

    # The freshly created droplet will be in a locked and unusable
    # state for a while, and it cannot be deleted or modified in
    # this state. Wait for the action to finish and check the
    # reported result.
    if not self._GetActionResult(response['links']['actions'][0]['id']):
      raise errors.Resource.RetryableCreationError('Creation failed, see log.')

  def _GetActionResult(self, action_id,
                       wait_seconds=DEFAULT_ACTION_WAIT_SECONDS,
                       max_tries=DEFAULT_ACTION_MAX_TRIES):
    """Wait until a VM action completes."""
    for _ in xrange(max_tries):
      time.sleep(wait_seconds)
      stdout, ret = util.RunCurlCommand('GET', 'actions/%s' % action_id)
      if ret != 0:
        logging.warn('Unexpected action lookup failure.')
        return False
      response = json.loads(stdout)
      status = response['action']['status']
      logging.debug('action %d: status is "%s".', action_id, status)
      if status == 'completed':
        return True
      elif status == 'errored':
        return False
    # If we get here, waiting timed out. Treat as failure.
    logging.debug('action %d: timed out waiting.', action_id)
    return False

  @vm_util.Retry()
  def _PostCreate(self):
    """Get the instance's data."""
    stdout, _ = util.RunCurlCommand(
        'GET', 'droplets/%s' % self.droplet_id)
    response = json.loads(stdout)['droplet']
    for interface in response['networks']['v4']:
      if interface['type'] == 'public':
        self.ip_address = interface['ip_address']
      else:
        self.internal_ip = interface['ip_address']

  def _Delete(self):
    """Delete a DigitalOcean VM instance."""
    stdout, ret = util.RunCurlCommand(
        'DELETE', 'droplets/%s' % self.droplet_id)
    if ret != 0:
      if ret == 404:
        return  # Assume already deleted.
      raise errors.Resource.RetryableDeletionError('Deletion failed: %s' %
                                                   GetErrorMessage(stdout))

    # Get the droplet's actions so that we can look up the
    # ID for the deletion just issued.
    stdout, ret = util.RunCurlCommand(
        'GET', 'droplets/%s/actions' % self.droplet_id)
    if ret != 0:
      # There's a race condition here - if the lookup fails, assume it's
      # due to deletion already being complete. Don't raise an error in
      # that case,  the _Exists check should trigger retry if needed.
      return
    response = json.loads(stdout)['actions']

    # Get the action ID for the 'destroy' action. This assumes there's only
    # one of them, but AFAIK there can't be more since 'destroy' locks the VM.
    destroy = [v for v in response if v['type'] == 'destroy'][0]

    # Wait for completion. Actions are global objects, so the action
    # status should still be retrievable after the VM got destroyed.
    # We don't care about the result, let the _Exists check decide if we
    # need to try again.
    self._GetActionResult(destroy['id'])

  def _Exists(self):
    """Returns true if the VM exists."""
    _, ret = util.RunCurlCommand(
        'GET', 'droplets/%s' % self.droplet_id,
        suppress_warning=True)
    if ret == 0:
      return True
    if ret == 404:
      # Definitely doesn't exist.
      return False
    # Unknown status - assume it doesn't exist. TODO(klausw): retry?
    return False

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    if disk_spec.disk_type != disk.STANDARD:
      # TODO(klausw): support type BOOT_DISK once that's implemented.
      raise errors.Error('DigitalOcean does not support disk type %s.' %
                         disk_spec.disk_type)

    if self.scratch_disks:
      # We have a "disk" already, don't add more. TODO(klausw): permit
      # multiple creation for type BOOT_DISK once that's implemented.
      raise errors.Error('DigitalOcean does not support multiple disks.')

    # Just create a local directory at the specified path, don't mount
    # anything.
    self.RemoteCommand('sudo mkdir -p {0} && sudo chown -R $USER:$USER {0}'
                       .format(disk_spec.mount_point))
    self.scratch_disks.append(digitalocean_disk.DigitalOceanDisk(disk_spec))


class ContainerizedDigitalOceanVirtualMachine(
        DigitalOceanVirtualMachine,
        linux_virtual_machine.ContainerizedDebianMixin):
    DEFAULT_IMAGE = UBUNTU_IMAGE


class DebianBasedDigitalOceanVirtualMachine(DigitalOceanVirtualMachine,
                                            linux_virtual_machine.DebianMixin):
    DEFAULT_IMAGE = UBUNTU_IMAGE


class RhelBasedDigitalOceanVirtualMachine(DigitalOceanVirtualMachine,
                                          linux_virtual_machine.RhelMixin):
    pass
