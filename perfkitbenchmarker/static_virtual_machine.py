# Copyright 2014 Google Inc. All rights reserved.
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

"""Class to represent a Static Virtual Machine object.

All static VMs provided will be used before any non-static VMs are provisioned.
For example, in a test that uses 4 VMs, if 3 static VMs are provided, all
of them will be used and one additional non-static VM will be provisioned.
The VM's should be set up with passwordless ssh and passwordless sudo (neither
sshing nor running a sudo command should prompt the user for a password).

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""


import json
import logging
import threading

from perfkitbenchmarker import package_managers
from perfkitbenchmarker import virtual_machine


class StaticVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a Static Virtual Machine."""

  vm_pool = []
  vm_pool_lock = threading.Lock()

  is_static = True

  def __init__(self, ip_address, user_name, keyfile_path, internal_ip=None,
               zone=None, local_disks=None, scratch_disk_mountpoints=None,
               ssh_port=22, install_packages=True):
    """Initialize a static virtual machine.

    Args:
      ip_address: The ip address of the vm.
      user_name: The username of the vm that the keyfile corresponds to.
      keyfile_path: The absolute path of the private keyfile for the vm.
      internal_ip: The internal ip address of the vm.
      zone: The zone of the VM.
      local_disks: A list of the paths of local disks on the VM.
      scratch_disk_mountpoints: A list of scratch disk mountpoints.
      ssh_port: The port number to use for SSH and SCP commands.
      install_packages: If false, no packages will be installed. This is
          useful if benchmark dependencies have already been installed.
    """
    vm_spec = virtual_machine.BaseVirtualMachineSpec(
        None, None, None, None, None)
    super(StaticVirtualMachine, self).__init__(vm_spec)
    self.ip_address = ip_address
    self.internal_ip = internal_ip
    self.zone = zone or ('Static - %s@%s' % (user_name, ip_address))
    self.user_name = user_name
    self.ssh_port = ssh_port
    self.ssh_private_key = keyfile_path
    self.local_disks = local_disks or []
    self.scratch_disk_mountpoints = scratch_disk_mountpoints or []
    self.install_packages = install_packages

  def _Create(self):
    """StaticVirtualMachines do not implement _Create()."""
    pass

  def _Delete(self):
    """StaticVirtualMachines do not implement _Delete()."""
    pass

  def DeleteScratchDisks(self):
    """StaticVirtualMachines do not delete scratch disks."""
    pass

  def GetScratchDir(self, disk_num=0):
    """Gets the path to the scratch directory.

    Args:
      disk_num: The number of the mounted disk.
    Returns:
      The mounted disk directory path.
    Raises:
      IndexError: On missing scratch disks.
    """
    try:
      scratch_dir = self.scratch_disk_mountpoints[disk_num]
    except IndexError:
      logging.exception('No scratch disk configured for disk_num %d.  '
                        'Add one to the static VM file using '
                        '"scratch_disk_mountpoints"', disk_num)
      raise
    return scratch_dir

  @classmethod
  def ReadStaticVirtualMachineFile(cls, file_obj):
    """Read a file describing the static VMs to use.

    This function will read the static VM information from the provided file,
    instantiate VMs corresponding to the info, and add the VMs to the static
    VM pool. The provided file should contain a single array in JSON-format.
    Each element in the array must be an object with required format:

      ip_address: string.
      user_name: string.
      keyfile_path: string.
      ssh_port: integer, optional. Default 22
      internal_ip: string, optional.
      zone: string, optional.
      local_disks: array of strings, optional.
      scratch_disk_mountpoints: array of strings, optional
      os_type: string, optional (see package_managers)
      install_packages: bool, optional

    See the constructor for descriptions.

    Args:
      file_obj: An open handle to a file containing the static VM info.

    Raises:
      ValueError: On missing required keys, or invalid keys.
    """
    vm_arr = json.load(file_obj)

    if not isinstance(vm_arr, list):
      raise ValueError('Invalid static VM file. Expected array, got: %s.' %
                       type(vm_arr))

    required_keys = frozenset(['ip_address', 'user_name', 'keyfile_path'])
    optional_keys = frozenset(['internal_ip', 'zone', 'local_disks',
                               'scratch_disk_mountpoints', 'os_type',
                               'ssh_port', 'install_packages'])
    allowed_keys = required_keys | optional_keys

    def VerifyItemFormat(item):
      """Verify that the decoded JSON object matches the required schema."""
      item_keys = frozenset(item)
      extra_keys = sorted(item_keys - allowed_keys)
      missing_keys = required_keys - item_keys
      if extra_keys:
        raise ValueError('Unexpected keys: {0}'.format(', '.join(extra_keys)))
      elif missing_keys:
        raise ValueError('Missing required keys: {0}'.format(
            ', '.join(missing_keys)))

    for item in vm_arr:
      VerifyItemFormat(item)

      ip_address = item['ip_address']
      user_name = item['user_name']
      keyfile_path = item['keyfile_path']
      internal_ip = item.get('internal_ip')
      zone = item.get('zone')
      local_disks = item.get('local_disks', [])
      if not isinstance(local_disks, list):
        raise ValueError('Expected a list of local disks, got: {0}'.format(
            local_disks))
      scratch_disk_mountpoints = item.get('scratch_disk_mountpoints', [])
      if not isinstance(scratch_disk_mountpoints, list):
        raise ValueError(
            'Expected a list of disk mount points, got: {0}'.format(
                scratch_disk_mountpoints))
      ssh_port = item.get('ssh_port', 22)
      os_type = item.get('os_type')
      install_packages = item.get('install_packages', True)
      vm_class = GetStaticVirtualMachineClass(os_type)
      vm = vm_class(ip_address, user_name, keyfile_path, internal_ip, zone,
                    local_disks, scratch_disk_mountpoints, ssh_port,
                    install_packages)
      cls.vm_pool.append(vm)

  @classmethod
  def GetStaticVirtualMachine(cls):
    """Pull a Static VM from the pool of static VMs.

    If there are no VMs left in the pool, the method will return None.

    Returns:
        A static VM from the pool, or None if there are no static VMs left.
    """
    with cls.vm_pool_lock:
      if cls.vm_pool:
        return cls.vm_pool.pop(0)
      else:
        return None

  def GetLocalDisks(self):
    """Returns a list of local disks on the VM."""
    return self.local_disks


def GetStaticVirtualMachineClass(os_type):
  """Returns the static VM class that corresponds to the os_type."""
  class_dict = {
      'debian': DebianBasedStaticVirtualMachine,
      'rhel': RhelBasedStaticVirtualMachine,
  }
  if os_type in class_dict:
    return class_dict[os_type]
  else:
    logging.warning('Could not find os type for VM. Defaulting to debian.')
    return DebianBasedStaticVirtualMachine


class DebianBasedStaticVirtualMachine(StaticVirtualMachine,
                                      package_managers.AptMixin):
  pass


class RhelBasedStaticVirtualMachine(StaticVirtualMachine,
                                    package_managers.YumMixin):
  pass
