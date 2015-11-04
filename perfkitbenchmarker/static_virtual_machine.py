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

All static VMs provided in a given group will be used before any non-static
VMs are provisioned. For example, in a test that uses 4 VMs, if 3 static VMs
are provided, all of them will be used and one additional non-static VM
will be provisioned. The VM's should be set up with passwordless ssh and
passwordless sudo (neither sshing nor running a sudo command should prompt
the user for a password).

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""


import json
import logging
import threading

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import windows_virtual_machine

WINDOWS = 'windows'
DEBIAN = 'debian'
RHEL = 'rhel'
UBUNTU_CONTAINER = 'ubuntu_container'
FLAGS = flags.FLAGS


class StaticVmSpec(virtual_machine.BaseVmSpec):
  """Object containing all info needed to create a Static VM."""

  def __init__(self, ip_address=None, user_name=None, ssh_private_key=None,
               internal_ip=None, ssh_port=22, install_packages=True,
               password=None, disk_specs=None, os_type=None, **kwargs):
    """Initialize the StaticVmSpec object.

    Args:
      ip_address: The public ip address of the VM.
      user_name: The username of the VM that the keyfile corresponds to.
      ssh_private_key: The absolute path to the private keyfile to use to ssh
          to the VM.
      internal_ip: The internal ip address of the VM.
      ssh_port: The port number to use for SSH and SCP commands.
      install_packages: If false, no packages will be installed. This is
          useful if benchmark dependencies have already been installed.
      password: The password used to log into the VM (Windows Only).
      disk_specs: A list of dictionaries containing kwargs used to create
          disk.BaseDiskSpecs.
      os_type: The OS type of the VM. See the flag of the same name for more
          information.
    """
    super(StaticVmSpec, self).__init__(**kwargs)
    self.ip_address = ip_address
    self.user_name = user_name
    self.ssh_private_key = ssh_private_key
    self.internal_ip = internal_ip
    self.ssh_port = ssh_port
    self.install_packages = install_packages
    self.password = password
    self.os_type = os_type
    self.disk_specs = disk_specs


class StaticDisk(disk.BaseDisk):
  """Object representing a static Disk."""

  def _Create(self):
    """StaticDisks don't implement _Create()."""
    pass

  def _Delete(self):
    """StaticDisks don't implement _Delete()."""
    pass

  def Attach(self):
    """StaticDisks don't implement Attach()."""
    pass

  def Detach(self):
    """StaticDisks don't implement Detach()."""
    pass


class StaticVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a Static Virtual Machine."""

  is_static = True
  vm_pool = []
  vm_pool_lock = threading.Lock()

  def __init__(self, vm_spec):
    """Initialize a static virtual machine.

    Args:
      vm_spec: A StaticVmSpec object containing arguments.
    """
    super(StaticVirtualMachine, self).__init__(None, vm_spec, None, None)
    self.ip_address = vm_spec.ip_address
    self.user_name = vm_spec.user_name
    self.ssh_private_key = vm_spec.ssh_private_key
    self.internal_ip = vm_spec.internal_ip
    self.zone = self.zone or ('Static - %s@%s' % (self.user_name,
                                                  self.ip_address))
    self.ssh_port = vm_spec.ssh_port
    self.install_packages = vm_spec.install_packages
    self.password = vm_spec.password

    if vm_spec.disk_specs:
      for spec in vm_spec.disk_specs:
        self.disk_specs.append(disk.BaseDiskSpec(**spec))

  def _Create(self):
    """StaticVirtualMachines do not implement _Create()."""
    pass

  def _Delete(self):
    """StaticVirtualMachines do not implement _Delete()."""
    pass

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    spec = self.disk_specs[len(self.scratch_disks)]
    self.scratch_disks.append(StaticDisk(spec))

  def DeleteScratchDisks(self):
    """StaticVirtualMachines do not delete scratch disks."""
    pass

  def GetLocalDisks(self):
    """Returns a list of local disks on the VM."""
    return [disk_spec.device_path
            for disk_spec in self.disk_specs if disk_spec.device_path]


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

    Args:
      file_obj: An open handle to a file containing the static VM info.

    Raises:
      ValueError: On missing required keys, or invalid keys.
    """
    vm_arr = json.load(file_obj)

    if not isinstance(vm_arr, list):
      raise ValueError('Invalid static VM file. Expected array, got: %s.' %
                       type(vm_arr))

    required_keys = frozenset(['ip_address', 'user_name'])

    linux_required_keys = required_keys | frozenset(['keyfile_path'])

    required_keys_by_os = {
        WINDOWS: required_keys | frozenset(['password']),
        DEBIAN: linux_required_keys,
        RHEL: linux_required_keys,
        UBUNTU_CONTAINER: linux_required_keys,
    }
    required_keys = required_keys_by_os[FLAGS.os_type]

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
      keyfile_path = item.get('keyfile_path')
      internal_ip = item.get('internal_ip')
      zone = item.get('zone')
      local_disks = item.get('local_disks', [])
      password = item.get('password')

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

      if ((os_type == WINDOWS and FLAGS.os_type != WINDOWS) or
          (os_type != WINDOWS and FLAGS.os_type == WINDOWS)):
        raise ValueError('Please only use Windows VMs when using '
                         '--os_type=windows and vice versa.')

      disk_kwargs_list = []
      for path in scratch_disk_mountpoints:
        disk_kwargs_list.append({'mount_point': path})
      for local_disk in local_disks:
        disk_kwargs_list.append({'device_path': local_disk})

      vm_spec = StaticVmSpec(
          ip_address=ip_address, user_name=user_name, ssh_port=ssh_port,
          install_packages=install_packages, ssh_private_key=keyfile_path,
          internal_ip=internal_ip, zone=zone, disk_specs=disk_kwargs_list,
          password=password)

      vm_class = GetStaticVmClass(os_type)
      vm = vm_class(vm_spec)
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


def GetStaticVmClass(os_type):
  """Returns the static VM class that corresponds to the os_type."""
  class_dict = {
      DEBIAN: DebianBasedStaticVirtualMachine,
      RHEL: RhelBasedStaticVirtualMachine,
      WINDOWS: WindowsBasedStaticVirtualMachine,
      UBUNTU_CONTAINER: ContainerizedStaticVirtualMachine,
  }
  if os_type in class_dict:
    return class_dict[os_type]
  else:
    logging.warning('Could not find os type for VM. Defaulting to debian.')
    return DebianBasedStaticVirtualMachine


class ContainerizedStaticVirtualMachine(
        StaticVirtualMachine, linux_virtual_machine.ContainerizedDebianMixin):
    pass


class DebianBasedStaticVirtualMachine(StaticVirtualMachine,
                                      linux_virtual_machine.DebianMixin):
    pass


class RhelBasedStaticVirtualMachine(StaticVirtualMachine,
                                    linux_virtual_machine.RhelMixin):
    pass


class WindowsBasedStaticVirtualMachine(StaticVirtualMachine,
                                       windows_virtual_machine.WindowsMixin):
    pass
