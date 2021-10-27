# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

import collections
import json
import logging
import threading

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import os_types
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine

FLAGS = flags.FLAGS

flags.DEFINE_list('static_vm_tags', None,
                  'The tags of static VMs for PKB to run with. Even if other '
                  'VMs are specified in a config, if they aren\'t in this list '
                  'they will be skipped during VM creation.')


class StaticVmSpec(virtual_machine.BaseVmSpec):
  """Object containing all info needed to create a Static VM."""

  CLOUD = 'Static'

  def __init__(self, component_full_name, ip_address=None, user_name=None,
               ssh_private_key=None, internal_ip=None, ssh_port=22,
               password=None, disk_specs=None, os_type=None, tag=None,
               zone=None, **kwargs):
    """Initialize the StaticVmSpec object.

    Args:
      component_full_name: string. Fully qualified name of the configurable
          component containing the config options.
      ip_address: The public ip address of the VM.
      user_name: The username of the VM that the keyfile corresponds to.
      ssh_private_key: The absolute path to the private keyfile to use to ssh
          to the VM.
      internal_ip: The internal ip address of the VM.
      ssh_port: The port number to use for SSH and SCP commands.
      password: The password used to log into the VM (Windows Only).
      disk_specs: None or a list of dictionaries containing kwargs used to
          create disk.BaseDiskSpecs.
      os_type: The OS type of the VM. See the flag of the same name for more
          information.
      tag: A string that allows the VM to be included or excluded from a run
          by using the 'static_vm_tags' flag.
      zone: The VM's zone.
      **kwargs: Other args for the superclass.
    """
    super(StaticVmSpec, self).__init__(component_full_name, **kwargs)
    self.ip_address = ip_address
    self.user_name = user_name
    self.ssh_private_key = ssh_private_key
    self.internal_ip = internal_ip
    self.ssh_port = ssh_port
    self.password = password
    self.os_type = os_type
    self.tag = tag
    self.zone = zone
    self.disk_specs = [
        disk.BaseDiskSpec(
            '{0}.disk_specs[{1}]'.format(component_full_name, i),
            flag_values=kwargs.get('flag_values'), **disk_spec)
        for i, disk_spec in enumerate(disk_specs or ())]


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

  CLOUD = 'Static'
  is_static = True
  vm_pool = collections.deque()
  vm_pool_lock = threading.Lock()

  def __init__(self, vm_spec):
    """Initialize a static virtual machine.

    Args:
      vm_spec: A StaticVmSpec object containing arguments.
    """
    super(StaticVirtualMachine, self).__init__(vm_spec)
    self.ip_address = vm_spec.ip_address
    self.user_name = vm_spec.user_name
    self.ssh_private_key = vm_spec.ssh_private_key
    self.internal_ip = vm_spec.internal_ip
    self.zone = self.zone or ('Static - %s@%s' % (self.user_name,
                                                  self.ip_address))
    self.ssh_port = vm_spec.ssh_port
    self.password = vm_spec.password
    self.disk_specs = vm_spec.disk_specs
    self.from_pool = False
    self.preemptible = False

  def _Suspend(self):
    """Suspends VM."""
    raise NotImplementedError()

  def _Resume(self):
    """Resumes VM."""
    raise NotImplementedError()

  def _Create(self):
    """StaticVirtualMachines do not implement _Create()."""
    pass

  # StaticVirtualMachines do not implement _Start or _Stop
  def _Start(self):
    """Starts the VM."""
    raise NotImplementedError()

  def _Stop(self):
    """Stops the VM."""
    raise NotImplementedError()

  def _Delete(self):
    """Returns the virtual machine to the pool."""
    if self.from_pool:
      with self.vm_pool_lock:
        self.vm_pool.appendleft(self)

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
        os_types.WINDOWS: required_keys | frozenset(['password']),
        os_types.DEBIAN: linux_required_keys,
        os_types.RHEL: linux_required_keys,
        os_types.CLEAR: linux_required_keys,
        os_types.UBUNTU_CONTAINER: linux_required_keys,
    }

    # assume linux_required_keys for unknown os_type
    required_keys = required_keys_by_os.get(FLAGS.os_type, linux_required_keys)

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

      if ((os_type == os_types.WINDOWS and FLAGS.os_type != os_types.WINDOWS) or
          (os_type != os_types.WINDOWS and FLAGS.os_type == os_types.WINDOWS)):
        raise ValueError('Please only use Windows VMs when using '
                         '--os_type=windows and vice versa.')

      disk_kwargs_list = []
      for path in scratch_disk_mountpoints:
        disk_kwargs_list.append({'mount_point': path})
      for local_disk in local_disks:
        disk_kwargs_list.append({'device_path': local_disk})

      vm_spec = StaticVmSpec(
          'static_vm_file', ip_address=ip_address, user_name=user_name,
          ssh_port=ssh_port, install_packages=install_packages,
          ssh_private_key=keyfile_path, internal_ip=internal_ip, zone=zone,
          disk_specs=disk_kwargs_list, password=password,
          flag_values=flags.FLAGS)

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
        vm = cls.vm_pool.popleft()
        vm.Create()
        vm.from_pool = True
        return vm
      else:
        return None


def GetStaticVmClass(os_type):
  """Returns the static VM class that corresponds to the os_type."""
  if not os_type:
    os_type = os_types.DEFAULT
    logging.warning('Could not find os type for VM. Defaulting to %s.', os_type)
  return resource.GetResourceClass(virtual_machine.BaseVirtualMachine,
                                   CLOUD=StaticVirtualMachine.CLOUD,
                                   OS_TYPE=os_type)


class Ubuntu1604BasedStaticVirtualMachine(
    StaticVirtualMachine, linux_virtual_machine.Ubuntu1604Mixin):
  pass


class Ubuntu1804BasedStaticVirtualMachine(
    StaticVirtualMachine, linux_virtual_machine.Ubuntu1804Mixin):
  pass


class Ubuntu2004BasedStaticVirtualMachine(
    StaticVirtualMachine, linux_virtual_machine.Ubuntu2004Mixin):
  pass


class ClearBasedStaticVirtualMachine(StaticVirtualMachine,
                                     linux_virtual_machine.ClearMixin):
  pass


class Rhel7BasedStaticVirtualMachine(StaticVirtualMachine,
                                     linux_virtual_machine.Rhel7Mixin):
  pass


class Rhel8BasedStaticVirtualMachine(StaticVirtualMachine,
                                     linux_virtual_machine.Rhel8Mixin):
  pass


class CentOs7BasedStaticVirtualMachine(StaticVirtualMachine,
                                       linux_virtual_machine.CentOs7Mixin):
  pass


class CentOs8BasedStaticVirtualMachine(StaticVirtualMachine,
                                       linux_virtual_machine.CentOs8Mixin):
  pass


class Debian9BasedStaticVirtualMachine(StaticVirtualMachine,
                                       linux_virtual_machine.Debian9Mixin):
  pass


class Debian10BasedStaticVirtualMachine(StaticVirtualMachine,
                                        linux_virtual_machine.Debian10Mixin):
  pass
