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

"""The static VM spec.

Used to define static VMs (ie not created by PKB). See static_virtual_machine.py
for more details.
"""

from perfkitbenchmarker import disk
from perfkitbenchmarker import virtual_machine_spec


class StaticVmSpec(virtual_machine_spec.BaseVmSpec):
  """Object containing all info needed to create a Static VM."""

  CLOUD = 'Static'

  def __init__(
      self,
      component_full_name,
      ip_address=None,
      user_name=None,
      ssh_private_key=None,
      internal_ip=None,
      internal_ips=None,
      ssh_port=22,
      password=None,
      disk_specs=None,
      os_type=None,
      tag=None,
      zone=None,
      **kwargs
  ):
    """Initialize the StaticVmSpec object.

    Args:
      component_full_name: string. Fully qualified name of the configurable
        component containing the config options.
      ip_address: The public ip address of the VM.
      user_name: The username of the VM that the keyfile corresponds to.
      ssh_private_key: The absolute path to the private keyfile to use to ssh to
        the VM.
      internal_ip: The internal ip address of the VM.
      internal_ips: The internal ip addresses of the VMs.
      ssh_port: The port number to use for SSH and SCP commands.
      password: The password used to log into the VM (Windows Only).
      disk_specs: None or a list of dictionaries containing kwargs used to
        create disk.BaseDiskSpecs.
      os_type: The OS type of the VM. See the flag of the same name for more
        information.
      tag: A string that allows the VM to be included or excluded from a run by
        using the 'static_vm_tags' flag.
      zone: The VM's zone.
      **kwargs: Other args for the superclass.
    """
    super().__init__(component_full_name, **kwargs)
    self.ip_address = ip_address
    self.user_name = user_name
    self.ssh_private_key = ssh_private_key
    self.internal_ip = internal_ip
    self.internal_ips = internal_ips
    self.ssh_port = ssh_port
    self.password = password
    self.os_type = os_type
    self.tag = tag
    self.zone = zone
    self.disk_specs = [
        disk.BaseDiskSpec(
            '{}.disk_specs[{}]'.format(component_full_name, i),
            flag_values=kwargs.get('flag_values'),
            **disk_spec
        )
        for i, disk_spec in enumerate(disk_specs or ())
    ]
