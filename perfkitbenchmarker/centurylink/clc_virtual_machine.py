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

# ./pkb.py --cloud=Centurylink --machine_type=UBUNTU-12-64-TEMPLATE --benchmarks=iperf

import json
import logging
import os

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.centurylink import clc_disk
from perfkitbenchmarker.centurylink import util

CLOUD_CONFIG_TEMPLATE = '''#cloud-config
users:
  - name: {0}
    ssh-authorized-keys:
      - {1}
    sudo: ['ALL=(ALL) NOPASSWD:ALL']
    groups: sudo
    shell: /bin/bash

'''

class CenturylinkVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a Centurylink Virtual Machine."""

  DEFAULT_ZONE = 'NY1'
  DEFAULT_MACHINE_TYPE = 'UBUNTU-12-64-TEMPLATE'
  DEFAULT_IMAGE = 'ubuntu-14-04-x64'

  def __init__(self, vm_spec):
    """Initialize a Centurylink virtual machine."""

    super(CenturylinkVirtualMachine, self).__init__(vm_spec)
    self.server_name =''
    self.disk_size = 1
    self.max_local_disks = 1
    self.local_disk_counter = 0

    env = util.GetDefaultCenturylinkEnv()
    self.api_key = env['API_KEY']
    self.api_passwd = env['API_PASSWD']

  @classmethod
  def SetVmSpecDefaults(cls, vm_spec):
    """Updates the VM spec with cloud specific defaults."""

    if vm_spec.machine_type is None:
      vm_spec.machine_type = cls.DEFAULT_MACHINE_TYPE
    if vm_spec.zone is None:
      vm_spec.zone = cls.DEFAULT_ZONE
    if vm_spec.image is None:
      vm_spec.image = cls.DEFAULT_IMAGE

  def _Create(self):
    """Create a Centurylink VM instance."""

    env = util.GetDefaultCenturylinkEnv()
    GROUP = env['GROUP']
    LOCATION = env['LOCATION']
    MACHINE_TYPE = self.machine_type
    NAME = env['VM_NAME']
    DESC = env['VM_DESC']
    BACKUP_LEVEL = env['BACKUP_LEVEL']
    NETWORK = env['NETWORK']
    CPU = env['CPU']
    RAM = env['RAM']

    ### Create a server with the specified inputs ###
    create_cmd = "clc --v1-api-key %s --v1-api-passwd %s servers create \
    --name %s \
    --location %s \
    --group '%s' \
    --description '%s' \
    --template %s \
    --backup-level %s \
    --network %s \
    --cpu %d \
    --ram %d" % (self.api_key, self.api_passwd, NAME, LOCATION, GROUP, DESC, MACHINE_TYPE, BACKUP_LEVEL, NETWORK, CPU, RAM)

    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    server_name = util.ParseCLIResponse(stdout)
    self.server_name = server_name

    self.CreateScratchDisk()

  def CreateScratchDisk(self, disk_spec):

    scratch_disk = clc_disk.CenturylinkDisk(disk_spec, self.api_key, self.api_passwd, self.server_name, self.disk_size)
    scratch_disk._Create()

  def _Delete(self):
    """Delete a Centurylink VM instance."""
    
    delete_cmd = "clc --async --v1-api-key %s --v1-api-passwd %s servers delete \
    --server %s" % (self.api_key, self.api_passwd, self.server_name)

    vm_util.IssueCommand(delete_cmd)

class DebianBasedCenturylinkVirtualMachine(CenturylinkVirtualMachine,
                                         linux_virtual_machine.DebianMixin):
  pass