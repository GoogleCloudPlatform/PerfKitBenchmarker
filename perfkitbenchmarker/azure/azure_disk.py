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

"""Module containing classes related to Azure disks.

Disks can be created, deleted, attached to VMs, and detached from VMs.
At this time, Azure only supports one disk type, so the disk spec's disk type
is ignored.
See http://msdn.microsoft.com/en-us/library/azure/dn790303.aspx for more
information about azure disks.
"""

import json
import logging
import threading

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.azure import azure_network

AZURE_PATH = 'azure'

NONE = 'None'
READ_ONLY = 'ReadOnly'
READ_WRITE = 'ReadWrite'
flags.DEFINE_enum(
    'azure_host_caching', NONE,
    [NONE, READ_ONLY, READ_WRITE],
    'The type of host caching to use on Azure data disks.')


FLAGS = flags.FLAGS

DRIVE_START_LETTER = 'c'
DISK_TYPE = {disk.STANDARD: None, disk.REMOTE_SSD: None}


class AzureDisk(disk.BaseDisk):
  """Object representing an Azure Disk."""

  _lock = threading.Lock()
  num_disks = {}

  def __init__(self, disk_spec, vm_name):
    super(AzureDisk, self).__init__(disk_spec)
    self.host_caching = FLAGS.azure_host_caching
    self.name = None
    self.vm_name = vm_name
    self.lun = None

  def _Create(self):
    """Creates the disk."""
    assert self.disk_type in DISK_TYPE, self.disk_type

    if self.disk_type == disk.REMOTE_SSD:
      assert FLAGS.azure_storage_type == azure_network.PLRS
    else:
      assert FLAGS.azure_storage_type != azure_network.PLRS

    with self._lock:
      create_cmd = [AZURE_PATH,
                    'vm',
                    'disk',
                    'attach-new',
                    '--host-caching=%s' % self.host_caching,
                    self.vm_name,
                    str(self.disk_size)]
      vm_util.IssueRetryableCommand(create_cmd)

      if self.vm_name not in AzureDisk.num_disks:
        AzureDisk.num_disks[self.vm_name] = 0
      self.lun = AzureDisk.num_disks[self.vm_name]
      AzureDisk.num_disks[self.vm_name] += 1
      self.created = True

  def _Delete(self):
    """Deletes the disk."""
    delete_cmd = [AZURE_PATH,
                  'vm',
                  'disk',
                  'delete',
                  '--blob-delete',
                  self.name]
    logging.info('Deleting disk %s. This may fail while the associated VM '
                 'is deleted, but will be retried.', self.name)
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the disk exists."""
    if self.name is None and self.created:
      return True
    elif self.name is None:
      return False
    show_cmd = [AZURE_PATH,
                'vm',
                'disk',
                'show',
                '--json',
                self.name]
    stdout, _, _ = vm_util.IssueCommand(show_cmd, suppress_warning=True)
    try:
      json.loads(stdout)
    except ValueError:
      return False
    return True

  @vm_util.Retry()
  def _PostCreate(self):
    """Get the disk's name."""
    show_cmd = [AZURE_PATH,
                'vm',
                'show',
                '--json',
                self.vm_name]
    stdout, _, _ = vm_util.IssueCommand(show_cmd)
    response = json.loads(stdout)
    data_disk = response['DataDisks'][self.lun]
    assert ((self.lun == 0 and 'logicalUnitNumber' not in data_disk)
            or (self.lun == int(data_disk['logicalUnitNumber'])))
    self.name = data_disk['name']

  def Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: The AzureVirtualMachine instance to which the disk will be attached.
    """
    pass  # TODO(user): Implement Attach()
    # (not critical because disks are attached to VMs when created)

  def Detach(self):
    """Detaches the disk from a VM."""
    pass  # TODO(user): Implement Detach()

  def GetDevicePath(self):
    """Returns the path to the device inside the VM."""
    if self.disk_type == disk.LOCAL:
      return '/dev/sdb'
    else:
      return '/dev/sd%s' % chr(ord(DRIVE_START_LETTER) + self.lun)
