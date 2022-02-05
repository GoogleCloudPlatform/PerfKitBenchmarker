# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
"""Class to represent a GCE Virtual Machine object.

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""

import base64
import json
import logging
import threading
from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import providers
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util


class AliVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing an AliCloud Virtual Machine."""
  CLOUD = providers.TENCENTCLOUD

  # Subclasses should override the default image OR
  # both the image family and image_project.
  DEFAULT_IMAGE = None
  DEFAULT_IMAGE_FAMILY = None
  DEFAULT_IMAGE_PROJECT = None

  def __init__(self, vm_spec):
    """Initialize a AliCloud virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the VM.
    """
    pass

  @vm_util.Retry(poll_interval=1, log_errors=False)
  def _WaitForInstanceStatus(self, status_list):
    """Waits until the instance's status is in status_list."""
    pass

  @vm_util.Retry(poll_interval=5, max_retries=30, log_errors=False)
  def _WaitForEipStatus(self, status_list):
    """Waits until the instance's status is in status_list."""
    pass

  def _AllocatePubIp(self, region, instance_id):
    """Allocate a public ip address and associate it to the instance."""
    pass


  @classmethod
  def _GetDefaultImage(cls, region):
    """Returns the default image given the machine type and region.

    If no default is configured, this will return None.
    """
    pass


  @vm_util.Retry()
  def _PostCreate(self):
    """Get the instance's data and tag it."""
    pass


  def _CreateDependencies(self):
    """Create VM dependencies."""
    pass

  def _DeleteDependencies(self):
    """Delete VM dependencies."""
    pass

  def _Create(self):
    """Create a VM instance."""
    pass


  def _Delete(self):
    """Delete a VM instance."""
    pass


  def _Exists(self):
    """Returns true if the VM exists."""
    pass


  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    pass


  def AddMetadata(self, **kwargs):
    """Adds metadata to the VM."""
    pass


class AliCloudKeyFileManager(object):
  """Object for managing AliCloud Keyfiles."""
  _lock = threading.Lock()
  imported_keyfile_set = set()
  deleted_keyfile_set = set()
  run_uri_key_names = {}

  @classmethod
  def ImportKeyfile(cls, region):
    """Imports the public keyfile to AliCloud."""
    pass

  @classmethod
  def DeleteKeyfile(cls, region, key_name):
    """Deletes the imported KeyPair for a run_uri."""
    pass

  @classmethod
  def GetKeyNameForRun(cls):
      pass

  @classmethod
  def GetPublicKey(cls):
    pass
