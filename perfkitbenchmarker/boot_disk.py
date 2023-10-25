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
"""Module containing abstract classes related to boot disks."""

from perfkitbenchmarker import resource


class BootDiskSpec(object):
  """Stores the information needed to create a  boot disk.

  Attributes:
    boot_disk_size: None or int. The size of the boot disk in GB.
    boot_disk_type: string or None. The type of the boot disk.
  """

  def __init__(self, boot_disk_size, boot_disk_type):
    self.boot_disk_size: int = boot_disk_size
    self.boot_disk_type: str = boot_disk_type


class BootDisk(resource.BaseResource):
  """Base class for boot disks.

  Logic implemented by cloud-specific children classes.
  """

  def Attach(self, vm):
    pass

  def PostCreate(self):
    pass

  def _Create(self):
    pass

  def _Delete(self):
    pass

  def GetCreationCommand(self):
    pass
