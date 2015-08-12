# Copyright 2015 Canonical, Ltd. All rights reserved.
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

"""Module containing classes related to Juju disks.

At this time, Juju does not implement any standalone disk objects,
the instances come with directly integrated storage.
"""

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors


class JujuDisk(disk.BaseDisk):
  """Dummy Object representing a Juju Disk."""

  def __init__(self, disk_spec):
    super(JujuDisk, self).__init__(disk_spec)

  def Attach(self, vm):
    pass

  def Detach(self):
    pass

  def GetDevicePath(self):
    # Juju VMs only have a single disk block device which is
    # in use for the live filesystem, so it's not usable as a scratch
    # disk device.
    raise errors.Error('GetDevicePath not supported for DigitalOcean.')

  def _Create(self):
    pass

  def _Delete(self):
    pass
