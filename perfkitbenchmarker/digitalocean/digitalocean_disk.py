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
"""Module containing classes related to DigitalOcean disks.

At this time, DigitalOcean does not implement any standalone disk objects,
the instances come with directly integrated storage.
"""

from perfkitbenchmarker import disk


class DigitaloceanDisk(disk.BaseDisk):
  """Dummy Object representing a DigitalOcean Disk."""

  def __init__(self, disk_spec):
    super(DigitaloceanDisk, self).__init__(disk_spec)

  def Attach(self, vm):
    pass

  def Detach(self):
    pass

  def GetDevicePath(self):
    return '/dev/nosuchdevice'

  def _Create(self):
    pass

  def _Delete(self):
    pass
