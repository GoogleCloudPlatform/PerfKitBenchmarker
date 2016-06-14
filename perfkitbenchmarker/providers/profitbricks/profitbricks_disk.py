# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing classes related to ProfitBricks disks.
"""

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors


class ProfitBricksDisk(disk.BaseDisk):
    """Dummy Object representing a ProfitBricks Disk."""

    def __init__(self, disk_spec):
        super(ProfitBricksDisk, self).__init__(disk_spec)

    def Attach(self, vm):
        pass

    def Detach(self):
        pass

    def GetDevicePath(self):
        # ProfitBricks VMs only have a single disk block device which is
        # in use for the live filesystem, so it's not usable as a scratch
        # disk device.
        raise errors.Error('GetDevicePath not supported for ProfitBricks.')

    def _Create(self):
        pass

    def _Delete(self):
        pass
