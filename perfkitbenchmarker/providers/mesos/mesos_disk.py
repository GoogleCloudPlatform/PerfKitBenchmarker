# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags

FLAGS = flags.FLAGS


class MesosDisk(disk.BaseDisk):
  """
  Base class for Mesos Disks.
  """

  def __init__(self, disk_spec):
    super(MesosDisk, self).__init__(disk_spec)
    self.mount_point = disk_spec.mount_point

  def _Create(self):
    return

  def _Delete(self):
    return

  def Attach(self, vm):
    return

  def Detach(self):
    return

  def GetDevicePath(self):
    return None


class LocalDisk(MesosDisk):
  """
  Represents DAS (direct-attached storage). For each disk, a new directory
  on a host is created and then mounted as a volume to a Docker instance.
  """

  def __init__(self, disk_num, disk_spec, name):
    super(LocalDisk, self).__init__(disk_spec)
    self.name = 'local-disk-%s-%s' % (name, disk_num)

  def AttachVolumeInfo(self, container_body):
    # Intentionally not using 'volumes' API as it doesn't allow to
    # create local volumes automatically - when empty hostPath is passed to
    # Marathon's API then the sandbox path is used as a host path. However,
    # new directory should be created and used for this specific purpose.
    volume_param = {
        'key': 'volume',
        'value': self.mount_point
    }
    container_body['docker']['parameters'].append(volume_param)
