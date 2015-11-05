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
"""Module containing classes related to CloudStack disks.

Disks can be created, deleted, attached to VMs, and detached from VMs.
"""

import string
import logging

from perfkitbenchmarker import disk
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import flags
from perfkitbenchmarker.providers.cloudstack import util

FLAGS = flags.FLAGS


class CloudStackDisk(disk.BaseDisk):
  """Object representing a Cloudstack Disk."""


  def __init__(self, disk_spec, name, zone_id, project_id=None):
    super(CloudStackDisk, self).__init__(disk_spec)

    self.cs = util.CsClient(
        FLAGS.CS_API_URL,
        FLAGS.CS_API_KEY,
        FLAGS.CS_API_SECRET
    )

    self.attached_vm_name = None
    self.attached_vm_id = None
    self.name = name

    self.zone_id = zone_id
    self.project_id = project_id

    self.disk_offering_id = self._GetBestOfferingId(self.disk_size)
    assert self.disk_offering_id, "Unable get disk offering of given size"

    if disk_spec.disk_type:
        logging.warn("Cloudstack does not support disk types")


  @vm_util.Retry(max_retries=3)
  def _Create(self):
    """Creates the disk."""


    volume = self.cs.create_volume(self.name,
                                   self.disk_offering_id,
                                   self.zone_id,
                                   self.project_id)

    assert volume, "Unable to create volume"

    self.volume_id = volume['id']
    self.disk_type = volume['type']
    self.actual_disk_size = int(volume['size']) / (2 ** 30)  # In GB


  def _Delete(self):
    """Deletes the disk."""
    vol = self.cs.get_volume(self.name, self.project_id)
    if vol:
        self.cs.delete_volume(self.volume_id)


  def _Exists(self):
    """Returns true if the disk exists."""
    vol = self.cs.get_volume(self.name, self.project_id)
    if vol:
        return True
    return False


  @vm_util.Retry(max_retries=3)
  def Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: The CloudStackVirtualMachine instance to which
      the disk will be attached.

    """

    res = self.cs.attach_volume(self.volume_id, vm.id)
    assert res, "Unable to attach volume"

    self.device_id = res['deviceid']

    self.device_path = "/dev/xvd" + \
        str(string.ascii_lowercase[self.device_id])



  def Detach(self):
    """Detaches the disk from a VM."""

    self.cs.detach_volume(self.volume_id)


  def _GetBestOfferingId(self, disk_size):
    """ Given a disk_size (in GB), try to find a disk
    offering that is atleast as big as the requested
    one.
    """

    disk_offerings = self.cs.list_disk_offerings()
    sorted_do = sorted(disk_offerings, key=lambda x: x['disksize'])

    for do in sorted_do:
        if int(do['disksize']) >= disk_size:
            return do['id']

    return None
