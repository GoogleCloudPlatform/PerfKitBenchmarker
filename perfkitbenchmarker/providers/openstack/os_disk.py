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

import json
import logging

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.openstack import utils as os_utils

REMOTE_VOLUME_DEFAULT_SIZE_GB = 50

FLAGS = flags.FLAGS


def CreateVolume(resource, name):
  """Creates a remote (Cinder) block volume."""
  vol_cmd = os_utils.OpenStackCLICommand(resource, 'volume', 'create', name)
  vol_cmd.flags['availability-zone'] = resource.zone
  vol_cmd.flags['size'] = (FLAGS.openstack_volume_size or
                           REMOTE_VOLUME_DEFAULT_SIZE_GB)
  stdout, _, _ = vol_cmd.Issue()
  vol_resp = json.loads(stdout)
  return vol_resp


def CreateBootVolume(resource, name, image):
  """Creates a remote (Cinder) block volume with a boot image."""
  vol_cmd = os_utils.OpenStackCLICommand(resource, 'volume', 'create', name)
  vol_cmd.flags['availability-zone'] = resource.zone
  vol_cmd.flags['image'] = image
  vol_cmd.flags['size'] = (FLAGS.openstack_volume_size or
                           GetImageMinDiskSize(resource, image))
  stdout, _, _ = vol_cmd.Issue()
  vol_resp = json.loads(stdout)
  return vol_resp


def GetImageMinDiskSize(resource, image):
  """Returns minimum disk size required by the image."""
  image_cmd = os_utils.OpenStackCLICommand(resource, 'image', 'show', image)
  stdout, _, _ = image_cmd.Issue()
  image_resp = json.loads(stdout)
  volume_size = max((int(image_resp['min_disk']),
                     REMOTE_VOLUME_DEFAULT_SIZE_GB,))
  return volume_size


def DeleteVolume(resource, volume_id):
  """Deletes a remote (Cinder) block volume."""
  vol_cmd = os_utils.OpenStackCLICommand(resource, 'volume', 'delete',
                                         volume_id)
  del vol_cmd.flags['format']  # volume delete does not support json output
  vol_cmd.Issue()


@vm_util.Retry(poll_interval=5, max_retries=-1, timeout=300, log_errors=False,
               retryable_exceptions=errors.Resource.RetryableCreationError)
def WaitForVolumeCreation(resource, volume_id):
  """Waits until volume is available"""
  vol_cmd = os_utils.OpenStackCLICommand(resource, 'volume', 'show', volume_id)
  stdout, stderr, _ = vol_cmd.Issue()
  if stderr:
    raise errors.Error(stderr)
  resp = json.loads(stdout)
  if resp['status'] != 'available':
    msg = 'Volume is not ready. Retrying to check status.'
    raise errors.Resource.RetryableCreationError(msg)


class OpenStackDisk(disk.BaseDisk):

  def __init__(self, disk_spec, name, zone, image=None):
    super(OpenStackDisk, self).__init__(disk_spec)
    self.attached_vm_id = None
    self.image = image
    self.name = name
    self.zone = zone
    self.id = None

  def _Create(self):
    vol_resp = CreateVolume(self, self.name)
    self.id = vol_resp['id']
    WaitForVolumeCreation(self, self.id)

  def _Delete(self):
    if self.id is None:
      logging.info('Volume %s was not created. Skipping deletion.' % self.name)
      return
    DeleteVolume(self, self.id)
    self._WaitForVolumeDeletion()

  def _Exists(self):
    if self.id is None:
      return False
    cmd = os_utils.OpenStackCLICommand(self, 'volume', 'show', self.id)
    stdout, stderr, _ = cmd.Issue(suppress_warning=True)
    if stdout and stdout.strip():
      return stdout
    return not stderr

  def Attach(self, vm):
    self._AttachVolume(vm)
    self._WaitForVolumeAttachment(vm)
    self.attached_vm_id = vm.id

  def Detach(self):
    self._DetachVolume()
    self.attached_vm_id = None
    self.device_path = None

  def _AttachVolume(self, vm):
    if self.id is None:
      raise errors.Error('Cannot attach remote volume %s' % self.name)
    if vm.id is None:
      msg = 'Cannot attach remote volume %s to non-existing %s VM' % (self.name,
                                                                      vm.name)
      raise errors.Error(msg)
    cmd = os_utils.OpenStackCLICommand(
        self, 'server', 'add', 'volume', vm.id, self.id)
    del cmd.flags['format']
    _, stderr, _ = cmd.Issue()
    if stderr:
      raise errors.Error(stderr)

  @vm_util.Retry(poll_interval=1, max_retries=-1, timeout=300, log_errors=False,
                 retryable_exceptions=errors.Resource.RetryableCreationError)
  def _WaitForVolumeAttachment(self, vm):
    if self.id is None:
      return
    cmd = os_utils.OpenStackCLICommand(self, 'volume', 'show', self.id)
    stdout, stderr, _ = cmd.Issue()
    if stderr:
      raise errors.Error(stderr)
    resp = json.loads(stdout)
    attachments = resp['attachments']
    self.device_path = self._GetDeviceFromAttachment(attachments)
    msg = 'Remote volume %s has been attached to %s.' % (self.name, vm.name)
    logging.info(msg)

  def _GetDeviceFromAttachment(self, attachments):
    device = None
    for attachment in attachments:
      if attachment['volume_id'] == self.id:
        device = attachment['device']
    if not device:
      msg = '%s is not yet attached. Retrying to check status.' % self.name
      raise errors.Resource.RetryableCreationError(msg)
    return device

  def _DetachVolume(self):
    if self.id is None:
      raise errors.Error('Cannot detach remote volume %s' % self.name)
    if self.attached_vm_id is None:
      raise errors.Error('Cannot detach remote volume from a non-existing VM.')
    cmd = os_utils.OpenStackCLICommand(
        self, 'server', 'remove', 'volume', self.attached_vm_id, self.id)
    del cmd.flags['format']
    _, stderr, _ = cmd.Issue()
    if stderr:
      raise errors.Error(stderr)

  @vm_util.Retry(poll_interval=1, max_retries=-1, timeout=300, log_errors=False,
                 retryable_exceptions=errors.Resource.RetryableDeletionError)
  def _WaitForVolumeDeletion(self):
    if self.id is None:
      return
    cmd = os_utils.OpenStackCLICommand(self, 'volume', 'show', self.id)
    stdout, stderr, _ = cmd.Issue(suppress_warning=True)
    if stderr.strip():
      return  # Volume could not be found, inferred that has been deleted.
    resp = json.loads(stdout)
    if resp['status'] in ('building', 'available', 'in-use', 'deleting',):
      msg = ('Volume %s has not yet been deleted. Retrying to check status.'
             % self.id)
      raise errors.Resource.RetryableDeletionError(msg)
