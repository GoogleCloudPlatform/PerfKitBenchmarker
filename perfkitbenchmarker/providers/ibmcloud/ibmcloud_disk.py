# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing classes related to IBM Cloud disks."""

import json
import logging
import re
import time

from typing import Any, Dict, Optional

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.ibmcloud import ibm_api as ibm

FLAGS = flags.FLAGS

_MAX_DISK_ATTACH_SECONDS = 300
_MAX_FIND_DEVICE_SECONDS = 120


class IbmCloudDisk(disk.BaseDisk):
  """Object holding the information needed to create an IbmCloudDisk.

  Attributes:
    disk_spec: disk spec.
    zone: zone name.
    name: disk name.
    encryption_key: enryption key.
  """

  def __init__(self,
               disk_spec: disk.BaseDisk,
               name: str,
               zone: str,
               encryption_key: Optional[str] = None):
    super(IbmCloudDisk, self).__init__(disk_spec)
    self.name = name
    self.zone = zone
    self.encryption_key = encryption_key
    self.vol_id = None
    self.attached_vm = None
    self.attached_vdisk_uri = None
    self.device_path = None

  def _Create(self):
    """Creates an external block volume."""
    volcmd = ibm.IbmAPICommand(self)
    volcmd.flags.update({
        'name': self.name,
        'zone': self.zone,
        'iops': FLAGS.ibmcloud_volume_iops,
        'profile': FLAGS.ibmcloud_volume_profile,
        'capacity': self.disk_size
    })
    if self.encryption_key is not None:
      volcmd.flags['encryption_key'] = self.encryption_key
    if FLAGS.ibmcloud_rgid:
      volcmd.flags['resource_group'] = FLAGS.ibmcloud_rgid
    logging.info('Volume create, volcmd.flags %s', volcmd.flags)
    resp = json.loads(volcmd.CreateVolume())
    self.vol_id = resp['id']
    logging.info('Volume id: %s', self.vol_id)

  def _Delete(self):
    """Deletes an external block volume."""
    if self.vol_id is None:
      logging.info('Volume %s was not created. Skipping deletion.', self.name)
      return
    volcmd = ibm.IbmAPICommand(self)
    volcmd.flags['volume'] = self.vol_id
    logging.info('Volume delete, volcmd.flags %s', volcmd.flags)
    volcmd.DeleteVolume()
    logging.info('Volume deleted: %s', self.vol_id)
    self.vol_id = None

  def _Exists(self):
    if self.vol_id is None:
      return False
    return True

  def _FindVdiskIdByName(self, vdisks: Dict[Any, Any], name: str):
    """Finds vdisk id by name."""
    for vdisk in vdisks:
      if vdisk['name'] == name:
        return vdisk['id']
    return None

  def Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: instance of the vm to which the disk will be attached.
    """
    if self.vol_id is None:
      raise errors.Error(f'Cannot attach remote volume {self.name}')
    if vm.vmid is None:
      raise errors.Error(f'Cannot attach remote volume {self.name} '
                         'to non-existing {vm.name} VM')

    volcmd = ibm.IbmAPICommand(self)
    volcmd.flags.update({
        'name': self.name,
        'instanceid': vm.vmid,
        'volume': self.vol_id,
        'delete': True
    })

    # wait till volume is PROVISIONED before attach
    status = None
    endtime = time.time() + _MAX_DISK_ATTACH_SECONDS
    while time.time() < endtime:
      status = json.loads(volcmd.ShowVolume())['status']
      logging.info('Checking volume status: %s', status)
      if status == ibm.States.AVAILABLE:
        logging.info('Volume is available')
        break
      time.sleep(2)

    if status != ibm.States.AVAILABLE:
      logging.error('Failed to create a volume')
      raise errors.Error('IBMCLOUD ERROR: failed to provision a volume.')

    resp = json.loads(volcmd.InstanceCreateVolume())
    logging.info('Attached volume, volcmd.flags %s', volcmd.flags)
    volume = resp['id']
    logging.info('VDISK ID: %s, vdiskname: %s, instanceid: %s', volume,
                 self.name, vm.vmid)
    self.attached_vdisk_uri = volume
    self.attached_vm = vm
    self._WaitForVolumeAttachment(vm)
    self._GetDeviceFromVDisk(vm)

  @vm_util.Retry(
      poll_interval=1,
      max_retries=-1,
      timeout=_MAX_DISK_ATTACH_SECONDS,
      log_errors=False,
      retryable_exceptions=errors.Resource.RetryableCreationError)
  def _WaitForVolumeAttachment(self, vm):
    """Waits and checks until the volume status is attached."""
    if self.vol_id is None:
      return
    volcmd = ibm.IbmAPICommand(self)
    volcmd.flags.update({
        'instanceid': self.attached_vm.vmid,
        'volume': self.vol_id
    })
    logging.info('Checking volume on instance %s, volume: %s', vm.vmid,
                 self.vol_id)
    status = None
    endtime = time.time() + _MAX_DISK_ATTACH_SECONDS
    while time.time() < endtime:
      resp = volcmd.InstanceShowVolume()
      if resp:
        resp = json.loads(resp)
        status = resp.get('status', 'unknown')
        logging.info('Checking instance volume status: %s', status)
        if status == ibm.States.ATTACHED:
          logging.info('Remote volume %s has been attached to %s', self.name,
                       vm.name)
          break
      time.sleep(2)
    if status != ibm.States.ATTACHED:
      logging.error('Failed to attach the volume')
      raise errors.Error('IBMCLOUD ERROR: failed to attach a volume.')

  @vm_util.Retry(max_retries=3)
  def _GetDeviceFromVDisk(self, vm):
    """Gets device path for the volume."""
    cmd = 'fdisk -l'
    endtime = time.time() + _MAX_FIND_DEVICE_SECONDS
    self.device_path = None
    while time.time() < endtime:
      stdout, _ = self.attached_vm.RemoteCommand(cmd, should_log=True)
      # parse for lines that contain disk size in bytes
      disks = re.findall(r'\Disk (\S+): .* (\d+) bytes,', stdout)
      for device_path, disk_size in disks:
        logging.info('disk_path: %s, disk_size: %s', device_path, disk_size)
        if int(disk_size) >= self.disk_size * 1e9:
          if device_path not in vm.device_paths_detected:
            self.device_path = device_path
            vm.device_paths_detected.add(device_path)
            logging.info('device path found: %s', self.device_path)
            break
      if self.device_path:
        break
      time.sleep(10)

    if not self.device_path:
      raise errors.Error('IBMCLOUD ERROR: failed to find device path.')

  def Detach(self):
    """Deletes the volume from instance."""
    if self.attached_vm is None:
      logging.warning('Cannot detach remote volume from a non-existing VM.')
      return
    if self.attached_vdisk_uri is None:
      logging.warning('Cannot detach remote volume from a non-existing VDisk.')
      return
    volcmd = ibm.IbmAPICommand(self)
    volcmd.flags['volume'] = self.attached_vdisk_uri
    resp = volcmd.InstanceDeleteVolume()
    logging.info('Deleted volume: %s', resp)
    self.attached_vdisk_uri = None
    self.device_path = None
