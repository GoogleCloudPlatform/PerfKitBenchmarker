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
"""Module containing classes related to IBM Cloud disks """

import json
import logging
import time
import re

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.ibmcloud import utils

FLAGS = flags.FLAGS

ATTACHED = 'attached'
AVAILABLE = 'available'


class IbmCloudDisk(disk.BaseDisk):

  def __init__(self, disk_spec, name, zone):
    super(IbmCloudDisk, self).__init__(disk_spec)
    self.name = name
    self.zone = zone
    self.vol_id = None
    self.attached_vm = None
    self.attached_vdisk_URI = None
    self.device_path = None

  def _Create(self):
    """Creates an external block volume."""
    volcmd = utils.IbmAPICommand(self)
    volcmd.flags['name'] = self.name
    volcmd.flags['zone'] = self.zone
    volcmd.flags['iops'] = FLAGS.ibmcloud_volume_iops
    volcmd.flags['profile'] = FLAGS.ibmcloud_volume_profile
    volcmd.flags['capacity'] = FLAGS.data_disk_size
    if FLAGS.ibmcloud_datavol_encryption_key:
      volcmd.flags['encryption_key'] = FLAGS.ibmcloud_datavol_encryption_key
    if FLAGS.ibmcloud_rgid:
      volcmd.flags['resource_group'] = FLAGS.ibmcloud_rgid

    logging.info('Volume create, volcmd.flags %s', volcmd.flags)
    resp = json.loads(volcmd.CreateVolume())
    self.vol_id = resp['id']
    logging.info('Volume id: %s', self.vol_id)

  def _Delete(self):
    """Deletes an external block volume."""
    if self.vol_id is None:
      logging.info('Volume %s was not created. Skipping deletion.' % self.name)
      return
    volcmd = utils.IbmAPICommand(self)
    volcmd.flags['volume'] = self.vol_id
    logging.info('Volume delete, volcmd.flags %s', volcmd.flags)
    volcmd.DeleteVolume()
    logging.info('Volume deleted: %s', self.vol_id)
    self.vol_id = None

  def _Exists(self):
    if self.vol_id is None:
      return False
    return True

  def _FindVdiskIdByName(self, vdisks, name):
    """Finds vdisk id by name"""
    for vdisk in vdisks:
      if vdisk['name'] == name:
        return vdisk['id']
    return None

  def Attach(self, vm):
    """Attaches an external volume """
    if self.vol_id is None:
      raise errors.Error('Cannot attach remote volume %s' % self.name)
    if vm.vmid is None:
      msg = 'Cannot attach remote volume %s to non-existing %s VM' % (self.name,
                                                                      vm.name)
      raise errors.Error(msg)

    volcmd = utils.IbmAPICommand(self)
    volcmd.flags['name'] = self.name
    volcmd.flags['instance'] = vm.vmid
    volcmd.flags['volume'] = self.vol_id
    volcmd.flags['delete'] = True

    # wait till volume is PROVISIONED before attach
    resp = json.loads(volcmd.ShowVolume())
    status = resp['status']
    endtime = time.time() + 300
    while status != AVAILABLE and time.time() < endtime:
      time.sleep(2)
      status = json.loads(volcmd.ShowVolume())['status']
      logging.info('Checking volume status: %s', status)

    if status != AVAILABLE:
      logging.error('Failed to create a volume')
      raise errors.Error('IBMCLOUD ERROR: failed to provision a volume.')

    resp = json.loads(volcmd.InstanceCreateVolume())
    logging.info('Attached volume, volcmd.flags %s', volcmd.flags)
    volume = resp['id']
    logging.info('VDISK ID: %s, vdiskname: %s, instanceid: %s', volume, self.name, vm.vmid)
    self.attached_vdisk_URI = volume
    self.attached_vm = vm
    self._WaitForVolumeAttachment(vm)
    self._GetDeviceFromVDisk(vm)

  @vm_util.Retry(poll_interval=1, max_retries=-1, timeout=300, log_errors=False,
                 retryable_exceptions=errors.Resource.RetryableCreationError)
  def _WaitForVolumeAttachment(self, vm):
    """Waits and checks until the volume status is attached """
    if self.vol_id is None:
      return
    volcmd = utils.IbmAPICommand(self)
    volcmd.flags['instance'] = self.attached_vm.vmid
    volcmd.flags['volume'] = self.vol_id
    logging.info('Checking volume on instance %s, volume: %s', vm.vmid, self.vol_id)
    resp = volcmd.InstanceShowVolume()
    logging.info('Show volume resp: %s', resp)
    status = None
    if resp:
      resp = json.loads(resp)
      status = resp['status'] if resp and 'status' in resp else 'unknown'

    endtime = time.time() + 300
    while status != ATTACHED and time.time() < endtime:
      time.sleep(2)
      resp = volcmd.InstanceShowVolume()
      if resp:
        resp = json.loads(resp)
        status = resp['status'] if resp and 'status' in resp else 'unknown'
        logging.info('Checking instance volume status: %s', status)

    if status == ATTACHED:
      logging.info('Remote volume %s has been attached to %s', self.name, vm.name)
    else:
      logging.error('Failed to attach the volume')
      raise errors.Error('IBMCLOUD ERROR: failed to attach a volume.')

  def _GetDeviceFromVDisk(self, vm):
    """Gets device path for the volume """
    cmd = 'fdisk -l'
    endtime = time.time() + 120
    self.device_path = None
    while time.time() < endtime:
      stdout, _ = self.attached_vm.RemoteCommand(cmd, should_log=True)
      # parse for lines that contain "Disk /dev/vdd: 1000 GiB"
      disks = re.findall(r'\Disk /.*/.*: \d+ GiB', stdout)
      for line in disks:
        paths = line.split(' ')
        if len(paths) >= 4 and paths[2] == str(FLAGS.data_disk_size):
          _path = paths[1][0:len(paths[1]) - 1]
          if _path not in vm.device_paths_detected:
            self.device_path = _path
            vm.device_paths_detected.add(_path)
            logging.info('device path found: %s', self.device_path)
            break
      if self.device_path:
        break
      time.sleep(10)

    if not self.device_path:
      raise errors.Error('IBMCLOUD ERROR: failed to find device path.')

  def Detach(self):
    """Deletes the volume from instance """
    if self.attached_vm is None:
      raise errors.Error('Cannot detach remote volume from a non-existing VM.')
    if self.attached_vdisk_URI is None:
      raise errors.Error('Cannot detach remote volume from a non-existing VDisk.')
    volcmd = utils.IbmAPICommand(self)
    volcmd.flags['volume'] = self.attached_vdisk_URI
    resp = volcmd.InstanceDeleteVolume()
    logging.info('Deleted volume: %s', resp)
    self.attached_vdisk_URI = None
    self.device_path = None
