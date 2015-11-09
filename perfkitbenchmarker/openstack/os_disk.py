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

import logging
import time

from perfkitbenchmarker.openstack import utils as os_utils
from perfkitbenchmarker import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker.openstack.utils import retry_authorization


FLAGS = flags.FLAGS


class OpenStackDisk(disk.BaseDisk):

    def __init__(self, disk_spec, name, zone, image=None):
        super(OpenStackDisk, self).__init__(disk_spec)
        self.__nclient = os_utils.NovaClient()
        self.attached_vm_name = None
        self.attached_vm_id = -1
        self.image = image
        self.name = name
        self.zone = zone
        self.device = ""
        self.virtual_disks = (c for c in "cdefghijklmnopqrstuvwxyz")

    def _Create(self):
        self._disk = self.__nclient.volumes.create(self.disk_size,
                                                   display_name=self.name,
                                                   availability_zone=self.zone,
                                                   imageRef=self.image,
                                                   )

        is_unavailable = True
        while is_unavailable:
            time.sleep(1)
            volume = self.__nclient.volumes.get(self._disk.id)
            if volume:
                is_unavailable = not (volume.status == "available")
        self._disk = volume

    @retry_authorization(max_retries=4)
    def _Delete(self):
        sleep = 1
        sleep_count = 0
        try:
            self.__nclient.volumes.delete(self._disk)
            is_deleted = False
            while not is_deleted:
                is_deleted = len(self.__nclient.volumes.findall(
                    display_name=self.name)) == 0
                time.sleep(sleep)
                sleep_count += 1
                if sleep_count == 10:
                    sleep = 5
        except (os_utils.NotFound, os_utils.BadRequest):
            logging.info('Volume already deleted')

    def _Exists(self):
        try:
            if len(self.__nclient.volumes.findall(display_name=self.name)):
                return True
            else:
                return False
        except (os_utils.NotFound, os_utils.BadRequest):
            return False

    def Attach(self, vm):
        self.attached_vm_name = vm.name
        self.attached_vm_id = vm.id
        device_hint_name = "/dev/vd" + self.virtual_disks.next()
        result = self.__nclient.volumes.create_server_volume(vm.id,
                                                             self._disk.id,
                                                             device_hint_name)
        self.attach_id = result.id
        self.device = "/dev/disk/by-id/virtio-" + result.id[:20]

        is_unattached = True
        while is_unattached:
            time.sleep(1)
            volume = self.__nclient.volumes.get(result.id)
            if volume:
                is_unattached = not(volume.status == "in-use"
                                    and volume.attachments)

    def GetDevicePath(self):
        return self.device

    def Detach(self):
        self.__nclient.volumes.delete_server_volume(self.attached_vm_id,
                                                    self.attach_id)
