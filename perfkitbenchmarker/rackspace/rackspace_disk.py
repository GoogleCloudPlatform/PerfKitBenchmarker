# Copyright 2014 Google Inc. All rights reserved.
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
"""Module containing classes related to Rackspace volumes.

Volumes can be created, deleted, attached to VMs, and detached from VMs.
Use 'cinder type-list' to determine valid disk types.
"""

import ast
import sys
import threading
import time

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.rackspace import util

FLAGS = flags.FLAGS
flags.DEFINE_string(
    'image_id', None, 'The ID of the image from which you want to'
    ' create the volume.')

DISK_TYPE = {disk.STANDARD: 'SATA', disk.SSD: 'SSD'}


class RackspaceDisk(disk.BaseDisk):
    """Object representing a Rackspace Volume."""

    n = 4
    _lock = threading.Lock()

    def __init__(self, disk_spec, name, image=None):
        super(RackspaceDisk, self).__init__(disk_spec)
        self.attached_vm_name = None
        self.image = image
        self.name = name
        self.id = ''
        self.num_retried_attach = 0
        with RackspaceDisk._lock:
            self.disk_num = RackspaceDisk.n
            RackspaceDisk.n += 1

    def _Create(self):
        """Creates the volume."""
        create_cmd = [FLAGS.cinder_path,
                      'create',
                      '--display-name', self.name,
                      '--volume-type', DISK_TYPE[self.disk_type]]
        create_cmd.extend(util.GetDefaultRackspaceCinderFlags(self))
        create_cmd.append(str(self.disk_size))
        stdout, _, _ = vm_util.IssueCommand(create_cmd)
        attrs = stdout.split('\n')
        for attr in attrs[3:-2]:
            pv = [v.strip() for v in attr.split('|') if v != '|' and v != '']
            if pv[0] == 'id':
                self.id = pv[1]
                break

    def _Delete(self):
        """Deletes the volume."""
        delete_cmd = [FLAGS.cinder_path, 'delete']
        delete_cmd.extend(util.GetDefaultRackspaceCinderFlags(self))
        delete_cmd.append(self.name)
        vm_util.IssueCommand(delete_cmd)

    def _Exists(self):
        """Returns true if the volume exists."""
        getdisk_cmd = [FLAGS.cinder_path, 'show']
        getdisk_cmd.extend(util.GetDefaultRackspaceCinderFlags(self))
        getdisk_cmd.append(self.name)
        stdout, _, _ = vm_util.IssueCommand(getdisk_cmd)
        if stdout.strip() == '':
            return False
        attrs = stdout.split('\n')
        for attr in attrs[3:-2]:
            pv = [v.strip() for v in attr.split('|') if v != '|' and v != '']
            if pv[0] == 'status' and pv[1] == 'available':
                return True
        return False

    def Attach(self, vm):
        """Attaches the volume to a VM.

        Args:
          vm: The RackspaceVirtualMachine instance to which the volume will be
              attached.
        """
        self.attached_vm_name = vm.name
        attach_cmd = [FLAGS.nova_path]
        attach_cmd.extend(util.GetDefaultRackspaceNovaFlags(self))
        attach_cmd.extend(['volume-attach'])
        attach_cmd.extend([self.attached_vm_name,
                           self.id,
                           ""])
        vm_util.IssueRetryableCommand(attach_cmd)

        getdisk_cmd = [FLAGS.cinder_path, 'show']
        getdisk_cmd.extend(util.GetDefaultRackspaceCinderFlags(self))
        getdisk_cmd.append(self.name)
        attached = False
        for i in xrange(12):
            stdout, _, _ = vm_util.IssueCommand(getdisk_cmd)
            attrs = stdout.split('\n')
            for attr in attrs[3:-2]:
                pv = [v.strip() for v in attr.split('|')
                      if v != '|' and v != '']
                if pv[0] == 'attachments' and vm.id in pv[1]:
                    attached = True
            if attached:
                return
            time.sleep(10)

        if self.num_retried_attach > 5:
            raise Exception("Failed to attach all scratch disks to vms. "
                            "Exiting...")
            sys.exit(0)
        self.num_retried_attach += 1
        self.Attach(vm)

    def Detach(self):
        """Detaches the disk from a VM."""
        detach_cmd = [FLAGS.nova_path]
        detach_cmd.extend(util.GetDefaultRackspaceNovaFlags(self))
        detach_cmd.extend(['volume-detach'])
        detach_cmd.extend([self.attached_vm_name, self.id])
        vm_util.IssueRetryableCommand(detach_cmd)
        self.attached_vm_name = None

    def GetDevicePath(self):
        """Returns the path to the device inside the VM."""
        # print 'self.disk_num: %d' % self.disk_num
        # if self.disk_type == 'SSD':
        #    return '/dev/sd%s' % chr(self.disk_num + ord('a'))
        # return '/dev/xvd%s' % chr(self.disk_num + ord('a'))
        getdisk_cmd = [FLAGS.cinder_path, 'show']
        getdisk_cmd.extend(util.GetDefaultRackspaceCinderFlags(self))
        getdisk_cmd.append(self.name)
        stdout, _, _ = vm_util.IssueCommand(getdisk_cmd)
        if stdout.strip() == '':
            return False
        attrs = stdout.split('\n')
        attachment_info_raw = []
        for attr in attrs[3:-2]:
            pv = [v.strip() for v in attr.split('|') if v != '|' and v != '']
            if pv[0] == 'attachments':
                attachment_info_raw = pv[1]
        attachment_info = ast.literal_eval(attachment_info_raw)
        if 'device' in attachment_info:
            return str(attachment_info['device'])
        return ''
