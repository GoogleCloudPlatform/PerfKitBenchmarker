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
"""Class to represent a Rackspace Virtual Machine object.

Zones:
    DFW (Dallas-Fort Worth)
    IAD (Northern Virginia)
    ORD (Chicago)
    LON (London)
    SYD (Sydney)
    HKG (Hong Kong)
Machine Types:
run 'nova flavor-list'
Images:
run 'nova image-list'

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""

import tempfile
import threading
import time

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import package_managers
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.rackspace import rackspace_disk
from perfkitbenchmarker.rackspace import util

FLAGS = flags.FLAGS

BOOT_DISK_SIZE_GB = 75
BOOT_DISK_TYPE = 'SATA'


class RackspaceVirtualMachine(virtual_machine.BaseVirtualMachine):

    count = 1
    _lock = threading.Lock()
    has_keypair = False

    "Object representing a Rackspace Virtual Machine"
    def __init__(self, vm_spec):
        """Initialize Rackspace virtual machine

        Args:
          vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
        """

        super(RackspaceVirtualMachine, self).__init__(vm_spec)
        with RackspaceVirtualMachine._lock:
            self.name = 'perfkit-%s-%s' % (FLAGS.run_uri,
                                           RackspaceVirtualMachine.count)
            self.device_id = RackspaceVirtualMachine.count
            RackspaceVirtualMachine.count += 1
        self.id = ''
        self.ip_address6 = ''
        self.ip_address = ''
        self.key_name = 'perfkit-%s' % FLAGS.run_uri
        self.num_disks = 1
        disk_spec = disk.BaseDiskSpec(BOOT_DISK_SIZE_GB, BOOT_DISK_TYPE, None)
        self.boot_disk = rackspace_disk.RackspaceDisk(
            disk_spec, self.name, self.image)

    def CreateKeyPair(self):
        """Imports the public keyfile to Rackspace."""
        with open(self.ssh_public_key) as f:
            public_key = f.read().rstrip('\n')
        with tempfile.NamedTemporaryFile(dir=vm_util.GetTempDir(),
                                         prefix='key-metadata') as tf:
            tf.write('%s\n' % (public_key))
            tf.flush()
            key_cmd = [FLAGS.nova_path]
            key_cmd.extend(util.GetDefaultRackspaceNovaFlags(self))
            key_cmd.extend(['keypair-add',
                           '--pub-key', tf.name])
            key_cmd.append(self.key_name)
            vm_util.IssueRetryableCommand(key_cmd)

    def DeleteKeyPair(self):
        """Deletes the imported keyfile for an account."""
        key_cmd = [FLAGS.nova_path]
        key_cmd.extend(util.GetDefaultRackspaceNovaFlags(self))
        key_cmd.extend(['keypair-delete', self.key_name])
        vm_util.IssueCommand(key_cmd)

    def _CreateDependencies(self):
        """Create VM dependencies."""
        with RackspaceVirtualMachine._lock:
            if not RackspaceVirtualMachine.has_keypair:
                self.CreateKeyPair()
                RackspaceVirtualMachine.has_keypair = True

    def _DeleteDependencies(self):
        """Delete VM dependencies."""
        with RackspaceVirtualMachine._lock:
            if RackspaceVirtualMachine.has_keypair:
                self.DeleteKeyPair()
                RackspaceVirtualMachine.has_keypair = False

    def _Create(self):
        """Create a Rackspace VM instance."""
        with tempfile.NamedTemporaryFile(dir=vm_util.GetTempDir(),
                                         prefix='user-data') as tf:
            u = self.user_name
            script = ['#cloud-config\n',
                      '\n',
                      'runcmd:\n',
                      '\n',
                      '- useradd -s /bin/bash -U -G sudo %s\n' % u,
                      '- chown -R %s:%s /home/%s/\n' % (u, u, u),
                      '- chown %s:%s /home/%s/.ssh/authorized_keys\n' % (u,
                                                                         u,
                                                                         u),
                      '- chmod 600 /home/%s/.ssh/authorized_keys\n' % u,
                      '- awk \'/(ALL:ALL)/{c++;if(c==2){sub("(ALL:ALL)",'
                      '"NOPASSWD:");c=0}}1\' /etc/sudoers > t\n',
                      '- cp t /etc/sudoers\n',
                      '- sed -i \'s/(NOPASSWD:)/NOPASSWD:/\' /etc/sudoers\n']
            tf.write(''.join(script))
            tf.flush()
            super(RackspaceVirtualMachine, self)._Create()
            create_cmd = [FLAGS.nova_path]
            create_cmd.extend(util.GetDefaultRackspaceNovaFlags(self))
            create_cmd.extend(['boot',
                               '--flavor', self.machine_type,
                               '--image', self.image,
                               '--file', '/home/%s/.ssh/authorized_keys=%s' % (
                                   u, self.ssh_public_key),
                               '--key-name', self.key_name,
                               '--config-drive', 'true',
                               '--user-data', tf.name])
            create_cmd.append(self.name)
            vm_util.IssueCommand(create_cmd)

    @vm_util.Retry()
    def _PostCreate(self):
        """Get the instance's data."""
        n = 0
        while True:
            getinstance_cmd = [FLAGS.nova_path]
            getinstance_cmd.extend(util.GetDefaultRackspaceNovaFlags(self))
            getinstance_cmd.extend(['show', self.name])
            stdout, _, _ = vm_util.IssueCommand(getinstance_cmd)
            attrs = stdout.split('\n')
            for attr in attrs[3:-2]:
                pv = [v.strip() for v in attr.split('|')
                      if v != '|' and v != '']
                if pv[0] == 'accessIPv4':
                    self.ip_address = pv[1]
                if pv[0] == 'accessIPv6':
                    self.ip_address6 = pv[1]
                if pv[0] == 'private network':
                    self.internal_ip = pv[1]
                if pv[0] == 'id':
                    self.id = pv[1]
                if pv[0] == 'status' and pv[1] == 'ACTIVE':
                    if self.ip_address != '':
                        return
            if n == 1800:
                break
            n += 1
            time.sleep(10)

    def _Delete(self):
        """Delete a Rackspace VM instance."""
        delete_cmd = [FLAGS.nova_path]
        delete_cmd.extend(util.GetDefaultRackspaceNovaFlags(self))
        delete_cmd.extend(['delete', self.id])
        vm_util.IssueCommand(delete_cmd)

    def _Exists(self):
        """Returns true if the VM exists."""
        getinstance_cmd = [FLAGS.nova_path]
        getinstance_cmd.extend(util.GetDefaultRackspaceNovaFlags(self))
        getinstance_cmd.extend(['show', self.name])
        stdout, stderr, _ = vm_util.IssueCommand(getinstance_cmd)
        if stdout.strip() == '':
            return False
        else:
            attrs = stdout.split('\n')
            for attr in attrs[3:-2]:
                pv = [v.strip() for v in attr.split('|')
                      if v != '|' and v != '']
                if pv[0] == 'OS-EXT-STS:task_state' and pv[1] == 'deleting':
                    time.sleep(10)
                    self._Exists()
                else:
                    return True
            return True

    def CreateScratchDisk(self, disk_spec):
        """Create a VM's scratch disk.

        Args:
          disk_spec: virtual_machine.BaseDiskSpec object of the disk.
        """
        name = '%s-scratch-%s' % (self.name, len(self.scratch_disks))
        scratch_disk = rackspace_disk.RackspaceDisk(disk_spec, name)
        self.scratch_disks.append(scratch_disk)

        scratch_disk.Create()
        scratch_disk.Attach(self)
        self.num_disks += 1

        self.FormatDisk(scratch_disk.GetDevicePath())
        self.MountDisk(scratch_disk.GetDevicePath(), disk_spec.mount_point)

    def FormatDisk(self, device_path):
        """Formats a disk attached to the VM."""
        fmt_cmd = ('sudo mke2fs -F -E lazy_itable_init=0 -O '
                   '^has_journal -t ext4 -b 4096 %s' % device_path)
        self.RemoteCommand(fmt_cmd)

    def GetName(self):
        """Get a Rackspace VM's unique name."""
        return self.name

    def GetLocalDrives(self):
        """Returns a list of local drives on the VM.

        Returns:
          A list of strings, where each string is the absolute path to the local
              drives on the VM (e.g. '/dev/xvdb').
        """
        return ['/dev/xvd%s' % (chr(i + ord('a')))
                for i in xrange(4, 4 + self.num_disks)]

    def SetupLocalDrives(self, mount_path=virtual_machine.LOCAL_MOUNT_PATH):
        """Set up any local drives that exist.

        Args:
          mount_path: The path where the local drives should be mounted. If this
              is None, then the device won't be formatted or mounted.

        Returns:
          A boolean indicating whether the setup occured.
        """
        self.RemoteCommand('sudo umount /mnt')
        return super(RackspaceVirtualMachine, self).SetupLocalDrives(
            Smount_path=mount_path)

    def AddMetadata(self, **kwargs):
        """Adds metadata to the Rackspace VM"""
        if not kwargs:
            return
        cmd = [FLAGS.nova_path, 'meta', 'set']
        for key, value in kwargs.iteritems():
            cmd.append('{0}={1}'.format(key, value))
        vm_util.IssueCommand(cmd)


class DebianBasedRackspaceVirtualMachine(RackspaceVirtualMachine,
                                         package_managers.AptMixin):
  pass


class RhelBasedRackspaceVirtualMachine(RackspaceVirtualMachine,
                                       package_managers.YumMixin):
  pass
