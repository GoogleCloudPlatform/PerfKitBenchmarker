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
import re
import threading
import time

from perfkitbenchmarker import virtual_machine, linux_virtual_machine
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.openstack import os_disk
from perfkitbenchmarker.providers.openstack import os_network
from perfkitbenchmarker.providers.openstack import utils as os_utils

UBUNTU_IMAGE = 'ubuntu-14.04'
NONE = 'None'

FLAGS = flags.FLAGS

LSBLK_REGEX = (r'NAME="(.*)"\s+MODEL="(.*)"\s+SIZE="(.*)"'
               r'\s+TYPE="(.*)"\s+MOUNTPOINT="(.*)"\s+LABEL="(.*)"')
LSBLK_PATTERN = re.compile(LSBLK_REGEX)


class OpenStackVirtualMachine(virtual_machine.BaseVirtualMachine):
    """Object representing an OpenStack Virtual Machine"""

    CLOUD = 'OpenStack'
    DEFAULT_USERNAME = 'ubuntu'
    # Subclasses should override the default image.
    DEFAULT_IMAGE = None
    _floating_ip_lock = threading.Lock()

    def __init__(self, vm_spec):
        """Initialize an OpenStack virtual machine.

        Args:
          vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
        """
        super(OpenStackVirtualMachine, self).__init__(vm_spec)
        self.firewall = os_network.OpenStackFirewall.GetFirewall()
        self.name = 'perfkit_vm_%d_%s' % (self.instance_number, FLAGS.run_uri)
        self.key_name = 'perfkit_key_%d_%s' % (self.instance_number,
                                               FLAGS.run_uri)
        self.client = os_utils.NovaClient()
        self.public_network = os_network.OpenStackPublicNetwork(
            FLAGS.openstack_public_network
        )
        self.id = None
        self.pk = None
        self.user_name = self.DEFAULT_USERNAME
        self.boot_wait_time = None
        self.image = self.image or self.DEFAULT_IMAGE
        self.boot_device = None
        self.root_disk_allocated = False
        self.mounted_disks = set()

    def _Create(self):
        image = self.client.images.findall(name=self.image)[0]
        flavor = self.client.flavors.findall(name=self.machine_type)[0]

        network = self.client.networks.find(
            label=FLAGS.openstack_private_network)
        nics = [{'net-id': network.id}]
        image_id = image.id
        boot_from_vol = []
        scheduler_hints = None

        if FLAGS.openstack_scheduler_policy != NONE:
            group_name = 'perfkit_%s' % FLAGS.run_uri
            try:
                group = self.client.server_groups.findall(name=group_name)[0]
            except IndexError:
                group = self.client.server_groups.create(
                    policies=[FLAGS.openstack_scheduler_policy],
                    name=group_name)
            scheduler_hints = {'group': group.id}

        if FLAGS.openstack_boot_from_volume:

            if FLAGS.openstack_volume_size:
                volume_size = FLAGS.openstack_volume_size
            else:
                volume_size = flavor.disk

            image_id = None
            boot_from_vol = [{'boot_index': 0,
                              'uuid': image.id,
                              'volume_size': volume_size,
                              'source_type': 'image',
                              'destination_type': 'volume',
                              'delete_on_termination': True}]

        vm = self.client.servers.create(
            name=self.name,
            image=image_id,
            flavor=flavor.id,
            key_name=self.key_name,
            security_groups=['perfkit_sc_group'],
            nics=nics,
            availability_zone=self.zone,
            block_device_mapping_v2=boot_from_vol,
            scheduler_hints=scheduler_hints,
            config_drive=FLAGS.openstack_config_drive)
        self.id = vm.id

    @vm_util.Retry(max_retries=4, poll_interval=2)
    def _PostCreate(self):
        status = 'BUILD'
        instance = None
        while status == 'BUILD':
            time.sleep(5)
            instance = self.client.servers.get(self.id)
            status = instance.status

        with self._floating_ip_lock:
            self.floating_ip = self.public_network.get_or_create()
            instance.add_floating_ip(self.floating_ip)
            logging.info('floating-ip associated: {}'.format(
                self.floating_ip.ip))

        while not self.public_network.is_attached(self.floating_ip):
            time.sleep(1)

        self.ip_address = self.floating_ip.ip
        self.internal_ip = instance.networks[
            FLAGS.openstack_private_network][0]

    @os_utils.retry_authorization(max_retries=4)
    def _Delete(self):
        from novaclient.exceptions import NotFound
        try:
            self.client.servers.delete(self.id)
            time.sleep(5)
        except NotFound:
            logging.info('Instance not found, may have been already deleted')

        self.public_network.release(self.floating_ip)

    @os_utils.retry_authorization(max_retries=4)
    def _Exists(self):
        from novaclient.exceptions import NotFound
        try:
            return self.client.servers.get(self.id) is not None
        except NotFound:
            return False

    @vm_util.Retry(log_errors=False, poll_interval=1)
    def WaitForBootCompletion(self):
        # Do one longer sleep, then check at shorter intervals.
        if self.boot_wait_time is None:
          self.boot_wait_time = 15
        time.sleep(self.boot_wait_time)
        self.boot_wait_time = 5
        resp, _ = self.RemoteCommand('hostname', retries=1)
        if self.bootable_time is None:
            self.bootable_time = time.time()
        if self.hostname is None:
            self.hostname = resp[:-1]

    def OnStartup(self):
        super(OpenStackVirtualMachine, self).OnStartup()
        self.boot_device = self._GetBootDevice()

    def CreateScratchDisk(self, disk_spec):
        """
        Creates instances of OpenStackDisk child classes depending on disk type.
        """
        if disk_spec.disk_type == os_disk.ROOT:  # Ignore num_striped_disks
            if self.root_disk_allocated:
                raise errors.Error('Only a Root disk can be created per VM')
            device_path = '/dev/%s' % self.boot_device['name']
            scratch_disk = os_disk.OpenStackRootDisk(disk_spec, device_path)
            self.root_disk_allocated = True
            self.scratch_disks.append(scratch_disk)
            scratch_disk.Create()
            path = disk_spec.mount_point
            mk_cmd = ('sudo mkdir -p {0};'
                      'sudo chown -R $USER:$USER {0};').format(path)
            self.RemoteCommand(mk_cmd)
        else:
            scratch_disks = []
            for disk_num in range(disk_spec.num_striped_disks):
                if disk_spec.disk_type == os_disk.LOCAL:
                    scratch_disks.extend(self._CreateLocalDisks(disk_spec))
                elif disk_spec.disk_type == os_disk.VOLUME:
                    scratch_disk = os_disk.OpenStackBlockStorageDisk(disk_spec,
                                                                     self.name,
                                                                     self.zone)
                    scratch_disks.append(scratch_disk)
                else:
                    raise errors.Error('Unsupported data disk type: %s'
                                       % disk_spec.disk_type)

            self._CreateScratchDiskFromDisks(disk_spec, scratch_disks)

    def _CreateLocalDisks(self, disk_spec):
        blk_devices = self._GetBlockDevices()
        extra_blk_devices = []
        for dev in blk_devices:
            if self._IsDiskAvailable(dev):
                extra_blk_devices.append(dev)

        if len(extra_blk_devices) == 0:
          raise errors.Error(
              ''.join(('Machine type %s does not include' % self.machine_type,
                       ' local disks. Please use a different disk_type,',
                       ' or a machine_type that provides local disks.')))
        elif len(extra_blk_devices) < disk_spec.num_striped_disks:
          raise errors.Error(
              'Not enough local data disks.'
              ' Requesting %d disk, but only %d available.'
              % (disk_spec.num_striped_disks, len(extra_blk_devices)))

        disks = []
        for i in range(disk_spec.num_striped_disks):
            local_device = extra_blk_devices[i]
            local_disk = os_disk.OpenStackLocalDisk(
                disk_spec, '/dev/%s' % local_device['name'], self.name)
            self.mounted_disks.add(local_device['name'])
            disks.append(local_disk)

        return disks

    def _IsDiskAvailable(self, blk_device):
        return (blk_device['type'] != 'part' and
                blk_device['name'] != self.boot_device['name'] and
                'config' not in blk_device['label'] and
                blk_device['name'] not in self.mounted_disks)

    def _CreateDependencies(self):
        self.ImportKeyfile()
        self.AllowRemoteAccessPorts()

    def _DeleteDependencies(self):
        self.DeleteKeyfile()

    def ImportKeyfile(self):
        if not (self.client.keypairs.findall(name=self.key_name)):
            cat_cmd = ['cat',
                       vm_util.GetPublicKeyPath()]
            key_file, _ = vm_util.IssueRetryableCommand(cat_cmd)
            pk = self.client.keypairs.create(self.key_name,
                                             public_key=key_file)
        else:
            pk = self.client.keypairs.findall(name=self.key_name)[0]
        self.pk = pk

    @os_utils.retry_authorization(max_retries=4)
    def DeleteKeyfile(self):
        from novaclient.exceptions import NotFound
        try:
            self.client.keypairs.delete(self.pk)
        except NotFound:
            logging.info("Deleting key doesn't exists")

    def _GetBlockDevices(self):
        stdout, _ = self.RemoteCommand(
            'sudo lsblk -o NAME,MODEL,SIZE,TYPE,MOUNTPOINT,LABEL -n -b -P')
        lines = stdout.splitlines()
        groups = [LSBLK_PATTERN.match(line) for line in lines]
        tuples = [g.groups() for g in groups if g]
        colnames = ('name', 'model', 'size_bytes',
                    'type', 'mountpoint', 'label',)
        blk_devices = [dict(zip(colnames, t)) for t in tuples]
        for d in blk_devices:
            d['model'] = d['model'].rstrip()
            d['label'] = d['label'].rstrip()
            d['size_bytes'] = int(d['size_bytes'])
        return blk_devices

    def _GetBootDevice(self):
        blk_devices = self._GetBlockDevices()
        boot_blk_device = None
        for dev in blk_devices:
            if dev['mountpoint'] == '/':
                boot_blk_device = dev
                break

        if boot_blk_device is None:  # Unlikely
            raise errors.Error('Could not find disk with "/" root mount point.')

        if boot_blk_device['type'] == 'part':
            blk_device_name = boot_blk_device['name'].rstrip('0123456789')
            for dev in blk_devices:
                if dev['type'] == 'disk' and dev['name'] == blk_device_name:
                    boot_blk_device = dev
                    break
            else:  # Also, unlikely
                raise errors.Error('Could not find disk containing boot '
                                   'partition.')
        return boot_blk_device


class DebianBasedOpenStackVirtualMachine(OpenStackVirtualMachine,
                                         linux_virtual_machine.DebianMixin):
    DEFAULT_IMAGE = UBUNTU_IMAGE
