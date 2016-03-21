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
import threading
import time

from perfkitbenchmarker import virtual_machine, linux_virtual_machine
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import providers
from perfkitbenchmarker.providers.openstack import os_disk
from perfkitbenchmarker.providers.openstack import os_network
from perfkitbenchmarker.providers.openstack import utils as os_utils

RHEL_IMAGE = 'rhel-7.2'
UBUNTU_IMAGE = 'ubuntu-14.04'
NONE = 'None'

FLAGS = flags.FLAGS


class OpenStackVirtualMachine(virtual_machine.BaseVirtualMachine):
    """Object representing an OpenStack Virtual Machine"""

    CLOUD = providers.OPENSTACK
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
        self.firewall.AllowICMP(self)
        self.firewall.AllowPort(self, 1, os_network.MAX_PORT)
        self.name = 'perfkit_vm_%d_%s' % (self.instance_number, FLAGS.run_uri)
        self.key_name = 'perfkit_key_%d_%s' % (self.instance_number,
                                               FLAGS.run_uri)
        self.client = os_utils.NovaClient()
        # FIXME(meteorfox): Remove --openstack_public_network and
        # --openstack_private_network once depreciation time has expired
        self.network_name = (FLAGS.openstack_network or
                             FLAGS.openstack_private_network)
        self.floating_ip_pool_name = (FLAGS.openstack_floating_ip_pool or
                                      FLAGS.openstack_public_network)
        self.public_network = os_network.OpenStackPublicNetwork(
            FLAGS.openstack_floating_ip_pool)
        self.id = None
        self.pk = None
        self.user_name = FLAGS.openstack_image_username
        self.boot_wait_time = None
        self.image = self.image or self.DEFAULT_IMAGE
        self.public_net = None
        self.private_net = None
        self.floating_ip = None

    def _Create(self):
        image = self.client.images.findall(name=self.image)[0]
        flavor = self.client.flavors.findall(name=self.machine_type)[0]
        self.private_net = self.client.networks.find(label=self.network_name)
        if self.floating_ip_pool_name:
            self.public_net = self.client.networks.find(
                label=self.floating_ip_pool_name)

        if not self.private_net:
            if self.public_net:
                raise errors.Error(
                    'Cannot associate floating-ip address from pool %s without '
                    'an internally routable network. Make sure '
                    '--openstack_network flag is set.')
            else:
                raise errors.Error(
                    'Cannot build instance without a network. Make sure to set '
                    'either just --openstack_network or both '
                    '--openstack_network and --openstack_floating_ip_pool '
                    'flags.')

        nics = [{'net-id': self.private_net.id}]

        image_id = image.id
        boot_from_vol = []
        scheduler_hints = self._GetSchedulerHints()

        if FLAGS.openstack_boot_from_volume:
            volume_size = FLAGS.openstack_volume_size or flavor.disk
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

    def _GetSchedulerHints(self):
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
        return scheduler_hints

    @vm_util.Retry(max_retries=4, poll_interval=2)
    def _PostCreate(self):
        status = 'BUILD'
        instance = None
        while status == 'BUILD':
            time.sleep(5)
            instance = self.client.servers.get(self.id)
            status = instance.status
        # Unlikely to be false, previously checked to be true in self._Create()
        assert self.private_net is not None, '--openstack_network must be set.'
        self.internal_ip = instance.networks[self.network_name][0]
        self.ip_address = self.internal_ip
        if self.public_net:
            self.floating_ip = self._AllocateFloatingIP(instance)
            self.ip_address = self.floating_ip.ip

    def _AllocateFloatingIP(self, instance):
        with self._floating_ip_lock:
            floating_ip = self.public_network.get_or_create()
            instance.add_floating_ip(floating_ip)
            logging.info(
                'floating-ip associated: {}'.format(floating_ip.ip))
        while not self.public_network.is_attached(floating_ip):
            time.sleep(1)
        return floating_ip

    def _Delete(self):
        from novaclient.exceptions import NotFound
        try:
            self.client.servers.delete(self.id)
            self._WaitForDeleteCompletion()
        except NotFound:
            logging.info('Instance not found, may have been already deleted')

        if self.floating_ip:
            self.public_network.release(self.floating_ip)

    def _Exists(self):
        from novaclient.exceptions import NotFound
        try:
            return self.client.servers.get(self.id) is not None
        except NotFound:
            return False

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

    @vm_util.Retry(poll_interval=5, max_retries=-1, timeout=300,
                   log_errors=False,
                   retryable_exceptions=(
                       errors.Resource.RetryableDeletionError,))
    def _WaitForDeleteCompletion(self):
        instance = self.client.servers.get(self.id)
        if instance and instance.status == 'ACTIVE':
            raise errors.Resource.RetryableDeletionError(
                'VM: %s has not been deleted. Retrying to check status.'
                % self.name)

    def CreateScratchDisk(self, disk_spec):
        disks_names = ('%s-data-%d-%d'
                       % (self.name, len(self.scratch_disks), i)
                       for i in range(disk_spec.num_striped_disks))
        disks = [os_disk.OpenStackDisk(disk_spec, name, self.zone)
                 for name in disks_names]

        self._CreateScratchDiskFromDisks(disk_spec, disks)

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

    def DeleteKeyfile(self):
        from novaclient.exceptions import NotFound
        try:
            self.client.keypairs.delete(self.pk)
        except NotFound:
            logging.info("Deleting key doesn't exists")


class DebianBasedOpenStackVirtualMachine(OpenStackVirtualMachine,
                                         linux_virtual_machine.DebianMixin):
    DEFAULT_IMAGE = UBUNTU_IMAGE


class RhelBasedOpenStackVirtualMachine(OpenStackVirtualMachine,
                                       linux_virtual_machine.RhelMixin):
    DEFAULT_IMAGE = RHEL_IMAGE
