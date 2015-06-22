import logging
import random
import string
import time

from perfkitbenchmarker import virtual_machine, linux_virtual_machine
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.openstack import utils as os_utils, os_disk


FLAGS = flags.FLAGS

flags.DEFINE_string('os_auth_url', 'http://localhost:5000',
                    'This determine url to Keystone authenticate service.'
                    'It is require to discovery other OpenStack services URLs')

flags.DEFINE_string('os_username', 'admin',
                    'OpenStack login')

flags.DEFINE_string('os_passwd', 'admin',
                    'OpenStack password')

flags.DEFINE_string('os_tenant', 'admin',
                    'OpenStack tenant name')

flags.DEFINE_boolean('os_config_drive', False,
                     'Add possibilities to get metadata from external drive')


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


class OpenStackVirtualMachine(virtual_machine.BaseVirtualMachine):
    """Object representing an OpenStack Virtual Machine"""

    instance_counter = 0

    def __init__(self, vm_spec):
        super(OpenStackVirtualMachine, self).__init__(vm_spec)
        self.special_id = id_generator()
        self.name = 'perfkit_vm_%d_%s' % (self.instance_counter, self.special_id)
        self.key_name = 'perfkit_key_%d_%s' % (self.instance_counter, self.special_id)
        OpenStackVirtualMachine.instance_counter += 1
        self.client = os_utils.NovaClient(FLAGS.os_auth_url,
                                          FLAGS.os_username,
                                          FLAGS.os_passwd,
                                          FLAGS.os_tenant)
        self.id = -1
        self.pk = -1
        self.user_name = 'ubuntu'
        self.wait_time = 15

    def _Create(self):
        super(OpenStackVirtualMachine, self)._Create()
        image = self.client.images.findall(name=self.image)[0]
        flavor = self.client.flavors.findall(name=self.machine_type)[0]

        network = self.client.networks.find(label=FLAGS.os_private_network)
        nics = [{'net-id': network.id}]

        vm = self.client.servers.create(name=self.name,
                                        image=image.id,
                                        flavor=flavor.id,
                                        key_name=self.key_name,
                                        security_groups=['perfkit_sc_group'],
                                        nics=nics,
                                        availability_zone='nova',
                                        config_drive=FLAGS.os_config_drive)
        self.id = vm.id

    @vm_util.Retry(max_retries=4, poll_interval=2)
    def _PostCreate(self):
        import time

        status = 'BUILD'
        instance = None
        while status == 'BUILD':
            time.sleep(5)
            instance = self.client.servers.get(self.id)
            status = instance.status
        self.floating_ip = self.client.floating_ips.create(pool=FLAGS.os_public_network)
        instance.add_floating_ip(self.floating_ip)
        self.ip_address = self.floating_ip.ip
        self.internal_ip = instance.networks[FLAGS.os_private_network][0]

    @os_utils.retry_authorization(max_retries=4)
    def _Delete(self):
        try:
            self.client.servers.delete(self.id)
            time.sleep(5)
            self.client.floating_ips.delete(self.floating_ip)
        except os_utils.NotFound:
            logging.info('Instance already deleted')

    @os_utils.retry_authorization(max_retries=4)
    def _Exists(self):
        try:
            if self.client.servers.findall(name=self.name):
                return True
            else:
                return False
        except os_utils.NotFound:
            return False

    @vm_util.Retry(log_errors=False, poll_interval=1)
    def WaitForBootCompletion(self):
        time.sleep(self.wait_time)
        self.wait_time = 5
        resp, _ = self.RemoteCommand('hostname', retries=1)
        if self.bootable_time is None:
            self.bootable_time = time.time()
        if self.hostname is None:
            self.hostname = resp[:-1]

    def CreateScratchDisk(self, disk_spec):
        name = '%s-scratch-%s' % (self.name, len(self.scratch_disks))
        scratch_disk = os_disk.OpenStackDisk(disk_spec, name, self.zone, self.project)
        self.scratch_disks.append(scratch_disk)

        scratch_disk.Create()
        scratch_disk.Attach(self)

        self.FormatDisk(scratch_disk.GetDevicePath())
        self.MountDisk(scratch_disk.GetDevicePath(), disk_spec.mount_point)

    def _CreateDependencies(self):
        self.ImportKeyfile()

    def _DeleteDependencies(self):
        self.DeleteKeyfile()

    def ImportKeyfile(self):
        if not (self.client.keypairs.findall(name=self.key_name)):
            cat_cmd = ['cat',
                       vm_util.GetPublicKeyPath()]
            key_file, _ = vm_util.IssueRetryableCommand(cat_cmd)
            pk = self.client.keypairs.create(self.key_name, public_key=key_file)
        else:
            pk = self.client.keypairs.findall(name=self.key_name)[0]
        self.pk = pk

    @os_utils.retry_authorization(max_retries=4)
    def DeleteKeyfile(self):
        try:
            self.client.keypairs.delete(self.pk)
        except os_utils.NotFound:
            logging.info("Deleting key doesn't exists")


class DebianBasedOpenStackVirtualMachine(OpenStackVirtualMachine,
                                         linux_virtual_machine.DebianMixin):
    pass
