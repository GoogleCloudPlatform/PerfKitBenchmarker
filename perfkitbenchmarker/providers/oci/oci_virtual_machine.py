"""Class to represent an Oracle Virtual Machine object.

Machine Types:
https://docs.oracle.com/en-us/iaas/Content/Compute/References/computeshapes.htm

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""


import itertools
import json
import logging
import threading

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import providers
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.oci import util, oci_disk, oci_network

FLAGS = flags.FLAGS
SSH_PORT = 22
INSTANCE_EXISTS_STATUSES = frozenset(
    ['CREATING_IMAGE', 'MOVING', 'PROVISIONING', 'RUNNING', 'STARTING', 'STOPPED', 'STOPPING', 'TERMINATED',
     'TERMINATING'
     ])


class OciVmSpec(virtual_machine.BaseVmSpec):
    CLOUD = providers.OCI

    def __init__(self, *args, **kwargs):
        self.num_local_ssds: int = None
        super(OciVmSpec, self).__init__(*args, **kwargs)

    @classmethod
    def _ApplyFlags(cls, config_values, flag_values):
        super(OciVmSpec, cls)._ApplyFlags(config_values, flag_values)
        if flag_values['oci_compute_units'].present:
            config_values['oci_compute_units'] = flag_values.oci_compute_units
        if flag_values['oci_compute_memory'].present:
            config_values['oci_compute_memory'] = flag_values.oci_compute_memory
        if flag_values['oci_availability_domain'].present:
            config_values['oci_availability_domain'] = flag_values.oci_availability_domain
        if flag_values['oci_fault_domain'].present:
            config_values['oci_fault_domain'] = flag_values.oci_fault_domain
        if flag_values['oci_boot_disk_size'].present:
            config_values['oci_boot_disk_size'] = flag_values.oci_boot_disk_size
        if flag_values['oci_use_vcn'].present:
            config_values['oci_use_vcn'] = flag_values.oci_use_vcn
        if flag_values['oci_num_local_ssds'].present:
            config_values['num_local_ssds'] = flag_values.oci_num_local_ssds
        if flag_values['machine_type'].present:
            config_values['machine_type'] = flag_values.machine_type
        if flag_values['oci_network_name'].present:
            config_values['oci_network_name'] = flag_values.oci_network_name
        if flag_values['oci_profile'].present:
            config_values['oci_profile'] = flag_values.oci_profile

    @classmethod
    def _GetOptionDecoderConstructions(cls):
        """Gets decoder classes and constructor args for each configurable option.

        Returns:
          dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
              The pair specifies a decoder class and its __init__() keyword
              arguments to construct in order to decode the named option.
        """
        result = super(OciVmSpec, cls)._GetOptionDecoderConstructions()
        result.update({
            'oci_compute_units': (option_decoders.IntDecoder, {'default': None}),
            'oci_compute_memory': (option_decoders.IntDecoder, {'default': None}),
            'oci_availability_domain': (option_decoders.StringDecoder, {'default': None}),
            'oci_fault_domain': (option_decoders.StringDecoder, {'default': None}),            
            'oci_boot_disk_size': (option_decoders.IntDecoder, {'default': 50}),
            'oci_use_vcn': (option_decoders.BooleanDecoder, {'default': True}),
            'num_local_ssds': (option_decoders.IntDecoder, {'default': 0}),
            'machine_type': (option_decoders.StringDecoder, {'default': 'VM.Standard.A1.Flex'}),
            'region': (option_decoders.StringDecoder, {'default': None}),
            'oci_network_name': (option_decoders.StringDecoder, {'default': None}),
            'oci_profile': (option_decoders.StringDecoder, {'default': 'DEFAULT'}),
        })
        return result


class OciVirtualMachine(virtual_machine.BaseVirtualMachine):
    CLOUD = providers.OCI

    _counter_lock = threading.Lock()
    _counter = itertools.count()

    def __init__(self, vm_spec):
        super(OciVirtualMachine, self).__init__(vm_spec)
        with self._counter_lock:
            self.instance_number = next(self._counter)

        MAX_LOCAL_DISKS = 32
        self.name = 'perfkit-%s-%s' % (FLAGS.run_uri, self.instance_number)
        self.ocid = ''
        self.image = vm_spec.image or None
        self.operating_system = None
        self.operating_system_version = None
        self.key_pair_name = ""
        #self.profile = vm_spec.oci_profile
        #profiles are maintained in ~/.oci/oci_cli_rc file
        self.profile = vm_spec.zone #profiles are maintained in ~/.oci/oci_cli_rc file
        self.region = vm_spec.zone
        self.subnet = None
        self.availability_domain = vm_spec.oci_availability_domain
        self.fault_domain = vm_spec.oci_fault_domain        
        self.machine_type = vm_spec.machine_type
        self.compute_units = vm_spec.oci_compute_units
        self.compute_memory = vm_spec.oci_compute_memory
        self.bv_size = vm_spec.oci_boot_disk_size
        self.ip_address = None
        self.internal_ip = None
        self.status = None
        self.user_name = 'perfkit'
        self.network = oci_network.OciNetwork.GetNetwork(self)
        self.local_disk_counter = 0
        self.num_local_ssds = vm_spec.num_local_ssds
        self.max_local_disks = MAX_LOCAL_DISKS
        self.tags = util.MakeFormattedDefaultTags()
        self.firewall = oci_network.OCIFirewall.GetFirewall()

    @vm_util.Retry(poll_interval=60, log_errors=False)
    def _WaitForInstanceStatus(self, status_list):
        """Waits until the instance's status is in status_list."""
        logging.info('Waits until the instance\'s status is one of statuses: %s',
                     status_list)
        status_cmd = util.OCI_PREFIX + [
            'compute',
            'instance',
            'list',
            f'--display-name {self.name}',
            f'--profile {self.profile}',
            '--sort-order DESC']
        status_cmd = util.GetEncodedCmd(status_cmd)
        out, _ = vm_util.IssueRetryableCommand(status_cmd)
        state = json.loads(out)
        check_state = state['data'][0]['lifecycle-state']
        self.status = check_state
        assert check_state in status_list

    @vm_util.Retry(poll_interval=5, log_errors=False)
    def _WaitForIPStatus(self, status_list):
        """Waits until the instance's status is in status_list."""
        logging.info('Waits until the instance\'s status is one of statuses: %s',
                     status_list)
        ipstatus_cmd = util.OCI_PREFIX + [
            'compute',
            'instance',
            'list-vnics',
            f'--instance-id {self.ocid}',
            f'--profile {self.profile}']
        ipstatus_cmd = util.GetEncodedCmd(ipstatus_cmd)
        out, _ = vm_util.IssueRetryableCommand(ipstatus_cmd)
        state = json.loads(out)
        check_state = state['data'][0]['lifecycle-state']
        assert check_state in status_list

    def _Create(self):
        if self.compute_units is None:
            self.compute_units = 1

        if self.compute_memory is None:
            self.compute_memory = self.compute_units * 4
        ad_list = []
        if self.availability_domain is None:
            ad_list = util.GetAvailabilityDomainFromRegion(self.region)
            self.availability_domain = ad_list[0]
        if self.fault_domain is None:
            fd_list = util.GetFaultDomainFromAvailabilityDomain(self.availability_domain, self.profile)
            self.fault_domain = fd_list[0]

        if self.image is not None:
            oci_image, oci_os_name, oci_os_version = util.GetOciImageIdFromName(self.image, self.machine_type, self.profile)
        else:
            oci_os_name = util.GetOsFromImageFamily(self.DEFAULT_IMAGE_FAMILY)
            oci_os_version = util.GetOsVersionFromOs(self.DEFAULT_IMAGE_PROJECT, oci_os_name)
            oci_image = util.GetOciImageIdFromImage(oci_os_name, oci_os_version, self.machine_type, self.profile)
        self.image = oci_image

        shape_config = "'{\"memoryInGBs\":%s,\"ocpus\":%s}'" % (self.compute_memory, self.compute_units)

        key_file_path = vm_util.GetPublicKeyPath()

        public_key = util.GetPublicKey()

        if "Oracle" in oci_os_name:
            user_data = util.ADD_CLOUDINIT_ORACLE_TEMPLATE.format(user_name=self.user_name,
                                                                  public_key=public_key)
        else:
            user_data = util.ADD_CLOUDINIT_TEMPLATE.format(user_name=self.user_name,
                                                           public_key=public_key)
        user_data_filepath = '/tmp/user_data-' + self.name + '.sh'
        with open(user_data_filepath, 'w') as user_data_file:
            user_data_file.write(user_data)

        create_cmd = util.OCI_PREFIX + [
            'compute',
            'instance',
            'launch',
            f'--subnet-id {self.network.network_id}',
            f'--display-name {self.name}',
            f'--hostname-label {self.name}',
            f'--region {self.region}',
            f'--availability-domain {self.availability_domain}',
            f'--fault-domain {self.fault_domain}',
            f'--image-id {self.image}',
            f'--shape {self.machine_type}',
            '--shape-config ',
            f' {shape_config}',
            f'--user-data-file {user_data_filepath}',
            f' --boot-volume-size-in-gbs {self.bv_size}',
            f'--freeform-tags {self.tags}',
            f'--ssh-authorized-keys-file {key_file_path}',
            '--assign-public-ip true',
            f'--profile {self.profile}']
        create_cmd = util.GetEncodedCmd(create_cmd)
        stdout, _, ret = vm_util.IssueCommand(create_cmd)
        ociid = json.loads(stdout)
        self.ocid = ociid['data']['id']
        self._WaitForInstanceStatus(['RUNNING'])
        self._GetPublicIP()

    def _GetPublicIP(self):
        self._WaitForIPStatus(['AVAILABLE', 'RUNNING'])
        ip_cmd = util.OCI_PREFIX + [
            'compute',
            'instance',
            'list-vnics',
            f'--instance-id {self.ocid}',
            f'--profile {self.profile}']
        ip_cmd = util.GetEncodedCmd(ip_cmd)
        out, _, _ = vm_util.IssueCommand(ip_cmd)
        ips = json.loads(out)
        self.internal_ip = ips['data'][0]['private-ip']
        self.ip_address = ips['data'][0]['public-ip']

    def _Delete(self):
        if self.status == 'RUNNING':
            delete_cmd = util.OCI_PREFIX + [
                'compute',
                'instance',
                'terminate',
                f'--instance-id {self.ocid}',
                '--preserve-boot-volume false',
                f'--profile {self.profile}',
                '--force']
            delete_cmd = util.GetEncodedCmd(delete_cmd)
            out, _ = vm_util.IssueRetryableCommand(delete_cmd)
            self._WaitForInstanceStatus(['TERMINATED'])

    def _Exists(self):
        """Returns true if the VM exists."""
        if self.status == 'TERMINATED':
            return False
        return self.status in INSTANCE_EXISTS_STATUSES

    def CreateScratchDisk(self, disk_spec_id, disk_spec):
        """Create a VM's scratch disk.

        Args:
          disk_spec: virtual_machine.BaseDiskSpec object of the disk.
        """
        disk_number = disk_spec_id
        self.local_disk_counter += 1
        if self.local_disk_counter > self.max_local_disks:
            raise errors.Error('Not enough local disks.')
        logging.info("Now starting to create disks")
        data_disk = oci_disk.OciDisk(disk_spec, self.profile, self.name, self.availability_domain, disk_number)
        self.scratch_disks.append(data_disk)
        data_disk.Create()
        data_disk.GetFreeDeviceName(self)
        data_disk.Attach(disk_spec, self)
        if disk_spec.disk_type == 'iscsi':
            data_disk.ExecuteAttachIscsiCommands(self)
        self.FormatDisk(data_disk.GetDevicePath(), disk.LOCAL)
        self.MountDisk(data_disk.GetDevicePath(), disk_spec.mount_point,
                       disk.LOCAL, data_disk.mount_options,
                       data_disk.fstab_options)
        
    def AllowPort(self, start_port, end_port=None, source_range=None):

        # TODO: Potentially replace for case where firewall skip flag is in place
        super(OciVirtualMachine, self).AllowPort(start_port, end_port, source_range)

    #def _PostCreate(self):
    #    self.firewall.AllowPort(self, SSH_PORT, SSH_PORT)

class Ubuntu2204BasedOCIVirtualMachine(OciVirtualMachine,
                                       linux_virtual_machine.Ubuntu2204Mixin):
    DEFAULT_IMAGE_FAMILY = 'ubuntu-os-cloud'
    DEFAULT_IMAGE_PROJECT = 'ubuntu-2204-lts'


class Ubuntu2004BasedOCIVirtualMachine(OciVirtualMachine,
                                       linux_virtual_machine.Ubuntu2004Mixin):
    DEFAULT_IMAGE_FAMILY = 'ubuntu-os-cloud'
    DEFAULT_IMAGE_PROJECT = 'ubuntu-2004-lts'


class Ubuntu1804BasedOCIVirtualMachine(OciVirtualMachine,
                                       linux_virtual_machine.Ubuntu1804Mixin):
    DEFAULT_IMAGE_FAMILY = 'ubuntu-os-cloud'
    DEFAULT_IMAGE_PROJECT = 'ubuntu-1804-lts'


class Oracle9BasedVirtualMachine(OciVirtualMachine,
                                 linux_virtual_machine.Oracle9Mixin):
    DEFAULT_IMAGE_FAMILY = 'Oracle Linux'
    DEFAULT_IMAGE_PROJECT = '9'


class Oracle8BasedVirtualMachine(OciVirtualMachine,
                                 linux_virtual_machine.Oracle8Mixin):
    DEFAULT_IMAGE_FAMILY = 'Oracle Linux'
    DEFAULT_IMAGE_PROJECT = '8'
