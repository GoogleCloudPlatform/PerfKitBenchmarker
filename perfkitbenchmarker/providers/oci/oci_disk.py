
"""Module containing classes related to Oracle disks."""

import json
import logging
import threading
from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.oci import util

FLAGS = flags.FLAGS

# https://docs.oracle.com/en-us/iaas/Content/Block/Concepts/blockvolumeperformance.htm

# Acceptable values for vpus per GB is
# 0: Represents Lower Cost option.
# 10: Represents Balanced option.
# 20: Represents Higher Performance option.
DEFAULT_VPUS_PER_GB = 20

DISK_CREATE_STATUSES = frozenset(
    ['AVAILABLE', 'FAULTY', 'PROVISIONING', 'RESTORING', 'TERMINATED', 'TERMINATING']
)

DISK_ATTACH_STATUS = frozenset(
    ['ATTACHED', 'ATTACHING', 'DETACHED', 'DETACHING']
)


class OciDisk(disk.BaseDisk):
    _lock = threading.Lock()
    vm_devices = {}

    def __init__(self, disk_spec, profile, vm_name, availability_domain, disk_number):
        super(OciDisk, self).__init__(disk_spec)
        self.id = None
        self.profile = profile
        self.availability_domain = availability_domain
        self.disk_size = disk_spec.disk_size or 100
        self.vpus_per_gb: int = DEFAULT_VPUS_PER_GB
        self.status = None
        self.name = f'{vm_name}-{disk_number}'
        self.attachment_id = None
        self.device_name = None
        self.iqn: Optional[str] = None
        self.port: Optional[str] = None
        self.ipv4: Optional[str] = None
        self.tags = util.MakeFormattedDefaultTags()


    def _Create(self):
        """Creates the disk."""
        create_cmd = util.OCI_PREFIX + [
            'bv',
            'volume',
            'create',
            f'--availability-domain {self.availability_domain}',
            f'--size-in-gbs {self.disk_size}',
            f'--display-name {self.name}',
            f'--freeform-tags {self.tags}',
            f'--vpus-per-gb {str(self.vpus_per_gb)}',
            f'--profile {self.profile}']
        create_cmd = util.GetEncodedCmd(create_cmd)
        stdout, _, _ = vm_util.IssueCommand(create_cmd, raise_on_failure=False)
        response = json.loads(stdout)
        self.id = response['data']['id']
        self._WaitForDiskStatus(['AVAILABLE'])

    def _Delete(self):
        """Deletes the disk."""
        # oci bv volume delete
        delete_cmd = util.OCI_PREFIX + [
            'bv',
            'volume ',
            'delete',
            f'--volume-id {self.id}',
            f'--profile {self.profile}',
            '--force']
        delete_cmd = util.GetEncodedCmd(delete_cmd)
        out, _ = vm_util.IssueRetryableCommand(delete_cmd)
        self._WaitForDiskStatus(['TERMINATED'])

    @vm_util.Retry(poll_interval=60, log_errors=False)
    def _WaitForDiskStatus(self, status_list):
        """Waits until the disk's status is in status_list."""
        logging.info('Waiting until the instance status is: %s' % status_list)
        status_cmd = util.OCI_PREFIX + [
            'bv',
            'volume',
            'get',
            f'--volume-id {self.id}',
            f'--profile {self.profile}']
        status_cmd = util.GetEncodedCmd(status_cmd)
        out, _ = vm_util.IssueRetryableCommand(status_cmd)
        state = json.loads(out)
        check_state = state['data']['lifecycle-state']
        self.status = check_state
        assert check_state in status_list

    def Attach(self, disk_spec, vm):
        attach_cmd = util.OCI_PREFIX + [
            'compute',
            'volume-attachment',
            'attach',
            f'--volume-id {self.id}',
            f'--instance-id {vm.ocid}',
            f'--type {self.disk_type}',
            f'--device {self.device_name}',
            f'--profile {self.profile}']
        logging.info('Attaching Oci disk %s.' % self.id)
        attach_cmd = util.GetEncodedCmd(attach_cmd)
        stdout, _ = vm_util.IssueRetryableCommand(attach_cmd)
        response = json.loads(stdout)
        self.attachment_id = response['data']['id']
        self._WaitForDiskAttachStatus(disk_spec, ['ATTACHED'])

    def Detach(self, disk_spec, vm):
        if disk_spec.disk_type == 'iscsi':
            self.ExecuteDetachIscsiCommands(vm)
        detach_cmd = util.OCI_PREFIX + [
            'compute',
            'volume-attachment',
            'detach',
            f'--volume-attachment-id {self.attachment_id}',
            f'--profile {self.profile}',
            '--force']
        logging.info('Detaching Oci disk %s.' % self.id)
        detach_cmd = util.GetEncodedCmd(detach_cmd)
        out, _ = vm_util.IssueRetryableCommand(detach_cmd)
        self._WaitForDiskAttachStatus(disk_spec, ['DETACHED'])

    @vm_util.Retry(poll_interval=60, log_errors=False)
    def _WaitForDiskAttachStatus(self, disk_spec, status_list):
        """Waits until the disk's attach status is in status_list."""
        logging.info('Waiting until the instance status is : %s' % status_list)
        status_cmd = util.OCI_PREFIX + [
            'compute',
            'volume-attachment',
            'get',
            f'--volume-attachment-id {self.attachment_id}',
            f'--profile {self.profile}']
        status_cmd = util.GetEncodedCmd(status_cmd)
        out, _ = vm_util.IssueRetryableCommand(status_cmd)
        state = json.loads(out)        
        check_state = state['data']['lifecycle-state']
        self.status = check_state
        if self.status == 'ATTACHED' and disk_spec.disk_type == 'iscsi':
            self.iqn = state['data']['iqn']
            self.ipv4 = state['data']['ipv4']
            self.port = state['data']['port']
        assert check_state in status_list

    def ExecuteAttachIscsiCommands(self, vm):
        vm.RemoteCommand(f"sudo iscsiadm -m node -o new -T {self.iqn} -p {self.ipv4}:{self.port}")
        vm.RemoteCommand(f"sudo iscsiadm -m node -o update -T {self.iqn} -n node.startup -v automatic")
        vm.RemoteCommand(f"sudo iscsiadm -m node -T {self.iqn} -p {self.ipv4}:{self.port} -l")

    def ExecuteDetachIscsiCommands(self, vm):
        vm.RemoteCommand(f"sudo iscsiadm -m node -T {self.iqn} -p {self.ipv4}:{self.port} -u")
        vm.RemoteCommand(f"sudo iscsiadm -m node -o delete -T {self.iqn} -p {self.ipv4}:{self.port}")

    def GetDevicePath(self):
        """Returns the path to the device inside the VM."""
        return self.device_name

    def GetFreeDeviceName(self, vm):
        free_device_cmd = util.OCI_PREFIX + [
            'compute',
            'device',
            'list-instance',
            f'--instance-id {vm.ocid}',
            f'--profile {self.profile}']
        free_device_cmd = util.GetEncodedCmd(free_device_cmd)
        out, _ = vm_util.IssueRetryableCommand(free_device_cmd)
        stdout, _ = vm_util.IssueRetryableCommand(free_device_cmd)
        response = json.loads(stdout)
        for free_disk in range(0, 31):
            if response['data'][free_disk]['is-available'] is True:
                self.device_name = response['data'][free_disk]['name']
                break
