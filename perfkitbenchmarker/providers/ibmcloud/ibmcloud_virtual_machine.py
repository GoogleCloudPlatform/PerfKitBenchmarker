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
"""Class to represent a IBM Cloud Virtual Machine."""

import base64
import json
import logging
import os
import sys
import threading
import time

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import providers
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_virtual_machine
from perfkitbenchmarker.providers.ibmcloud import ibm_api as ibm
from perfkitbenchmarker.providers.ibmcloud import ibmcloud_disk
from perfkitbenchmarker.providers.ibmcloud import ibmcloud_network
from perfkitbenchmarker.providers.ibmcloud import util

FLAGS = flags.FLAGS

_DEFAULT_VOLUME_IOPS = 3000
_WAIT_TIME_DEBIAN = 60
_WAIT_TIME_RHEL = 60
_WAIT_TIME_UBUNTU = 600


class IbmCloudVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a IBM Cloud Virtual Machine."""

  CLOUD = providers.IBMCLOUD
  IMAGE_NAME_PREFIX = None

  _lock = threading.Lock()
  validated_resources_set = set()
  validated_subnets = 0

  def __init__(self, vm_spec: virtual_machine.BaseVmSpec):
    """Initialize a IBM Cloud virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(IbmCloudVirtualMachine, self).__init__(vm_spec)
    self.user_name = FLAGS.ibmcloud_image_username
    self.boot_volume_size = FLAGS.ibmcloud_boot_volume_size
    self.boot_volume_iops = FLAGS.ibmcloud_boot_volume_iops
    self.volume_iops = _DEFAULT_VOLUME_IOPS
    if FLAGS.ibmcloud_volume_iops:
      self.volume_iops = FLAGS.ibmcloud_volume_iops
    self.volume_profile = FLAGS.ibmcloud_volume_profile
    self.image = FLAGS.image
    self.os_data = None
    self.user_data = None
    self.vmid = None
    self.vm_created = False
    self.vm_deleted = False
    self.profile = FLAGS.machine_type
    self.prefix = FLAGS.ibmcloud_prefix
    self.zone = 'us-south-1'  # default
    self.fip_address = None
    self.fip_id = None
    self.network = None
    self.subnet = FLAGS.ibmcloud_subnet
    self.subnets = {}
    self.vpcid = FLAGS.ibmcloud_vpcid
    self.key = FLAGS.ibmcloud_pub_keyid
    self.boot_encryption_key = None
    self.data_encryption_key = None
    self.extra_vdisks_created = False
    self.device_paths_detected = set()

  def _CreateRiasKey(self):
    """Creates a ibmcloud key from the generated ssh key."""
    logging.info('Creating rias key')
    with open(vm_util.GetPublicKeyPath(), 'r') as keyfile:
      pubkey = keyfile.read()
    logging.info('ssh private key file: %s, public key file: %s',
                 vm_util.GetPrivateKeyPath(), vm_util.GetPublicKeyPath())
    cmd = ibm.IbmAPICommand(self)
    cmd.flags['name'] = self.prefix + str(flags.FLAGS.run_uri) + 'key'
    cmd.flags['pubkey'] = pubkey
    return cmd.CreateKey()

  def _Suspend(self):
    raise NotImplementedError()

  def _Resume(self):
    raise NotImplementedError()

  def _CheckImage(self):
    """Verifies we have an imageid to use."""
    cmd = ibm.IbmAPICommand(self)
    cmd.flags['image_name'] = self.image or self._GetDefaultImageName()
    logging.info('Looking up image: %s', cmd.flags['image_name'])
    self.imageid = cmd.GetImageId()
    if self.imageid is None:
      logging.info('Failed to find valid image id')
      sys.exit(1)
    else:
      logging.info('Image id found: %s', self.imageid)

  @classmethod
  def _GetDefaultImageName(cls):
    """Returns the default image name prefx."""
    return cls.IMAGE_NAME_PREFIX

  def _SetupResources(self):
    """Looks up the resources needed, if not found, creates new."""
    logging.info('Checking resources')
    cmd = ibm.IbmAPICommand(self)
    cmd.flags.update({
        'prefix': self.prefix,
        'zone': self.zone,
        'items': 'vpcs'
    })
    self.vpcid = cmd.GetResource()
    logging.info('Vpc found: %s', self.vpcid)
    cmd.flags['items'] = 'subnets'
    self.subnet = cmd.GetResource()
    logging.info('Subnet found: %s', self.subnet)

    if not self.vpcid:
      logging.info('Creating a vpc')
      self.network.Create()
      self.vpcid = self.network.vpcid

    if not self.subnet:
      logging.info('Creating a subnet')
      self.network.CreateSubnet(self.vpcid)
      self.subnet = self.network.subnet

    if FLAGS.ibmcloud_subnets_extra > 0:
      # these are always created outside perfkit
      cmd.flags['prefix'] = ibmcloud_network.SUBNET_SUFFIX_EXTRA
      self.subnets = cmd.ListSubnetsExtra()
      logging.info('Extra subnets found: %s', self.subnets)

    # look up for existing key that matches this run uri
    cmd.flags['items'] = 'keys'
    cmd.flags['prefix'] = self.prefix + FLAGS.run_uri
    self.key = cmd.GetResource()
    logging.info('Key found: %s', self.key)
    if self.key is None:
      cmd.flags['items'] = 'keys'
      cmd.flags['prefix'] = self.prefix + FLAGS.run_uri
      self.key = self._CreateRiasKey()
      if self.key is None:
        raise errors.Error('IBM Cloud ERROR: Failed to create a rias key')
      logging.info('Created a new key: %s', self.key)

    logging.info('Looking up the image: %s', self.imageid)
    cmd.flags['imageid'] = self.imageid
    self.os_data = util.GetOsInfo(cmd.ImageShow())
    logging.info('Image os: %s', self.os_data)
    logging.info('Checking resources finished')

  def _DeleteKey(self):
    """Deletes the rias key."""
    with self._lock:
      # key is not dependent on vpc, one key is used
      if self.key not in IbmCloudVirtualMachine.validated_resources_set:
        time.sleep(5)
        cmd = ibm.IbmAPICommand(self)
        cmd.flags['items'] = 'keys'
        cmd.flags['id'] = self.key
        cmd.DeleteResource()
        IbmCloudVirtualMachine.validated_resources_set.add(self.key)

  def _Create(self):
    """Creates and starts a IBM Cloud VM instance."""
    self._CreateInstance()
    if self.subnet:  # this is for the primary vnic and fip
      self.fip_address, self.fip_id = self.network.CreateFip(
          self.name + 'fip', self.vmid)
      self.ip_address = self.fip_address
      self.internal_ip = self._WaitForIPAssignment(self.subnet)
      logging.info('Fip: %s, ip: %s', self.ip_address, self.internal_ip)

    if self.subnets:
      # create the extra vnics
      cmd = ibm.IbmAPICommand(self)
      cmd.flags['instanceid'] = self.vmid
      for subnet_name in self.subnets.keys():
        cmd.flags['name'] = subnet_name
        cmd.flags['subnet'] = self.subnets[subnet_name]['id']  # subnet id
        logging.info('Creating extra vnic for vmid: %s, subnet: %s',
                     self.vmid, cmd.flags['subnet'])
        vnicid, ip_addr = cmd.InstanceVnicCreate()
        logging.info('Extra vnic created for vmid: %s, vnicid: %s, ip_addr: %s',
                     self.vmid, vnicid, ip_addr)
        self.subnets[subnet_name]['vnicid'] = vnicid
        self.subnets[subnet_name]['ip_addr'] = ip_addr
      logging.info('Extra vnics created for vmid: %s, subnets: %s',
                   self.vmid, self.subnets)

  def _Delete(self):
    """Delete all the resources that were created."""
    if self.vm_deleted:
      return
    self._StopInstance()
    if self.fip_address:
      self.network.DeleteFip(self.vmid, self.fip_address, self.fip_id)
    time.sleep(10)
    self._DeleteInstance()
    if not FLAGS.ibmcloud_resources_keep:
      self.network.Delete()
    self._DeleteKey()

  def _DeleteDependencies(self):
    """Delete dependencies."""
    pass

  def _Exists(self):
    return self.vm_created and not self.vm_deleted

  def _CreateDependencies(self):
    """Validate and Create dependencies prior creating the VM."""
    self._CheckPrerequisites()

  def _CheckPrerequisites(self):
    """Checks prerequisites are met otherwise aborts execution."""
    with self._lock:
      logging.info('Validating prerequisites.')
      logging.info('zones: %s', FLAGS.zones)
      if len(FLAGS.zones) > 1:
        for zone in FLAGS.zones:
          if zone not in IbmCloudVirtualMachine.validated_resources_set:
            self.zone = zone
            break
      else:
        self.zone = FLAGS.zones[0]
      logging.info('zone to use %s', self.zone)
      self._CheckImage()
      self.network = ibmcloud_network.IbmCloudNetwork(self.prefix, self.zone)
      self._SetupResources()
      IbmCloudVirtualMachine.validated_resources_set.add(self.zone)
      logging.info('Prerequisites validated.')

  def _CreateInstance(self):
    """Creates IBM Cloud VM instance."""
    cmd = ibm.IbmAPICommand(self)
    cmd.flags.update({
        'name': self.name,
        'imageid': self.imageid,
        'profile': self.profile,
        'vpcid': self.vpcid,
        'subnet': self.subnet,
        'key': self.key,
        'zone': self.zone
    })
    cmd.user_data = self.user_data
    if self.boot_volume_size > 0:
      cmd.flags['capacity'] = self.boot_volume_size
    cmd.flags['iops'] = self.boot_volume_iops
    if self.boot_encryption_key:
      cmd.flags['encryption_key'] = self.boot_encryption_key
    logging.info('Creating instance, flags: %s', cmd.flags)
    resp = json.loads(cmd.CreateInstance())
    if 'id' not in resp:
      raise errors.Error(f'IBM Cloud ERROR: Failed to create instance: {resp}')
    self.vmid = resp['id']
    self.vm_created = True
    logging.info('Instance created, id: %s', self.vmid)
    logging.info('Waiting for instance to start, id: %s', self.vmid)
    cmd.flags['instanceid'] = self.vmid
    status = cmd.InstanceStatus()
    assert status == ibm.States.RUNNING
    if status != ibm.States.RUNNING:
      logging.error('Instance start failed, status: %s', status)
    logging.info('Instance %s status %s', self.vmid, status)

  def _StartInstance(self):
    """Starts a IBM Cloud VM instance."""
    cmd = ibm.IbmAPICommand(self)
    cmd.flags['instanceid'] = self.vmid
    status = cmd.InstanceStart()
    logging.info('start_instance_poll: last status is %s', status)
    assert status == ibm.States.RUNNING
    if status != ibm.States.RUNNING:
      logging.error('Instance start failed, status: %s', status)

  def _WaitForIPAssignment(self, networkid: str):
    """Finds the IP address assigned to the vm."""
    ip_v4_address = '0.0.0.0'
    count = 0
    while (ip_v4_address == '0.0.0.0' and
           count * FLAGS.ibmcloud_polling_delay < 240):
      time.sleep(FLAGS.ibmcloud_polling_delay)
      count += 1
      cmd = ibm.IbmAPICommand(self)
      cmd.flags['instanceid'] = self.vmid
      logging.info('Looking for IP for instance %s, networkid: %s',
                   self.vmid, networkid)

      resp = cmd.InstanceShow()
      for network in resp['network_interfaces']:
        if network['subnet']['id'] == networkid:
          ip_v4_address = network['primary_ipv4_address']
          break
      logging.info('Waiting on ip assignment: %s', ip_v4_address)
    if ip_v4_address == '0.0.0.0':
      raise ValueError('Failed to retrieve ip address')
    return ip_v4_address

  def _StopInstance(self):
    """Stops a IBM Cloud VM instance."""
    cmd = ibm.IbmAPICommand(self)
    cmd.flags['instanceid'] = self.vmid
    status = cmd.InstanceStop()
    logging.info('stop_instance_poll: last status is %s', status)
    if status != ibm.States.STOPPED:
      logging.error('Instance stop failed, status: %s', status)

  def _DeleteInstance(self):
    """Deletes a IBM Cloud VM instance."""
    cmd = ibm.IbmAPICommand(self)
    cmd.flags['instanceid'] = self.vmid
    cmd.InstanceDelete()
    self.vm_deleted = True
    logging.info('Instance deleted: %s', cmd.flags['instanceid'])

  def CreateScratchDisk(self, disk_spec: disk.BaseDisk):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    disks_names = ('%s-data-%d-%d'
                   % (self.name, len(self.scratch_disks), i)
                   for i in range(disk_spec.num_striped_disks))
    disks = [ibmcloud_disk.IbmCloudDisk(disk_spec, name, self.zone,
                                        encryption_key=self.data_encryption_key)
             for name in disks_names]

    self._CreateScratchDiskFromDisks(disk_spec, disks)

  def DownloadPreprovisionedData(self, install_path, module_name, filename):
    """Creats a temp file, no download."""
    self.RemoteCommand('echo "1234567890" > ' +
                       os.path.join(install_path, filename))

  def ShouldDownloadPreprovisionedData(self, module_name, filename):
    """Returns whether or not preprovisioned data is available."""
    return True


class DebianBasedIbmCloudVirtualMachine(IbmCloudVirtualMachine,
                                        linux_virtual_machine.BaseDebianMixin):

  def PrepareVMEnvironment(self):
    time.sleep(_WAIT_TIME_DEBIAN)
    self.RemoteCommand('DEBIAN_FRONTEND=noninteractive apt-get -y update')
    self.RemoteCommand('DEBIAN_FRONTEND=noninteractive apt-get -y install sudo')
    super(DebianBasedIbmCloudVirtualMachine, self).PrepareVMEnvironment()


class Debian9BasedIbmCloudVirtualMachine(DebianBasedIbmCloudVirtualMachine,
                                         linux_virtual_machine.Debian9Mixin):
  IMAGE_NAME_PREFIX = 'ibm-debian-9-'


class Debian10BasedIbmCloudVirtualMachine(DebianBasedIbmCloudVirtualMachine,
                                          linux_virtual_machine.Debian10Mixin):
  IMAGE_NAME_PREFIX = 'ibm-debian-10-'


class Ubuntu1604BasedIbmCloudVirtualMachine(
    IbmCloudVirtualMachine, linux_virtual_machine.Ubuntu1604Mixin):
  IMAGE_NAME_PREFIX = 'ibm-ubuntu-16-04-'

  def PrepareVMEnvironment(self):
    self.RemoteCommand('DEBIAN_FRONTEND=noninteractive apt-get -y update')
    super(Ubuntu1604BasedIbmCloudVirtualMachine, self).PrepareVMEnvironment()


class Ubuntu1804BasedIbmCloudVirtualMachine(
    IbmCloudVirtualMachine, linux_virtual_machine.Ubuntu1804Mixin):
  IMAGE_NAME_PREFIX = 'ibm-ubuntu-18-04-'

  def PrepareVMEnvironment(self):
    logging.info('Pausing for 10 min before update and installs')
    time.sleep(_WAIT_TIME_UBUNTU)
    self.RemoteCommand('DEBIAN_FRONTEND=noninteractive apt-get -y update')
    super(Ubuntu1804BasedIbmCloudVirtualMachine, self).PrepareVMEnvironment()


class RhelBasedIbmCloudVirtualMachine(IbmCloudVirtualMachine,
                                      linux_virtual_machine.BaseRhelMixin):

  def PrepareVMEnvironment(self):
    time.sleep(_WAIT_TIME_RHEL)
    super(RhelBasedIbmCloudVirtualMachine, self).PrepareVMEnvironment()


class Rhel7BasedIbmCloudVirtualMachine(RhelBasedIbmCloudVirtualMachine,
                                       linux_virtual_machine.Rhel7Mixin):
  IMAGE_NAME_PREFIX = 'ibm-redhat-7-6-minimal-amd64'


class Rhel8BasedIbmCloudVirtualMachine(RhelBasedIbmCloudVirtualMachine,
                                       linux_virtual_machine.Rhel8Mixin):
  IMAGE_NAME_PREFIX = 'ibm-redhat-8-'


class WindowsIbmCloudVirtualMachine(IbmCloudVirtualMachine,
                                    windows_virtual_machine.BaseWindowsMixin):
  """Support for Windows machines on IBMCloud."""

  def __init__(self, vm_spec):
    super(WindowsIbmCloudVirtualMachine, self).__init__(vm_spec)
    self.user_name = 'Administrator'
    self.user_data = util.USER_DATA

  @vm_util.Retry()
  def _GetDecodedPasswordData(self):
    # Retrieve a base64 encoded, encrypted password for the VM.
    cmd = ibm.IbmAPICommand(self)
    cmd.flags['instanceid'] = self.vmid
    resp = cmd.InstanceInitializationShow()
    logging.info('Instance %s, resp %s', self.vmid, resp)
    encrypted = None
    if resp and 'password' in resp and 'encrypted_password' in resp['password']:
      encrypted = resp['password']['encrypted_password']
    if encrypted is None:
      raise ValueError('Failed to retrieve encrypted password')
    return base64.b64decode(encrypted)

  def _PostCreate(self):
    """Retrieve generic VM info and then retrieve the VM's password."""
    super(WindowsIbmCloudVirtualMachine, self)._PostCreate()

    # Get the decoded password data.
    decoded_password_data = self._GetDecodedPasswordData()

    # Write the encrypted data to a file, and use openssl to
    # decrypt the password.
    with vm_util.NamedTemporaryFile() as tf:
      tf.write(decoded_password_data)
      tf.close()
      decrypt_cmd = [
          'openssl', 'rsautl', '-decrypt', '-in', tf.name, '-inkey',
          vm_util.GetPrivateKeyPath()
      ]
      password, _ = vm_util.IssueRetryableCommand(decrypt_cmd)
      self.password = password
    logging.info('Password decrypted for %s, %s', self.fip_address, self.vmid)


class Windows2012CoreIbmCloudVirtualMachine(
    WindowsIbmCloudVirtualMachine,
    windows_virtual_machine.Windows2012CoreMixin):
  IMAGE_NAME_PREFIX = 'ibm-windows-server-2012-full'


class Windows2016CoreIbmCloudVirtualMachine(
    WindowsIbmCloudVirtualMachine,
    windows_virtual_machine.Windows2016CoreMixin):
  IMAGE_NAME_PREFIX = 'ibm-windows-server-2016-full'


class Windows2019CoreIbmCloudVirtualMachine(
    WindowsIbmCloudVirtualMachine,
    windows_virtual_machine.Windows2019CoreMixin):
  IMAGE_NAME_PREFIX = 'ibm-windows-server-2019-full'
