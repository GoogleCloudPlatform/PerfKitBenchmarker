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

"""Class to represent an SoftLayer Virtual Machine object.

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""

import json
import logging
import threading
import string
import random
from time import sleep

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_virtual_machine
from perfkitbenchmarker.providers.softlayer import util
from perfkitbenchmarker.providers.softlayer import softlayer_disk
from perfkitbenchmarker import providers

FLAGS = flags.FLAGS

INSTANCE_EXISTS_STATUSES = frozenset(['ACTIVE'])
INSTANCE_QUIESCING_TRANSACTION = frozenset(['CLOUD_RECLAIM_PREP'])
INSTANCE_DELETED_STATUSES = frozenset(['DISCONNECTED'])
INSTANCE_KNOWN_STATUSES = INSTANCE_EXISTS_STATUSES | INSTANCE_DELETED_STATUSES
DRIVE_START_LETTER = 'c'


class SoftLayerVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing an SoftLayer Virtual Machine."""

  CLOUD = providers.SOFTLAYER
  DEFAULT_ROOT_DISK_TYPE = 'standard'


  _lock = threading.Lock()  # ensure only one thread imports & exports sshkey
  key_label = None
  deleted_keyfile = False
  imported_keyfile = False

  def __init__(self, vm_spec):
    """Initialize a SoftLayer virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """

    super(SoftLayerVirtualMachine, self).__init__(vm_spec)
    self.user_name = FLAGS.softlayer_user_name
    self.user_data = None
    self.max_local_disks = 1

  def ImportKeyfile(self):
    """Imports the public keyfile to SoftLayer."""

    with self._lock:
      if SoftLayerVirtualMachine.imported_keyfile:
        return

      import_cmd = util.SoftLayer_PREFIX + [
          'sshkey',
          'add',
          '--in-file',
          vm_util.GetPublicKeyPath(),
          self.key_label]

      util.IssueRetryableCommand(import_cmd)
      SoftLayerVirtualMachine.imported_keyfile = True


  def DeleteKeyfile(self):
    """Deletes the public keyfile to SoftLayer."""

    with self._lock:
      if SoftLayerVirtualMachine.deleted_keyfile:
        return

      delete_cmd = util.SoftLayer_PREFIX + [
          '-y',
          '--format',
          'json',
          'sshkey',
          'remove',
          self.key_label]

      util.IssueRetryableCommand(delete_cmd)

      SoftLayerVirtualMachine.deleted_keyfile = True
      if SoftLayerVirtualMachine.imported_keyfile:
        SoftLayerVirtualMachine.imported_keyfile = False

  @vm_util.Retry(log_errors=False)
  def _PostCreate(self):
    """Check the status of the system to ensure that it is active
      Add a non-root user"""

    describe_cmd = util.SoftLayer_PREFIX + [
        '--format',
        'json',
        'vs',
        'detail',
        '%s' % self.id]

    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    transaction = response['active_transaction']

    # if not ready raise exception so _PostCreate is called again
    if transaction is not None:
      log_msg = ("Post create check for instance %s. Not ready."
                 " Active transaction in progress: %s."
                 % (self.id, transaction))
      logging.info(log_msg)
      sleep(20)
      raise Exception

    self.internal_ip = response['private_ip']
    self.ip_address = response['public_ip']
    domain = response['domain']
    logging.info('domain name use is %s' % (domain))

    if self.ip_address is None:
      logging.error('Did not find an IP address')
      raise Exception

    util.AddDefaultTags(self.id, self.zone)
    if(domain != util.defaultDomain):
      #CPOVRB insert name into NS and wait 30 seconds to ensure activation of name 
      logging.info("reached DNS insert Point for IP: %s." % (self.ip_address))
      dnsbld_cmd  =  util.SoftLayer_PREFIX + [
        'dns',
        'record-add',
        '%s'  % domain,
        '%s'  % self.hostname,
        'A',
        '%s'  % self.ip_address]
      logging.info(dnsbld_cmd)
      stdout, _, _ = vm_util.IssueCommand(dnsbld_cmd)
      #CPOVRB
      #CPOMRS Check to see if DNS addition is working
      #DNS should not be used unless needed as it will cause an aditional 15minutes loadtime.
      dnsgood = False;
      while(not dnsgood):
        hostcmd = ['host',
          self.hostname + '.' + domain]
        stdin, _, _ = vm_util.IssueCommand(hostcmd)
        if "NXDOMAIN" not in stdin:
	  logging.info('DNS working')
          dnsgood = True
        else:
	  logging.info('DNS failed retrying in 60 seconds')
          sleep(60)
      #CPOMRS

    # CPOMRS - Delete 127.0.1.1 hosts entry
    self.RemoteCommand('sed -i \'/127.0.1.1/d\' /etc/hosts')
    # CPOMRS

    # Add user and move the key to the non root user
    if self.user_name != "root":
      self.user_name = "root"
      self.RemoteCommand('useradd -m %s'
                         % FLAGS.softlayer_user_name)
      self.RemoteCommand(
          'echo "%s  ALL=(ALL:ALL) NOPASSWD: ALL" >> /etc/sudoers'
          % FLAGS.softlayer_user_name)
      self.RemoteCommand('mkdir /home/%s/.ssh'
                         % FLAGS.softlayer_user_name)
      self.RemoteCommand('chmod 700 /home/%s/.ssh/'
                         % FLAGS.softlayer_user_name)
      self.RemoteCommand('cp ~/.ssh/*  /home/%s/.ssh/'
                         % FLAGS.softlayer_user_name)
      self.RemoteCommand(
          'cp /root/.ssh/authorized_keys /home/%s/authorized_keys'
          % FLAGS.softlayer_user_name)
      self.RemoteCommand('chown -R %s /home/%s/'
                         % (FLAGS.softlayer_user_name,
                            FLAGS.softlayer_user_name))
      self.RemoteCommand('rm -rf /root/.ssh')
      self.user_name = FLAGS.softlayer_user_name

  def IdGenerator(self, size=6, chars=string.ascii_uppercase + string.digits):
    """Create a random name"""
    return ''.join(random.choice(chars) for _ in range(size))

  def _CreateDependencies(self):
    """Create VM dependencies."""

    # first thread creates one key used by all VMs in the benchmark
    with self._lock:
      if SoftLayerVirtualMachine.key_label is None:
        SoftLayerVirtualMachine.key_label = "Perfkit-Key-" + self.IdGenerator()

    self.ImportKeyfile()

    self.hostname = "Perfkit-Host-" + self.IdGenerator()


  def _DeleteDependencies(self):
    """Delete VM dependencies."""
    self.DeleteKeyfile()

  def _Create(self):
    """Create a VM instance."""

    memory = '4096'
    cpus = '4'
    dedicated = False
    #os = 'UBUNTU_14_64'
    os = 'UBUNTU_16_64'
    nic = '1000'
    private_vlan_id = None
    public_vlan_id = None
    san = False
    disk_size0 = 25
    disk_size1 = 25

    try:
      vm_attributes = json.loads(self.machine_type)
      if 'cpus' in vm_attributes:
        cpus = vm_attributes['cpus']

      if 'dedicated' in vm_attributes:
        dedicated = vm_attributes['dedicated']

      if 'memory' in vm_attributes:
        memory = vm_attributes['memory']

      if 'disk_size0' in vm_attributes:
        disk_size0 = vm_attributes['disk_size0']

      if 'disk_size1' in vm_attributes:
        disk_size1 = vm_attributes['disk_size1']

      if 'os' in vm_attributes:
        os = vm_attributes['os']

      if 'san' in vm_attributes:
        san = vm_attributes['san']

      if 'nic' in vm_attributes:
        nic = vm_attributes['nic']

      if 'private_vlan_id' in vm_attributes:
        private_vlan_id = vm_attributes['private_vlan_id']

      if 'public_vlan_id' in vm_attributes:
        public_vlan_id = vm_attributes['public_vlan_id']

      if 'domain' in vm_attributes:
        domain  = vm_attributes['domain']
      else:
        domain = util.defaultDomain

    except ValueError as detail:
      logging.error('JSON error: ', detail, ' in ', self.machine_type)
      raise Exception("Error in JSON: " + self.machine_type)

    if isinstance(self, WindowsSoftLayerVirtualMachine):
      os = 'WIN_LATEST_64'
      self.hostname = "pefkithost" + self.IdGenerator(3).lower()
      if disk_size0 < 100:
        disk_size0 = 100

    create_cmd = util.SoftLayer_PREFIX + [
        '--format',
        'json',
        '-y',
        'vs',
        'create',
        '--wait',
        '60',
        '--datacenter',
        '%s' % self.zone,
        '--memory',
        '%s' % memory,
        '--hostname',
        '%s' % self.hostname,
        '--domain',
        '%s' % domain,    
        '--cpu',
        '%s' % cpus,
        '--os',
        '%s' % os,
        '--network',
        '%d' % nic,
        '--disk',
        '%s' % disk_size0,
        '--key',
        SoftLayerVirtualMachine.key_label]

    # additional disk for disk benchmarks
    create_cmd = create_cmd + ['--disk', '%s' % disk_size1]

    if san is True:
        create_cmd = create_cmd + ['--san']

    if dedicated is True:
        create_cmd = create_cmd + ['--dedicated']

    if public_vlan_id is not None:
        create_cmd = create_cmd + ['--vlan-public', '%s' % public_vlan_id]

    if private_vlan_id is not None:
        create_cmd = create_cmd + \
            ['--vlan-private', '%s' % private_vlan_id]

### Added by Yen for #1978 
    stdout, _, _ = vm_util.IssueCommand(create_cmd, raise_on_failure=False)
###

#    response = json.loads(stdout)
#    self.id = response['id']

### Added by Yen in order to return VM id 

    #list of VM id
    list_id_cmd = util.SoftLayer_PREFIX + [
        '--format',
        'json',
        'vs',
        'list',
        '--hostname',
        '%s' % self.hostname,]
      
    stdout, _, _ = vm_util.IssueCommand(list_id_cmd)
    response = json.loads(stdout)
    self.id = response[0]['id']

### 

# CPOMRS
  def UnregisterDNS(self):
    """Unregister Hostname from DNS"""
    #Get Domain name
    vm_attributes = json.loads(self.machine_type)

    if 'domain' in vm_attributes:
      domain  = vm_attributes['domain']
    else:
      return

    #Get list of Zones
    dnszone_cmd = util.SoftLayer_PREFIX + [
      'dns',
      'zone-list']

    stdout, _, _ = vm_util.IssueCommand(dnszone_cmd)

    #Strip out Zone ID based on Zone name
    for line in stdout.splitlines():
      if domain in line:
        domainid = line.split(' ', 1)[0]

    #Get List of Records related to Zone
    dnsreclist_cmd = util.SoftLayer_PREFIX + [
      'dns',
      'record-list',
      domainid.replace('\n','')]

    stdout, _, _ = vm_util.IssueCommand(dnsreclist_cmd)

    #Strip out Record ID based on Hostname
    for line in stdout.splitlines():
      if self.hostname in line:
        recordid = line.split(' ', 1)[0]

    #Delete Record based on Record ID
    dnsrecdel_cmd = util.SoftLayer_PREFIX + [
      '-y',
      'dns',
      'record-remove',
      recordid.replace('\n','')]

    vm_util.IssueCommand(dnsrecdel_cmd)
# CPOMRS


  def _Delete(self):
    """Delete a VM instance."""
    # CPOMRS Call to delete the DNS record 
    self.UnregisterDNS()
    # CPOMRS

    delete_cmd = util.SoftLayer_PREFIX + [
        '-y',
        'vs',
        'cancel',
        '%s' % self.id]

    vm_util.IssueCommand(delete_cmd)

    logging.info("Sleeping so that delete command has time to register")
    sleep(90)

    def _Exists(self):
      """Returns true if the VM exists."""

      describe_cmd = util.SoftLayer_PREFIX + [
          '--format',
          'json',
          'vs',
          'list',
          '--columns',
          'id',
          '--sortby',
          'id',
          '--domain',
          'perfkit.org']

      found = False
      stdout, _ = util.IssueRetryableCommand(describe_cmd)
      response = json.loads(stdout)
      for system in response:
        if system['id'] == self.id:
          found = True
          break

      if found is False:
        return False

      describe_cmd = util.SoftLayer_PREFIX + [
          '--format',
          'json',
          'vs',
          'detail',
          '%s' % self.id]
      try:
        stdout, _ = util.IssueRetryableCommand(describe_cmd)
        response = json.loads(stdout)
        status = response['status']
        active_transaction = response['active_transaction']
        if active_transaction in INSTANCE_QUIESCING_TRANSACTION:
          return False
        return status in INSTANCE_EXISTS_STATUSES
      except errors.VmUtil.CalledProcessException:
        return False

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.
        Args:
          disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    # Instantiate the disk(s) that we want to create.
    disks = []
    for _ in range(disk_spec.num_striped_disks):
      data_disk = softlayer_disk.SoftLayerDisk(disk_spec,
                                               self.zone,
                                               self.machine_type)
      drive_num = ord(DRIVE_START_LETTER) + self.local_disk_counter
      data_disk.device_letter = chr(drive_num)
      # Local disk numbers start at 1 (0 is the system disk).
      data_disk.disk_number = self.local_disk_counter + 1
      self.local_disk_counter += 1
      if self.local_disk_counter > self.max_local_disks:
        raise errors.Error('Not enough local disks.')
      disks.append(data_disk)

    self._CreateScratchDiskFromDisks(disk_spec, disks)

  def GetLocalDisks(self):
    """Returns a list of local disks on the VM.

      Returns:
        A list of strings, where each string is the absolute path to the local
            disks on the VM (e.g. '/dev/sdb').
      """
    return ['/dev/xvd%s' % chr(ord(DRIVE_START_LETTER) + i)
            for i in range(1)]

  def AddMetadata(self, **kwargs):
    """Adds metadata to the VM."""
    util.AddTags(self.id, self.zone, **kwargs)


class DebianBasedSoftLayerVirtualMachine(SoftLayerVirtualMachine,
                                         linux_virtual_machine.DebianMixin):
  pass


class JujuBasedSoftLayerVirtualMachine(SoftLayerVirtualMachine,
                                       linux_virtual_machine.JujuMixin):
  pass


class RhelBasedSoftLayerVirtualMachine(SoftLayerVirtualMachine,
                                       linux_virtual_machine.RhelMixin):
  pass


class WindowsSoftLayerVirtualMachine(SoftLayerVirtualMachine,
                                     windows_virtual_machine.WindowsMixin):
  """Object representing a SoftLayer Windows Virtual Machine."""

  def __init__(self, vm_spec):
    super(WindowsSoftLayerVirtualMachine, self).__init__(vm_spec)
    self.user_name = 'Administrator'
    self.user_data = (
        '<powershell>%s</powershell>' % windows_virtual_machine.STARTUP_SCRIPT)

  @vm_util.Retry()
  def _GetDecodedPasswordData(self):
    """Get the Password of the VM"""
    get_password_cmd = util.SoftLayer_PREFIX + [
        '--format',
        'json',
        'vs',
        'credentials',
        '%s' % self.id]
    stdout, _ = util.IssueRetryableCommand(get_password_cmd)
    response = json.loads(stdout)
    password_data = response[0]['password']

    # if no data retry.
    if not password_data:
      raise ValueError('No PasswordData in response.')

    return password_data

  def _PostCreate(self):
    """Retrieve generic VM info and then retrieve the VM's password."""
    super(WindowsSoftLayerVirtualMachine, self)._PostCreate()

    # Get the decoded password data.
    decoded_password_data = self._GetDecodedPasswordData()

    # Write the encrypted data to a file, and use openssl to
    # decrypt the password.
    with vm_util.NamedTemporaryFile() as tf:
      tf.write(decoded_password_data)
      tf.close()
      decrypt_cmd = ['openssl',
                     'rsautl',
                     '-decrypt',
                     '-in',
                     tf.name,
                     '-inkey',
                     vm_util.GetPrivateKeyPath()]
      password, _ = vm_util.IssueRetryableCommand(decrypt_cmd)
      self.password = password
