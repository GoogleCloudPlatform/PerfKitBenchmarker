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
from time import sleep

"""Class to represent a SoftLayer Virtual Machine object.

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""

import base64
import json
import logging
import threading
import string
import random
import time
import sys


from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_virtual_machine
from perfkitbenchmarker.providers.softlayer import util
from perfkitbenchmarker.providers.softlayer import softlayer_disk
from perfkitbenchmarker.providers.softlayer import softlayer_network
from perfkitbenchmarker import providers

FLAGS = flags.FLAGS


INSTANCE_EXISTS_STATUSES = frozenset(['ACTIVE'])
INSTANCE_DELETED_STATUSES = frozenset(['DISCONNECTED'])
INSTANCE_KNOWN_STATUSES = INSTANCE_EXISTS_STATUSES | INSTANCE_DELETED_STATUSES
DRIVE_START_LETTER = 'c'

def GetBlockDeviceMap(machine_type):
  """Returns the block device map to expose all devices for a given machine.

  Args:
    machine_type: The machine type to create a block device map for.

  Returns:
    The json representation of the block device map for a machine compatible
    with the AWS CLI, or if the machine type has no local disks, it will
    return None.
  """
  print machine_type
  mappings = [{'VirtualName': 'ephemeral%s' % i,
                 'DeviceName': '/dev/xvd%s' % chr(ord(DRIVE_START_LETTER) + i)}
                for i in range(1)]
  print mappings
  return json.dumps(mappings)
  

class SoftLayerVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing an SoftLayer Virtual Machine."""

  keyLabel = None
  delete_issued = False

  CLOUD = providers.SOFTLAYER
  #IMAGE_NAME_FILTER = 'Ubuntu'
  DEFAULT_ROOT_DISK_TYPE = 'standard'

  _lock = threading.Lock()
  imported_keyfile_set = set()
  deleted_keyfile_set = set()

  def __init__(self, vm_spec):
    """Initialize a SoftLayer virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(SoftLayerVirtualMachine, self).__init__(vm_spec)
    self.region = util.GetRegionFromZone(self.zone)
    self.user_name = FLAGS.softlayer_user_name
    self.user_data = None
    self.max_local_disks = 5
    self.num_local_disks = 1

  def ImportKeyfile(self):
    """Imports the public keyfile to SoftLayer."""

    with self._lock:

        if self.region in self.imported_keyfile_set:
            return

        import_cmd = util.SoftLayer_PREFIX + [
          'sshkey',
          'add',
          '--in-file',
          vm_util.GetPublicKeyPath(),
          self.keyLabel]
        util.IssueRetryableCommand(import_cmd)
        self.imported_keyfile_set.add(self.region)
        if self.region in self.deleted_keyfile_set:
            self.deleted_keyfile_set.remove(self.region)

  def DeleteKeyfile(self):
    """Deletes the imported keyfile for a region."""
    with self._lock:
        if self.region in self.deleted_keyfile_set:
            return

        delete_cmd = util.SoftLayer_PREFIX + [
          '-y',
          '--format',
          'json',    
          'sshkey',
          'remove',
          self.keyLabel]

        stdout, _ = util.IssueRetryableCommand(delete_cmd)
        print stdout
        
        self.deleted_keyfile_set.add(self.region)
        if self.region in self.imported_keyfile_set:
            self.imported_keyfile_set.remove(self.region)

  @vm_util.Retry(log_errors=False)
  def _PostCreate(self):
    """Get the instance's data and tag it."""
    describe_cmd = util.SoftLayer_PREFIX + [
        '--format',
        'json',
        'vs',
        'detail',
        '%s' % self.id]

    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    transaction = response['active_transaction']

    if transaction != None:
        logging.info('Post create check for instance %s. Not ready. Active transaction in progress: %s.', self.id, transaction)
        sleep(10)
        raise Exception

    self.internal_ip = response['private_ip']
    self.ip_address = response['public_ip']

    util.AddDefaultTags(self.id, self.region)



  def IdGenerator(self, size=6, chars=string.ascii_uppercase + string.digits):
      return ''.join(random.choice(chars) for _ in range(size))

  def _CreateDependencies(self):
    """Create VM dependencies."""
    if SoftLayerVirtualMachine.keyLabel == None:
        SoftLayerVirtualMachine.keyLabel  = "Perfkit-Key-" + self.IdGenerator()

    self.ImportKeyfile()

    self.hostname = "Pefkit-Host-" + self.IdGenerator();
    self.AllowRemoteAccessPorts()

    self.active_try_count = 0

  def _DeleteDependencies(self):
    """Delete VM dependencies."""
    self.DeleteKeyfile()

  def _Create(self):
    """Create a VM instance."""
    
    memory = '1024'
    cpus = '1'
    dedicated = False
    os = 'UBUNTU_LATEST_64'
    nic = '10'
    private_vlan_id = None
    public_vlan_id = None
    san = False
    disk_size = '25'
    
    
    try:
        vm_attributes = json.loads(self.machine_type)
        if 'cpus' in vm_attributes:
            cpus = vm_attributes['cpus']

        if 'dedicated' in vm_attributes:
            dedicated = vm_attributes['dedicated'].upper() in ['TRUE', 'T']

        if 'memory' in vm_attributes:
            memory = vm_attributes['memory']

        if 'os' in vm_attributes:
            os = vm_attributes['os']

        if 'san' in vm_attributes:
            san = vm_attributes['san'].upper() in ['TRUE', 'T']

        if 'nic' in vm_attributes:
            nic = vm_attributes['nic']

        if 'private_vlan_id' in vm_attributes:
            private_vlan_id = vm_attributes['private_vlan_id']

        if 'public_vlan_id' in vm_attributes:
            public_vlan_id = vm_attributes['public_vlan_id']
    except ValueError as detail:
        logging.error('JSON error: ' , detail, ' in ' , self.machine_type)
        raise Exception("Error in JSON: " + self.machine_type)

    if isinstance(self, WindowsSoftLayerVirtualMachine):
        os = 'WIN_LATEST_64'
        self.hostname = "pefkithost" + self.IdGenerator(3).lower();
        disk_size = '100'
    
     
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
        'perfkit.org',
        '--cpu',
        '%s' % cpus,
        '--os',
        '%s' % os,
        '--network',
        '%s' % nic,
         '--disk',
         '%s' % disk_size,
        '--key',
         SoftLayerVirtualMachine.keyLabel
        ]
    
    
    
    # TODO: HANDLE ADDITIONAL DISKS BETTER 
    if self.max_local_disks > 1:
        create_cmd = create_cmd + ['--disk', '25']
           
    if san == True:
        create_cmd = create_cmd + ['--san']       

    if dedicated == True:
        create_cmd = create_cmd + ['--dedicated']       

    if public_vlan_id != None:
        create_cmd = create_cmd + ['--vlan-public', '%s' % public_vlan_id]

    if private_vlan_id != None:
        create_cmd = create_cmd + ['--vlan-private', '%s' % private_vlan_id]


    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['id']

  def _Delete(self):
    """Delete a VM instance."""
    delete_cmd = util.SoftLayer_PREFIX + [
        '-y',
        'vs',
        'cancel',
        '%s' % self.id]

    vm_util.IssueCommand(delete_cmd)
    logging.info("Sleeping so that delete command has time to register")
    sleep(60)
    #self._WaitForDeleteToComplete()


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

    if found == False:
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
        assert status in INSTANCE_KNOWN_STATUSES, status
        return status in INSTANCE_EXISTS_STATUSES
    except errors.VmUtil.CalledProcessException as e:
        return False

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    print "CreateScratchDisk"
    
    print disk_spec
    return
     
    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    # Instantiate the disk(s) that we want to create.
    disks = []
    for _ in range(disk_spec.num_striped_disks):
      data_disk = softlayer_disk.SoftLayerDisk(disk_spec, self.zone, self.machine_type)
      if disk_spec.disk_type == disk.LOCAL:
        data_disk.device_letter = chr(ord(DRIVE_START_LETTER) +
                                      self.local_disk_counter)
        # Local disk numbers start at 1 (0 is the system disk).
        data_disk.disk_number = self.local_disk_counter + 1
        self.local_disk_counter += 1
        if self.local_disk_counter > self.max_local_disks:
          raise errors.Error('Not enough local disks.')
      else:
        # Remote disk numbers start at 1 + max_local disks (0 is the system disk
        # and local disks occupy [1, max_local_disks]).
        data_disk.disk_number = (self.remote_disk_counter +
                                 1 + self.max_local_disks)
        self.remote_disk_counter += 1
      disks.append(data_disk)

    self._CreateScratchDiskFromDisks(disk_spec, disks)

def GetLocalDisks(self):
    """Returns a list of local disks on the VM.

    Returns:
      A list of strings, where each string is the absolute path to the local
          disks on the VM (e.g. '/dev/sdb').
    """
    return ['/dev/xvd%s' % chr(ord(DRIVE_START_LETTER) + i)
            for i in xrange(NUM_LOCAL_VOLUMES[self.machine_type])]

def AddMetadata(self, **kwargs):
    """Adds metadata to the VM."""
    util.AddTags(self.id, self.region, **kwargs)


class DebianBasedSoftLayerVirtualMachine(SoftLayerVirtualMachine,
                                   linux_virtual_machine.DebianMixin):
  IMAGE_NAME_FILTER = 'ubuntu/images/*/ubuntu-trusty-14.04-amd64-*'


class JujuBasedSoftLayerVirtualMachine(SoftLayerVirtualMachine,
                                 linux_virtual_machine.JujuMixin):
  IMAGE_NAME_FILTER = 'ubuntu/images/*/ubuntu-trusty-14.04-amd64-*'


class RhelBasedSoftLayerVirtualMachine(SoftLayerVirtualMachine,
                                 linux_virtual_machine.RhelMixin):
  pass


class WindowsSoftLayerVirtualMachine(SoftLayerVirtualMachine,
                               windows_virtual_machine.WindowsMixin):

  IMAGE_NAME_FILTER = 'Windows_Server-2012-R2_RTM-English-64Bit-Core-*'
  DEFAULT_ROOT_DISK_TYPE = 'gp2'

  def __init__(self, vm_spec):
    super(WindowsSoftLayerVirtualMachine, self).__init__(vm_spec)
    self.user_name = 'Administrator'
    self.user_data = ('<powershell>%s</powershell>' %
                      windows_virtual_machine.STARTUP_SCRIPT)

  @vm_util.Retry()
  def _GetDecodedPasswordData(self):
    # Retreive a base64 encoded, encrypted password for the VM.
    get_password_cmd = util.SoftLayer_PREFIX + [
         '--format',
         'json',
         'vs',
        'credentials',
        '%s' % self.id]
    stdout, _ = util.IssueRetryableCommand(get_password_cmd)
    response = json.loads(stdout)
    password_data = response[0]['password']

    # SoftLayer may not populate the password data until some time after
    # the VM shows as running. Simply retry until the data shows up.
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
