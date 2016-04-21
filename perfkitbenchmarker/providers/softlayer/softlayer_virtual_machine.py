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

#HVM = 'HVM'
#PV = 'PV'
#NON_HVM_PREFIXES = ['m1', 'c1', 't1', 'm2']
#US_EAST_1 = 'us-east-1'
#US_WEST_1 = 'us-west-1'
#US_WEST_2 = 'us-west-2'
#EU_WEST_1 = 'eu-west-1'
#AP_NORTHEAST_1 = 'ap-northeast-1'
#AP_SOUTHEAST_1 = 'ap-southeast-1'
#AP_SOUTHEAST_2 = 'ap-southeast-2'
#SA_EAST_1 = 'sa-east-1'
#PLACEMENT_GROUP_PREFIXES = frozenset(['c3', 'c4', 'cc2', 'cg1', 'g2', 'cr1', 'r3', 'hi1', 'i2'])
#NUM_LOCAL_VOLUMES = {
#    'c1.medium': 1, 'c1.xlarge': 4,
#    'c3.large': 2, 'c3.xlarge': 2, 'c3.2xlarge': 2, 'c3.4xlarge': 2,
#    'c3.8xlarge': 2, 'cc2.8xlarge': 4,
#    'cg1.4xlarge': 2, 'cr1.8xlarge': 2, 'g2.2xlarge': 1,
#    'hi1.4xlarge': 2, 'hs1.8xlarge': 24,
#    'i2.xlarge': 1, 'i2.2xlarge': 2, 'i2.4xlarge': 4, 'i2.8xlarge': 8,
#    'm1.small': 1, 'm1.medium': 1, 'm1.large': 2, 'm1.xlarge': 4,
#    'm2.xlarge': 1, 'm2.2xlarge': 1, 'm2.4xlarge': 2,
#    'm3.medium': 1, 'm3.large': 1, 'm3.xlarge': 2, 'm3.2xlarge': 2,
#    'r3.large': 1, 'r3.xlarge': 1, 'r3.2xlarge': 1, 'r3.4xlarge': 1,
#    'r3.8xlarge': 2, 'd2.xlarge': 3, 'd2.2xlarge': 6, 'd2.4xlarge': 12,
#    'd2.8xlarge': 24,
#}
#DRIVE_START_LETTER = 'b'

INSTANCE_EXISTS_STATUSES = frozenset(['ACTIVE'])
INSTANCE_DELETED_STATUSES = frozenset(['DISCONNECTED'])
INSTANCE_KNOWN_STATUSES = INSTANCE_EXISTS_STATUSES | INSTANCE_DELETED_STATUSES



class SoftLayerVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing an SoftLayer Virtual Machine."""

  keyLabel = None
  delete_issued = False

  CLOUD = providers.SOFTLAYER
  #IMAGE_NAME_FILTER = 'Ubuntu'
  #DEFAULT_ROOT_DISK_TYPE = 'standard'

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
#    if self.machine_type in NUM_LOCAL_VOLUMES:
#      self.max_local_disks = NUM_LOCAL_VOLUMES[self.machine_type]
    self.user_data = None
    #self.network = softLayer_network.SoftLayerNetwork.GetNetwork(self)              
    #self.firewall = softLayer_network.SoftLayerFirewall.GetFirewall() 

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
          'sshkey', 
          'remove',
          self.keyLabel]
        
        util.IssueRetryableCommand(delete_cmd)
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
            
    #util.AddDefaultTags(self.id, self.region)

        
  
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
    os = 'UBUNTU_LATEST_64'
    nic = '10'
    private_vlan_id = None
    public_vlan_id = None
    
    try: 
        vm_attributes = json.loads(self.machine_type)
        if 'cpus' in vm_attributes:
            cpus = vm_attributes['cpus']
                        
        if 'memory' in vm_attributes:
            memory = vm_attributes['memory']
        
        if 'os' in vm_attributes:
            os = vm_attributes['os']
        
        if 'nic' in vm_attributes:
            nic = vm_attributes['nic']
            
        if 'private_vlan_id' in vm_attributes:
            private_vlan_id = vm_attributes['private_vlan_id']
            
        if 'public_vlan_id' in vm_attributes:
            public_vlan_id = vm_attributes['public_vlan_id']
    except:
        logging.warning("Using default values for VM. Error in machine type JSON: %s" % self.machine_type)
        
    create_cmd = util.SoftLayer_PREFIX + [
        '--format',
        'json',                                          
        '-y',
        'vs',
        'create',
        '--wait',
        '60',
        '--datacenter',
        '%s' % self.region,
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
        '--key',
         SoftLayerVirtualMachine.keyLabel
        ]

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
    password_data = response['password']

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
