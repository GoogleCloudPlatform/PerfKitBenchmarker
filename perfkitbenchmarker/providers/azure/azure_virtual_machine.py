# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Class to represent an Azure Virtual Machine object.

Zones:
run 'azure vm location list'
Machine Types:
http://msdn.microsoft.com/en-us/library/azure/dn197896.aspx
Images:
run 'azure vm image list'

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""

import itertools
import json
import posixpath

from perfkitbenchmarker import custom_virtual_machine_spec
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_virtual_machine
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_disk
from perfkitbenchmarker.providers.azure import azure_network

import yaml

FLAGS = flags.FLAGS


class AzureVmSpec(virtual_machine.BaseVmSpec):
  """Object containing the information needed to create a AzureVirtualMachine.

  Attributes:
    tier: None or string. performance tier of the machine.
    compute_units: int.  number of compute units for the machine.
    accelerated_networking: boolean. True if supports accelerated_networking.
  """

  CLOUD = providers.AZURE

  def __init__(self, *args, **kwargs):
    super(AzureVmSpec, self).__init__(*args, **kwargs)
    if isinstance(self.machine_type,
                  custom_virtual_machine_spec.AzurePerformanceTierDecoder):
      self.tier = self.machine_type.tier
      self.compute_units = self.machine_type.compute_units
      self.machine_type = None
    else:
      self.tier = None
      self.compute_units = None

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. May
          be modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
          provided config values.
    """
    super(AzureVmSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['machine_type'].present:
      config_values['machine_type'] = yaml.load(flag_values.machine_type)
    if flag_values['azure_accelerated_networking'].present:
      config_values['accelerated_networking'] = (
          flag_values.azure_accelerated_networking)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(AzureVmSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'machine_type': (custom_virtual_machine_spec.AzureMachineTypeDecoder,
                         {}),
        'accelerated_networking': (
            option_decoders.BooleanDecoder, {'default': False}),
    })
    return result


# Per-VM resources are defined here.
class AzurePublicIPAddress(resource.BaseResource):
  """Class to represent an Azure Public IP Address."""

  def __init__(self, location, name):
    super(AzurePublicIPAddress, self).__init__()
    self.location = location
    self.name = name
    self._deleted = False
    self.resource_group = azure_network.GetResourceGroup()

  def _Create(self):
    vm_util.IssueCommand(
        [azure.AZURE_PATH, 'network', 'public-ip', 'create',
         '--location', self.location,
         '--name', self.name] + self.resource_group.args)

  def _Exists(self):
    if self._deleted:
      return False

    stdout, _, _ = vm_util.IssueCommand(
        [azure.AZURE_PATH, 'network', 'public-ip', 'show',
         '--output', 'json',
         '--name', self.name] + self.resource_group.args)
    try:
      json.loads(stdout)
      return True
    except:
      return False

  def GetIPAddress(self):
    stdout, _ = vm_util.IssueRetryableCommand(
        [azure.AZURE_PATH, 'network', 'public-ip', 'show',
         '--output', 'json',
         '--name', self.name] + self.resource_group.args)

    response = json.loads(stdout)
    return response['ipAddress']

  def _Delete(self):
    self._deleted = True


class AzureNIC(resource.BaseResource):
  """Class to represent an Azure NIC."""

  def __init__(self, subnet, name, public_ip, accelerated_networking):
    super(AzureNIC, self).__init__()
    self.subnet = subnet
    self.name = name
    self.public_ip = public_ip
    self._deleted = False
    self.resource_group = azure_network.GetResourceGroup()
    self.location = self.subnet.vnet.location
    self.args = ['--nics', self.name]
    self.accelerated_networking = accelerated_networking

  def _Create(self):
    cmd = [azure.AZURE_PATH, 'network', 'nic', 'create',
           '--location', self.location,
           '--vnet-name', self.subnet.vnet.name,
           '--subnet', self.subnet.name,
           '--public-ip-address', self.public_ip,
           '--name', self.name] + self.resource_group.args
    if self.accelerated_networking:
      cmd += ['--accelerated-networking', 'true']
    vm_util.IssueCommand(cmd)

  def _Exists(self):
    if self._deleted:
      return False
    # Same deal as AzurePublicIPAddress. 'show' doesn't error out if
    # the resource doesn't exist, but no-op 'set' does.
    stdout, _, _ = vm_util.IssueCommand(
        [azure.AZURE_PATH, 'network', 'nic', 'show',
         '--output', 'json',
         '--name', self.name] + self.resource_group.args)
    try:
      json.loads(stdout)
      return True
    except:
      return False

  def GetInternalIP(self):
    """Grab some data."""

    stdout, _ = vm_util.IssueRetryableCommand(
        [azure.AZURE_PATH, 'network', 'nic', 'show',
         '--output', 'json',
         '--name', self.name] + self.resource_group.args)

    response = json.loads(stdout)
    return response['ipConfigurations'][0]['privateIpAddress']

  def _Delete(self):
    self._deleted = True


class AzureVirtualMachineMetaClass(resource.AutoRegisterResourceMeta):
  """Metaclass for AzureVirtualMachine.

  Registers default image pattern for each operating system.
  """

  def __init__(self, name, bases, dct):
    super(AzureVirtualMachineMetaClass, self).__init__(name, bases, dct)
    if hasattr(self, 'OS_TYPE'):
      assert self.OS_TYPE, '{0} did not override OS_TYPE'.format(self.__name__)
      assert self.IMAGE_URN, (
          '{0} did not override IMAGE_URN'.format(self.__name__))


class AzureVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing an Azure Virtual Machine."""

  __metaclass__ = AzureVirtualMachineMetaClass
  CLOUD = providers.AZURE
  # Subclasses should override IMAGE_URN.
  IMAGE_URN = None

  def __init__(self, vm_spec):
    """Initialize an Azure virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVmSpec object of the vm.
    """
    super(AzureVirtualMachine, self).__init__(vm_spec)
    self.network = azure_network.AzureNetwork.GetNetwork(self)
    self.firewall = azure_network.AzureFirewall.GetFirewall()
    self.max_local_disks = 1
    self._lun_counter = itertools.count()
    self._deleted = False

    self.resource_group = azure_network.GetResourceGroup()
    self.public_ip = AzurePublicIPAddress(self.zone, self.name + '-public-ip')
    self.nic = AzureNIC(self.network.subnet,
                        self.name + '-nic', self.public_ip.name,
                        vm_spec.accelerated_networking)
    self.storage_account = self.network.storage_account
    self.image = vm_spec.image or self.IMAGE_URN

    disk_spec = disk.BaseDiskSpec('azure_os_disk')
    self.os_disk = azure_disk.AzureDisk(disk_spec,
                                        self.name,
                                        self.machine_type,
                                        self.storage_account,
                                        None,
                                        is_image=True)

  def _CreateDependencies(self):
    """Create VM dependencies."""
    self.public_ip.Create()
    self.nic.Create()

  def _Create(self):
    create_cmd = (
        [azure.AZURE_PATH, 'vm', 'create',
         '--location', self.zone,
         '--image', self.image,
         '--size', self.machine_type,
         '--admin-username', self.user_name,
         '--availability-set', self.network.avail_set.name,
         '--storage-sku', self.storage_account.storage_type,
         '--name', self.name] +
        self.resource_group.args +
        self.nic.args)

    if self.password:
      create_cmd.extend(['--admin-password', self.password])
    else:
      create_cmd.extend(['--ssh-key-value', self.ssh_public_key])

    # Uses a custom default because create for larger sizes sometimes times out.
    azure_vm_create_timeout = 600
    vm_util.IssueCommand(create_cmd, timeout=azure_vm_create_timeout)

  def _Exists(self):
    """Returns True if the VM exists."""
    if self._deleted:
      return False
    show_cmd = [
        azure.AZURE_PATH, 'vm', 'show', '--output', 'json',
        '--name', self.name
    ] + self.resource_group.args
    stdout, _, _ = vm_util.IssueCommand(show_cmd)
    try:
      json.loads(stdout)
      return True
    except:
      return False

  def _Delete(self):
    # The VM will be deleted when the resource group is.
    self._deleted = True

  @vm_util.Retry()
  def _PostCreate(self):
    """Get VM data."""
    stdout, _ = vm_util.IssueRetryableCommand(
        [azure.AZURE_PATH, 'vm', 'show',
         '--output', 'json',
         '--name', self.name] + self.resource_group.args)
    response = json.loads(stdout)
    self.os_disk.name = response['storageProfile']['osDisk']['name']
    self.os_disk.created = True
    self.internal_ip = self.nic.GetInternalIP()
    self.ip_address = self.public_ip.GetIPAddress()

  def AddMetadata(self, **tags):
    tag_list = ['tags.%s=%s' % (k, v) for k, v in tags.iteritems()]
    vm_util.IssueRetryableCommand(
        [azure.AZURE_PATH, 'vm', 'update',
         '--name', self.name] + self.resource_group.args +
        ['--set'] + tag_list)

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    disks = []

    for _ in xrange(disk_spec.num_striped_disks):
      if disk_spec.disk_type == disk.LOCAL:
        # Local disk numbers start at 1 (0 is the system disk).
        disk_number = self.local_disk_counter + 1
        self.local_disk_counter += 1
        lun = None
        if self.local_disk_counter > self.max_local_disks:
          raise errors.Error('Not enough local disks.')
      else:
        # Remote disk numbers start at 1 + max_local disks (0 is the system disk
        # and local disks occupy [1, max_local_disks]).
        disk_number = self.remote_disk_counter + 1 + self.max_local_disks
        self.remote_disk_counter += 1
        lun = next(self._lun_counter)
      data_disk = azure_disk.AzureDisk(disk_spec, self.name, self.machine_type,
                                       self.storage_account, lun)
      data_disk.disk_number = disk_number
      disks.append(data_disk)

    self._CreateScratchDiskFromDisks(disk_spec, disks)

  def DownloadPreprovisionedBenchmarkData(self, install_path, benchmark_name,
                                          filename):
    """Downloads a data file from Azure blob storage with pre-provisioned data.

    Use --azure_preprovisioned_data_bucket to specify the name of the account.

    Note: Azure blob storage does not allow underscores in the container name,
    so this method replaces any underscores in benchmark_name with dashes.
    Make sure that the same convention is used when uploading the data
    to Azure blob storage. For example: when uploading data for
    'benchmark_name' to Azure, create a container named 'benchmark-name'.

    Args:
      install_path: The install path on this VM.
      benchmark_name: Name of the benchmark associated with this data file.
      filename: The name of the file that was downloaded.
    """
    self.Install('azure_cli')
    self.Install('azure_credentials')
    self.RemoteCommand(
        GenerateDownloadPreprovisionedBenchmarkDataCommand(install_path,
                                                           benchmark_name,
                                                           filename))

  def GetResourceMetadata(self):
    result = super(AzureVirtualMachine, self).GetResourceMetadata()
    result['accelerated_networking'] = self.nic.accelerated_networking
    return result


class DebianBasedAzureVirtualMachine(AzureVirtualMachine,
                                     linux_virtual_machine.DebianMixin):
  IMAGE_URN = 'Canonical:UbuntuServer:14.04.4-LTS:latest'


class Ubuntu1404BasedAzureVirtualMachine(AzureVirtualMachine,
                                         linux_virtual_machine.Ubuntu1404Mixin):
  IMAGE_URN = 'Canonical:UbuntuServer:14.04.4-LTS:latest'


class Ubuntu1604BasedAzureVirtualMachine(AzureVirtualMachine,
                                         linux_virtual_machine.Ubuntu1604Mixin):
  IMAGE_URN = 'Canonical:UbuntuServer:16.04-LTS:latest'


class Ubuntu1710BasedAzureVirtualMachine(AzureVirtualMachine,
                                         linux_virtual_machine.Ubuntu1710Mixin):
  IMAGE_URN = 'Canonical:UbuntuServer:17.10:latest'


class RhelBasedAzureVirtualMachine(AzureVirtualMachine,
                                   linux_virtual_machine.RhelMixin):
  IMAGE_URN = 'RedHat:RHEL:7.2:latest'

  def __init__(self, vm_spec):
    super(RhelBasedAzureVirtualMachine, self).__init__(vm_spec)
    self.python_package_config = 'python'
    self.python_dev_package_config = 'python-devel'
    self.python_pip_package_config = 'python2-pip'


class CentosBasedAzureVirtualMachine(AzureVirtualMachine,
                                     linux_virtual_machine.Centos7Mixin):
  IMAGE_URN = 'OpenLogic:CentOS:7.4:latest'

  def __init__(self, vm_spec):
    super(CentosBasedAzureVirtualMachine, self).__init__(vm_spec)
    self.python_package_config = 'python'
    self.python_dev_package_config = 'python-devel'
    self.python_pip_package_config = 'python2-pip'


class WindowsAzureVirtualMachine(AzureVirtualMachine,
                                 windows_virtual_machine.WindowsMixin):
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2012-R2-Datacenter:latest'

  def __init__(self, vm_spec):
    super(WindowsAzureVirtualMachine, self).__init__(vm_spec)
    self.user_name = self.name
    self.password = vm_util.GenerateRandomWindowsPassword()

  def _PostCreate(self):
    super(WindowsAzureVirtualMachine, self)._PostCreate()
    config_dict = {'commandToExecute': windows_virtual_machine.STARTUP_SCRIPT}
    config = json.dumps(config_dict)
    vm_util.IssueRetryableCommand(
        [azure.AZURE_PATH, 'vm', 'extension', 'set',
         '--vm-name', self.name,
         '--name', 'CustomScriptExtension',
         '--publisher', 'Microsoft.Compute',
         '--version', '1.4',
         '--protected-settings=%s' % config] + self.resource_group.args)


def GenerateDownloadPreprovisionedBenchmarkDataCommand(install_path,
                                                       benchmark_name,
                                                       filename):
  """Returns a string used to download preprovisioned data."""
  benchmark_name_with_underscores_removed = benchmark_name.replace(
      '_', '-')
  destpath = posixpath.join(install_path, filename)
  # TODO(ferneyhough): Refactor this so that this mkdir command
  # is run on all clouds, and is os-agnostic (this is linux specific).
  mkdir_command = 'mkdir -p %s' % posixpath.dirname(destpath)
  download_command = ('az storage blob download '
                      '--no-progress '
                      '--account-name %s '
                      '--container-name %s '
                      '--name %s '
                      '--file %s' % (
                          FLAGS.azure_preprovisioned_data_bucket,
                          benchmark_name_with_underscores_removed,
                          filename,
                          destpath))
  return '{0} && {1}'.format(mkdir_command, download_command)
