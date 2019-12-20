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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import itertools
import json
import logging
import posixpath
import re
import threading

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
from perfkitbenchmarker.providers.azure import util

import six
from six.moves import range
import yaml

FLAGS = flags.FLAGS
NUM_LOCAL_VOLUMES = {
    'Standard_L8s_v2': 1,
    'Standard_L16s_v2': 2,
    'Standard_L32s_v2': 4,
    'Standard_L64s_v2': 8,
    'Standard_L80s_v2': 10
}


class AzureVmSpec(virtual_machine.BaseVmSpec):
  """Object containing the information needed to create a AzureVirtualMachine.

  Attributes:
    tier: None or string. performance tier of the machine.
    compute_units: int.  number of compute units for the machine.
    accelerated_networking: boolean. True if supports accelerated_networking.
    boot_disk_size: None or int. The size of the boot disk in GB.
    boot_disk_type: string or None. The type of the boot disk.
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
      config_values: dict mapping config option names to provided values. May be
        modified by this function.
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
        'machine_type':
            (custom_virtual_machine_spec.AzureMachineTypeDecoder, {}),
        'accelerated_networking': (option_decoders.BooleanDecoder, {
            'default': False
        }),
        'boot_disk_size': (option_decoders.IntDecoder, {
            'default': None
        }),
        'boot_disk_type': (option_decoders.StringDecoder, {
            'default': None
        }),
    })
    return result


# Per-VM resources are defined here.
class AzurePublicIPAddress(resource.BaseResource):
  """Class to represent an Azure Public IP Address."""

  def __init__(self, location, availability_zone, name):
    super(AzurePublicIPAddress, self).__init__()
    self.location = location
    self.availability_zone = availability_zone
    self.name = name
    self._deleted = False
    self.resource_group = azure_network.GetResourceGroup()

  def _Create(self):
    cmd = [
        azure.AZURE_PATH, 'network', 'public-ip', 'create', '--location',
        self.location, '--name', self.name
    ] + self.resource_group.args

    if self.availability_zone:
      cmd += ['--zone', self.availability_zone]

    vm_util.IssueCommand(cmd)

  def _Exists(self):
    if self._deleted:
      return False

    stdout, _, _ = vm_util.IssueCommand(
        [
            azure.AZURE_PATH, 'network', 'public-ip', 'show', '--output',
            'json', '--name', self.name
        ] + self.resource_group.args,
        raise_on_failure=False)
    try:
      json.loads(stdout)
      return True
    except ValueError:
      return False

  def GetIPAddress(self):
    stdout, _ = vm_util.IssueRetryableCommand([
        azure.AZURE_PATH, 'network', 'public-ip', 'show', '--output', 'json',
        '--name', self.name
    ] + self.resource_group.args)

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
    cmd = [
        azure.AZURE_PATH, 'network', 'nic', 'create', '--location',
        self.location, '--vnet-name', self.subnet.vnet.name, '--subnet',
        self.subnet.name, '--public-ip-address', self.public_ip, '--name',
        self.name
    ] + self.resource_group.args
    if self.accelerated_networking:
      cmd += ['--accelerated-networking', 'true']
    vm_util.IssueCommand(cmd)

  def _Exists(self):
    if self._deleted:
      return False
    # Same deal as AzurePublicIPAddress. 'show' doesn't error out if
    # the resource doesn't exist, but no-op 'set' does.
    stdout, _, _ = vm_util.IssueCommand(
        [
            azure.AZURE_PATH, 'network', 'nic', 'show', '--output', 'json',
            '--name', self.name
        ] + self.resource_group.args,
        raise_on_failure=False)
    try:
      json.loads(stdout)
      return True
    except ValueError:
      return False

  def GetInternalIP(self):
    """Grab some data."""

    stdout, _ = vm_util.IssueRetryableCommand([
        azure.AZURE_PATH, 'network', 'nic', 'show', '--output', 'json',
        '--name', self.name
    ] + self.resource_group.args)

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
      assert self.IMAGE_URN, ('{0} did not override IMAGE_URN'.format(
          self.__name__))


class AzureDedicatedHostGroup(resource.BaseResource):
  """Object representing an Azure host group (a collection of dedicated hosts).

  A host group is required for dedicated host creation.
  Attributes:
    name: The name of the vm - to be part of the host group name.
    location: The region the host group will exist in.
    resource_group: The group of resources for the host group.
  """

  def __init__(self, name, location, resource_group, availability_zone):
    super(AzureDedicatedHostGroup, self).__init__()
    self.name = name + 'Group'
    self.location = location
    self.resource_group = resource_group
    self.availability_zone = availability_zone

  def _Create(self):
    """See base class."""
    create_cmd = ([
        azure.AZURE_PATH,
        'vm',
        'host',
        'group',
        'create',
        '--name',
        self.name,
        '--location',
        self.location,
        # number of fault domains (physical racks) to span across
        # TODO(buggay): add support for multiple fault domains
        # https://docs.microsoft.com/en-us/azure/virtual-machines/windows/dedicated-hosts#high-availability-considerations
        '--platform-fault-domain-count',
        '1',
    ] + self.resource_group.args)

    if self.availability_zone:
      create_cmd.extend(['--zone', self.availability_zone])

    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """See base class."""
    delete_cmd = ([
        azure.AZURE_PATH,
        'vm',
        'host',
        'group',
        'delete',
        '--host-group',
        self.name,
    ] + self.resource_group.args)
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """See base class."""
    show_cmd = [
        azure.AZURE_PATH, 'vm', 'host', 'group', 'show', '--output', 'json',
        '--name', self.name
    ] + self.resource_group.args
    stdout, _, _ = vm_util.IssueCommand(show_cmd, raise_on_failure=False)
    try:
      json.loads(stdout)
      return True
    except ValueError:
      return False


def _GetSkuType(machine_type):
  """Returns the host SKU type derived from the VM machine type."""
  # TODO(buggay): add support for FSv2 machine types when no longer in preview
  # https://docs.microsoft.com/en-us/azure/virtual-machines/windows/dedicated-hosts
  sku = ''
  if re.match('Standard_D[0-9]*s_v3', machine_type):
    sku = 'DSv3-Type1'
  elif re.match('Standard_E[0-9]*s_v3', machine_type):
    sku = 'ESv3-Type1'
  else:
    raise ValueError('Dedicated hosting does not support machine type %s.' %
                     machine_type)
  return sku


class AzureDedicatedHost(resource.BaseResource):
  """Object representing an Azure host.

  Attributes:
    host_group: The required host group to which the host will belong.
    name: The name of the vm - to be part of the host name.
    location: The region the host will exist in.
    resource_group: The group of resources for the host.
  """
  _lock = threading.Lock()
  # globals guarded by _lock
  host_group_map = {}

  def __init__(self, name, location, resource_group, sku_type,
               availability_zone):
    super(AzureDedicatedHost, self).__init__()
    self.name = name + '-Host'
    self.location = location
    self.resource_group = resource_group
    self.sku_type = sku_type
    self.availability_zone = availability_zone
    self.host_group = None
    self.fill_fraction = 0.0

  def _CreateDependencies(self):
    """See base class."""
    with self._lock:
      if self.location not in self.host_group_map:
        new_host_group = AzureDedicatedHostGroup(self.name, self.location,
                                                 self.resource_group,
                                                 self.availability_zone)
        new_host_group.Create()
        self.host_group_map[self.location] = new_host_group.name
      self.host_group = self.host_group_map[self.location]

  def _Create(self):
    """See base class."""
    create_cmd = ([
        azure.AZURE_PATH,
        'vm',
        'host',
        'create',
        '--host-group',
        self.host_group,
        '--name',
        self.name,
        '--sku',
        self.sku_type,
        '--location',
        self.location,
        # the specific fault domain (physical rack) for the host dependent on
        # the number (count) of fault domains of the host group
        # TODO(buggay): add support for specifying multiple fault domains if
        # benchmarks require
        '--platform-fault-domain',
        '0',
    ] + self.resource_group.args)
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """See base class."""
    delete_cmd = ([
        azure.AZURE_PATH,
        'vm',
        'host',
        'delete',
        '--host-group',
        self.host_group,
        '--name',
        self.name,
        '--yes',
    ] + self.resource_group.args)
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """See base class."""
    show_cmd = [
        azure.AZURE_PATH,
        'vm',
        'host',
        'show',
        '--output',
        'json',
        '--name',
        self.name,
        '--host-group',
        self.host_group,
    ] + self.resource_group.args
    stdout, _, _ = vm_util.IssueCommand(show_cmd, raise_on_failure=False)
    try:
      json.loads(stdout)
      return True
    except ValueError:
      return False


class AzureVirtualMachine(
    six.with_metaclass(AzureVirtualMachineMetaClass,
                       virtual_machine.BaseVirtualMachine)):
  """Object representing an Azure Virtual Machine."""
  CLOUD = providers.AZURE
  # Subclasses should override IMAGE_URN.
  IMAGE_URN = None

  _lock = threading.Lock()
  # TODO(buggay): remove host groups & hosts as globals -> create new spec
  # globals guarded by _lock
  host_map = collections.defaultdict(list)

  def __init__(self, vm_spec):
    """Initialize an Azure virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVmSpec object of the vm.
    """
    super(AzureVirtualMachine, self).__init__(vm_spec)

    # PKB zone can be either a location or a location with an availability zone.
    # Format for Azure availability zone support is "location-availability_zone"
    # Example: eastus2-1 is Azure location eastus2 with availability zone 1.

    self.location = util.GetLocationFromZone(self.zone)
    self.availability_zone = util.GetAvailabilityZoneFromZone(self.zone)
    self.use_dedicated_host = vm_spec.use_dedicated_host
    self.num_vms_per_host = vm_spec.num_vms_per_host
    self.network = azure_network.AzureNetwork.GetNetwork(self)
    self.firewall = azure_network.AzureFirewall.GetFirewall()
    self.max_local_disks = NUM_LOCAL_VOLUMES.get(self.machine_type) or 1
    self._lun_counter = itertools.count()
    self._deleted = False

    self.resource_group = azure_network.GetResourceGroup()
    self.public_ip = AzurePublicIPAddress(self.location, self.availability_zone,
                                          self.name + '-public-ip')
    self.nic = AzureNIC(self.network.subnet, self.name + '-nic',
                        self.public_ip.name, vm_spec.accelerated_networking)
    self.storage_account = self.network.storage_account
    self.image = vm_spec.image or self.IMAGE_URN
    self.host = None
    if self.use_dedicated_host:
      self.host_series_sku = _GetSkuType(self.machine_type)
      self.host_list = None

    disk_spec = disk.BaseDiskSpec('azure_os_disk')
    disk_spec.disk_type = (
        vm_spec.boot_disk_type or self.storage_account.storage_type)
    if vm_spec.boot_disk_size:
      disk_spec.disk_size = vm_spec.boot_disk_size
    self.os_disk = azure_disk.AzureDisk(
        disk_spec,
        self.name,
        self.machine_type,
        self.storage_account,
        None,
        is_image=True)

  def _CreateDependencies(self):
    """Create VM dependencies."""
    self.public_ip.Create()
    self.nic.Create()

    if self.use_dedicated_host:
      with self._lock:
        self.host_list = self.host_map[(self.host_series_sku, self.location)]
        if (not self.host_list or (self.num_vms_per_host and
                                   self.host_list[-1].fill_fraction +
                                   1.0 / self.num_vms_per_host > 1.0)):
          new_host = AzureDedicatedHost(self.name, self.location,
                                        self.resource_group,
                                        self.host_series_sku,
                                        self.availability_zone)
          self.host_list.append(new_host)
          new_host.Create()
        self.host = self.host_list[-1]
        if self.num_vms_per_host:
          self.host.fill_fraction += 1.0 / self.num_vms_per_host

  def _Create(self):
    """See base class."""
    if self.os_disk.disk_size:
      disk_size_args = ['--os-disk-size-gb', str(self.os_disk.disk_size)]
    else:
      disk_size_args = []

    tags = {}
    tags.update(self.vm_metadata)
    tags.update(util.GetResourceTags(self.resource_group.timeout_minutes))
    tag_args = ['--tags'] + util.FormatTags(tags)

    create_cmd = ([
        azure.AZURE_PATH, 'vm', 'create', '--location', self.location,
        '--image', self.image, '--size', self.machine_type, '--admin-username',
        self.user_name, '--storage-sku', self.os_disk.disk_type, '--name',
        self.name
    ] + disk_size_args + self.resource_group.args + self.nic.args + tag_args)

    if self.availability_zone:
      create_cmd.extend(['--zone', self.availability_zone])

    # Resources in Availability Set are not allowed to be
    # deployed to particular hosts.
    if self.use_dedicated_host:
      create_cmd.extend(
          ['--host-group', self.host.host_group, '--host', self.host.name])
      num_hosts = len(self.host_list)

    if self.network.avail_set:
      create_cmd.extend(['--availability-set', self.network.avail_set.name])

    if self.password:
      create_cmd.extend(['--admin-password', self.password])
    else:
      create_cmd.extend(['--ssh-key-value', self.ssh_public_key])

    # Uses a custom default because create has a very long tail.
    azure_vm_create_timeout = 1200
    _, stderr, retcode = vm_util.IssueCommand(
        create_cmd, timeout=azure_vm_create_timeout, raise_on_failure=False)
    if retcode and ('Error Code: QuotaExceeded' in stderr or
                    'exceeding quota limit' in stderr):
      raise errors.Benchmarks.QuotaFailure(
          virtual_machine.QUOTA_EXCEEDED_MESSAGE + stderr)
    # TODO(buggay) refactor to share code with gcp_virtual_machine.py
    if (self.use_dedicated_host and retcode and
        'AllocationFailed' in stderr):
      if self.num_vms_per_host:
        raise errors.Resource.CreationError(
            'Failed to create host: %d vms of type %s per host exceeds '
            'memory capacity limits of the host' %
            (self.num_vms_per_host, self.machine_type))
      else:
        logging.warning(
            'Creation failed due to insufficient host capacity. A new host will '
            'be created and instance creation will be retried.')
        with self._lock:
          if num_hosts == len(self.host_list):
            new_host = AzureDedicatedHost(self.name, self.location,
                                          self.resource_group,
                                          self.host_series_sku,
                                          self.availability_zone)
            self.host_list.append(new_host)
            new_host.Create()
          self.host = self.host_list[-1]
        raise errors.Resource.RetryableCreationError()
    if (not self.use_dedicated_host and retcode and
        ('AllocationFailed' in stderr or
         'OverconstrainedZonalAllocationRequest' in stderr)):
      raise errors.Benchmarks.InsufficientCapacityCloudFailure(stderr)
    if retcode:
      raise errors.Resource.CreationError(
          'Failed to create VM: %s return code: %s' % (stderr, retcode))

  def _Exists(self):
    """Returns True if the VM exists."""
    if self._deleted:
      return False
    show_cmd = [
        azure.AZURE_PATH, 'vm', 'show', '--output', 'json', '--name', self.name
    ] + self.resource_group.args
    stdout, _, _ = vm_util.IssueCommand(show_cmd, raise_on_failure=False)
    try:
      json.loads(stdout)
      return True
    except ValueError:
      return False

  def _Delete(self):
    # The VM will be deleted when the resource group is.
    self._deleted = True

  @vm_util.Retry()
  def _PostCreate(self):
    """Get VM data."""
    stdout, _ = vm_util.IssueRetryableCommand([
        azure.AZURE_PATH, 'vm', 'show', '--output', 'json', '--name', self.name
    ] + self.resource_group.args)
    response = json.loads(stdout)
    self.os_disk.name = response['storageProfile']['osDisk']['name']
    self.os_disk.created = True
    vm_util.IssueCommand([
        azure.AZURE_PATH, 'disk', 'update', '--name', self.os_disk.name,
        '--set',
        util.GetTagsJson(self.resource_group.timeout_minutes)
    ] + self.resource_group.args)
    self.internal_ip = self.nic.GetInternalIP()
    self.ip_address = self.public_ip.GetIPAddress()

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.

    Raises:
      CreationError: If an SMB disk is listed but the SMB service not created.
    """
    disks = []

    for _ in range(disk_spec.num_striped_disks):
      if disk_spec.disk_type == disk.NFS:
        data_disk = self._GetNfsService().CreateNfsDisk()
        disks.append(data_disk)
        continue
      elif disk_spec.disk_type == disk.SMB:
        data_disk = self._GetSmbService().CreateSmbDisk()
        disks.append(data_disk)
        continue
      elif disk_spec.disk_type == disk.LOCAL:
        # Local disk numbers start at 1 (0 is the system disk).
        disk_number = self.local_disk_counter + 1
        self.local_disk_counter += 1
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

  def DownloadPreprovisionedData(self, install_path, module_name, filename):
    """Downloads a data file from Azure blob storage with pre-provisioned data.

    Use --azure_preprovisioned_data_bucket to specify the name of the account.

    Note: Azure blob storage does not allow underscores in the container name,
    so this method replaces any underscores in module_name with dashes.
    Make sure that the same convention is used when uploading the data
    to Azure blob storage. For example: when uploading data for
    'module_name' to Azure, create a container named 'benchmark-name'.

    Args:
      install_path: The install path on this VM.
      module_name: Name of the module associated with this data file.
      filename: The name of the file that was downloaded.
    """
    self.Install('azure_cli')
    self.Install('azure_credentials')
    self.RemoteCommand(
        GenerateDownloadPreprovisionedDataCommand(install_path, module_name,
                                                  filename))

  def ShouldDownloadPreprovisionedData(self, module_name, filename):
    """Returns whether or not preprovisioned data is available."""
    self.Install('azure_cli')
    self.Install('azure_credentials')
    return FLAGS.azure_preprovisioned_data_bucket and self.TryRemoteCommand(
        GenerateStatPreprovisionedDataCommand(module_name, filename))

  def GetResourceMetadata(self):
    result = super(AzureVirtualMachine, self).GetResourceMetadata()
    result['accelerated_networking'] = self.nic.accelerated_networking
    result['boot_disk_type'] = self.os_disk.disk_type
    result['boot_disk_size'] = self.os_disk.disk_size or 'default'
    if self.use_dedicated_host:
      result['num_vms_per_host'] = self.num_vms_per_host
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


class Ubuntu1804BasedAzureVirtualMachine(AzureVirtualMachine,
                                         linux_virtual_machine.Ubuntu1804Mixin):
  IMAGE_URN = 'Canonical:UbuntuServer:18.04-LTS:latest'


class RhelBasedAzureVirtualMachine(AzureVirtualMachine,
                                   linux_virtual_machine.RhelMixin):
  IMAGE_URN = 'RedHat:RHEL:7.4:latest'

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


class CoreOsBasedAzureVirtualMachine(AzureVirtualMachine,
                                     linux_virtual_machine.CoreOsMixin):
  IMAGE_URN = 'CoreOS:CoreOS:Stable:latest'


class WindowsAzureVirtualMachine(AzureVirtualMachine,
                                 windows_virtual_machine.WindowsMixin):
  """Class supporting Windows Azure virtual machines."""

  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2012-R2-Datacenter:latest'

  def __init__(self, vm_spec):
    super(WindowsAzureVirtualMachine, self).__init__(vm_spec)
    # The names of Windows VMs on Azure are limited to 15 characters so let's
    # drop the pkb prefix if necessary.
    if len(self.name) > 15:
      self.name = re.sub('^pkb-', '', self.name)
    self.user_name = self.name
    self.password = vm_util.GenerateRandomWindowsPassword()

  def _PostCreate(self):
    super(WindowsAzureVirtualMachine, self)._PostCreate()
    config_dict = {'commandToExecute': windows_virtual_machine.STARTUP_SCRIPT}
    config = json.dumps(config_dict)
    vm_util.IssueRetryableCommand([
        azure.AZURE_PATH, 'vm', 'extension', 'set', '--vm-name', self.name,
        '--name', 'CustomScriptExtension', '--publisher', 'Microsoft.Compute',
        '--version', '1.4',
        '--protected-settings=%s' % config
    ] + self.resource_group.args)


# Azure seems to have dropped support for 2012 Server Core. It is neither here:
# https://docs.microsoft.com/en-us/azure/virtual-machines/windows/cli-ps-findimage#table-of-commonly-used-windows-images
# nor in `az vm image list -p MicrosoftWindowsServer -f WindowsServer -s 2012`
# Rather than exclude this just allow 2012 to refer to the 2012 Base image.
class Windows2012CoreAzureVirtualMachine(
    WindowsAzureVirtualMachine, windows_virtual_machine.Windows2012CoreMixin):
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2012-R2-Datacenter:latest'


class Windows2016CoreAzureVirtualMachine(
    WindowsAzureVirtualMachine, windows_virtual_machine.Windows2016CoreMixin):
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2016-Datacenter-Server-Core:latest'


class Windows2019CoreAzureVirtualMachine(
    WindowsAzureVirtualMachine, windows_virtual_machine.Windows2019CoreMixin):
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2019-Datacenter-Core:latest'


class Windows2012BaseAzureVirtualMachine(
    WindowsAzureVirtualMachine, windows_virtual_machine.Windows2012BaseMixin):
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2012-R2-Datacenter:latest'


class Windows2016BaseAzureVirtualMachine(
    WindowsAzureVirtualMachine, windows_virtual_machine.Windows2016BaseMixin):
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2016-Datacenter:latest'


class Windows2019BaseAzureVirtualMachine(
    WindowsAzureVirtualMachine, windows_virtual_machine.Windows2019BaseMixin):
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2019-Datacenter:latest'


def GenerateDownloadPreprovisionedDataCommand(install_path, module_name,
                                              filename):
  """Returns a string used to download preprovisioned data."""
  module_name_with_underscores_removed = module_name.replace('_', '-')
  destpath = posixpath.join(install_path, filename)
  # TODO(ferneyhough): Refactor this so that this mkdir command
  # is run on all clouds, and is os-agnostic (this is linux specific).
  mkdir_command = 'mkdir -p %s' % posixpath.dirname(destpath)
  download_command = (
      'az storage blob download '
      '--no-progress '
      '--account-name %s '
      '--container-name %s '
      '--name %s '
      '--file %s' % (FLAGS.azure_preprovisioned_data_bucket,
                     module_name_with_underscores_removed, filename, destpath))
  return '{0} && {1}'.format(mkdir_command, download_command)


def GenerateStatPreprovisionedDataCommand(module_name, filename):
  """Returns a string used to download preprovisioned data."""
  module_name_with_underscores_removed = module_name.replace('_', '-')
  return ('az storage blob show '
          '--account-name %s '
          '--container-name %s '
          '--name %s' % (FLAGS.azure_preprovisioned_data_bucket,
                         module_name_with_underscores_removed, filename))
