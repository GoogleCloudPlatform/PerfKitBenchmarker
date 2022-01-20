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


import abc
import collections
import itertools
import json
import logging
import posixpath
import re
import threading

from absl import flags
from perfkitbenchmarker import custom_virtual_machine_spec
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import placement_group
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

# https://docs.microsoft.com/en-us/azure/virtual-machines/windows/scheduled-events
_SCHEDULED_EVENTS_CMD = ('curl -H Metadata:true http://169.254.169.254/metadata'
                         '/scheduledevents?api-version=2019-01-01')

_SCHEDULED_EVENTS_CMD_WIN = ('Invoke-RestMethod -Headers @{"Metadata"="true"} '
                             '-Uri http://169.254.169.254/metadata/'
                             'scheduledevents?api-version=2019-01-01 | '
                             'ConvertTo-Json')


class AzureVmSpec(virtual_machine.BaseVmSpec):
  """Object containing the information needed to create a AzureVirtualMachine.

  Attributes:
    tier: None or string. performance tier of the machine.
    compute_units: int.  number of compute units for the machine.
    accelerated_networking: boolean. True if supports accelerated_networking.
    boot_disk_size: None or int. The size of the boot disk in GB.
    boot_disk_type: string or None. The type of the boot disk.
    low_priority: boolean. True if the VM should be low-priority, else False.
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
      config_values['machine_type'] = yaml.safe_load(flag_values.machine_type)
    if flag_values['azure_accelerated_networking'].present:
      config_values['accelerated_networking'] = (
          flag_values.azure_accelerated_networking)
    if flag_values['azure_low_priority_vms'].present:
      config_values['low_priority'] = flag_values.azure_low_priority_vms

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
        'low_priority': (option_decoders.BooleanDecoder, {
            'default': False
        }),
    })
    return result


# Per-VM resources are defined here.
class AzurePublicIPAddress(resource.BaseResource):
  """Class to represent an Azure Public IP Address."""

  def __init__(self, region, availability_zone, name, dns_name=None):
    super(AzurePublicIPAddress, self).__init__()
    self.region = region
    self.availability_zone = availability_zone
    self.name = name
    self._deleted = False
    self.resource_group = azure_network.GetResourceGroup()
    self.dns_name = dns_name

  def _Create(self):
    cmd = [
        azure.AZURE_PATH, 'network', 'public-ip', 'create', '--location',
        self.region, '--name', self.name
    ] + self.resource_group.args

    if self.availability_zone:
      # Availability Zones require Standard IPs.
      cmd += ['--zone', self.availability_zone, '--sku', 'Standard']

    if self.dns_name:
      cmd += ['--dns-name', self.dns_name]

    _, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)

    if retcode and re.search(r'Cannot create more than \d+ public IP addresses',
                             stderr):
      raise errors.Benchmarks.QuotaFailure(
          virtual_machine.QUOTA_EXCEEDED_MESSAGE + stderr)

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

  def __init__(self,
               subnet,
               name,
               public_ip,
               accelerated_networking,
               network_security_group=None,
               private_ip=None):
    super(AzureNIC, self).__init__()
    self.subnet = subnet
    self.name = name
    self.public_ip = public_ip
    self.private_ip = private_ip
    self._deleted = False
    self.resource_group = azure_network.GetResourceGroup()
    self.region = self.subnet.vnet.region
    self.args = ['--nics', self.name]
    self.accelerated_networking = accelerated_networking
    self.network_security_group = network_security_group

  def _Create(self):
    cmd = [
        azure.AZURE_PATH, 'network', 'nic', 'create', '--location',
        self.region, '--vnet-name', self.subnet.vnet.name, '--subnet',
        self.subnet.name, '--public-ip-address', self.public_ip, '--name',
        self.name
    ] + self.resource_group.args
    if self.private_ip:
      cmd += ['--private-ip-address', self.private_ip]
    if self.accelerated_networking:
      cmd += ['--accelerated-networking', 'true']
    if self.network_security_group:
      cmd += ['--network-security-group', self.network_security_group.name]
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


class AzureDedicatedHostGroup(resource.BaseResource):
  """Object representing an Azure host group (a collection of dedicated hosts).

  A host group is required for dedicated host creation.
  Attributes:
    name: The name of the vm - to be part of the host group name.
    location: The region the host group will exist in.
    resource_group: The group of resources for the host group.
  """

  def __init__(self, name, region, resource_group, availability_zone):
    super(AzureDedicatedHostGroup, self).__init__()
    self.name = name + 'Group'
    self.region = region
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
        self.region,
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
    region: The region the host will exist in.
    resource_group: The group of resources for the host.
  """
  _lock = threading.Lock()
  # globals guarded by _lock
  host_group_map = {}

  def __init__(self, name, region, resource_group, sku_type,
               availability_zone):
    super(AzureDedicatedHost, self).__init__()
    self.name = name + '-Host'
    self.region = region
    self.resource_group = resource_group
    self.sku_type = sku_type
    self.availability_zone = availability_zone
    self.host_group = None
    self.fill_fraction = 0.0

  def _CreateDependencies(self):
    """See base class."""
    with self._lock:
      if self.region not in self.host_group_map:
        new_host_group = AzureDedicatedHostGroup(self.name, self.region,
                                                 self.resource_group,
                                                 self.availability_zone)
        new_host_group.Create()
        self.host_group_map[self.region] = new_host_group.name
      self.host_group = self.host_group_map[self.region]

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
        self.region,
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


class AzureVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing an Azure Virtual Machine."""
  CLOUD = providers.AZURE

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

    # PKB zone can be either a region or a region with an availability zone.
    # Format for Azure availability zone support is "region-availability_zone"
    # Example: eastus2-1 is Azure region eastus2 with availability zone 1.

    self.region = util.GetRegionFromZone(self.zone)
    self.availability_zone = util.GetAvailabilityZoneFromZone(self.zone)
    self.use_dedicated_host = vm_spec.use_dedicated_host
    self.num_vms_per_host = vm_spec.num_vms_per_host
    self.network = azure_network.AzureNetwork.GetNetwork(self)
    self.firewall = azure_network.AzureFirewall.GetFirewall()
    self.max_local_disks = NUM_LOCAL_VOLUMES.get(self.machine_type) or 1
    self._lun_counter = itertools.count()
    self._deleted = False

    self.resource_group = azure_network.GetResourceGroup()
    self.public_ip = AzurePublicIPAddress(self.region, self.availability_zone,
                                          self.name + '-public-ip')
    self.nic = AzureNIC(self.network.subnet, self.name + '-nic',
                        self.public_ip.name, vm_spec.accelerated_networking,
                        self.network.nsg)
    self.storage_account = self.network.storage_account
    self.image = vm_spec.image or type(self).IMAGE_URN
    self.host = None
    if self.use_dedicated_host:
      self.host_series_sku = _GetSkuType(self.machine_type)
      self.host_list = None
    self.low_priority = vm_spec.low_priority
    self.low_priority_status_code = None
    self.spot_early_termination = False
    self.ultra_ssd_enabled = False

    disk_spec = disk.BaseDiskSpec('azure_os_disk')
    disk_spec.disk_type = (
        vm_spec.boot_disk_type or self.storage_account.storage_type)
    if vm_spec.boot_disk_size:
      disk_spec.disk_size = vm_spec.boot_disk_size
    self.os_disk = azure_disk.AzureDisk(
        disk_spec,
        self,
        None,
        is_image=True)

  @property
  @classmethod
  @abc.abstractmethod
  def IMAGE_URN(cls):
    raise NotImplementedError()

  def _CreateDependencies(self):
    """Create VM dependencies."""
    self.public_ip.Create()
    self.nic.Create()

    if self.use_dedicated_host:
      with self._lock:
        self.host_list = self.host_map[(self.host_series_sku, self.region)]
        if (not self.host_list or (self.num_vms_per_host and
                                   self.host_list[-1].fill_fraction +
                                   1.0 / self.num_vms_per_host > 1.0)):
          new_host = AzureDedicatedHost(self.name, self.region,
                                        self.resource_group,
                                        self.host_series_sku,
                                        self.availability_zone)
          self.host_list.append(new_host)
          new_host.Create()
        self.host = self.host_list[-1]
        if self.num_vms_per_host:
          self.host.fill_fraction += 1.0 / self.num_vms_per_host

  def _RequiresUltraDisk(self):
    return any(disk_spec.disk_type == azure_disk.ULTRA_STORAGE
               for disk_spec in self.disk_specs)

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
        azure.AZURE_PATH, 'vm', 'create', '--location', self.region,
        '--image', self.image, '--size', self.machine_type, '--admin-username',
        self.user_name, '--storage-sku', self.os_disk.disk_type, '--name',
        self.name
    ] + disk_size_args + self.resource_group.args + self.nic.args + tag_args)

    if self._RequiresUltraDisk():
      self.ultra_ssd_enabled = True
      create_cmd.extend(['--ultra-ssd-enabled'])

    if self.availability_zone:
      create_cmd.extend(['--zone', self.availability_zone])

    # Resources in Availability Set are not allowed to be
    # deployed to particular hosts.
    if self.use_dedicated_host:
      create_cmd.extend(
          ['--host-group', self.host.host_group, '--host', self.host.name])
      num_hosts = len(self.host_list)

    if self.network.placement_group:
      create_cmd.extend(self.network.placement_group.AddVmArgs())

    if self.low_priority:
      create_cmd.extend(['--priority', 'Spot'])

    if self.password:
      create_cmd.extend(['--admin-password', self.password])
    else:
      create_cmd.extend(['--ssh-key-value', self.ssh_public_key])

    # Uses a custom default because create has a very long tail.
    azure_vm_create_timeout = 1800
    _, stderr, retcode = vm_util.IssueCommand(
        create_cmd, timeout=azure_vm_create_timeout, raise_on_failure=False)
    if retcode:
      if 'quota' in stderr.lower():
        raise errors.Benchmarks.QuotaFailure(
            virtual_machine.QUOTA_EXCEEDED_MESSAGE + stderr)
      elif re.search(r'requested VM size \S+ is not available', stderr):
        raise errors.Benchmarks.UnsupportedConfigError(stderr)
      elif self.low_priority and 'OverconstrainedAllocationRequest' in stderr:
        raise errors.Benchmarks.InsufficientCapacityCloudFailure(stderr)
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
            new_host = AzureDedicatedHost(self.name, self.region,
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

  def _Start(self):
    """Starts the VM."""
    start_cmd = ([azure.AZURE_PATH, 'vm', 'start', '--name', self.name] +
                 self.resource_group.args)
    vm_util.IssueCommand(start_cmd)
    self.ip_address = self.public_ip.GetIPAddress()

  def _Stop(self):
    """Stops the VM."""
    stop_cmd = ([azure.AZURE_PATH, 'vm', 'stop', '--name', self.name] +
                self.resource_group.args)
    vm_util.IssueCommand(stop_cmd)
    # remove resources, similar to GCE stop
    deallocate_cmd = (
        [azure.AZURE_PATH, 'vm', 'deallocate', '--name', self.name] +
        self.resource_group.args)
    vm_util.IssueCommand(deallocate_cmd)

  def _Suspend(self):
    """Suspends the VM."""
    raise NotImplementedError()

  def _Resume(self):
    """Resumes the VM."""
    raise NotImplementedError()

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
        # Remote disk numbers start at max_local disks, Azure does not separate
        # local disk and system disk.
        disk_number = self.remote_disk_counter + self.max_local_disks
        self.remote_disk_counter += 1
      lun = next(self._lun_counter)
      data_disk = azure_disk.AzureDisk(disk_spec, self, lun)
      data_disk.disk_number = disk_number
      disks.append(data_disk)

    self._CreateScratchDiskFromDisks(disk_spec, disks)

  def InstallCli(self):
    """Installs the Azure cli and credentials on this Azure vm."""
    self.Install('azure_cli')
    self.Install('azure_credentials')

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
    # N.B. Should already be installed by ShouldDownloadPreprovisionedData
    self.Install('azure_cli')
    self.RemoteCommand(
        GenerateDownloadPreprovisionedDataCommand(install_path, module_name,
                                                  filename))

  def ShouldDownloadPreprovisionedData(self, module_name, filename):
    """Returns whether or not preprovisioned data is available."""
    # Do not install credentials. Data are fetched using locally generated
    # connection strings and do not use credentials on the VM.
    self.Install('azure_cli')
    return FLAGS.azure_preprovisioned_data_bucket and self.TryRemoteCommand(
        GenerateStatPreprovisionedDataCommand(module_name, filename))

  def GetResourceMetadata(self):
    result = super(AzureVirtualMachine, self).GetResourceMetadata()
    result['accelerated_networking'] = self.nic.accelerated_networking
    result['boot_disk_type'] = self.os_disk.disk_type
    result['boot_disk_size'] = self.os_disk.disk_size
    if self.network.placement_group:
      result['placement_group_strategy'] = self.network.placement_group.strategy
    else:
      result['placement_group_strategy'] = placement_group.PLACEMENT_GROUP_NONE
    result['preemptible'] = self.low_priority
    if self.use_dedicated_host:
      result['num_vms_per_host'] = self.num_vms_per_host
    return result

  def _UpdateInterruptibleVmStatusThroughMetadataService(self):
    stdout, stderr, return_code = self.RemoteCommandWithReturnCode(
        _SCHEDULED_EVENTS_CMD)
    if return_code:
      logging.error('Checking Interrupt Error: %s', stderr)
    else:
      events = json.loads(stdout).get('Events', [])
      self.spot_early_termination = any(
          event.get('EventType') == 'Preempt' for event in events)
      if self.spot_early_termination:
        logging.info('Spotted early termination on %s', self)

  def IsInterruptible(self):
    """Returns whether this vm is a interruptible vm (e.g. spot, preemptible).

    Returns:
      True if this vm is a interruptible vm.
    """
    return self.low_priority

  def WasInterrupted(self):
    """Returns whether this low-priority vm was terminated early by Azure.

    Returns: True if this vm was terminated early by Azure.
    """
    return self.spot_early_termination

  def GetVmStatusCode(self):
    """Returns the early termination code if any.

    Returns: Early termination code.
    """
    return self.low_priority_status_code


class Debian9BasedAzureVirtualMachine(AzureVirtualMachine,
                                      linux_virtual_machine.Debian9Mixin):
  # From https://wiki.debian.org/Cloud/MicrosoftAzure
  IMAGE_URN = 'credativ:Debian:9:latest'


class Debian10BasedAzureVirtualMachine(AzureVirtualMachine,
                                       linux_virtual_machine.Debian10Mixin):
  # From https://wiki.debian.org/Cloud/MicrosoftAzure
  IMAGE_URN = 'Debian:debian-10:10:latest'


class Debian11BasedAzureVirtualMachine(AzureVirtualMachine,
                                       linux_virtual_machine.Debian11Mixin):
  # From https://wiki.debian.org/Cloud/MicrosoftAzure
  IMAGE_URN = 'Debian:debian-11:11:latest'


class Ubuntu1604BasedAzureVirtualMachine(AzureVirtualMachine,
                                         linux_virtual_machine.Ubuntu1604Mixin):
  IMAGE_URN = 'Canonical:UbuntuServer:16.04-LTS:latest'


class Ubuntu1804BasedAzureVirtualMachine(AzureVirtualMachine,
                                         linux_virtual_machine.Ubuntu1804Mixin):
  IMAGE_URN = 'Canonical:UbuntuServer:18.04-LTS:latest'


class Ubuntu2004BasedAzureVirtualMachine(AzureVirtualMachine,
                                         linux_virtual_machine.Ubuntu2004Mixin):
  IMAGE_URN = 'Canonical:0001-com-ubuntu-server-focal:20_04-lts:latest'


class Rhel7BasedAzureVirtualMachine(AzureVirtualMachine,
                                    linux_virtual_machine.Rhel7Mixin):
  IMAGE_URN = 'RedHat:RHEL:7-LVM:latest'


class Rhel8BasedAzureVirtualMachine(AzureVirtualMachine,
                                    linux_virtual_machine.Rhel8Mixin):
  IMAGE_URN = 'RedHat:RHEL:8-LVM:latest'


class CentOs7BasedAzureVirtualMachine(AzureVirtualMachine,
                                      linux_virtual_machine.CentOs7Mixin):
  IMAGE_URN = 'OpenLogic:CentOS-LVM:7-lvm:latest'


class CentOs8BasedAzureVirtualMachine(AzureVirtualMachine,
                                      linux_virtual_machine.CentOs8Mixin):
  IMAGE_URN = 'OpenLogic:CentOS-LVM:8-lvm:latest'


# TODO(pclay): Add Fedora CoreOS when available:
#   https://docs.fedoraproject.org/en-US/fedora-coreos/provisioning-azure/


class BaseWindowsAzureVirtualMachine(AzureVirtualMachine,
                                     windows_virtual_machine.BaseWindowsMixin):
  """Class supporting Windows Azure virtual machines."""

  # This ia a required attribute, but this is a base class.
  IMAGE_URN = 'non-existent'

  def __init__(self, vm_spec):
    super(BaseWindowsAzureVirtualMachine, self).__init__(vm_spec)
    # The names of Windows VMs on Azure are limited to 15 characters so let's
    # drop the pkb prefix if necessary.
    if len(self.name) > 15:
      self.name = re.sub('^pkb-', '', self.name)
    self.user_name = self.name
    self.password = vm_util.GenerateRandomWindowsPassword()

  def _PostCreate(self):
    super(BaseWindowsAzureVirtualMachine, self)._PostCreate()
    config_dict = {'commandToExecute': windows_virtual_machine.STARTUP_SCRIPT}
    config = json.dumps(config_dict)
    vm_util.IssueRetryableCommand([
        azure.AZURE_PATH, 'vm', 'extension', 'set', '--vm-name', self.name,
        '--name', 'CustomScriptExtension', '--publisher', 'Microsoft.Compute',
        '--version', '1.4',
        '--protected-settings=%s' % config
    ] + self.resource_group.args)

  def _UpdateInterruptibleVmStatusThroughMetadataService(self):
    stdout, _ = self.RemoteCommand(_SCHEDULED_EVENTS_CMD_WIN)
    events = json.loads(stdout).get('Events', [])
    self.spot_early_termination = any(
        event.get('EventType') == 'Preempt' for event in events)
    if self.spot_early_termination:
      logging.info('Spotted early termination on %s', self)


# Azure seems to have dropped support for 2012 Server Core. It is neither here:
# https://docs.microsoft.com/en-us/azure/virtual-machines/windows/cli-ps-findimage#table-of-commonly-used-windows-images
# nor in `az vm image list -p MicrosoftWindowsServer -f WindowsServer -s 2012`
# Rather than exclude this just allow 2012 to refer to the 2012 Base image.
class Windows2012CoreAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine,
    windows_virtual_machine.Windows2012CoreMixin):
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2012-R2-Datacenter:latest'


class Windows2016CoreAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine,
    windows_virtual_machine.Windows2016CoreMixin):
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2016-Datacenter-Server-Core:latest'


class Windows2019CoreAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine,
    windows_virtual_machine.Windows2019CoreMixin):
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2019-Datacenter-Core:latest'


class Windows2012DesktopAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine,
    windows_virtual_machine.Windows2012DesktopMixin):
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2012-R2-Datacenter:latest'


class Windows2016DesktopAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine,
    windows_virtual_machine.Windows2016DesktopMixin):
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2016-Datacenter:latest'


class Windows2019DesktopAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine,
    windows_virtual_machine.Windows2019DesktopMixin):
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2019-Datacenter:latest'


class Windows2019DesktopSQLServer2019StandardAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine,
    windows_virtual_machine.Windows2019SQLServer2019Standard):
  IMAGE_URN = 'MicrosoftSQLServer:sql2019-ws2019:standard:latest'


class Windows2019DesktopSQLServer2019EnterpriseAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine,
    windows_virtual_machine.Windows2019SQLServer2019Enterprise):
  IMAGE_URN = 'MicrosoftSQLServer:sql2019-ws2019:enterprise:latest'


def GenerateDownloadPreprovisionedDataCommand(install_path, module_name,
                                              filename):
  """Returns a string used to download preprovisioned data."""
  module_name_with_underscores_removed = module_name.replace('_', '-')
  destpath = posixpath.join(install_path, filename)
  if install_path:
    # TODO(ferneyhough): Refactor this so that this mkdir command
    # is run on all clouds, and is os-agnostic (this is linux specific).
    mkdir_command = 'mkdir -p %s' % posixpath.dirname(destpath)

  account_name = FLAGS.azure_preprovisioned_data_bucket
  connection_string = util.GetAzureStorageConnectionString(account_name, [])
  download_command = (
      'az storage blob download '
      '--no-progress '
      '--account-name {account_name} '
      '--container-name {container_name} '
      '--name {name} '
      '--file {file} '
      '--connection-string "{connection_string}"'.format(
          account_name=account_name,
          container_name=module_name_with_underscores_removed,
          name=filename,
          file=destpath,
          connection_string=connection_string))
  if install_path:
    return '{0} && {1}'.format(mkdir_command, download_command)
  return download_command


def GenerateStatPreprovisionedDataCommand(module_name, filename):
  """Returns a string used to download preprovisioned data."""
  module_name_with_underscores_removed = module_name.replace('_', '-')
  account_name = FLAGS.azure_preprovisioned_data_bucket
  connection_string = util.GetAzureStorageConnectionString(account_name, [])
  return ('az storage blob show '
          '--account-name {account_name} '
          '--container-name {container_name} '
          '--name {name} '
          '--connection-string "{connection_string}"'.format(
              account_name=account_name,
              container_name=module_name_with_underscores_removed,
              name=filename,
              connection_string=connection_string))
