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
import ntpath
import posixpath
import re
import threading

from absl import flags
from perfkitbenchmarker import custom_virtual_machine_spec
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import os_types
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_virtual_machine
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_disk
from perfkitbenchmarker.providers.azure import azure_disk_strategies
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import util
import six
import yaml

FLAGS = flags.FLAGS
NUM_LOCAL_VOLUMES: dict[str, int] = {
    'Standard_L8s_v2': 1,
    'Standard_L16s_v2': 2,
    'Standard_L32s_v2': 4,
    'Standard_L48s_v2': 6,
    'Standard_L64s_v2': 8,
    'Standard_L80s_v2': 10,
    'Standard_L8s_v3': 1,
    'Standard_L16s_v3': 2,
    'Standard_L32s_v3': 4,
    'Standard_L48s_v3': 6,
    'Standard_L64s_v3': 8,
    'Standard_L80s_v3': 10,
    'Standard_L8as_v3': 1,
    'Standard_L16as_v3': 2,
    'Standard_L32as_v3': 4,
    'Standard_L48as_v3': 6,
    'Standard_L64as_v3': 8,
    'Standard_L80as_v3': 10,
}

_MACHINE_TYPES_ONLY_SUPPORT_GEN2_IMAGES = (
    'Standard_ND96asr_v4',
    'Standard_ND96asr_A100_v4',
    'Standard_ND96amsr_A100_v4',
    'Standard_ND96isr_H100_v5',
    'Standard_M208ms_v2',
    'Standard_M208s_v2',
    'Standard_M416ms_v2',
    'Standard_M416s_v2',
    'Standard_ND40rs_v2',
    'Standard_M32ms_v2',
    'Standard_M64s_v2',
    'Standard_M64ms_v2',
    'Standard_M128s_v2',
    'Standard_M128ms_v2',
    'Standard_M192is_v2',
    'Standard_M192ims_v2',
    'Standard_M32dms_v2',
    'Standard_M64ds_v2',
    'Standard_M128ds_v2',
    'Standard_M128dms_v2',
    'Standard_M192ids_v2',
    'Standard_M192idms_v2',
    'Standard_DC2s_v2',
    'Standard_DC2s_v3',
    'Standard_DC32ds_v3',
    'Standard_DC32s_v3',
    'Standard_DC48ds_v3',
    'Standard_DC48s_v3',
    'Standard_DC4ds_v3',
    'Standard_DC4s_v2',
    'Standard_DC4s_v3',
    'Standard_DC8_v2',
    'Standard_DC8ds_v3',
    'Standard_DC8s_v3',
    'Standard_FX12mds',
    'Standard_FX24mds',
    'Standard_FX36mds',
    'Standard_FX48mds',
    'Standard_FX4mds',
    'Standard_M64dms_v2',
    'Standard_DC16ds_v3',
    'Standard_DC16s_v3',
    'Standard_DC1ds_v3',
    'Standard_DC1s_v3',
    'Standard_DC24ds_v3',
    'Standard_DC24s_v3',
    'Standard_DC2ds_v3',
    'Standard_DC1s_v2',
)

# https://docs.microsoft.com/en-us/azure/virtual-machines/windows/scheduled-events
_SCHEDULED_EVENTS_CMD = (
    'curl -H Metadata:true http://169.254.169.254/metadata'
    '/scheduledevents?api-version=2019-01-01'
)

_SCHEDULED_EVENTS_CMD_WIN = (
    'Invoke-RestMethod -Headers @{"Metadata"="true"} '
    '-Uri http://169.254.169.254/metadata/'
    'scheduledevents?api-version=2019-01-01 | '
    'ConvertTo-Json'
)

# Recognized Errors
_OS_PROVISIONING_TIMED_OUT = 'OSProvisioningTimedOut'

# Map to neoverse-n1
AZURE_ARM_TYPES = [
    r'(Standard_D[0-9]+pl?d?s_v5)',
    r'(Standard_E[0-9]+pd?s_v5)',
]

CONFIDENTIAL_MILAN_TYPES = [
    r'(Standard_DC[0-9]+as?d?s_v5)',
    r'(Standard_EC[0-9]+as?d?s_v5)',
]

# Reference -
# https://learn.microsoft.com/en-us/azure/virtual-machines/trusted-launch#virtual-machines-sizes
TRUSTED_LAUNCH_UNSUPPORTED_TYPES = AZURE_ARM_TYPES + [
    r'(Standard_A[0-9]+_v2)',
    r'(Standard_D[0-9]+_v2)',
    r'(Standard_D[0-9]+_v3)',
    r'(Standard_E[0-9]+_v3)',
    r'(Standard_M[0-9]+.*)',
    r'(Standard_ND[0-9]+a.*)',
]

TRUSTED_LAUNCH_UNSUPPORTED_OS_TYPES = [
    os_types.ROCKY_LINUX8,
    os_types.ROCKY_LINUX9,
]

NVME_MACHINE_FAMILIES = [
    'Standard_Eibs_v5',
    'Standard_Ebs_v5',
    'Standard_Eibds_v5',
    'Standard_Ebds_v5',
    'Standard_Ms_v3',
    'Standard_Mds_v3',
]


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

  CLOUD = provider_info.AZURE

  def __init__(self, *args, **kwargs):
    super(AzureVmSpec, self).__init__(*args, **kwargs)
    if isinstance(
        self.machine_type,
        custom_virtual_machine_spec.AzurePerformanceTierDecoder,
    ):
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
          flag_values.azure_accelerated_networking
      )
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
        'machine_type': (
            custom_virtual_machine_spec.AzureMachineTypeDecoder,
            {},
        ),
        'accelerated_networking': (
            option_decoders.BooleanDecoder,
            {'default': False},
        ),
        'boot_disk_size': (option_decoders.IntDecoder, {'default': None}),
        'boot_disk_type': (option_decoders.StringDecoder, {'default': None}),
        'low_priority': (option_decoders.BooleanDecoder, {'default': False}),
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
        azure.AZURE_PATH,
        'network',
        'public-ip',
        'create',
        '--location',
        self.region,
        '--name',
        self.name,
    ] + self.resource_group.args

    if self.availability_zone:
      # Availability Zones require Standard IPs.
      # TODO(user): Consider setting this by default
      cmd += ['--zone', self.availability_zone, '--sku', 'Standard']

    if self.dns_name:
      cmd += ['--dns-name', self.dns_name]

    _, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)

    if retcode and re.search(
        r'Cannot create more than \d+ public IP addresses', stderr
    ):
      raise errors.Benchmarks.QuotaFailure(
          virtual_machine.QUOTA_EXCEEDED_MESSAGE + stderr
      )

  def _Exists(self):
    if self._deleted:
      return False

    stdout, _, _ = vm_util.IssueCommand(
        [
            azure.AZURE_PATH,
            'network',
            'public-ip',
            'show',
            '--output',
            'json',
            '--name',
            self.name,
        ]
        + self.resource_group.args,
        raise_on_failure=False,
    )
    try:
      json.loads(stdout)
      return True
    except ValueError:
      return False

  def GetIPAddress(self):
    stdout, _ = vm_util.IssueRetryableCommand(
        [
            azure.AZURE_PATH,
            'network',
            'public-ip',
            'show',
            '--output',
            'json',
            '--name',
            self.name,
        ]
        + self.resource_group.args
    )

    response = json.loads(stdout)
    return response['ipAddress']

  def _Delete(self):
    self._deleted = True


class AzureNIC(resource.BaseResource):
  """Class to represent an Azure NIC."""

  def __init__(
      self,
      network: azure_network.AzureNetwork,
      name: str,
      public_ip: str | None,
      accelerated_networking: bool,
      private_ip=None,
  ):
    super(AzureNIC, self).__init__()
    self.network = network
    self.name = name
    self.public_ip = public_ip
    self.private_ip = private_ip
    self._deleted = False
    self.resource_group = azure_network.GetResourceGroup()
    self.region = self.network.region
    self.args = ['--nics', self.name]
    self.accelerated_networking = accelerated_networking

  def _Create(self):
    cmd = [
        azure.AZURE_PATH, 'network', 'nic', 'create',
        '--location', self.region,
        '--name', self.name
    ]  # pyformat: disable
    cmd += self.resource_group.args
    if self.network.subnet.id:
      # pre-existing subnet from --azure_subnet_id
      cmd += ['--subnet', self.network.subnet.id]
    else:
      # auto created subnet
      cmd += [
          '--vnet-name',
          self.network.subnet.vnet.name,
          '--subnet',
          self.network.subnet.name,
      ]
    if self.public_ip:
      cmd += ['--public-ip-address', self.public_ip]
    if self.private_ip:
      cmd += ['--private-ip-address', self.private_ip]
    if self.accelerated_networking:
      cmd += ['--accelerated-networking', 'true']
    if self.network.nsg:
      cmd += ['--network-security-group', self.network.nsg.name]
    vm_util.IssueCommand(cmd)

  def _Exists(self):
    if self._deleted:
      return False
    # Same deal as AzurePublicIPAddress. 'show' doesn't error out if
    # the resource doesn't exist, but no-op 'set' does.
    stdout, _, _ = vm_util.IssueCommand(
        [
            azure.AZURE_PATH,
            'network',
            'nic',
            'show',
            '--output',
            'json',
            '--name',
            self.name,
        ]
        + self.resource_group.args,
        raise_on_failure=False,
    )
    try:
      json.loads(stdout)
      return True
    except ValueError:
      return False

  def GetInternalIP(self):
    """Grab some data."""

    stdout, _ = vm_util.IssueRetryableCommand(
        [
            azure.AZURE_PATH,
            'network',
            'nic',
            'show',
            '--output',
            'json',
            '--name',
            self.name,
        ]
        + self.resource_group.args
    )

    response = json.loads(stdout)
    ip_config = response['ipConfigurations'][0]
    # Azure CLI used privateIpAddress between versions 1.2.0 and 2.45.0
    # https://learn.microsoft.com/en-us/cli/azure/release-notes-azure-cli?toc=%2Fcli%2Fazure%2Ftoc.json&bc=%2Fcli%2Fazure%2Fbreadcrumb%2Ftoc.json#network
    for key in ('privateIPAddress', 'privateIpAddress'):
      if key in ip_config:
        return ip_config[key]
    raise KeyError('No known private IP address key found.')

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
    create_cmd = [
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
        # TODO(user): add support for multiple fault domains
        # https://docs.microsoft.com/en-us/azure/virtual-machines/windows/dedicated-hosts#high-availability-considerations
        '--platform-fault-domain-count',
        '1',
    ] + self.resource_group.args

    if self.availability_zone:
      create_cmd.extend(['--zone', self.availability_zone])

    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """See base class."""
    delete_cmd = [
        azure.AZURE_PATH,
        'vm',
        'host',
        'group',
        'delete',
        '--host-group',
        self.name,
    ] + self.resource_group.args
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """See base class."""
    show_cmd = [
        azure.AZURE_PATH,
        'vm',
        'host',
        'group',
        'show',
        '--output',
        'json',
        '--name',
        self.name,
    ] + self.resource_group.args
    stdout, _, _ = vm_util.IssueCommand(show_cmd, raise_on_failure=False)
    try:
      json.loads(stdout)
      return True
    except ValueError:
      return False


def _GetSkuType(machine_type):
  """Returns the host SKU type derived from the VM machine type."""
  # TODO(user): add support for FSv2 machine types when no longer in preview
  # https://docs.microsoft.com/en-us/azure/virtual-machines/windows/dedicated-hosts
  sku = ''
  assert isinstance(machine_type, str)
  if re.match('Standard_D[0-9]*s_v3', machine_type):
    sku = 'DSv3-Type1'
  elif re.match('Standard_E[0-9]*s_v3', machine_type):
    sku = 'ESv3-Type1'
  else:
    raise ValueError(
        'Dedicated hosting does not support machine type %s.' % machine_type
    )
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

  def __init__(self, name, region, resource_group, sku_type, availability_zone):
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
        new_host_group = AzureDedicatedHostGroup(
            self.name, self.region, self.resource_group, self.availability_zone
        )
        new_host_group.Create()
        self.host_group_map[self.region] = new_host_group.name
      self.host_group = self.host_group_map[self.region]

  def _Create(self):
    """See base class."""
    create_cmd = [
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
        # TODO(user): add support for specifying multiple fault domains if
        # benchmarks require
        '--platform-fault-domain',
        '0',
    ] + self.resource_group.args
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """See base class."""
    delete_cmd = [
        azure.AZURE_PATH,
        'vm',
        'host',
        'delete',
        '--host-group',
        self.host_group,
        '--name',
        self.name,
        '--yes',
    ] + self.resource_group.args
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


def _MachineTypeIsArm(machine_type):
  """Check if the machine type uses ARM."""
  return any(
      re.search(machine_series, machine_type)
      for machine_series in AZURE_ARM_TYPES
  )


class AzureVirtualMachine(
    six.with_metaclass(abc.ABCMeta, virtual_machine.BaseVirtualMachine)
):
  """Object representing an Azure Virtual Machine."""

  CLOUD = provider_info.AZURE

  _lock = threading.Lock()
  # TODO(user): remove host groups & hosts as globals -> create new spec
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

    assert isinstance(self.zone, str)
    assert isinstance(self.machine_type, str)
    self.region = util.GetRegionFromZone(self.zone)
    self.availability_zone = util.GetAvailabilityZoneFromZone(self.zone)
    self.use_dedicated_host = vm_spec.use_dedicated_host
    self.num_vms_per_host = vm_spec.num_vms_per_host
    self.network = azure_network.AzureNetwork.GetNetwork(self)
    self.firewall = azure_network.AzureFirewall.GetFirewall()
    if azure_disk.HasTempDrive(self.machine_type):
      self.max_local_disks = NUM_LOCAL_VOLUMES.get(self.machine_type, 1)
    else:
      self.max_local_disks = 0
    self.lun_counter = itertools.count()
    self._deleted = False
    self.resource_group = azure_network.GetResourceGroup()

    # Configure NIC
    if self.assign_external_ip:
      public_ip_name = self.name + '-public-ip'
      self.public_ip = AzurePublicIPAddress(
          self.region, self.availability_zone, public_ip_name
      )
    else:
      public_ip_name = None
      self.public_ip = None
    self.nic = AzureNIC(
        self.network,
        self.name + '-nic',
        public_ip_name,
        vm_spec.accelerated_networking,
    )

    self.storage_account = self.network.storage_account
    self.machine_type_is_confidential = any(
        re.search(machine_series, self.machine_type)
        for machine_series in CONFIDENTIAL_MILAN_TYPES
    )
    self.trusted_launch_unsupported_type = (
        any(
            re.search(machine_series, self.machine_type)
            for machine_series in TRUSTED_LAUNCH_UNSUPPORTED_TYPES
        )
        or self.OS_TYPE in TRUSTED_LAUNCH_UNSUPPORTED_OS_TYPES
    )
    arm_arch = 'neoverse-n1' if _MachineTypeIsArm(self.machine_type) else None
    if arm_arch:
      self.host_arch = arm_arch
      self.is_aarch64 = True
    if vm_spec.image:
      self.image = vm_spec.image
    elif self.machine_type in _MACHINE_TYPES_ONLY_SUPPORT_GEN2_IMAGES:
      if hasattr(type(self), 'GEN2_IMAGE_URN'):
        self.image = type(self).GEN2_IMAGE_URN
      else:
        raise errors.Benchmarks.UnsupportedConfigError('No Azure gen2 image.')
    elif self.SupportsNVMe():
      if hasattr(type(self), 'GEN2_IMAGE_URN'):
        self.image = type(self).GEN2_IMAGE_URN
        self.image_supports_nvme = True
      else:
        self.image = type(self).IMAGE_URN
        self.image_supports_nvme = False
    elif arm_arch:
      if hasattr(type(self), 'ARM_IMAGE_URN'):
        self.image = type(self).ARM_IMAGE_URN
      else:
        raise errors.Benchmarks.UnsupportedConfigError('No Azure ARM image.')
    elif self.machine_type_is_confidential:
      self.image = type(self).CONFIDENTIAL_IMAGE_URN
    else:
      self.image = type(self).IMAGE_URN

    self.host = None
    if self.use_dedicated_host:
      self.host_series_sku = _GetSkuType(self.machine_type)
      self.host_list = None
    self.low_priority = vm_spec.low_priority
    self.low_priority_status_code = None
    self.spot_early_termination = False
    self.ultra_ssd_enabled = False
    self.boot_disk_size = vm_spec.boot_disk_size
    self.boot_disk_type = vm_spec.boot_disk_type
    self.create_os_disk_strategy = (
        azure_disk_strategies.AzureCreateOSDiskStrategy(
            self, disk.BaseDiskSpec('azure_os_disk'), 1
        )
    )
    self.create_disk_strategy = azure_disk_strategies.GetCreateDiskStrategy(
        self, None, 0
    )

  @property
  @classmethod
  @abc.abstractmethod
  def IMAGE_URN(cls):
    raise NotImplementedError()

  def _CreateDependencies(self):
    """Create VM dependencies."""
    if self.public_ip:
      self.public_ip.Create()
    self.nic.Create()

    if self.use_dedicated_host:
      with self._lock:
        self.host_list = self.host_map[(self.host_series_sku, self.region)]
        if not self.host_list or (
            self.num_vms_per_host
            and self.host_list[-1].fill_fraction + 1.0 / self.num_vms_per_host
            > 1.0
        ):
          new_host = AzureDedicatedHost(
              self.name,
              self.region,
              self.resource_group,
              self.host_series_sku,
              self.availability_zone,
          )
          self.host_list.append(new_host)
          new_host.Create()
        self.host = self.host_list[-1]
        if self.num_vms_per_host:
          self.host.fill_fraction += 1.0 / self.num_vms_per_host

  def _RequiresUltraDisk(self):
    return hasattr(self.create_disk_strategy, 'disk_specs') and any(
        disk_spec.disk_type == azure_disk.ULTRA_STORAGE
        for disk_spec in self.create_disk_strategy.disk_specs
    )

  def _Create(self):
    """See base class."""
    assert self.network is not None

    os_disk_args = self.create_os_disk_strategy.GetCreationCommand()[
        'create-disk'
    ]
    confidential_args = []
    if self.machine_type_is_confidential:
      confidential_args = [
          '--enable-vtpm',
          'true',
          '--enable-secure-boot',
          'true',
          '--security-type',
          'ConfidentialVM',
          '--os-disk-security-encryption-type',
          'VMGuestStateOnly',
      ]

    tags = {}
    tags.update(self.vm_metadata)
    tags.update(util.GetResourceTags(self.resource_group.timeout_minutes))
    # Signal (along with timeout_utc) that VM is short lived.
    tags['vm_nature'] = 'ephemeral'
    tag_args = ['--tags'] + util.FormatTags(tags)

    create_cmd = (
        [
            azure.AZURE_PATH,
            'vm',
            'create',
            '--location',
            self.region,
            '--image',
            self.image,
            '--size',
            self.machine_type,
            '--admin-username',
            self.user_name,
            '--name',
            self.name,
        ]
        + os_disk_args
        + confidential_args
        + self.resource_group.args
        + self.nic.args
        + tag_args
    )
    if self.SupportsNVMe() and self.image_supports_nvme:
      create_cmd.extend(['--disk-controller-type', 'NVMe'])
    if self.trusted_launch_unsupported_type:
      create_cmd.extend(['--security-type', 'Standard'])
    if self.boot_startup_script:
      create_cmd.extend(['--custom-data', self.boot_startup_script])

    if self._RequiresUltraDisk():
      self.ultra_ssd_enabled = True
      create_cmd.extend(['--ultra-ssd-enabled'])

    if self.availability_zone:
      create_cmd.extend(['--zone', self.availability_zone])

    # Resources in Availability Set are not allowed to be
    # deployed to particular hosts.
    if self.use_dedicated_host:
      create_cmd.extend(
          ['--host-group', self.host.host_group, '--host', self.host.name]
      )
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
        create_cmd, timeout=azure_vm_create_timeout, raise_on_failure=False
    )
    if retcode:
      if 'quota' in stderr.lower():
        raise errors.Benchmarks.QuotaFailure(
            virtual_machine.QUOTA_EXCEEDED_MESSAGE + stderr
        )
      elif self.low_priority and (
          re.search(r'requested VM size \S+ is not available', stderr)
          or re.search(r'not available in location .+ for subscription', stderr)
          or re.search(
              r'Following SKUs have failed for Capacity Restrictions', stderr
          )
      ):
        raise errors.Benchmarks.InsufficientCapacityCloudFailure(stderr)
      elif re.search(
          r'requested VM size \S+ is not available', stderr
      ) or re.search(r'not available in location .+ for subscription', stderr):
        raise errors.Benchmarks.UnsupportedConfigError(stderr)
      elif self.low_priority and 'OverconstrainedAllocationRequest' in stderr:
        raise errors.Benchmarks.InsufficientCapacityCloudFailure(stderr)
      elif _OS_PROVISIONING_TIMED_OUT in stderr:
        raise errors.Resource.ProvisionTimeoutError(stderr)
      elif "Virtual Machine Scale Set with '<NULL>' security type." in stderr:
        raise errors.Resource.CreationError(
            f'Failed to create VM: {self.machine_type} is likely a confidential'
            ' machine, which PKB does not support at this time.\n\n'
            f' Full error: {stderr} return code: {retcode}'
        )
      elif "cannot boot Hypervisor Generation '1'" in stderr:
        raise errors.Resource.CreationError(
            f'Failed to create VM: {self.machine_type} is unable to support V1 '
            'Hypervision. Please update _MACHINE_TYPES_ONLY_SUPPORT_GEN2_IMAGES'
            ' in azure_virtual_machine.py.\n\n'
            f' Full error: {stderr} return code: {retcode}'
        )
      elif self.use_dedicated_host and 'AllocationFailed' in stderr:
        if self.num_vms_per_host:
          raise errors.Resource.CreationError(
              'Failed to create host: %d vms of type %s per host exceeds '
              'memory capacity limits of the host'
              % (self.num_vms_per_host, self.machine_type)
          )
        else:
          logging.warning(
              'Creation failed due to insufficient host capacity. A new host '
              'will be created and instance creation will be retried.'
          )
          with self._lock:
            if num_hosts == len(self.host_list):
              new_host = AzureDedicatedHost(
                  self.name,
                  self.region,
                  self.resource_group,
                  self.host_series_sku,
                  self.availability_zone,
              )
              self.host_list.append(new_host)
              new_host.Create()
            self.host = self.host_list[-1]
          raise errors.Resource.RetryableCreationError()
      elif not self.use_dedicated_host and 'AllocationFailed' in stderr:
        raise errors.Benchmarks.InsufficientCapacityCloudFailure(stderr)
      elif (
          not self.use_dedicated_host
          and 'OverconstrainedZonalAllocationRequest' in stderr
      ):
        raise errors.Benchmarks.UnsupportedConfigError(stderr)
      elif 'SkuNotAvailable' in stderr:
        raise errors.Benchmarks.UnsupportedConfigError(stderr)
      else:
        raise errors.Resource.CreationError(
        )

  def _Exists(self):
    """Returns True if the VM exists."""
    if self._deleted:
      return False
    show_cmd = [
        azure.AZURE_PATH,
        'vm',
        'show',
        '--output',
        'json',
        '--name',
        self.name,
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
    start_cmd = [
        azure.AZURE_PATH,
        'vm',
        'start',
        '--name',
        self.name,
    ] + self.resource_group.args
    vm_util.IssueCommand(start_cmd)
    if self.public_ip:
      self.ip_address = self.public_ip.GetIPAddress()

  def _Stop(self):
    """Stops the VM."""
    stop_cmd = [
        azure.AZURE_PATH,
        'vm',
        'stop',
        '--name',
        self.name,
    ] + self.resource_group.args
    vm_util.IssueCommand(stop_cmd)
    # remove resources, similar to GCE stop
    deallocate_cmd = [
        azure.AZURE_PATH,
        'vm',
        'deallocate',
        '--name',
        self.name,
    ] + self.resource_group.args
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
    stdout, _ = vm_util.IssueRetryableCommand(
        [
            azure.AZURE_PATH,
            'vm',
            'show',
            '--output',
            'json',
            '--name',
            self.name,
        ]
        + self.resource_group.args
    )
    response = json.loads(stdout)
    self.create_os_disk_strategy.disk.name = response['storageProfile'][
        'osDisk'
    ]['name']
    self.create_os_disk_strategy.disk.created = True
    vm_util.IssueCommand(
        [
            azure.AZURE_PATH,
            'disk',
            'update',
            '--name',
            self.create_os_disk_strategy.disk.name,
            '--set',
            util.GetTagsJson(self.resource_group.timeout_minutes),
        ]
        + self.resource_group.args
    )
    self.internal_ip = self.nic.GetInternalIP()
    if self.public_ip:
      self.ip_address = self.public_ip.GetIPAddress()

  def SetupAllScratchDisks(self):
    """Set up all scratch disks of the current VM."""
    self.create_disk_strategy.GetSetupDiskStrategy().SetUpDisk()

  def SetDiskSpec(self, disk_spec, disk_count):
    """Sets Disk Specs of the current VM. Calls before the VM is created."""
    self.create_disk_strategy = azure_disk_strategies.GetCreateDiskStrategy(
        self, disk_spec, disk_count
    )

  def InstallCli(self):
    """Installs the Azure cli and credentials on this Azure vm."""
    self.Install('azure_cli')
    self.Install('azure_credentials')

  def DownloadPreprovisionedData(
      self,
      install_path,
      module_name,
      filename,
      timeout=virtual_machine.PREPROVISIONED_DATA_TIMEOUT,
  ):
    """Downloads a data file from Azure blob storage with pre-provisioned data.

    Use --azure_preprovisioned_data_account to specify the name of the account.

    Note: Azure blob storage does not allow underscores in the container name,
    so this method replaces any underscores in module_name with dashes.
    Make sure that the same convention is used when uploading the data
    to Azure blob storage. For example: when uploading data for
    'module_name' to Azure, create a container named 'benchmark-name'.

    Args:
      install_path: The install path on this VM.
      module_name: Name of the module associated with this data file.
      filename: The name of the file that was downloaded.
      timeout: Timeout value for downloading preprovisionedData, Five minutes by
        default.
    """
    # N.B. Should already be installed by ShouldDownloadPreprovisionedData
    self.Install('azure_cli')
    self.RobustRemoteCommand(
        GenerateDownloadPreprovisionedDataCommand(
            install_path, module_name, filename
        ),
        timeout=timeout,
    )

  def ShouldDownloadPreprovisionedData(self, module_name, filename):
    """Returns whether or not preprovisioned data is available."""
    # Do not install credentials. Data are fetched using locally generated
    # connection strings and do not use credentials on the VM.
    self.Install('azure_cli')
    return FLAGS.azure_preprovisioned_data_account and self.TryRemoteCommand(
        GenerateStatPreprovisionedDataCommand(module_name, filename)
    )

  def GetResourceMetadata(self):
    assert self.network is not None
    result = super(AzureVirtualMachine, self).GetResourceMetadata()
    result['accelerated_networking'] = self.nic.accelerated_networking
    result['boot_disk_type'] = self.create_os_disk_strategy.disk.disk_type
    result['boot_disk_size'] = self.create_os_disk_strategy.disk.disk_size
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
        _SCHEDULED_EVENTS_CMD
    )
    if return_code:
      logging.error('Checking Interrupt Error: %s', stderr)
    else:
      events = json.loads(stdout).get('Events', [])
      self.spot_early_termination = any(
          event.get('EventType') == 'Preempt' for event in events
      )
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

  def SupportsNVMe(self):
    """Returns whether this vm supports NVMe.

    Returns:
      True if this vm supports NVMe.
    """
    return util.GetMachineFamily(self.machine_type) in NVME_MACHINE_FAMILIES


class Debian11BasedAzureVirtualMachine(
    AzureVirtualMachine, linux_virtual_machine.Debian11Mixin
):
  # From https://wiki.debian.org/Cloud/MicrosoftAzure
  GEN2_IMAGE_URN = 'Debian:debian-11:11-gen2:latest'
  IMAGE_URN = 'Debian:debian-11:11:latest'
  ARM_IMAGE_URN = 'Debian:debian-11:11-backports-arm64:latest'


class Debian12BasedAzureVirtualMachine(
    AzureVirtualMachine, linux_virtual_machine.Debian12Mixin
):
  # From https://wiki.debian.org/Cloud/MicrosoftAzure
  GEN2_IMAGE_URN = 'Debian:debian-12:12-gen2:latest'
  IMAGE_URN = 'Debian:debian-12:12:latest'
  ARM_IMAGE_URN = 'Debian:debian-12:12-arm64:latest'


class Ubuntu2004BasedAzureVirtualMachine(
    AzureVirtualMachine, linux_virtual_machine.Ubuntu2004Mixin
):
  GEN2_IMAGE_URN = (
      'Canonical:0001-com-ubuntu-server-focal:20_04-lts-gen2:latest'
  )
  IMAGE_URN = 'Canonical:0001-com-ubuntu-server-focal:20_04-lts:latest'
  CONFIDENTIAL_IMAGE_URN = (
      'Canonical:0001-com-ubuntu-confidential-vm-focal:20_04-lts-cvm:latest'
  )
  ARM_IMAGE_URN = (
      'Canonical:0001-com-ubuntu-server-focal:20_04-lts-arm64:latest'
  )


class Ubuntu2204BasedAzureVirtualMachine(
    AzureVirtualMachine, linux_virtual_machine.Ubuntu2204Mixin
):
  GEN2_IMAGE_URN = (
      'Canonical:0001-com-ubuntu-server-jammy:22_04-lts-gen2:latest'
  )
  IMAGE_URN = 'Canonical:0001-com-ubuntu-server-jammy:22_04-lts:latest'
  CONFIDENTIAL_IMAGE_URN = (
      'Canonical:0001-com-ubuntu-confidential-vm-jammy:22_04-lts-cvm:latest'
  )
  ARM_IMAGE_URN = (
      'Canonical:0001-com-ubuntu-server-jammy:22_04-lts-arm64:latest'
  )


class Ubuntu2404BasedAzureVirtualMachine(
    AzureVirtualMachine, linux_virtual_machine.Ubuntu2404Mixin
):
  GEN2_IMAGE_URN = 'Canonical:ubuntu-24_04-lts:server:latest'
  IMAGE_URN = 'Canonical:ubuntu-24_04-lts:server-gen1:latest'
  CONFIDENTIAL_IMAGE_URN = 'Canonical:ubuntu-24_04-lts:cvm:latest'
  ARM_IMAGE_URN = 'Canonical:ubuntu-24_04-lts:server-arm64:latest'


class Rhel8BasedAzureVirtualMachine(
    AzureVirtualMachine, linux_virtual_machine.Rhel8Mixin
):
  GEN2_IMAGE_URN = 'RedHat:RHEL:8-lvm-gen2:latest'
  IMAGE_URN = 'RedHat:RHEL:8-LVM:latest'


class Rhel9BasedAzureVirtualMachine(
    AzureVirtualMachine, linux_virtual_machine.Rhel9Mixin
):
  GEN2_IMAGE_URN = 'RedHat:RHEL:9-lvm-gen2:latest'
  IMAGE_URN = 'RedHat:RHEL:9-lvm:latest'


# Rocky Linux is now distributed via a community gallery:
# https://rockylinux.org/news/rocky-on-azure-community-gallery
# TODO(user): Support Select images from community galleries and
# re-enable.
# class RockyLinux8BasedAzureVirtualMachine(
#     AzureVirtualMachine, linux_virtual_machine.RockyLinux8Mixin
# )
# class RockyLinux9BasedAzureVirtualMachine(
#     AzureVirtualMachine, linux_virtual_machine.RockyLinux9Mixin
# )


# TODO(user): Add Fedora CoreOS when available:
#   https://docs.fedoraproject.org/en-US/fedora-coreos/provisioning-azure/


class BaseWindowsAzureVirtualMachine(
    AzureVirtualMachine, windows_virtual_machine.BaseWindowsMixin
):
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
    vm_util.IssueRetryableCommand(
        [
            azure.AZURE_PATH,
            'vm',
            'extension',
            'set',
            '--vm-name',
            self.name,
            '--name',
            'CustomScriptExtension',
            '--publisher',
            'Microsoft.Compute',
            '--version',
            '1.4',
            '--protected-settings=%s' % config,
        ]
        + self.resource_group.args
    )

  def _UpdateInterruptibleVmStatusThroughMetadataService(self):
    stdout, _ = self.RemoteCommand(_SCHEDULED_EVENTS_CMD_WIN)
    events = json.loads(stdout).get('Events', [])
    self.spot_early_termination = any(
        event.get('EventType') == 'Preempt' for event in events
    )
    if self.spot_early_termination:
      logging.info('Spotted early termination on %s', self)

  def DownloadPreprovisionedData(
      self,
      install_path,
      module_name,
      filename,
      timeout=virtual_machine.PREPROVISIONED_DATA_TIMEOUT,
  ):
    """Downloads a data file from Azure blob storage with pre-provisioned data.

    Use --azure_preprovisioned_data_account to specify the name of the account.

    Note: Azure blob storage does not allow underscores in the container name,
    so this method replaces any underscores in module_name with dashes.
    Make sure that the same convention is used when uploading the data
    to Azure blob storage. For example: when uploading data for
    'module_name' to Azure, create a container named 'benchmark-name'.

    Args:
      install_path: The install path on this VM.
      module_name: Name of the module associated with this data file.
      filename: The name of the file that was downloaded.
      timeout: Timeout value for downloading preprovisionedData, Five minutes by
        default.
    """
    # TODO(deitz): Add retry logic.
    temp_local_path = vm_util.GetTempDir()
    vm_util.IssueCommand(
        GenerateDownloadPreprovisionedDataCommand(
            temp_local_path, module_name, filename
        )
        .split('&')[-1]
        .split(),
        timeout=timeout,
    )
    self.PushFile(
        vm_util.PrependTempDir(filename), ntpath.join(install_path, filename)
    )
    vm_util.IssueCommand(['rm', vm_util.PrependTempDir(filename)])

  def ShouldDownloadPreprovisionedData(self, module_name, filename):
    """Returns whether or not preprovisioned data is available."""
    return (
        FLAGS.azure_preprovisioned_data_account
        and vm_util.IssueCommand(
            GenerateStatPreprovisionedDataCommand(module_name, filename).split(
                ' '
            ),
            raise_on_failure=False,
        )[-1]
        == 0
    )

  def DiskDriveIsLocal(self, device, model):
    """Helper method to determine if a disk drive is a local ssd to stripe."""
    # NVME Striped disks applies to Azure's L series
    # https://learn.microsoft.com/en-us/azure/virtual-machines/lsv3-series
    if 'Microsoft NVMe Direct Disk' in model:
      return True
    return False


class Windows2016CoreAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine, windows_virtual_machine.Windows2016CoreMixin
):
  GEN2_IMAGE_URN = 'MicrosoftWindowsServer:windowsserver-gen2preview:2016-datacenter-gen2:latest'
  IMAGE_URN = (
      'MicrosoftWindowsServer:WindowsServer:2016-Datacenter-Server-Core:latest'
  )


class Windows2019CoreAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine, windows_virtual_machine.Windows2019CoreMixin
):
  GEN2_IMAGE_URN = 'MicrosoftWindowsServer:windowsserver-gen2preview:2019-datacenter-gen2:latest'
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2019-Datacenter-Core:latest'


class Windows2022CoreAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine, windows_virtual_machine.Windows2022CoreMixin
):
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2022-Datacenter-Core:latest'


class Windows2016DesktopAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine,
    windows_virtual_machine.Windows2016DesktopMixin,
):
  GEN2_IMAGE_URN = 'MicrosoftWindowsServer:windowsserver-gen2preview:2016-datacenter-gen2:latest'
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2016-Datacenter:latest'


class Windows2019DesktopAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine,
    windows_virtual_machine.Windows2019DesktopMixin,
):
  GEN2_IMAGE_URN = 'MicrosoftWindowsServer:windowsserver-gen2preview:2019-datacenter-gen2:latest'
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2019-Datacenter:latest'


class Windows2022DesktopAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine,
    windows_virtual_machine.Windows2022DesktopMixin,
):
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2022-Datacenter:latest'


class Windows2019DesktopSQLServer2019StandardAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine,
    windows_virtual_machine.Windows2019SQLServer2019Standard,
):
  GEN2_IMAGE_URN = 'MicrosoftSQLServer:sql2019-ws2019:standard-gen2:latest'
  IMAGE_URN = 'MicrosoftSQLServer:sql2019-ws2019:standard:latest'


class Windows2019DesktopSQLServer2019EnterpriseAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine,
    windows_virtual_machine.Windows2019SQLServer2019Enterprise,
):
  GEN2_IMAGE_URN = 'MicrosoftSQLServer:sql2019-ws2019:enterprise-gen2:latest'
  IMAGE_URN = 'MicrosoftSQLServer:sql2019-ws2019:enterprise:latest'


class Windows2022DesktopSQLServer2019StandardAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine,
    windows_virtual_machine.Windows2022SQLServer2019Standard,
):
  IMAGE_URN = 'MicrosoftSQLServer:sql2019-ws2022:standard:latest'


class Windows2022DesktopSQLServer2019EnterpriseAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine,
    windows_virtual_machine.Windows2022SQLServer2019Enterprise,
):
  IMAGE_URN = 'MicrosoftSQLServer:sql2019-ws2022:enterprise:latest'


class Windows2022DesktopSQLServer2022StandardAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine,
    windows_virtual_machine.Windows2022SQLServer2022Standard,
):
  IMAGE_URN = 'MicrosoftSQLServer:sql2022-ws2022:standard-gen2:latest'


class Windows2022DesktopSQLServer2022EnterpriseAzureVirtualMachine(
    BaseWindowsAzureVirtualMachine,
    windows_virtual_machine.Windows2022SQLServer2022Enterprise,
):
  IMAGE_URN = 'MicrosoftSQLServer:sql2022-ws2022:enterprise-gen2:latest'


def GenerateDownloadPreprovisionedDataCommand(
    install_path, module_name, filename
):
  """Returns a string used to download preprovisioned data."""
  module_name_with_underscores_removed = module_name.replace('_', '-')
  destpath = posixpath.join(install_path, filename)
  if install_path:
    # TODO(user): Refactor this so that this mkdir command
    # is run on all clouds, and is os-agnostic (this is linux specific).
    mkdir_command = 'mkdir -p %s' % posixpath.dirname(destpath)

  account_name = FLAGS.azure_preprovisioned_data_account
  connection_string = util.GetAzureStorageConnectionString(
      account_name, subscription=FLAGS.azure_preprovisioned_data_subscription
  )
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
          connection_string=connection_string,
      )
  )
  if install_path:
    return '{0} && {1}'.format(mkdir_command, download_command)
  return download_command


def GenerateStatPreprovisionedDataCommand(module_name, filename):
  """Returns a string used to download preprovisioned data."""
  module_name_with_underscores_removed = module_name.replace('_', '-')
  account_name = FLAGS.azure_preprovisioned_data_account
  connection_string = util.GetAzureStorageConnectionString(
      account_name, subscription=FLAGS.azure_preprovisioned_data_subscription
  )
  return (
      'az storage blob show '
      '--account-name {account_name} '
      '--container-name {container_name} '
      '--name {name} '
      '--connection-string "{connection_string}"'.format(
          account_name=account_name,
          container_name=module_name_with_underscores_removed,
          name=filename,
          connection_string=connection_string,
      )
  )
