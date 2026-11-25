# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Class to represent an OCI Virtual Machine object.

Shapes:
https://docs.oracle.com/en-us/iaas/Content/Compute/References/computeshapes.htm

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""

import base64
import json
import re

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import disk_strategies
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import virtual_machine_spec
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.oci import oci_disk
from perfkitbenchmarker.providers.oci import oci_network
from perfkitbenchmarker.providers.oci import util

FLAGS = flags.FLAGS

DEFAULT_BOOT_DISK_SIZE_GB = 50
# Paravirtualized attachments are limited to 32 volumes per instance.
MAX_REMOTE_DISKS = 32

_INSUFFICIENT_CAPACITY_MESSAGE = 'Out of host capacity'
_QUOTA_MESSAGES = ('LimitExceeded', 'QuotaExceeded')

# Creates the PKB user and opens the host firewall. OCI platform images block
# all inbound traffic except SSH with iptables (Ubuntu) or firewalld (Oracle
# Linux); network access is controlled by the VCN security list instead.
CLOUD_CONFIG_TEMPLATE = """#cloud-config
users:
  - default
  - name: {user_name}
    ssh_authorized_keys:
      - {public_key}
    sudo: ['ALL=(ALL) NOPASSWD:ALL']
    shell: /bin/bash
runcmd:
  - [sh, -c, 'systemctl disable --now firewalld || true']
  - [sh, -c, 'iptables -F INPUT || true']
"""


class InstanceNotRunningError(Exception):
  """Raised while waiting for an instance that is not running yet."""


class OciVmSpec(virtual_machine_spec.BaseVmSpec):
  """Object containing the information needed to create an OciVirtualMachine.

  Attributes:
    ocpus: int. Number of OCPUs for Flex shapes.
    memory_in_gbs: int. Memory in GB for Flex shapes.
    availability_domain: str. The availability domain to launch the VM in.
    fault_domain: str. The fault domain to launch the VM in.
    preemptible: bool. Whether to launch a preemptible instance.
  """

  CLOUD = provider_info.OCI

  def __init__(self, *args, **kwargs):
    self.ocpus: int | None = None
    self.memory_in_gbs: int | None = None
    self.availability_domain: str | None = None
    self.fault_domain: str | None = None
    self.preemptible: bool = False
    super().__init__(*args, **kwargs)

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Args:
      config_values: dict mapping config option names to provided values. May be
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.
    """
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['oci_ocpus'].present:
      config_values['ocpus'] = flag_values.oci_ocpus
    if flag_values['oci_memory_in_gbs'].present:
      config_values['memory_in_gbs'] = flag_values.oci_memory_in_gbs
    if flag_values['oci_availability_domain'].present:
      config_values['availability_domain'] = flag_values.oci_availability_domain
    if flag_values['oci_fault_domain'].present:
      config_values['fault_domain'] = flag_values.oci_fault_domain
    if flag_values['oci_preemptible'].present:
      config_values['preemptible'] = flag_values.oci_preemptible

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'ocpus': (option_decoders.IntDecoder, {'default': None, 'min': 1}),
        'memory_in_gbs': (
            option_decoders.IntDecoder,
            {'default': None, 'min': 1},
        ),
        'availability_domain': (
            option_decoders.StringDecoder,
            {'default': None},
        ),
        'fault_domain': (option_decoders.StringDecoder, {'default': None}),
        'preemptible': (option_decoders.BooleanDecoder, {'default': False}),
    })
    return result


class OciVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing an OCI Virtual Machine."""

  CLOUD = provider_info.OCI
  # Subclasses set the platform image to use, e.g. ('Oracle Linux', '9').
  IMAGE_OPERATING_SYSTEM: str = None  # pyrefly: ignore[bad-assignment]
  IMAGE_OPERATING_SYSTEM_VERSION: str = None  # pyrefly: ignore[bad-assignment]

  def __init__(self, vm_spec: OciVmSpec):
    """Initialize an OCI virtual machine.

    Args:
      vm_spec: OciVmSpec object of the vm.
    """
    super().__init__(vm_spec)
    self.region = util.GetRegionFromZone(self.zone)
    self.availability_domain = vm_spec.availability_domain
    self.fault_domain = vm_spec.fault_domain
    self.preemptible = vm_spec.preemptible
    self.spot_early_termination = False
    self.is_flex_shape = self.machine_type.endswith('.Flex')
    self.ocpus = vm_spec.ocpus
    if self.is_flex_shape and self.ocpus is None:
      self.ocpus = 1
    self.memory_in_gbs = vm_spec.memory_in_gbs
    self.boot_disk_size = self.boot_disk_size or DEFAULT_BOOT_DISK_SIZE_GB
    self.max_local_disks = 0
    self.network = oci_network.OciNetwork.GetNetwork(self)
    self.firewall = oci_network.OciFirewall.GetFirewall()

  def _CreateDependencies(self):
    """Resolves the availability domain and image to launch with."""
    if not self.availability_domain:
      self.availability_domain = util.GetAvailabilityDomain(self.zone)
    if not self.image:
      self.image = util.GetImageId(
          self.IMAGE_OPERATING_SYSTEM,
          self.IMAGE_OPERATING_SYSTEM_VERSION,
          self.machine_type,
          self.region,
      )
    elif not self.image.startswith('ocid1.image.'):
      self.image = util.GetImageIdFromName(
          self.image, self.machine_type, self.region
      )

  def _GetLaunchMetadata(self) -> str:
    """Returns the --metadata argument with the SSH key and user data."""
    with open(self.ssh_public_key) as f:
      public_key = f.read().strip()
    user_data = CLOUD_CONFIG_TEMPLATE.format(
        user_name=self.user_name, public_key=public_key
    )
    return json.dumps({
        'ssh_authorized_keys': public_key,
        'user_data': base64.b64encode(user_data.encode()).decode(),
    })

  def _Create(self):
    """Launches the instance."""
    args = [
        'compute',
        'instance',
        'launch',
        '--availability-domain',
        self.availability_domain,
        '--display-name',
        self.name,
        '--hostname-label',
        re.sub(r'[^a-zA-Z0-9-]', '-', self.name)[:63],
        '--shape',
        self.machine_type,
        '--image-id',
        self.image,
        '--subnet-id',
        self.network.subnet_id,
        '--assign-public-ip',
        str(self.assign_external_ip is not False).lower(),
        '--boot-volume-size-in-gbs',
        self.boot_disk_size,
        '--metadata',
        self._GetLaunchMetadata(),
        '--freeform-tags',
        util.MakeFormattedDefaultTags(),
    ] + util.GetCompartmentArgs()
    if self.is_flex_shape:
      shape_config = {'ocpus': self.ocpus}
      if self.memory_in_gbs:
        shape_config['memoryInGBs'] = self.memory_in_gbs
      args += ['--shape-config', json.dumps(shape_config)]
    if self.fault_domain:
      args += ['--fault-domain', self.fault_domain]
    if self.preemptible:
      args += [
          '--preemptible-instance-config',
          json.dumps({
              'preemptionAction': {
                  'type': 'TERMINATE',
                  'preserveBootVolume': False,
              }
          }),
      ]
    response, stderr, retcode = util.IssueOciCommand(
        args, self.region, raise_on_failure=False
    )
    if retcode:
      if _INSUFFICIENT_CAPACITY_MESSAGE in stderr:
        raise errors.Benchmarks.InsufficientCapacityCloudFailure(stderr)
      if any(message in stderr for message in _QUOTA_MESSAGES):
        raise errors.Benchmarks.QuotaFailure(stderr)
      raise errors.Resource.CreationError(
          f'Failed to launch instance {self.name}: {stderr}'
      )
    self.id = response['data']['id']

  @vm_util.Retry(
      poll_interval=5,
      log_errors=False,
      retryable_exceptions=(InstanceNotRunningError,),
  )
  def _WaitUntilRunning(self):
    """Waits until the instance is running."""
    state = util.GetLifecycleState(self._GetInstanceArgs(), self.region)
    if state in util.DELETED_STATES:
      raise errors.Resource.CreationError(
          f'Instance {self.name} was terminated during creation.'
      )
    if state != 'RUNNING':
      raise InstanceNotRunningError(
          f'Instance {self.name} is {state}, waiting for RUNNING.'
      )

  def _GetInstanceArgs(self) -> list[str]:
    return ['compute', 'instance', 'get', '--instance-id', self.id]

  @vm_util.Retry()
  def _PostCreate(self):
    """Gets the instance's IP addresses."""
    response, _, _ = util.IssueOciCommand(
        ['compute', 'instance', 'list-vnics', '--instance-id', self.id],
        self.region,
    )
    vnic = response['data'][0]
    self.internal_ip = vnic['private-ip']
    self.internal_ips = [self.internal_ip]
    self.ip_address = vnic.get('public-ip')
    if self.ip_address:
      self.ip_addresses = [self.ip_address]

  def _Delete(self):
    """Terminates the instance and its boot volume."""
    util.IssueOciCommand(
        [
            'compute',
            'instance',
            'terminate',
            '--instance-id',
            self.id,
            '--preserve-boot-volume',
            'false',
            '--force',
            '--wait-for-state',
            'TERMINATED',
        ],
        self.region,
        raise_on_failure=False,
    )

  def _Exists(self) -> bool:
    """Returns true if the VM exists."""
    return bool(self.id) and util.ResourceExists(
        self._GetInstanceArgs(), self.region
    )

  def _InstanceAction(self, action: str, state: str):
    util.IssueOciCommand(
        [
            'compute',
            'instance',
            'action',
            '--instance-id',
            self.id,
            '--action',
            action,
            '--wait-for-state',
            state,
        ],
        self.region,
    )

  def _Start(self):
    """Starts the VM."""
    self._InstanceAction('START', 'RUNNING')

  def _Stop(self):
    """Stops the VM."""
    self._InstanceAction('STOP', 'STOPPED')

  def CreateScratchDisk(self, _, disk_spec):
    """Creates a VM's scratch disk from OCI block volumes.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    if disk_spec.disk_type == disk.LOCAL:
      raise errors.Config.InvalidValue(
          'Local disks are not supported on OCI. Use one of: '
          f'{", ".join(oci_disk.DISK_TYPE_TO_VPUS_PER_GB)}.'
      )
    if self.remote_disk_counter + disk_spec.num_striped_disks > (
        MAX_REMOTE_DISKS
    ):
      raise errors.Config.InvalidValue(
          f'OCI instances support at most {MAX_REMOTE_DISKS} block volumes.'
      )
    disks = []
    for _ in range(disk_spec.num_striped_disks):
      data_disk = oci_disk.OciBlockVolume(
          disk_spec,
          f'{self.name}-data-{self.remote_disk_counter}',
          self.region,
          self.availability_domain,
      )
      self.remote_disk_counter += 1
      data_disk.disk_number = self.remote_disk_counter
      disks.append(data_disk)

    scratch_disk = self._CreateScratchDiskFromDisks(disk_spec, disks)
    disk_strategies.PrepareScratchDiskStrategy().PrepareScratchDisk(
        self, scratch_disk, disk_spec
    )

  def IsInterruptible(self):
    """Returns whether this VM is a preemptible instance."""
    return self.preemptible

  def _UpdateInterruptibleVmStatusThroughApi(self):
    state = util.GetLifecycleState(self._GetInstanceArgs(), self.region)
    self.spot_early_termination = state in util.DELETED_STATES

  def WasInterrupted(self):
    """Returns whether this preemptible VM was terminated early by OCI."""
    return self.spot_early_termination

  def GetResourceMetadata(self):
    """Returns a dict containing VM metadata."""
    result = super().GetResourceMetadata()
    if not result:
      return result
    result.update({
        'region': self.region,
        'availability_domain': self.availability_domain,
        'preemptible': self.preemptible,
    })
    if self.fault_domain:
      result['fault_domain'] = self.fault_domain
    if self.ocpus is not None:
      result['ocpus'] = self.ocpus
    if self.memory_in_gbs is not None:
      result['memory_in_gbs'] = self.memory_in_gbs
    return result


class Ubuntu2204BasedOciVirtualMachine(
    OciVirtualMachine, linux_virtual_machine.Ubuntu2204Mixin
):
  IMAGE_OPERATING_SYSTEM = 'Canonical Ubuntu'
  IMAGE_OPERATING_SYSTEM_VERSION = '22.04'


class Ubuntu2404BasedOciVirtualMachine(
    OciVirtualMachine, linux_virtual_machine.Ubuntu2404Mixin
):
  IMAGE_OPERATING_SYSTEM = 'Canonical Ubuntu'
  IMAGE_OPERATING_SYSTEM_VERSION = '24.04'


class Ol8BasedOciVirtualMachine(
    OciVirtualMachine, linux_virtual_machine.Ol8Mixin
):
  IMAGE_OPERATING_SYSTEM = 'Oracle Linux'
  IMAGE_OPERATING_SYSTEM_VERSION = '8'


class Ol9BasedOciVirtualMachine(
    OciVirtualMachine, linux_virtual_machine.Ol9Mixin
):
  IMAGE_OPERATING_SYSTEM = 'Oracle Linux'
  IMAGE_OPERATING_SYSTEM_VERSION = '9'
