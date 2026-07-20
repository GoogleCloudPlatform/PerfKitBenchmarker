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
"""Azure VM Scaling Set resource."""

import json
from typing import Any, cast

from perfkitbenchmarker import errors
from perfkitbenchmarker import managed_vm_group
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import vm_group_decoders
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_virtual_machine
from perfkitbenchmarker.providers.azure import util

VmReference = managed_vm_group.BaseManagedVmGroup.VmReference


class AzureVmScalingSet(managed_vm_group.BaseManagedVmGroup):
  """Azure VM Scaling Set."""

  CLOUD = provider_info.AZURE

  def __init__(
      self,
      spec: vm_group_decoders.VmGroupSpec,
      vm_configs: list[virtual_machine.BaseVirtualMachine],
  ):
    super().__init__(spec, vm_configs)
    self.vm_config: azure_virtual_machine.AzureVirtualMachine = cast(
        azure_virtual_machine.AzureVirtualMachine, self.vm_config
    )
    self.zoned_vm_configs: list[azure_virtual_machine.AzureVirtualMachine] = (
        cast(
            list[azure_virtual_machine.AzureVirtualMachine],
            self.zoned_vm_configs,
        )
    )
    # VMSS cannot use preconstructed NICs and IPs.
    # AFAICT there is no reason to pre-create them for VMs either.
    for resource in self.vm_config.nics + self.vm_config.public_ips:
      # Resources are used as clumsy booleans throughout Azure VM
      # Disable resource management, but keep them around.
      resource.user_managed = True
      resource.name = 'placeholder'
    self.name = self.vm_config.name
    self.region = self.vm_config.region
    self.resource_group = self.vm_config.resource_group

  def _Cmd(self, command: str, *args) -> list[str]:
    """Constructs an Azure CLI vmss command."""
    return (
        [azure.AZURE_PATH, 'vmss', command]
        + self.resource_group.args
        + list(args)
    )

  def _Create(self):
    """Creates the VM Scaling Set."""
    # TODO(pclay): Share a create command between VM and VMSS.
    cmd = self._Cmd(
        'create',
        '--name',
        self.name,
        '--image',
        self.vm_config.image,
        '--vm-sku',
        self.vm_config.machine_type,
        '--instance-count',
        str(self.vm_count),
        '--location',
        self.region,
        '--admin-username',
        self.vm_config.user_name,
        '--ssh-key-value',
        self.vm_config.ssh_public_key,
        '--vnet-name',
        self.vm_config.network.vnet.name,  # pyrefly: ignore[missing-attribute]
        '--subnet',
        self.vm_config.network.subnet.name,  # pyrefly: ignore[missing-attribute]
        '--upgrade-policy-mode',
        'Manual',
        '--lb',
        '',  # Do not create a load balancer
    )

    if self.vm_config.availability_zone:
      zones = [vm.availability_zone for vm in self.zoned_vm_configs]
      cmd.extend(['--zones', ','.join(zones)])

    if self.vm_config.boot_startup_script:
      cmd.extend(['--custom-data', self.vm_config.boot_startup_script])

    # Add tags
    tags = {}
    tags.update(self.vm_config.vm_metadata)
    tags.update(util.GetResourceTags(self.resource_group.timeout_minutes))
    tags['vm_nature'] = 'ephemeral'
    cmd += ['--tags'] + util.FormatTags(tags)

    if self.vm_config.nics[0].accelerated_networking:
      cmd.extend(['--accelerated-networking', 'true'])

    if self.vm_config.assign_external_ip:
      cmd.append('--public-ip-per-vm')

    vm_util.IssueCommand(cmd)

  def _Delete(self):
    cmd = self._Cmd('delete', '--name', self.name)
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _Get(self) -> dict[str, Any] | None:
    cmd = self._Cmd('show', '--name', self.name)
    stdout, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      if 'not found' in stderr or 'ResourceNotFound' in stderr:
        return None
      raise errors.Resource.GetError(
          f'Failed to describe VMSS {self.name}:\n{stderr}\n'
      )
    return json.loads(stdout)

  def _Exists(self) -> bool:
    return bool(self._Get())

  def _IsReady(self) -> bool:
    # Check if all instances are provisioned
    data = self._Get()
    assert data is not None
    return data['provisioningState'] == 'Succeeded'

  def _CheckForReadinessErrors(self) -> None:
    """Retrieves VM provisioning errors by inspecting VMs in the scale set."""
    instances = self._GetInstances()
    failed_instance_ids = []
    for i in instances:
      state = i.get('provisioningState') or i.get('properties', {}).get(
          'provisioningState'
      )
      if state == 'Failed':
        failed_instance_ids.append(i['instanceId'])

    if failed_instance_ids:
      cmd = self._Cmd('get-instance-view', '--name', self.name)
      stdout, stderr, _ = vm_util.IssueCommand(cmd, raise_on_failure=False)
      raise errors.Resource.CreationError(
          f'Failed to provision VMSS {self.name} because VMs failed: '
          f'{", ".join(failed_instance_ids)}. Details:\n{stdout or stderr}'
      )

  def _GetInstances(self) -> list[dict[str, Any]]:
    """Returns the instances in the VMSS."""
    cmd = self._Cmd('list-instances', '--name', self.name)
    stdout, _, _ = vm_util.IssueCommand(cmd)
    return json.loads(stdout)

  def _GetCurrentVms(
      self,
  ) -> list[VmReference]:
    instances = self._GetInstances()
    vms = []
    for i in instances:
      # PKB assumes GCP-style "zones" of region with an optional zone suffix.
      assert i['location'] == self.region
      zone = self.region
      if zones := i.get('zones'):
        assert len(zones) == 1
        zone += '-' + zones[0]
      vms.append(VmReference(name=i['instanceId'], zone=zone))
    return vms

  def _Resize(self, new_vm_count: int):
    cmd = self._Cmd(
        'scale',
        '--name',
        self.name,
        '--new-capacity',
        str(new_vm_count),
    )
    vm_util.IssueCommand(cmd)

  def _AddVms(self, *_, **__):
    raise NotImplementedError(
        'Azure only supports scaling up by resizing the VMSS.'
    )

  def _RemoveVms(self, vm_names: list[str]):
    cmd = self._Cmd('delete-instances', '--name', self.name)
    cmd.extend(['--instance-ids'] + vm_names)
    vm_util.IssueCommand(cmd)
