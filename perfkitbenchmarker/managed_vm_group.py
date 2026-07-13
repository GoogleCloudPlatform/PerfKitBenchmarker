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
"""Module containing class for managed VM groups."""

import abc
from collections.abc import Callable
import copy
import dataclasses
import time
from typing import Any, Sequence
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import vm_group_decoders


def GetManagedVmGroupClass(cloud):
  """Get the Managed VM group corresponding to the cloud."""
  return resource.GetResourceClass(BaseManagedVmGroup, CLOUD=cloud)


class BaseManagedVmGroup(resource.BaseResource):
  """Base class representing a Managed VM group.

  Attributes:
    name: The name of the managed VM group.
    region: The region where the managed VM group is located.
    vms: A sequence of all VMs in the managed VM group.

  Internal Attributes:
    spec: The spec of the managed VM group.
    vm_config: A virtual machine instance representing the prototype/template VM
      configuration used to clone new VMs in the group. Individual VM operations
      like `_Create()` and `_Delete()` are NOT called on this object (creation
      and deletion are managed by the VM group at the cloud level), but it
      defines the template settings.
    zoned_vm_configs: A list of all template VM instances, one for each
      configured zone in the group. Used to pre-create and cleanup zone-specific
      VM dependencies (like firewalls, networks, subnets) in every candidate
      zone.
    _vms: A mapping of VM names to active VM objects representing actual
      instances running in the cloud.
  """

  name: str
  region: str

  RESOURCE_TYPE = 'BaseManagedVmGroup'
  CLOUD = None

  def __init__(
      self,
      spec: vm_group_decoders.VmGroupSpec,
      vm_configs: list[virtual_machine.BaseVirtualMachine],
  ):
    super().__init__()
    self.spec: vm_group_decoders.VmGroupSpec = spec
    assert vm_configs, 'No VM configs provided.'
    # `vm_config` is the primary prototype/template VM configuration.
    # We do NOT run `_Create` or `_Delete` on it. Instead, we use it to
    # copy-clone the target VMs.
    self.vm_config = vm_configs[0]
    # `zoned_vm_configs` contains a template VM configuration for each zone.
    # Used to create/delete dependencies (e.g. networks, firewalls) in all
    # zones.
    self.zoned_vm_configs = vm_configs
    self.vm_config.metadata['in_managed_vm_group'] = True
    # When we clone the VM config and rename it, our assumptions about the
    # disk names are wrong.
    # TODO(pclay): improve support for disks.
    self.vm_config.disks = []  # pyrefly: ignore[missing-attribute]
    self.vm_config.user_managed = True

    self.name = self.vm_config.name
    self.vm_count = self.spec.vm_count
    # `_vms` mapping VM names to active VM objects representing actual instances
    # running in the cloud. They are populated by copy-cloning `vm_config` and
    # initializing names/zones. Then passed back to the benchmark spec for
    # benchmarking.
    self._vms: dict[str, virtual_machine.BaseVirtualMachine] = {}
    self._deleted_vms: list[virtual_machine.BaseVirtualMachine] = []

    self.zones: list[str] = [vm.zone for vm in vm_configs]  # pyrefly: ignore[bad-assignment]

    self.last_operation_start_time: float | None = None
    self.last_ready_time: float | None = None

  @property
  def is_regional(self) -> bool:
    assert self.zones
    return len(self.zones) > 1 or self.zones[0] == self.region

  @property
  def zone(self) -> str | None:
    assert self.zones
    if self.is_regional:
      return None
    return self.zones[0]

  def GetResourceMetadata(self) -> dict[Any, Any]:
    metadata = super().GetResourceMetadata().copy()
    vm = (list(self.vms) + self._deleted_vms + [self.vm_config])[0]
    metadata.update(vm.GetResourceMetadata())
    metadata['region'] = self.region
    metadata['zones'] = self.zones
    metadata['is_regional'] = self.is_regional
    return metadata

  @property
  def vms(self) -> Sequence[virtual_machine.BaseVirtualMachine]:
    """Returns the VMs in the managed VM group."""
    return list(self._vms.values())

  def _CreateDependencies(self):
    background_tasks.RunThreaded(
        lambda vm: vm._CreateDependencies(),  # pylint: disable=protected-access
        self.zoned_vm_configs,
    )

  def _DeleteDependencies(self):
    background_tasks.RunThreaded(
        lambda vm: vm._DeleteDependencies(),  # pylint: disable=protected-access
        self.zoned_vm_configs,
    )

  @dataclasses.dataclass
  class VmReference:
    name: str
    zone: str | None = None

  def _ConfigureVm(
      self,
      vm: virtual_machine.BaseVirtualMachine,
      reference: VmReference,
  ):
    """Configures the VM with the data in the VM reference."""
    vm.name = reference.name
    if reference.zone:
      vm.zone = reference.zone

  @abc.abstractmethod
  def _GetCurrentVms(self) -> Sequence[VmReference]:
    """Returns the current VMs in the managed VM group."""
    raise NotImplementedError()

  @abc.abstractmethod
  def _AddVms(self, num_vms_to_add: int):
    raise NotImplementedError()

  @abc.abstractmethod
  def _RemoveVms(self, vm_names: list[str]):
    raise NotImplementedError()

  @abc.abstractmethod
  def _Resize(self, new_vm_count: int):
    raise NotImplementedError()

  def _PostCreate(self):
    # Needed for GetResourceMetadata.
    self.vm_config.created = True
    self.last_operation_start_time = self.create_start_time
    self._UpdateVmList()

  def AddVms(self, num_vms_to_add: int):
    """Adds VMs to the managed VM group."""
    assert num_vms_to_add, 'No VMs to add.'
    self._RunOperation(
        lambda: self._AddVms(num_vms_to_add),
        new_vm_count=self.vm_count + num_vms_to_add,
    )

  def RemoveVms(self, vm_names: list[str]):
    """Removes VMs from the managed VM group."""
    assert vm_names, 'No VM names provided.'
    self._RunOperation(
        lambda: self._RemoveVms(vm_names),
        new_vm_count=self.vm_count - len(vm_names),
    )

  def Resize(self, new_vm_count: int):
    """Resizes the managed VM group to the new VM count."""
    self._RunOperation(lambda: self._Resize(new_vm_count), new_vm_count)

  def _RunOperation(self, operation: Callable[[], None], new_vm_count: int):
    self.vm_count = new_vm_count

    self.last_operation_start_time = time.time()
    operation()
    self._WaitUntilReady()
    self.last_ready_time = time.time()
    self._UpdateVmList()

  def _UpdateVmList(self):
    """Updates the list of VMs in the managed VM group."""
    old_vms = set(self._vms.keys())
    found_vms = self._GetCurrentVms()
    if len(found_vms) != self.vm_count:
      raise errors.Resource.UpdateError(
          f'Managed VM group {self.name} has {len(found_vms)} instances,'
          f' expected {self.vm_count}.'
      )
    for name in old_vms - set(vm.name for vm in found_vms):
      vm = self._vms.pop(name)
      vm.deleted = True
      self._deleted_vms.append(vm)
    new_vms = [vm for vm in found_vms if vm.name not in old_vms]

    def WaitForNewVm(vm_reference: 'BaseManagedVmGroup.VmReference'):
      vm = copy.copy(self.vm_config)
      self._vms[vm_reference.name] = vm
      vm.create_start_time = self.last_operation_start_time  # pyrefly: ignore[bad-assignment]
      self._ConfigureVm(vm, vm_reference)
      # vm._WaitUntilRunning tends to rely on specifics of the creation request
      # Either an operation ID or a request ID, rather than just polling on the
      # VM ID, which makes it incompatible with managed VM groups.
      # If we ever start listing VMs in parallel with waiting for group
      # readiness, we may need to support this.
      vm._PostCreate()  # pylint: disable=protected-access
      vm.WaitForBootCompletion()

    background_tasks.RunThreaded(WaitForNewVm, new_vms)
