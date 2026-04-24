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
from typing import Sequence
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import vm_group_decoders


def GetManagedVmGroupClass(cloud):
  """Get the Managed VM group corresponding to the cloud."""
  return resource.GetResourceClass(BaseManagedVmGroup, CLOUD=cloud)


class BaseManagedVmGroup(resource.BaseResource):
  """Base class representing a Managed VM group."""

  name: str

  RESOURCE_TYPE = 'BaseManagedVmGroup'
  CLOUD = None

  def __init__(
      self,
      spec: vm_group_decoders.VmGroupSpec,
      vm_config: virtual_machine.BaseVirtualMachine,
  ):
    super().__init__()
    self.spec: vm_group_decoders.VmGroupSpec = spec
    self.vm_config = vm_config
    # When we clone the VM config and rename it, our assumptions about the
    # disk names are wrong.
    # TODO(pclay): improve support for disks.
    self.vm_config.disks = []
    self.vm_config.user_managed = True

    self.name = self.vm_config.name
    self.vm_count = self.spec.vm_count
    self._vms: dict[str, virtual_machine.BaseVirtualMachine] = {}

    self.last_operation_start_time: float | None = None
    self.last_ready_time: float | None = None

  @property
  def vms(self) -> Sequence[virtual_machine.BaseVirtualMachine]:
    """Returns the VMs in the managed VM group."""
    return list(self._vms.values())

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
    self.last_operation_start_time = self.create_start_time
    self._UpdateVmList()

  def AddVms(self, num_vms_to_add: int):
    """Adds VMs to the managed VM group."""
    self._RunOperation(
        lambda: self._AddVms(num_vms_to_add),
        new_vm_count=self.vm_count + num_vms_to_add,
    )

  def RemoveVms(self, vm_names: list[str]):
    """Removes VMs from the managed VM group."""
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
    for vm in old_vms - set(vm.name for vm in found_vms):
      del self._vms[vm]
    new_vms = [vm for vm in found_vms if vm.name not in old_vms]

    def WaitForNewVm(vm_reference: 'BaseManagedVmGroup.VmReference'):
      vm = copy.copy(self.vm_config)
      self._vms[vm_reference.name] = vm
      vm.create_start_time = self.last_operation_start_time
      self._ConfigureVm(vm, vm_reference)
      # vm._WaitUntilRunning tends to rely on specifics of the creation request
      # Either an operation ID or a request ID, rather than just polling on the
      # VM ID, which makes it incompatible with managed VM groups.
      # If we ever start listing VMs in parallel with waiting for group
      # readiness, we may need to support this.
      vm._PostCreate()  # pylint: disable=protected-access
      vm.WaitForBootCompletion()

    background_tasks.RunThreaded(WaitForNewVm, new_vms)
