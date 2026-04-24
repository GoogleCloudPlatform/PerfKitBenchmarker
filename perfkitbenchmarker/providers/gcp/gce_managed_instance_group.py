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
"""GCE Managed Instance Group resource."""

import json
from typing import Any, cast

from perfkitbenchmarker import errors
from perfkitbenchmarker import managed_vm_group
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import vm_group_decoders
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import util

VmReference = managed_vm_group.BaseManagedVmGroup.VmReference


class GceInstanceTemplate(resource.BaseResource):
  """GCE Instance Template.

  Contains an initialized GceVirtualMachine, which eventually has a
  gcloud compute instances create command that shares most flags with the
  instance template creation command.
  """

  CLOUD = provider_info.GCP

  FLAGS_NOT_SHARED_WITH_VM = ['async', 'zone']

  def __init__(
      self,
      vm_config: gce_virtual_machine.GceVirtualMachine,
      region: str,
      name: str,
  ):
    super().__init__()
    self.vm_config: gce_virtual_machine.GceVirtualMachine = vm_config
    self.name: str = name
    self.project: str = vm_config.project
    self.region: str = region
    self.qualified_name: str = (
        f'projects/{self.project}/regions/{self.region}'
        f'/instanceTemplates/{self.name}'
    )

  def _GcloudCmd(self, *args) -> util.GcloudCommand:
    return util.GcloudCommand(self, 'compute', 'instance-templates', *args)

  def _Create(self):
    # vm_config._CreateDependencies() pre-creates the gcloud instances create
    # command, which has most of the flags that we need for the instance
    # template.
    assert self.vm_config.create_cmd
    cmd = self._GcloudCmd(
        'create',
        self.name,
        '--instance-template-region',
        self.region,
    )
    cmd.flags = {
        k: v
        for k, v in self.vm_config.create_cmd.flags.items()
        if k not in self.FLAGS_NOT_SHARED_WITH_VM
    }
    cmd.Issue()

  def _Delete(self):
    cmd = self._GcloudCmd(
        self,
        'compute',
        'instance-templates',
        'delete',
        self.name,
        '--region',
        self.region,
    )
    cmd.Issue()

  def _Exists(self):
    cmd = self._GcloudCmd(
        'describe',
        self.name,
        '--region',
        self.region,
    )
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    return retcode == 0


class GceManagedInstanceGroup(managed_vm_group.BaseManagedVmGroup):
  """GCE Managed Instance Group."""

  CLOUD = provider_info.GCP

  def __init__(
      self,
      spec: vm_group_decoders.VmGroupSpec,
      vm_config: virtual_machine.BaseVirtualMachine,
  ):
    super().__init__(spec, vm_config)
    self.vm_config: gce_virtual_machine.GceVirtualMachine = cast(
        gce_virtual_machine.GceVirtualMachine, self.vm_config
    )
    self.project = self.vm_config.project
    # TODO(pclay): Add support for regional managed instance groups.
    # It's unclear how multiple zones would be plumbed through BaseVmSpec.
    self.zone = self.vm_config.zone
    self.region = util.GetRegionFromZone(self.zone)

    self.instance_template = GceInstanceTemplate(
        self.vm_config, self.region, name=self.name
    )

  def _CreateDependencies(self):
    super()._CreateDependencies()
    self.instance_template.Create()

  def _GcloudCmd(self, *args) -> util.GcloudCommand:
    return util.GcloudCommand(
        self, 'compute', 'instance-groups', 'managed', *args
    )

  def _Create(self):
    cmd = self._GcloudCmd(
        'create',
        self.name,
        '--template',
        self.instance_template.qualified_name,
        '--size',
        str(self.vm_count),
    )
    # TODO(pclay): Consider beta and --resource-manager-tags for labels.
    cmd.Issue()

  def _DeleteDependencies(self):
    self.instance_template.Delete()
    super()._DeleteDependencies()

  def _Delete(self):
    cmd = self._GcloudCmd('delete', self.name)
    cmd.Issue()

  def _Get(self) -> dict[str, Any] | None:
    cmd = self._GcloudCmd('describe', self.name)
    stdout, stderr, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      if 'not found' in stderr:
        return None
      raise errors.Resource.GetError(
          f'Failed to describe managed instance group {self.name}:\n{stderr}\n'
      )
    return json.loads(stdout)

  def _Exists(self) -> bool:
    return bool(self._Get())

  def _IsReady(self) -> bool:
    return self._Get()['status']['isStable']

  def _GetCurrentVms(self) -> list[VmReference]:
    cmd = self._GcloudCmd('list-instances', self.name)
    stdout, stderr, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      raise errors.Resource.GetError(
          f'Failed to list instances for managed instance group {self.name}:\n'
          f'{stderr}\n'
      )
    instances = json.loads(stdout)
    return [
        VmReference(name=instance['name'])
        for instance in instances
    ]

  def _AddVms(self, num_vms_to_add: int):
    cmd = self._GcloudCmd('create-instance', self.name)
    for i in range(num_vms_to_add):
      vm_name = f'{self.name}-{i}'
      cmd.args += ['--instance', vm_name]
    cmd.Issue()

  def _RemoveVms(self, vm_names: list[str]):
    cmd = self._GcloudCmd('delete-instances', self.name)
    for vm_name in vm_names:
      cmd.args += ['--instance', vm_name]
    cmd.Issue()

  def _Resize(self, new_vm_count: int):
    self._GcloudCmd('resize', self.name, '--size', str(new_vm_count)).Issue()
