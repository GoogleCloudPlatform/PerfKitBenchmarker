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
"""AWS Auto Scaling Group resource."""

import json
import re
from typing import Any, cast

from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import errors
from perfkitbenchmarker import managed_vm_group
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import vm_group_decoders
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from perfkitbenchmarker.providers.aws import util

VmReference = managed_vm_group.BaseManagedVmGroup.VmReference

MIN_ASG_SIZE = 0
# Large VM groups are currently unused so keep small.
MAX_ASG_SIZE = 10


class AwsLaunchTemplate(resource.BaseResource):
  """AWS Launch Template."""

  CLOUD = provider_info.AWS

  def __init__(
      self,
      vm_config: aws_virtual_machine.AwsVirtualMachine,
      name: str,
  ):
    super().__init__()
    self.vm_config = vm_config
    self.vm_config.client_token = None  # pyrefly: ignore[bad-assignment]
    self.name = name
    self.region = vm_config.region
    self.base_cmd = util.AWS_PREFIX + ['ec2', '--region', self.region]

  def _Create(self):
    """Creates the launch template."""
    # Unlike GCE, we need to know the types of values for JSON and we can't
    # easily parse them out of the generated run-instances command.
    launch_template_data = {
        'ImageId': self.vm_config.image,
        'InstanceType': self.vm_config.machine_type,
        'KeyName': aws_virtual_machine.AwsKeyFileManager.GetKeyNameForRun(),
        'TagSpecifications': [{
            'ResourceType': 'instance',
            'Tags': [
                {'Key': k, 'Value': v}
                for k, v in self.vm_config.aws_tags.items()
            ],
        }],
        'NetworkInterfaces': [{
            'AssociatePublicIpAddress': self.vm_config.assign_external_ip,
            'DeviceIndex': 0,
            'Groups': [self.vm_config.group_id],
            'SubnetId': self.vm_config.network.subnet.id,  # pyrefly: ignore[missing-attribute]
        }],
    }

    if self.vm_config.placement_group:
      launch_template_data['Placement'] = {
          'GroupName': self.vm_config.placement_group.name
      }

    # TODO(pclay): Add support for boot_startup_script.

    cmd = self.base_cmd + [
        'create-launch-template',
        '--launch-template-name',
        self.name,
        '--launch-template-data',
        json.dumps(launch_template_data),
    ]
    util.IssueRetryableCommand(cmd)

  def _Delete(self):
    cmd = self.base_cmd + [
        'delete-launch-template',
        '--launch-template-name',
        self.name,
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _Exists(self):
    cmd = self.base_cmd + [
        'describe-launch-templates',
        '--launch-template-names',
        self.name,
    ]
    _, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    return retcode == 0


class AwsAutoScalingGroup(managed_vm_group.BaseManagedVmGroup):
  """AWS Auto Scaling Group."""

  CLOUD = provider_info.AWS

  def __init__(
      self,
      spec: vm_group_decoders.VmGroupSpec,
      vm_configs: list[virtual_machine.BaseVirtualMachine],
  ):
    super().__init__(spec, vm_configs)
    self.vm_config: aws_virtual_machine.AwsVirtualMachine = cast(
        aws_virtual_machine.AwsVirtualMachine, self.vm_config
    )
    self.zoned_vm_configs: list[aws_virtual_machine.AwsVirtualMachine] = cast(
        list[aws_virtual_machine.AwsVirtualMachine], self.zoned_vm_configs
    )
    self.name = self.vm_config.name
    self.region = self.vm_config.region

    self.launch_template = AwsLaunchTemplate(self.vm_config, name=self.name)
    self.base_cmd = util.AWS_PREFIX + ['autoscaling', '--region', self.region]

  def _CreateDependencies(self):
    super()._CreateDependencies()
    self.launch_template.Create()

  def _Create(self):
    subnets = []
    for vm in self.zoned_vm_configs:
      subnets.append(vm.network.subnet.id)  # pyrefly: ignore[missing-attribute]
    cmd = self.base_cmd + [
        'create-auto-scaling-group',
        '--auto-scaling-group-name',
        self.name,
        '--launch-template',
        f'LaunchTemplateName={self.launch_template.name}',
        '--min-size',
        # Set global bounds for ASG size. These are not used unless we
        # attach a scaling policy (or other hooks)
        str(MIN_ASG_SIZE),
        '--max-size',
        str(MAX_ASG_SIZE),
        '--desired-capacity',
        str(self.vm_count),
        '--vpc-zone-identifier',
        ','.join(subnets),
    ]
    if self.vm_config.aws_tags:
      cmd.append('--tags')
      for k, v in self.vm_config.aws_tags.items():
        cmd.append(
            f'ResourceId={self.name},ResourceType=auto-scaling-group,'
            f'Key={k},Value={v},PropagateAtLaunch=true'
        )
    vm_util.IssueCommand(cmd)

  def _DeleteDependencies(self):
    self.launch_template.Delete()
    super()._DeleteDependencies()

  def _Delete(self):
    cmd = self.base_cmd + [
        'delete-auto-scaling-group',
        '--auto-scaling-group-name',
        self.name,
        '--force-delete',
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _Get(self) -> dict[str, Any] | None:
    cmd = self.base_cmd + [
        'describe-auto-scaling-groups',
        '--auto-scaling-group-names',
        self.name,
    ]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      raise errors.Resource.GetError(
          f'Failed to describe ASG {self.name}:\n{stderr}\n'
      )
    groups = json.loads(stdout)['AutoScalingGroups']
    return groups[0] if groups else None

  def _Exists(self) -> bool:
    return bool(self._Get())

  def _IsReady(self) -> bool:
    cmd = self.base_cmd + [
        'describe-scaling-activities',
        '--auto-scaling-group-name',
        self.name,
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    activities = json.loads(stdout)['Activities']
    unsuccessful_activities = [
        a for a in activities if a['StatusCode'] in ['Failed', 'Cancelled']
    ]
    if unsuccessful_activities:
      error_message = (
          'Found the following unsuccessful activities in ASG '
          f'{self.name}:\n'
      )
      for a in unsuccessful_activities:
        # Description usually includes StatusMessage, but include both for
        # completeness.
        error_message += f'- {a['Description']:}\n'
        error_message += f'  {a['StatusCode']} - {a['StatusMessage']:}\n'
      if 'InsufficientInstanceCapacity' in error_message or re.search(
          r'do not have sufficient \S+ capacity ', error_message
      ):
        raise errors.Benchmarks.InsufficientCapacityCloudFailure(error_message)
      if 'Unsupported' in error_message or 'not supported' in error_message:
        raise errors.Benchmarks.UnsupportedConfigError(error_message)
      raise errors.Resource.UpdateError(error_message)
    if not all(a['StatusCode'] == 'Successful' for a in activities):
      return False
    # If all sctivies are completed all VMs should be ready, but double check.
    # This is a small increase in latency.
    asg = self._Get()
    if not asg:
      return False
    instances = asg.get('Instances', [])
    if len(instances) != self.vm_count:
      return False
    return all(i['LifecycleState'] == 'InService' for i in instances)

  def _GetCurrentVms(
      self,
  ) -> list[managed_vm_group.BaseManagedVmGroup.VmReference]:
    asg = self._Get()
    if not asg:
      return []
    instances = asg.get('Instances', [])
    return [
        VmReference(name=i['InstanceId'], zone=i.get('AvailabilityZone'))
        for i in instances
    ]

  def _ConfigureVm(
      self,
      vm: virtual_machine.BaseVirtualMachine,
      reference: VmReference,
  ):
    super()._ConfigureVm(vm, reference)
    vm = cast(aws_virtual_machine.AwsVirtualMachine, vm)
    vm.id = reference.name

  def _AddVms(self, num_vms_to_add: int, zone: str | None = None):
    del num_vms_to_add
    cmd = self.base_cmd + [
        'launch-instances',
        '--auto-scaling-group-name',
        self.name,
        '--requested-capacity',
        # _RunOperation already updates the desired capacity.
        str(self.vm_count),
    ]
    if zone:
      cmd.extend(['--availability-zones', zone])
    vm_util.IssueCommand(cmd)

  def _RemoveVms(self, vm_names: list[str]):
    def RemoveVm(vm_name: str):
      cmd = self.base_cmd + [
          'terminate-instance-in-auto-scaling-group',
          '--instance-id',
          vm_name,
          '--should-decrement-desired-capacity',
      ]
      vm_util.IssueCommand(cmd)

    background_tasks.RunThreaded(RemoveVm, vm_names)

  def _Resize(self, new_vm_count: int):
    cmd = self.base_cmd + [
        'update-auto-scaling-group',
        '--auto-scaling-group-name',
        self.name,
        '--desired-capacity',
        str(new_vm_count),
    ]
    vm_util.IssueCommand(cmd)
