# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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

"""Benchmark for timing provisioning for managed disks.

This measures the time it takes to create the disk, attach, and run the command
`ls /dev/sdb`.

TODO(user) this benchmark currently only works for GCE, and needs some
refactoring to become cloud-agnostic.
"""

from typing import List

from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import sample
from perfkitbenchmarker.providers.aws import flags as aws_flags
from perfkitbenchmarker.providers.azure import flags as azure_flags
from perfkitbenchmarker.providers.gcp import flags as gcp_flags

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'provision_disk'
BENCHMARK_CONFIG = """
provision_disk:
  description: >
      Time spinning up a managed disk.
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n2-standard-2
          zone: us-central1-c
        Azure:
          machine_type: Standard_D2s_v5
          zone: eastus2-2
        AWS:
          machine_type: m5.large
          zone: us-east-1c
      disk_spec:
        GCP:
          disk_type: pd-ssd
          disk_size: 10
        Azure:
          disk_type: PremiumV2_LRS
          disk_size: 10
        AWS:
          disk_type: gp2
          disk_size: 10
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(_):
  pass


def CheckPrerequisites(benchmark_config):
  """Perform flag checks."""
  if not disk.IsRemoteDisk(
      benchmark_config.vm_groups['default'].disk_spec.disk_type
  ):
    raise ValueError('Disk type must be a remote disk')
  if FLAGS.cloud == 'GCP' and gcp_flags.GCP_CREATE_DISKS_WITH_VM.value:
    raise errors.Setup.InvalidFlagConfigurationError(
        'gcp_create_disks_with_vm must be set to false for GCP'
    )
  if FLAGS.cloud == 'Azure' and azure_flags.AZURE_ATTACH_DISK_WITH_CREATE.value:
    raise errors.Setup.InvalidFlagConfigurationError(
        'azure_attach_disk_with_create must be set to false for Azure'
    )
  if FLAGS.cloud == 'AWS' and aws_flags.AWS_CREATE_DISKS_WITH_VM.value:
    raise errors.Setup.InvalidFlagConfigurationError(
        'aws_create_disks_with_vm must be set to false for AWS'
    )


def _WaitUntilAttached(vm, dsk) -> None:
  while vm.RemoteCommand(f'ls {dsk.GetDevicePath()}')[1]:
    continue


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs the benchmark."""
  vm: linux_virtual_machine.BaseLinuxVirtualMachine = bm_spec.vms[0]

  # TODO(user) in order for this to be cloud agnostic, we need to
  # refactor the virtual machine code for all the clouds to use disk strategies
  # like GCE
  if vm.create_disk_strategy is None:
    raise ValueError('VM Create Disk Strategy is None')
  if vm.create_disk_strategy.DiskCreatedOnVMCreation():
    raise ValueError(
        'Disk created on vm creation, cannot measure provisioning time.'
        'Please check the flags.'
    )
  samples = []
  max_time_to_create = 0
  max_time_to_attach = 0
  for scratch_disk in vm.scratch_disks:
    scratch_disk_samples = scratch_disk.GetSamples()
    samples.extend(scratch_disk_samples)
    for sample_details in scratch_disk_samples:
      if sample_details.metric == 'Time to Create':
        max_time_to_create = max(max_time_to_create, sample_details.value)
      elif sample_details.metric == 'Time to Attach':
        max_time_to_attach = max(max_time_to_attach, sample_details.value)
  samples.extend([
      sample.Sample(
          'Time to Create and Attach Disk',
          max_time_to_create + max_time_to_attach,
          'seconds',
          vm.GetResourceMetadata(),
      ),
  ])
  return samples


def Cleanup(_):
  pass
