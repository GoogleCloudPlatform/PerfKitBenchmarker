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
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import sample
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
      disk_spec:
        GCP:
          disk_type: pd-ssd
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
        'Please set the flag --gcp_create_disks_with_vm=false to create disk'
        'after VM creation.'
    )
  samples = []
  disk_details = vm.create_disk_strategy.pd_disk_groups
  for disk_group in disk_details:
    for disk_details in disk_group:
      total_time = 0
      for sample_details in disk_details.GetSamples():
        if sample_details.metric == 'Time to Create':
          total_time += sample_details.value
        elif sample_details.metric == 'Time to Attach':
          total_time += sample_details.value
      samples.extend(disk_details.GetSamples())
      samples.extend([
          sample.Sample(
              'Time to Create and Attach Disk',
              total_time,
              'seconds',
              vm.GetResourceMetadata(),
          ),
      ])
  return samples


def Cleanup(_):
  pass
