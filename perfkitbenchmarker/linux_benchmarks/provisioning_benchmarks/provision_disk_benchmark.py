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

import time
from typing import List

from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import disk
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import sample
from perfkitbenchmarker.providers.gcp import gce_disk

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
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(_):
  pass


def _WaitUntilAttached(vm, dsk) -> None:
  while vm.RemoteCommand(f'ls {dsk.GetDevicePath()}')[1]:
    continue


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs the benchmark."""
  disk_spec_class = disk.GetDiskSpecClass(FLAGS.cloud)
  disk_spec = disk_spec_class('provisioning', flag_values=FLAGS)
  vm: linux_virtual_machine.BaseLinuxVirtualMachine = bm_spec.vms[0]

  # TODO(user) in order for this to be cloud agnostic, we need to
  # refactor the disk logic for each cloud into: 1) initializing the disk
  # object, 2) creating it, 3) attaching it, and 4) formatting/mounting and
  # other prep. We cannot directly call vm.CreateScratchDisk because it has
  # extra steps.
  managed_disk: disk.BaseDisk = gce_disk.GceDisk(
      disk_spec, f'pkb-provisioning-{FLAGS.run_uri}', vm.zone, vm.project
  )
  start_time = time.time()
  managed_disk.Create()
  create_time = time.time() - start_time
  managed_disk.Attach(vm)
  _WaitUntilAttached(vm, managed_disk)
  time_to_ls = time.time() - start_time

  samples = managed_disk.GetSamples()
  samples.extend([
      sample.Sample(
          'Time to Create Disk',
          create_time,
          'seconds',
      ),
      sample.Sample(
          'Time to Create and Attach Disk',
          time_to_ls,
          'seconds',
      ),
  ])
  return samples


def Cleanup(_):
  pass
