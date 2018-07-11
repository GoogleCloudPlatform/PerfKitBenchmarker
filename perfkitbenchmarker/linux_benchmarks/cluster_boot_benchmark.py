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

"""Records the time required to boot a cluster of VMs."""

import time

from perfkitbenchmarker import configs
from perfkitbenchmarker import os_types
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util


BENCHMARK_NAME = 'cluster_boot'
BENCHMARK_CONFIG = """
cluster_boot:
  description: >
      Create a cluster, record all times to boot.
      Specify the cluster size with --num_vms.
  vm_groups:
    default:
      vm_spec: *default_dual_core
      vm_count: null
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(unused_benchmark_spec):
  pass


def _GetTimeToSsh(vm, vm_index, num_vms):
  """Gets the time to ssh to a VM successfully for the first time.

  Args:
    vm: The VM, a BaseVirtualMachine subclass.
    vm_index: A unique 0-based index for the VM.
    num_vms: The number of VMs that were booted together.

  Returns:
    A performance samples for how long it took from the creation of the VM to
    when the benchmark was first able to SSH into the VM successfully.
  """
  metadata = {
      'machine_instance': vm_index,
      'num_vms': num_vms,
      'os_type': vm.OS_TYPE
  }
  assert vm.bootable_time
  assert vm.create_start_time
  assert vm.bootable_time >= vm.create_start_time
  value = vm.bootable_time - vm.create_start_time
  return sample.Sample('Boot Time', value, 'seconds', metadata)


def _GetTimeToUptime(vm, vm_index, num_vms):
  """Gets the time from creation of a VM to the recorded uptime.

  The uptime is recorded by the guest after the kernel is initialized. This
  metric takes into account the time to create the VM and start the guest OS.
  There is additional overhead for the time spent returning from calling SSH.

  Args:
    vm: The VM, a BaseVirtualMachine subclass.
    vm_index: A unique 0-based index for the VM.
    num_vms: The number of VMs that were booted together.

  Returns:
    A performance samples for how long it took from the creation of the VM to
    when the VM recorded its uptime.
  """
  metadata = {
      'machine_instance': vm_index,
      'os_type': vm.OS_TYPE
  }
  assert vm.create_start_time
  stdout, _ = vm.RemoteHostCommand("awk '{print $1}' /proc/uptime")
  value = time.time() - float(stdout.rstrip()) - vm.create_start_time
  assert value > 0, ('The uptime reported by the guest indicates the VM was '
                     'up before it was created.')
  return sample.Sample('Guest-Adjusted Boot Time', value, 'seconds', metadata)


def GetTimeToBoot(vms):
  """Creates Samples for the boot time of a list of VMs.

  The boot time is the time difference from before the VM is created to when
  the VM is responsive to SSH commands.

  Args:
    vms: List of BaseVirtualMachine subclasses.

  Returns:
    List of Samples containing the boot time.
  """
  get_time_args = [((vm, i, len(vms)), {}) for i, vm in enumerate(vms)]

  samples = vm_util.RunThreaded(_GetTimeToSsh, get_time_args)
  assert len(samples) == len(vms)

  # TODO(deitz): Extend this metric to support Windows VMs. Also, refactor the
  # code for cluster_boot_benchmark.py in windows_benchmarks so that the code
  # sharing makes more sense. Note that the cluster_boot_benchmark code in the
  # windows_benchmarks directory is not called since this benchmark code is
  # invoked as a special case in pkb.py.
  if not any(vm.OS_TYPE in os_types.WINDOWS_OS_TYPES for vm in vms):
    samples.extend(vm_util.RunThreaded(_GetTimeToUptime, get_time_args))
    assert len(samples) == 2 * len(vms)

  return samples


def Run(benchmark_spec):
  """Measure the boot time for all VMs.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    An empty list (all boot samples will be added later).
  """
  del benchmark_spec
  return []


def Cleanup(unused_benchmark_spec):
  pass
