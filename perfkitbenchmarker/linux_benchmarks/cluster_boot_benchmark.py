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

import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import sample

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


def GetTimeToBoot(vms):
  """Creates Samples for the boot time of a list of VMs.

  The boot time is the time difference from before the VM is created to when
  the VM is responsive to SSH commands.

  Args:
    vms: List of BaseVirtualMachine subclasses.

  Returns:
    List of Samples containing the boot time.
  """
  min_create_start_time = min(vm.create_start_time for vm in vms)

  max_create_delay_sec = 0
  max_boot_time_sec = 0
  samples = []
  os_types = set()
  for i, vm in enumerate(vms):
    assert vm.bootable_time
    assert vm.create_start_time
    assert vm.bootable_time >= vm.create_start_time
    os_types.add(vm.OS_TYPE)
    create_delay_sec = vm.create_start_time - min_create_start_time
    max_create_delay_sec = max(max_create_delay_sec, create_delay_sec)
    metadata = {
        'machine_instance': i,
        'num_vms': len(vms),
        'os_type': vm.OS_TYPE,
        'create_delay_sec': '%0.1f' % create_delay_sec
    }
    boot_time_sec = vm.bootable_time - min_create_start_time
    max_boot_time_sec = max(max_boot_time_sec, boot_time_sec)
    samples.append(
        sample.Sample('Boot Time', boot_time_sec, 'seconds', metadata))

  # Add a total cluster boot sample as the maximum boot time.
  metadata = {
      'num_vms': len(vms),
      'os_type': ','.join(sorted(os_types)),
      'max_create_delay_sec': '%0.1f' % max_create_delay_sec
  }
  samples.append(
      sample.Sample('Cluster Boot Time', max_boot_time_sec, 'seconds',
                    metadata))
  if max_create_delay_sec > 1:
    logging.warning(
        'The maximum delay between starting VM creations is %0.1fs.',
        max_create_delay_sec)

  return samples


def Run(benchmark_spec):
  """Measure the boot time for all VMs.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    An empty list (all boot samples will be added later).
  """
  del benchmark_spec
  return []


def Cleanup(unused_benchmark_spec):
  pass
