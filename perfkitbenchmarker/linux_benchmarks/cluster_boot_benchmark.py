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
import time
from typing import List
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util

BENCHMARK_NAME = 'cluster_boot'
BENCHMARK_CONFIG = """
cluster_boot:
  description: >
      Create a cluster, record all times to boot.
      Specify the cluster size with --num_vms.
  vm_groups:
    default:
      vm_spec:
        AWS:
          machine_type: m5.large
          zone: us-east-1
        Azure:
          machine_type: Standard_D2s_v3
          zone: eastus
          boot_disk_type: StandardSSD_LRS
        GCP:
          machine_type: n1-standard-2
          zone: us-central1-a
          boot_disk_type: pd-ssd
        IBMCloud:
          machine_type: cx2-2x4
          zone: us-south-1
        Kubernetes:
          image: null
        OpenStack:
          machine_type: t1.small
          zone: nova
      vm_count: null
  flags:
    # We don't want boot time samples to be affected from retrying, so don't
    # retry cluster_boot when rate limited.
    retry_on_rate_limited: False
"""

flags.DEFINE_boolean(
    'cluster_boot_time_reboot', False,
    'Whether to reboot the VMs during the cluster boot benchmark to measure '
    'reboot performance.')
flags.DEFINE_boolean(
    'cluster_boot_test_port_listening', False,
    'Test the time it takes to successfully connect to the port that is used to run the remote command.'
)
FLAGS = flags.FLAGS


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
    List of Samples containing the boot times and an overall cluster boot time.
  """
  if not vms:
    return []

  min_create_start_time = min(vm.create_start_time for vm in vms)

  max_create_delay_sec = 0
  max_boot_time_sec = 0
  max_port_listening_time_sec = 0
  max_rdp_port_listening_time_sec = 0
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
    if FLAGS.cluster_boot_test_port_listening:
      assert vm.port_listening_time
      assert vm.port_listening_time >= vm.create_start_time
      port_listening_time_sec = vm.port_listening_time - min_create_start_time
      max_port_listening_time_sec = max(max_port_listening_time_sec,
                                        port_listening_time_sec)
      samples.append(
          sample.Sample('Port Listening Time', port_listening_time_sec,
                        'seconds', metadata))
    # TODO(user): refactor so Windows specifics aren't in linux_benchmarks
    if FLAGS.cluster_boot_test_rdp_port_listening:
      assert vm.rdp_port_listening_time
      assert vm.rdp_port_listening_time >= vm.create_start_time
      rdp_port_listening_time_sec = (
          vm.rdp_port_listening_time - min_create_start_time)
      max_rdp_port_listening_time_sec = max(max_rdp_port_listening_time_sec,
                                            rdp_port_listening_time_sec)
      samples.append(
          sample.Sample('RDP Port Listening Time', rdp_port_listening_time_sec,
                        'seconds', metadata))

  # Add a total cluster boot sample as the maximum boot time.
  metadata = {
      'num_vms': len(vms),
      'os_type': ','.join(sorted(os_types)),
      'max_create_delay_sec': '%0.1f' % max_create_delay_sec
  }
  samples.append(
      sample.Sample('Cluster Boot Time', max_boot_time_sec, 'seconds',
                    metadata))
  if FLAGS.cluster_boot_test_port_listening:
    samples.append(
        sample.Sample('Cluster Port Listening Time',
                      max_port_listening_time_sec, 'seconds', metadata))
  if FLAGS.cluster_boot_test_rdp_port_listening:
    samples.append(
        sample.Sample('Cluster RDP Port Listening Time',
                      max_rdp_port_listening_time_sec, 'seconds', metadata))
  if max_create_delay_sec > 1:
    logging.warning(
        'The maximum delay between starting VM creations is %0.1fs.',
        max_create_delay_sec)

  return samples


def _MeasureReboot(vms):
  """Measures the time to reboot the cluster of VMs.

  Args:
    vms: List of BaseVirtualMachine subclasses.

  Returns:
    List of Samples containing the reboot times and an overall cluster reboot
    time.
  """
  before_reboot_timestamp = time.time()
  reboot_times = vm_util.RunThreaded(lambda vm: vm.Reboot(), vms)
  cluster_reboot_time = time.time() - before_reboot_timestamp
  return _GetVmOperationDataSamples(reboot_times, cluster_reboot_time, 'Reboot',
                                    vms)


def MeasureDelete(
    vms: List[virtual_machine.BaseVirtualMachine]) -> List[sample.Sample]:
  """Measures the time to delete the cluster of VMs.

  Args:
    vms: List of BaseVirtualMachine subclasses.

  Returns:
    List of Samples containing the delete times and an overall cluster delete
    time.
  """
  before_delete_timestamp = time.time()
  vm_util.RunThreaded(lambda vm: vm.Delete(), vms)
  delete_times = [vm.delete_end_time - vm.delete_start_time for vm in vms]
  max_delete_end_time = max([vm.delete_end_time for vm in vms])
  cluster_delete_time = max_delete_end_time - before_delete_timestamp
  return _GetVmOperationDataSamples(delete_times, cluster_delete_time, 'Delete',
                                    vms)


def _GetVmOperationDataSamples(
    operation_times: List[int], cluster_time: int, operation: str,
    vms: List[virtual_machine.BaseVirtualMachine]) -> List[sample.Sample]:
  """Append samples from given data.

  Args:
    operation_times: The list of times for each vms.
    cluster_time: The cluster time for the benchmark.
    operation: The benchmark operation being run, capitalized with no spaces.
    vms: list of virtual machines.

  Returns:
    List of samples constructed from data.
  """
  samples = []
  metadata_list = []
  for i, vm in enumerate(vms):
    metadata = {
        'machine_instance': i,
        'num_vms': len(vms),
        'os_type': vm.OS_TYPE
    }
    metadata_list.append(metadata)
  for operation_time, metadata in zip(operation_times, metadata_list):
    samples.append(
        sample.Sample(f'{operation} Time', operation_time, 'seconds', metadata))
  os_types = set([vm.OS_TYPE for vm in vms])
  metadata = {'num_vms': len(vms), 'os_type': ','.join(sorted(os_types))}
  samples.append(
      sample.Sample(f'Cluster {operation} Time', cluster_time, 'seconds',
                    metadata))
  return samples


def Run(benchmark_spec):
  """Measure the boot time for all VMs.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    An empty list (all boot samples will be added later).
  """
  samples = []
  if FLAGS.cluster_boot_time_reboot:
    samples.extend(_MeasureReboot(benchmark_spec.vms))
  return samples


def Cleanup(unused_benchmark_spec):
  pass
