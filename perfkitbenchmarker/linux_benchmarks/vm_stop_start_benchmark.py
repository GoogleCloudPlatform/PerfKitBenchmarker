# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Records the time required to stop and start a cluster of VMs."""

import time
from typing import Any, Dict, List
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util

BENCHMARK_NAME = 'vm_stop_start'
BENCHMARK_CONFIG = """
vm_stop_start:
  description: >
      Create a cluster, record all times to start and stop.
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
    # We don't want start time samples to be affected from retrying, so don't
    # retry vm_start when rate limited.
    retry_on_rate_limited: False
"""


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(unused_benchmark_spec: benchmark_spec.BenchmarkSpec):
  pass


def _MeasureStart(
    vms: List[virtual_machine.BaseVirtualMachine]) -> List[sample.Sample]:
  """Measures the times to start the VMs.

  Args:
    vms: The VMs to perform the measurement on.

  Returns:
    List of Samples containing the start times and an overall cluster start
    time.
  """
  before_start_timestamp = time.time()
  start_times = vm_util.RunThreaded(lambda vm: vm.Start(), vms)
  cluster_start_time = time.time() - before_start_timestamp
  return _GetVmOperationDataSamples(start_times, cluster_start_time, 'Start',
                                    vms)


def _MeasureStop(
    vms: List[virtual_machine.BaseVirtualMachine]) -> List[sample.Sample]:
  """Measures the times to stop the VMss.

  Args:
    vms: The VMs to perform the measurement on.

  Returns:
    List of Samples containing the stop times and an overall cluster stop
    time.
  """
  before_stop_timestamp = time.time()
  stop_times = vm_util.RunThreaded(lambda vm: vm.Stop(), vms)
  cluster_stop_time = time.time() - before_stop_timestamp
  return _GetVmOperationDataSamples(stop_times, cluster_stop_time, 'Stop',
                                    vms)


# TODO(nsmit): Refactor to be useable in other files
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


def Run(spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Measure the stop and start times for the VMs.

  Args:
    spec: The benchmark specification.

  Returns:
    An empty list (all samples will be added later).
  """
  samples = []
  samples.extend(_MeasureStop(spec.vms))
  samples.extend(_MeasureStart(spec.vms))
  return samples


def Cleanup(unused_benchmark_spec: benchmark_spec.BenchmarkSpec):
  pass
