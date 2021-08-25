"""Records the time it takes to suspend and resume a cluster of vms."""

import time
from typing import List

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util

BENCHMARK_NAME = 'suspend_resume'
BENCHMARK_CONFIG = """
suspend_resume:
  description: >
      Create a cluster, record all times to suspend and resume.
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
    aws_vm_hibernate: True
    # AWS boot disk size must be specified for an encrypted disk mapping to be
    # created.
    aws_boot_disk_size: 150
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(unused_benchmark_spec):
  pass


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


def GetTimeToSuspend(
    vms: List[virtual_machine.BaseVirtualMachine]) -> List[sample.Sample]:
  """Creates Samples for the suspend time of a list of vms.

  The suspend time is the duration form when the VM is suspended to when the VM
  is no longer responsive to SSH commands.

  Args:
    vms: A list of Virtual Machines(GCE) (already booted/running)

  Returns:
    List of samples containing the suspend times and cluster suspend time.
  """

  if not vms:
    return []

  before_suspend_timestamp = time.time()

  # call suspend function on the list of vms.
  suspend_durations = vm_util.RunThreaded(lambda vm: vm.Suspend(), vms)

  # get cluster_suspend_time(duration)
  cluster_suspend_time = time.time() - before_suspend_timestamp

  # get samples for the cluster of vms
  return _GetVmOperationDataSamples(suspend_durations, cluster_suspend_time,
                                    'Suspend', vms)


def GetTimeToResume(
    vms: List[virtual_machine.BaseVirtualMachine]) -> List[sample.Sample]:
  """Measures the time it takes to resume a cluster of vms that were suspended.

  Args:
    vms: List of BaseVirtualMachine subclasses.

  Returns:
    List of samples containing the resume times of the vms, the cluster
    resume time(duration) and the time stamps when the resume was completed.
  """

  if not vms:
    return []

  before_resume_timestamp = time.time()

  # call resume function on the list of vms to get the resume times
  resume_durations = vm_util.RunThreaded(lambda vm: vm.Resume(), vms)

  # get cluster resume time(duration)
  cluster_resume_time = time.time() - before_resume_timestamp

  # get samples for the cluster of vms
  return _GetVmOperationDataSamples(resume_durations, cluster_resume_time,
                                    'Resume', vms)


def Run(spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Measure the boot time for all VMs.

  Args:
    spec: The benchmark specification.

  Returns:
    An empty list (all boot samples will be added later).
  """
  samples = []
  samples.extend(GetTimeToSuspend(spec.vms))
  samples.extend(GetTimeToResume(spec.vms))
  return samples


def Cleanup(unused_benchmark_spec):
  pass
