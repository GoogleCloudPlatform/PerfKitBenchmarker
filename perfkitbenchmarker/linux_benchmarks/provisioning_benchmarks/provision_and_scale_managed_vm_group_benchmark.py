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
"""Benchmark that provisions and then optionally scales a managed VM group."""

from typing import Any

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import cluster_boot_benchmark

FLAGS = flags.FLAGS

RESIZE = 'resize'
ADD = 'add'
REMOVE = 'remove'

_SCALE_METHOD = flags.DEFINE_enum(
    'provision_managed_vm_group_scale_method',
    None,
    [RESIZE, ADD, REMOVE],
    'The method to use to scale the VM group. Will not scale if not set.',
)
_NEW_VM_COUNT = flags.DEFINE_integer(
    'provision_managed_vm_group_scale_vm_count',
    2,
    'The number of VMs to scale to. Requires '
    '--provision_managed_vm_group_scale_method to be set.',
    lower_bound=0,
)

BENCHMARK_NAME = 'provision_and_scale_managed_vm_group'
BENCHMARK_CONFIG = """
provision_and_scale_managed_vm_group:
  description: Create, optionally scale, and delete a managed VM group.
  vm_groups:
    default:
      vm_spec: *default_dual_core
      vm_count: null
      managed_spec: {}
  flags:
    skip_vm_preparation: true
    collect_delete_samples: true
"""


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the benchmark."""
  if not _SCALE_METHOD.value:
    # provisioning and boot metrics are reported by the resource framework.
    return []

  vm_group = benchmark_spec.managed_vm_groups['default']
  old_vm_count = vm_group.vm_count
  old_vms = set(vm.name for vm in vm_group.vms)
  if _NEW_VM_COUNT.value == old_vm_count:
    raise ValueError(
        'New VM count must be different from the current VM count.'
    )

  if _SCALE_METHOD.value == RESIZE:
    vm_group.Resize(_NEW_VM_COUNT.value)
  elif _SCALE_METHOD.value == REMOVE and _NEW_VM_COUNT.value < old_vm_count:
    vms = list(vm_group.vms.keys())
    vms_to_remove = vms[(old_vm_count - _NEW_VM_COUNT.value) :]
    vm_group.RemoveVms(vms_to_remove)
  elif _SCALE_METHOD.value == ADD and _NEW_VM_COUNT.value > old_vm_count:
    vm_group.AddVms(_NEW_VM_COUNT.value - old_vm_count)
  else:
    raise ValueError(
        f'Invalid scale method: {_SCALE_METHOD.value} for scaling from '
        f'{old_vm_count} to {_NEW_VM_COUNT.value}'
    )

  samples = []
  samples.append(
      sample.Sample(
          metric='scale_to_ready_duration',
          value=vm_group.last_ready_time - vm_group.last_operation_start_time,
          unit='seconds',
          metadata={
              'scale_method': _SCALE_METHOD.value,
              'original_vm_count': old_vm_count,
              'new_vm_count': _NEW_VM_COUNT.value,
          },
      )
  )
  if FLAGS.boot_samples:
    new_vms = [vm for vm in vm_group.vms if vm.name not in old_vms]
    samples.extend(cluster_boot_benchmark.GetTimeToBoot(new_vms))

  return samples


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  benchmark_config = configs.LoadConfig(
      BENCHMARK_CONFIG, user_config, BENCHMARK_NAME
  )
  if FLAGS.boot_samples:
    cluster_boot_benchmark.ConfigureStartupScript(benchmark_config)
  return benchmark_config


def Prepare(_):
  pass


def Cleanup(_):
  pass
