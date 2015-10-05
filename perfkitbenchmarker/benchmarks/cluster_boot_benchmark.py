# Copyright 2014 Google Inc. All rights reserved.
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

from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import sample

FLAGS = flags.FLAGS
BENCHMARK_INFO = {'name': 'cluster_boot',
                  'description': 'Create a cluster, record all times to boot. '
                  'Specify the cluster size with --num_vms.',
                  'scratch_disk': False,
                  'num_machines': None}  # Set in GetInfo()


def GetInfo():
  info = BENCHMARK_INFO.copy()
  info['num_machines'] = FLAGS.num_vms
  return info


def Prepare(unused_benchmark_spec):
  pass


def _GetTimeToBoot(vm, vm_index, result_list):
  metadata = {'machine_type': vm.machine_type, 'num_cpus': vm.num_cpus,
              'machine_instance': vm_index}
  assert vm.bootable_time
  assert vm.create_start_time
  assert vm.bootable_time >= vm.create_start_time
  value = vm.bootable_time - vm.create_start_time
  result_list.append(sample.Sample('Boot Time', value, 'seconds', metadata))


def Run(benchmark_spec):
  """Measure the boot time for all VMs.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects with individual machine boot times.
  """

  samples = []
  logging.info('Boot Results:')
  vms = benchmark_spec.vms
  params = [((vm, i, samples), {}) for i, vm in enumerate(vms)]
  vm_util.RunThreaded(_GetTimeToBoot, params)
  logging.info(samples)
  assert len(samples) == benchmark_spec.num_vms
  return samples


def Cleanup(unused_benchmark_spec):
  pass
