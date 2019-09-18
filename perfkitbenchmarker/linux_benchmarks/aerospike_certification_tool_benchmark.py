# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs a aerospike certification tool benchmark.

See https://github.com/aerospike/act for more info.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from perfkitbenchmarker import configs
from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import act
from six.moves import range

BENCHMARK_NAME = 'aerospike_certification_tool'
BENCHMARK_CONFIG = """
aerospike_certification_tool:
  description: Runs aerospike certification tool.
  vm_groups:
    default:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
      vm_count: 1
      disk_count: 0
"""

FLAGS = flags.FLAGS
flags.DEFINE_boolean(
    'act_stop_on_complete', True,
    'Stop the benchmark when completing current load. This can be useful '
    'deciding maximum sustained load for stress tests.')


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS.data_disk_type == disk.LOCAL:
    config['vm_groups']['default']['disk_count'] = (
        config['vm_groups']['default']['disk_count'] or None)
  else:
    config['vm_groups']['default']['disk_count'] = (
        config['vm_groups']['default']['disk_count'] or 1)
  return config


def Prepare(benchmark_spec):
  """Prepares act benchmark."""
  vm = benchmark_spec.vms[0]
  vm.Install('act')
  for load in FLAGS.act_load:
    if FLAGS.act_parallel:
      for i in range(len(vm.scratch_disks))[FLAGS.act_reserved_partitions:]:
        act.PrepActConfig(vm, float(load), i)
    else:
      act.PrepActConfig(vm, float(load))
  for d in vm.scratch_disks:
    vm.RemoteCommand('sudo umount %s' % d.mount_point)


def Run(benchmark_spec):
  """Runs act and reports the results."""
  vm = benchmark_spec.vms[0]
  act.RunActPrep(vm)
  samples = []
  run_samples = []
  for load in FLAGS.act_load:
    def _Run(act_load, index):
      run_samples.extend(act.RunAct(vm, act_load, index))

    if FLAGS.act_parallel:
      args = [((float(load), idx), {})
              for idx in range(
                  len(vm.scratch_disks))[FLAGS.act_reserved_partitions:]]
      vm_util.RunThreaded(_Run, args)
    else:
      run_samples.extend(act.RunAct(vm, float(load)))
    samples.extend(run_samples)
    if FLAGS.act_stop_on_complete and act.IsRunComplete(run_samples):
      break
    run_samples = []
  return samples


def Cleanup(benchmark_spec):
  del benchmark_spec
