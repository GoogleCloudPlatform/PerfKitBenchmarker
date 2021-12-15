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

import math
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
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
flags.DEFINE_boolean('act_dynamic_load', False,
                     'Dynamically adjust act test load. We start at initial '
                     'load from --act_load, if the underlying driver not '
                     'able to keep up, reduce the load and retry.')
ACT_DYNAMIC_LOAD_STEP = 0.9


def GetConfig(user_config):
  """Get benchmark config for act benchmark."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS.data_disk_type == disk.LOCAL:
    config['vm_groups']['default']['disk_count'] = (
        config['vm_groups']['default']['disk_count'] or None)
  else:
    config['vm_groups']['default']['disk_count'] = (
        config['vm_groups']['default']['disk_count'] or 1)
  disk_spec = config['vm_groups']['default']['disk_spec']
  for cloud in disk_spec:
    disk_spec[cloud]['mount_point'] = None
  return config


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Args:
    benchmark_config: Unused.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  del benchmark_config
  if FLAGS.act_dynamic_load and len(FLAGS.act_load) > 1:
    raise errors.Config.InvalidValue(
        'Attempting to apply dynamic load while setting multiple act_load '
        'steps.')


def Prepare(benchmark_spec):
  """Prepares act benchmark."""
  vm = benchmark_spec.vms[0]
  vm.Install('act')


def PrepareActConfig(vm, load):
  """Prepare config file for act benchmark."""
  if FLAGS.act_parallel:
    for i in range(FLAGS.act_reserved_partitions, len(vm.scratch_disks)):
      act.PrepActConfig(vm, float(load), i)
  else:
    act.PrepActConfig(vm, float(load))


def GenerateLoad():
  """Generate load for act test."""
  if FLAGS.act_dynamic_load:
    load = float(FLAGS.act_load[0])
    while load:
      yield load
      load = math.floor(ACT_DYNAMIC_LOAD_STEP * load)
  else:
    for load in FLAGS.act_load:
      yield load


def Run(benchmark_spec):
  """Runs act and reports the results."""
  vm = benchmark_spec.vms[0]
  act.RunActPrep(vm)
  samples = []
  run_samples = []
  for load in GenerateLoad():
    def _Run(act_load, index):
      run_samples.extend(act.RunAct(vm, act_load, index))

    PrepareActConfig(vm, load)
    if FLAGS.act_parallel:
      args = [((float(load), idx), {})
              for idx in range(
                  FLAGS.act_reserved_partitions, len(vm.scratch_disks))]
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
