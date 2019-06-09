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
"""Runs IOR and mdtest benchmarks.

IOR is a tool used for distributed testing of filesystem performance.
mdtest is used for distributed testing of filesystem metadata performance.

See https://github.com/hpc/ior for more info.
"""

import itertools
import posixpath

from perfkitbenchmarker import configs
from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import hpc_util
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ior

FLAGS = flags.FLAGS

flags.DEFINE_integer(
    'ior_num_procs', 256,
    'The number of MPI processes to use for IOR.')
flags.DEFINE_string(
    'ior_script', 'default_ior_script',
    'The IOR script to run. See '
    'https://github.com/hpc/ior/blob/master/doc/sphinx/userDoc/skripts.rst '
    'for more info.')
flags.DEFINE_integer(
    'mdtest_num_procs', 32,
    'The number of MPI processes to use for mdtest.')
flags.DEFINE_list(
    'mdtest_args', ['-n 1000 -u'],
    'Command line arguments to be passed to mdtest. '
    'Each set of args in the list will be run separately.')
flags.DEFINE_boolean(
    'mdtest_drop_caches', True,
    'Whether to drop caches between the create/stat/delete phases. '
    'If this is set, mdtest will be run 3 times with the -C, -T, and -r '
    'options and the client page caches will be dropped between runs.')


BENCHMARK_NAME = 'ior'
BENCHMARK_CONFIG = """
ior:
  description: Runs IOR and mdtest benchmarks.
  flags:
    data_disk_type: nfs
    data_disk_size: 2048
  vm_groups:
    default:
      vm_spec: *default_dual_core
      disk_spec: *default_500_gb
      vm_count: null
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install IOR on the vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  master_vm = vms[0]
  vm_util.RunThreaded(lambda vm: vm.Install('ior'), benchmark_spec.vms)
  hpc_util.CreateMachineFile(vms)
  master_vm.AuthenticateVm()


def Run(benchmark_spec):
  """Run the IOR benchmark on the vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  master_vm = benchmark_spec.vms[0]
  results = []
  # Run IOR benchmark.
  if FLAGS.ior_num_procs and FLAGS.ior_script:
    remote_script_path = posixpath.join(master_vm.scratch_disks[0].mount_point,
                                        FLAGS.ior_script)
    master_vm.PushDataFile(
        FLAGS.ior_script,
        remote_script_path,
        # SCP directly to SMB returns an error, so first copy to disk.
        should_double_copy=(FLAGS.data_disk_type == disk.SMB))
    results += ior.RunIOR(master_vm, FLAGS.ior_num_procs, remote_script_path)

  # Run mdtest benchmark.
  phase_args = ('-C', '-T', '-r') if FLAGS.mdtest_drop_caches else ('',)
  mdtest_args = (' '.join(args) for args in
                 itertools.product(FLAGS.mdtest_args, phase_args))
  for args in mdtest_args:
    results += ior.RunMdtest(master_vm, FLAGS.mdtest_num_procs, args)
    if FLAGS.mdtest_drop_caches:
      vm_util.RunThreaded(lambda vm: vm.DropCaches(), benchmark_spec.vms)

  return results


def Cleanup(unused_benchmark_spec):
  """Cleanup the IOR benchmark.

  Args:
    unused_benchmark_spec: The benchmark specification. Contains all data that
        is required to run the benchmark.
  """
  pass
