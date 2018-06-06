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

import posixpath

from perfkitbenchmarker import configs
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import flags
from perfkitbenchmarker import hpc_util
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ior

FLAGS = flags.FLAGS

flag_util.DEFINE_integerlist(
    'ior_num_procs', flag_util.IntegerList([256]),
    'The number of MPI processes to use for IOR.')
flags.DEFINE_string(
    'ior_script', 'default_ior_script',
    'The IOR script to run. See '
    'https://github.com/hpc/ior/blob/master/doc/sphinx/userDoc/skripts.rst '
    'for more info.')
flag_util.DEFINE_integerlist(
    'mdtest_num_procs', flag_util.IntegerList([32]),
    'The number of MPI processes to use for mdtest.')
flags.DEFINE_integer(
    'mdtest_files_per_proc', 1000,
    'The number of files/directories per mdtest process.',
    lower_bound=1)
flags.DEFINE_integer(
    'mdtest_iterations', 5,
    'The number of test iterations for mdtest.',
    lower_bound=1)


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
  remote_script_path = posixpath.join(master_vm.scratch_disks[0].mount_point,
                                      FLAGS.ior_script)
  master_vm.PushDataFile(FLAGS.ior_script, remote_script_path)
  for num_procs in FLAGS.ior_num_procs:
    results += ior.RunIOR(master_vm, num_procs, remote_script_path)
  for num_procs in FLAGS.mdtest_num_procs:
    results += ior.RunMdtest(master_vm, num_procs, FLAGS.mdtest_files_per_proc,
                             FLAGS.mdtest_iterations)
  return results


def Cleanup(unused_benchmark_spec):
  """Cleanup the IOR benchmark.

  Args:
    unused_benchmark_spec: The benchmark specification. Contains all data that
        is required to run the benchmark.
  """
  pass
