# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Runs io500 benchmarks.

See https://github.com/IO500/io500 for more info.
"""

import logging
import math

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import hpc_util
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample

FLAGS = flags.FLAGS


BENCHMARK_NAME = 'io500'
BENCHMARK_CONFIG = """
io500:
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
GIT_REPO = 'https://github.com/IO500/io500.git'
SUMMARY_REGEX = (r'\[SCORE \] Bandwidth ([\d\.]*) ([\w\/]*) : '
                 r'IOPS ([\d\.]*) ([\w]*) : TOTAL ([\d\.]*)')
RESULT_REGEX = r'\[RESULT\]\s+([\w\-]*)\s+([\d\.]+) ([\w\/]+) : time ([\d\.]+)'
IO500_OUTPUT = 'data'
BENCHMARK_DIR = '/opt/pkb'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install io500 on the vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  vms = benchmark_spec.vms
  headnode = benchmark_spec.vm_groups['default'][0]
  # Install mpi across nodes.
  background_tasks.RunThreaded(lambda vm: vm.Install('openmpi'), vms)
  background_tasks.RunThreaded(lambda vm: vm.Install('build_tools'), vms)
  background_tasks.RunThreaded(lambda vm: vm.AuthenticateVm(), vms)
  hpc_util.CreateMachineFile(vms)
  background_tasks.RunThreaded(lambda vm: vm.InstallPackages('pkg-config'), vms)
  # io500 needs to build a group of binaries.
  # Among them, the pinned commit for pfind is currently broken,
  # where it looks for *.o files. This commit aaba722 fixes the script
  # to look for *.a files.
  # Install binary on all vms.
  background_tasks.RunThreaded(lambda vm: vm.RemoteCommand(
      f'cd {BENCHMARK_DIR} && git clone {GIT_REPO} -b io500-isc24 && '
      'cd io500 && '
      'sed -i "s/778dca8/aaba722/g" prepare.sh && '
      './prepare.sh'), vms)
  # TODO(yuyanting) Make this a flag to accept other configs.
  local_path = data.ResourcePath('io500/io500.ini.j2')
  remote_path = f'{BENCHMARK_DIR}/io500.ini'
  background_tasks.RunThreaded(lambda vm: vm.RenderTemplate(
      template_path=local_path, remote_path=remote_path,
      context={'directory': headnode.scratch_disks[0].mount_point}
      ), vms)


def _Run(headnode, ranks, ppn):
  """Runs io500 with specified number of ranks.

  Sample output:
  [RESULT]        ior-easy-read        0.228625 GiB/s : time 179.716 seconds
  [RESULT]     mdtest-hard-stat        8.946267 kIOPS : time 3.524 seconds
  ...
  [SCORE ] Bandwidth 0.150245 GiB/s : IOPS 1.731613 kiops : TOTAL 0.510066
    [INVALID]

  Args:
    headnode: VirtualMachine object to run io500.
    ranks: Total ranks to run.
    ppn: Process per node.

  Returns:
    List of sample.Sample object.
  """
  results = []
  stdout, _ = headnode.RemoteCommand(
      f'cd {BENCHMARK_DIR} && '
      'mpirun --hostfile ~/MACHINEFILE -oversubscribe '
      f'-n {ranks} -npernode {ppn} '
      f'io500/io500 io500.ini')
  for line in stdout.splitlines():
    invalid = '[INVALID]' in line
    if invalid:
      logging.warning('Found invalid result: %s', line)
    if line.startswith('[RESULT]'):
      metric, val, unit, duration = regex_util.ExtractAllMatches(
          RESULT_REGEX, line)[0]
      if val == '0.000':
        # if we skip a test (by modifying config file), value will be 0.000.
        continue
      results.append(
          sample.Sample(
              metric, val, unit,
              {
                  'ranks': ranks,
                  'ppn': ppn,
                  'duration': duration,
                  'invalid': '[INVALID]' in line
              }
          ))
    elif line.startswith('[SCORE ]'):
      # Parse summary line
      # This is mostly useless, unless we run standard parameters.
      # raw_result = line.split()
      raw_result = regex_util.ExtractAllMatches(SUMMARY_REGEX, line)[0]
      metadata = {
          'ranks': ranks,
          'ppn': ppn,
          'invalid': '[INVALID]' in line
      }
      results.extend([
          sample.Sample('BANDWIDTH', raw_result[0], raw_result[1], metadata),
          sample.Sample('IOPS', raw_result[2], raw_result[3], metadata),
          sample.Sample('TOTAL', raw_result[4], '', metadata),
      ])
  return results


def Run(benchmark_spec):
  """Run the io500 benchmark on the vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  headnode = benchmark_spec.vm_groups['default'][0]
  num_nodes = len(benchmark_spec.vm_groups['default'])
  results = []
  num_cpus = headnode.NumCpusForBenchmark()
  for total_ranks in FLAGS.mpi_ranks_list or [num_nodes * num_cpus]:
    ppn = math.ceil(float(total_ranks) / num_nodes)
    results.extend(_Run(headnode, int(total_ranks), ppn))
  return results


def Cleanup(benchmark_spec):
  """Cleanup the IOR benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  headnode = benchmark_spec.vm_groups['default'][0]
  headnode.RemoteCommand(
      f'cd {headnode.scratch_disks[0].mount_point} && rm -rf {IO500_OUTPUT}'
  )
  pass
