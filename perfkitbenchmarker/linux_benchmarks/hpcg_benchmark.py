# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Run HPCG.

Requires:
* openmpi 1.10.2

Binaries are provided which are statically linked to CUDA 8.0.44 and
OpenMPI 1.10.2 and 1.6.5. If different versions of these libs are to be used,
modify the HPCG package to build from source, located here:
https://github.com/hpcg-benchmark/hpcg/
"""

import logging
import os
import re
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import hpc_util
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cuda_toolkit_8
from perfkitbenchmarker.linux_packages import hpcg

FLAGS = flags.FLAGS
RUN_SCRIPT = 'run_hpcg.sh'
CONFIG_FILE = 'hpcg.dat'
MACHINEFILE = 'HOSTFILE'

BENCHMARK_VERSION = 0.1
BENCHMARK_NAME = 'hpcg'
BENCHMARK_CONFIG = """
hpcg:
  description: Runs HPCG. Specify the number of VMs with --num_vms
  vm_groups:
    default:
      vm_spec:
        GCP:
          image: ubuntu-1604-xenial-v20170307
          image_project: ubuntu-os-cloud
          machine_type: n1-standard-4
          gpu_type: k80
          gpu_count: 1
          zone: us-east1-d
          boot_disk_size: 200
        AWS:
          image: ami-d15a75c7
          machine_type: p2.xlarge
          zone: us-east-1
          boot_disk_size: 200
        Azure:
          image: Canonical:UbuntuServer:16.04.0-LTS:latest
          machine_type: Standard_NC6
          zone: eastus
      vm_count: null
"""

flags.DEFINE_integer(
    'hpcg_runtime', 60, 'hpcg runtime in seconds', lower_bound=1)

flags.DEFINE_integer(
    'hpcg_gpus_per_node', None, 'The number of gpus per node.', lower_bound=1)

flag_util.DEFINE_integerlist(
    'hpcg_problem_size',
    flag_util.IntegerList([256, 256, 256]),
    'three dimensional problem size for each node. Must contain '
    'three integers')


class HpcgParseOutputException(Exception):
  pass


class HpcgIncorrectProblemSizeException(Exception):
  pass


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(_):
  """Verify that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  _LocalDataPath(RUN_SCRIPT)
  _LocalDataPath(CONFIG_FILE)


def _LocalDataPath(local_file):
  """Return local data path for given file.

  Args:
    local_file: name of local file to create full data path for

  Returns:
    path of local_file, in the data directory
  """
  return data.ResourcePath(local_file)


def _UpdateBenchmarkSpecWithFlags(benchmark_spec):
  """Update the benchmark_spec with supplied command line flags.

  Args:
    benchmark_spec: benchmark specification to update
  """
  gpus_per_node = (FLAGS.hpcg_gpus_per_node or
                   cuda_toolkit_8.QueryNumberOfGpus(benchmark_spec.vms[0]))
  cpus_per_rank = int(benchmark_spec.vms[0].num_cpus / gpus_per_node)
  num_vms = len(benchmark_spec.vms)
  total_gpus = gpus_per_node * num_vms

  benchmark_spec.gpus_per_node = gpus_per_node
  benchmark_spec.cpus_per_rank = cpus_per_rank
  benchmark_spec.num_vms = num_vms
  benchmark_spec.total_gpus = total_gpus
  benchmark_spec.hpcg_problem_size = FLAGS.hpcg_problem_size
  benchmark_spec.hpcg_runtime = FLAGS.hpcg_runtime


def _CopyAndUpdateRunScripts(vm, benchmark_spec):
  """Copy and update all necessary run scripts on the given vm.

  Args:
    vm: vm to place and update run scripts on
    benchmark_spec: benchmark specification
  """
  src_path = _LocalDataPath(CONFIG_FILE)
  dest_path = os.path.join(hpcg.HPCG_DIR, CONFIG_FILE)
  context = {
      'PROBLEM_SIZE': '%s %s %s' % (benchmark_spec.hpcg_problem_size[0],
                                    benchmark_spec.hpcg_problem_size[1],
                                    benchmark_spec.hpcg_problem_size[2]),
      'RUNTIME': benchmark_spec.hpcg_runtime
  }
  vm.RenderTemplate(src_path, dest_path, context)
  vm.PushDataFile(RUN_SCRIPT, os.path.join(hpcg.HPCG_DIR, RUN_SCRIPT))


def _PrepareHpcg(vm):
  """Install HPCG on a single vm.

  Args:
    vm: vm to operate on
  """
  logging.info('Installing HPCG on %s', vm)
  vm.Install('hpcg')
  vm.AuthenticateVm()
  cuda_toolkit_8.SetAndConfirmGpuClocks(vm)


def Prepare(benchmark_spec):
  """Install and set up HPCG on the target vms.

  Args:
    benchmark_spec: The benchmark specification
  """
  vms = benchmark_spec.vms
  vm_util.RunThreaded(_PrepareHpcg, vms)
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  for vm in vms:
    _CopyAndUpdateRunScripts(vm, benchmark_spec)
  hpc_util.CreateMachineFile(vms,
                             lambda _: benchmark_spec.gpus_per_node,
                             os.path.join(hpcg.HPCG_DIR, MACHINEFILE))


def _CreateMetadataDict(benchmark_spec):
  """Create metadata dict to be used in run results.

  Args:
    benchmark_spec: benchmark spec

  Returns:
    metadata dict
  """
  vm = benchmark_spec.vms[0]
  metadata = dict()
  metadata.update(cuda_toolkit_8.GetMetadata(vm))
  metadata['num_nodes'] = len(benchmark_spec.vms)
  metadata['cpus_per_rank'] = int(benchmark_spec.cpus_per_rank)
  metadata['total_gpus'] = int(benchmark_spec.total_gpus)
  metadata['benchmark_version'] = BENCHMARK_VERSION
  metadata['runtime'] = int(benchmark_spec.hpcg_runtime)
  metadata['problem_size'] = '%s,%s,%s' % (benchmark_spec.hpcg_problem_size[0],
                                           benchmark_spec.hpcg_problem_size[1],
                                           benchmark_spec.hpcg_problem_size[2])

  return metadata


def _GetEnvironmentVars(benchmark_spec):
  """Return a string containing HPCG-related environment variables.

  Args:
    benchmark_spec: benchmark spec

  Returns:
    string of environment variables
  """
  return ' '.join([
      'NUM_GPUS=%s' % benchmark_spec.total_gpus,
      'OMP_NUM_THREADS=%s' % benchmark_spec.cpus_per_rank
  ])


def _ExtractProblemSize(output):
  """Extract problem size from HPCG output.

  Args:
    output: HPCG output

  Returns:
    problem size in list format
  """
  regex = r'(\d+)x(\d+)x(\d+)\s+local domain'
  match = re.search(regex, output)
  try:
    return [int(match.group(1)),
            int(match.group(2)),
            int(match.group(3))]
  except:
    raise HpcgParseOutputException('Unable to parse HPCG output')


def _ExtractThroughput(output):
  """Extract throughput from HPCG output.

  Args:
    output: HPCG output

  Returns:
    throuput (float)
  """
  regex = r'final\s+=\s+(\S+)\s+GF'
  match = re.search(regex, output)
  try:
    return float(match.group(1))
  except:
    raise HpcgParseOutputException('Unable to parse HPCG output')


def _MakeSamplesFromOutput(benchmark_spec, output):
  """Create a sample continaing the measured HPCG throughput.

  Args:
    benchmark_spec: benchmark spec
    output: HPCG output

  Returns:
    a Sample containing the HPCG throughput in Gflops
  """
  actual_hpcg_problem_size = _ExtractProblemSize(output)
  if actual_hpcg_problem_size != benchmark_spec.hpcg_problem_size:
    raise HpcgIncorrectProblemSizeException(
        'Actual problem size of %s does not match expected problem '
        'size of %s' % (actual_hpcg_problem_size,
                        benchmark_spec.hpcg_problem_size))

  metadata = _CreateMetadataDict(benchmark_spec)
  hpcg_throughput = _ExtractThroughput(output)
  return [sample.Sample('HPCG Throughput', hpcg_throughput, 'Gflops', metadata)]


def Run(benchmark_spec):
  """Run HPCG on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  master_vm = vms[0]
  run_command = 'cd %s && %s ./%s' % (hpcg.HPCG_DIR,
                                      _GetEnvironmentVars(benchmark_spec),
                                      RUN_SCRIPT)
  output, _ = master_vm.RobustRemoteCommand(run_command, should_log=True)
  return _MakeSamplesFromOutput(benchmark_spec, output)


def Cleanup(benchmark_spec):
  """Cleanup HPCG on the cluster."""
  pass
