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
"""Runs the Stencil2D benchmark from the SHOC Benchmark Suite"""

import os
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import hpc_util
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import flag_util
from perfkitbenchmarker.linux_packages import shoc_benchmark_suite
from perfkitbenchmarker.linux_packages import cuda_toolkit_8

flags.DEFINE_integer(
    'stencil2d_iterations', 5, 'number of iterations to run', lower_bound=1)

flag_util.DEFINE_integerlist(
    'stencil2d_problem_sizes',
    flag_util.IntegerList([4096]),
    'problem sizes to run. Can specify a single '
    'number, like --stencil2d_problem_sizes=4096 '
    'or a list like --stencil2d_problem_sizes='
    '1024,4096',
    on_nonincreasing=flag_util.IntegerListParser.WARN)
FLAGS = flags.FLAGS

MACHINEFILE = 'machinefile'
BENCHMARK_NAME = 'stencil2d'
BENCHMARK_VERSION = '0.25'
BENCHMARK_CONFIG = """
stencil2d:
  description: Runs Stencil2D from SHOC Benchmark Suite.\
      Specify the number of VMs with --num_vms
  vm_groups:
    default:
      vm_spec:
        GCP:
          image: ubuntu-1604-xenial-v20170330
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


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  pass


def _InstallAndAuthenticateVm(vm):
  """Install SHOC, ensure correct GPU state, and authenticate the VM for ssh.

  Args:
    vm: vm to operate on.
  """
  vm.Install('shoc_benchmark_suite')
  cuda_toolkit_8.SetAndConfirmGpuClocks(vm)
  vm.AuthenticateVm()  # Configure ssh between vms for MPI


def Prepare(benchmark_spec):
  """Install SHOC and push the machinefile.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm_util.RunThreaded(_InstallAndAuthenticateVm, benchmark_spec.vms)

  master_vm = benchmark_spec.vms[0]
  benchmark_spec.num_gpus = cuda_toolkit_8.QueryNumberOfGpus(master_vm)
  hpc_util.CreateMachineFile(benchmark_spec.vms,
                             lambda _: benchmark_spec.num_gpus,
                             MACHINEFILE)


def _CreateMedianStencilOutputSample(stencil2d_output, sample_name, pretty_name,
                                     metadata):
  """Extract the specified sample from the stencil2d output.

  Args:
    stencil2d_output: output from a Stencil2D run
    sample_name: the name of the sample, as it appears in stencil2d_output
    pretty_name: the name to use for the created sample
    metadata: metadata to add the sample

  Returns:
    A sample.Sample with the name, pretty_name, containing the value and units
    as parsed from stencil2d_output, along with metadata.

  """
  results = [
      x for x in stencil2d_output.splitlines() if x.find(sample_name) != -1
  ][0].split()
  units = results[2]
  value = float(results[3])
  return sample.Sample(pretty_name, value, units, metadata)


def _MakeSamplesFromStencilOutput(stdout, metadata):
  """Make and return a list of samples, parsed from the Stencil2D output.

  Args:
    stdout: Stencil2D output
    metadata: metadata to append to the returned samples

  Returns:
    A list of sample.Samples
  """
  results = [
      _CreateMedianStencilOutputSample(stdout, 'DP_Sten2D(median)',
                                       'Stencil2D DP median', metadata),
      _CreateMedianStencilOutputSample(stdout, 'DP_Sten2D(stddev)',
                                       'Stencil2D DP stddev', metadata),
      _CreateMedianStencilOutputSample(stdout, 'SP_Sten2D(median)',
                                       'Stencil2D SP median', metadata),
      _CreateMedianStencilOutputSample(stdout, 'SP_Sten2D(stddev)',
                                       'Stencil2D SP stddev',
                                       metadata)  # pyformat: disable
  ]
  return results


def _RunSingleIteration(master_vm, problem_size, num_processes, num_iterations,
                        metadata):
  """Run a single iteration of Stencil2D and return a list of samples.

  Args:
    master_vm: master VM which will start the MPI job
    problem_size: a single dimension of the Stencil2D problem_size
    num_processes: total number of MPI processes to launch (number of GPUs)
    num_iterations: number of Stencil2D iterations to run
    metadata: metadata to append to returned samples

  Returns:
    A list of sample.Samples
  """
  stencil2d_path = os.path.join(shoc_benchmark_suite.SHOC_BIN_DIR, 'TP', 'CUDA',
                                'Stencil2D')
  current_problem_size = '%s,%s' % (problem_size, problem_size)
  run_command = ('mpirun --hostfile %s -np %s %s --customSize %s -n %s' %
                 (MACHINEFILE, num_processes, stencil2d_path,
                  current_problem_size, num_iterations))
  metadata['run_command'] = run_command
  metadata['problem_size'] = current_problem_size

  stdout, _ = master_vm.RemoteCommand(run_command, should_log=True)
  return _MakeSamplesFromStencilOutput(stdout, metadata.copy())


def Run(benchmark_spec):
  """Runs the Stencil2D benchmark. GPU clock speeds must be set already.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  num_gpus = benchmark_spec.num_gpus
  master_vm = vms[0]
  num_iterations = FLAGS.stencil2d_iterations
  problem_sizes = FLAGS.stencil2d_problem_sizes
  num_processes = len(vms) * num_gpus

  metadata = {}
  metadata.update(cuda_toolkit_8.GetMetadata(master_vm))
  metadata['benchmark_version'] = BENCHMARK_VERSION
  metadata['num_iterations'] = num_iterations
  metadata['num_nodes'] = len(vms)
  metadata['num_processes'] = num_processes

  results = []
  for problem_size in problem_sizes:
    results.extend(
        _RunSingleIteration(master_vm, problem_size, num_processes,
                            num_iterations, metadata))
  return results


def Cleanup(benchmark_spec):
  pass
