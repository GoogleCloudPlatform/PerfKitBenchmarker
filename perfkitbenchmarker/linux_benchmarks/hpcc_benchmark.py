# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Runs HPC Challenge.

Homepage: http://icl.cs.utk.edu/hpcc/

Most of the configuration of the HPC-Challenge revolves around HPL, the rest of
the HPCC piggybacks upon the HPL configration.

Homepage: http://www.netlib.org/benchmark/hpl/

HPL requires a BLAS library (Basic Linear Algebra Subprograms)
OpenBlas: http://www.openblas.net/
Intel MKL: https://software.intel.com/en-us/mkl

HPL also requires a MPI (Message Passing Interface) Library
OpenMPI: http://www.open-mpi.org/

MPI needs to be configured:
Configuring MPI:
http://techtinkering.com/2009/12/02/setting-up-a-beowulf-cluster-using-open-mpi-on-linux/

Once HPL is built the configuration file must be created:
Configuring HPL.dat:
http://www.advancedclustering.com/faq/how-do-i-tune-my-hpldat-file.html
http://www.netlib.org/benchmark/hpl/faqs.html
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import math
from typing import Any, Dict, List
from absl import flags
import dataclasses
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import hpc_util
from perfkitbenchmarker import linux_virtual_machine as linux_vm
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import hpcc
from perfkitbenchmarker.linux_packages import intel_repo
from perfkitbenchmarker.linux_packages import mkl
from perfkitbenchmarker.linux_packages import numactl
from perfkitbenchmarker.linux_packages import openblas

FLAGS = flags.FLAGS
LOCAL_HPCCINF_FILE = 'hpccinf.j2'
HPCCINF_FILE = 'hpccinf.txt'
MACHINEFILE = 'machinefile'
BLOCK_SIZE = 192
STREAM_METRICS = ['Copy', 'Scale', 'Add', 'Triad']

MKL_TGZ = 'l_mkl_2018.2.199.tgz'
BENCHMARK_DATA = {
    # Intel MKL package downloaded from:
    # https://software.intel.com/en-us/mkl
    # In order to get "l_mkl_2018.2.199.tgz", please choose the product
    # "Intel Performance Libraries for Linux*", choose the version
    # "2018 Update 2" and choose the download option "Intel
    # Math Kernel Library(Intel Mkl)".
    MKL_TGZ: 'e28d12173bef9e615b0ded2f95f59a42b3e9ad0afa713a79f8801da2bfb31936',
}

# File for mpirun to run that calls ./hpcc
HPCC_WRAPPER = 'hpcc_wrapper.sh'

BENCHMARK_NAME = 'hpcc'
BENCHMARK_CONFIG = """
hpcc:
  description: Runs HPCC. Specify the number of VMs with --num_vms
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: null
"""

SECONDS_PER_HOUR = 60 * 60


@dataclasses.dataclass(frozen=True)
class HpccDimensions:
  """Dimensions for the run.

  Replaces values in the data/hpccinf.txt file.  For more details see
  http://www.netlib.org/benchmark/hpl/tuning.html .  The value in quotes after
  the field name is the corresponding attribute name in the hpccinf.txt file.

  Attributes:
    problem_size: 'Ns': the problem size.
    block_size: 'NBs': number of blocks.
    num_rows: 'Ps': number of rows for each grid.
    num_columns: 'Qs': number of columns for each grid.
    pfacts: 'PFACTs': matrix-vector operation based factorization.
    nbmins: 'NBMINs': the number of columns at which to stop factorization.
    rfacts: 'RFACTs': type of recursive panel factorization.
    bcasts: 'BCASTs': methodology to broadcast the current panel.
    depths: 'DEPTHs': look ahread depth.
    swap: swapping algorithm to use.
    l1: 'L1': whether the upper triangle of the panel of columns should be
      stored in transposed form.
    u: 'U': whether the panel of rows U should be stored in transposed form.
    equilibration: whether to enable the equilibration phase.
  """
  problem_size: int
  block_size: int
  num_rows: int
  num_columns: int
  pfacts: int
  nbmins: int
  rfacts: int
  bcasts: int
  depths: int
  swap: int
  l1: int
  u: int
  equilibration: int


# Translating the --hpcc_ flags into numbers in the HPL configuration file
PFACT_RFACT_MAPPING = {'left': 0, 'crout': 1, 'right': 2}
BCAST_MAPPING = {'1rg': 0, '1rM': 1, '2rg': 2, '2rM': 3, 'Lng': 4, 'LnM': 5}
SWAP_MAPPING = {'bin-exch': 0, 'long': 1, 'mix': 2}
L1_U_MAPPING = {True: 0, False: 1}
EQUILIBRATION_MAPPING = {True: 1, False: 0}

flags.DEFINE_integer(
    'memory_size_mb', None,
    'The amount of memory in MB on each machine to use. By '
    'default it will use the entire system\'s memory.')
flags.DEFINE_string(
    'hpcc_binary', None,
    'The path of prebuilt hpcc binary to use. If not provided, '
    'this benchmark built its own using OpenBLAS.')
flags.DEFINE_list(
    'hpcc_mpi_env', [], 'Comma separated list containing environment variables '
    'to use with mpirun command. e.g. '
    'MKL_DEBUG_CPU_TYPE=7,MKL_ENABLE_INSTRUCTIONS=AVX512')
flags.DEFINE_float(
    'hpcc_timeout_hours', 4,
    'The number of hours to wait for the HPCC binary to '
    'complete before timing out and assuming it failed.')
flags.DEFINE_boolean(
    'hpcc_numa_binding', False,
    'If True, attempt numa binding with membind and cpunodebind.')

# HPL.dat configuration parameters
CONFIG_PROBLEM_SIZE = flags.DEFINE_integer(
    'hpcc_problem_size', None,
    'Size of problems to solve.  Leave as None to run one single problem '
    'whose size is based on the amount of memory.')
CONFIG_BLOCK_SIZE = flags.DEFINE_integer(
    'hpcc_block_size', None,
    'Block size.  Left as None to be based on the amount of memory.')
CONFIG_DIMENSIONS = flags.DEFINE_string(
    'hpcc_dimensions', None,
    'Number of rows and columns in the array: "1,2" is 1 row, 2 columns. '
    'Leave as None for computer to select based on number of CPUs.')
CONFIG_PFACTS = flags.DEFINE_enum(
    'hpcc_pfacts', 'right', sorted(PFACT_RFACT_MAPPING),
    'What type of matrix-vector operation based factorization to use.')
CONFIG_NBMINS = flags.DEFINE_integer(
    'hpcc_nbmins', 4,
    'The number of columns at which to stop panel factorization.')
CONFIG_RFACTS = flags.DEFINE_enum(
    'hpcc_rfacts', 'crout', sorted(PFACT_RFACT_MAPPING),
    'The type of recursive panel factorization to use.')
CONFIG_BCASTS = flags.DEFINE_enum(
    'hpcc_bcasts', '1rM', sorted(BCAST_MAPPING),
    'The broadcast methodology to use on the current panel.')
CONFIG_DEPTHS = flags.DEFINE_integer(
    'hpcc_depths', 1, 'Look ahead depth. '
    '0: next panel is factorized after current completely finished. '
    '1: next panel is immediately factorized after current is updated.')
CONFIG_SWAP = flags.DEFINE_enum('hpcc_swap', 'mix', sorted(SWAP_MAPPING),
                                'Swapping algorithm to use.')
CONFIG_L1 = flags.DEFINE_boolean(
    'hpcc_l1', True, 'Whether to store the upper triangle as transposed.')
CONFIG_U = flags.DEFINE_boolean('hpcc_u', True,
                                'Whether to store the U column as transposed.')
CONFIG_EQUILIBRATION = flags.DEFINE_boolean(
    'hpcc_equilibration', True, 'Whether to enable the equilibration phase.')


def GetConfig(user_config: Dict[Any, Any]) -> Dict[Any, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(_) -> None:
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
    NotImplementedError: On certain flag combination not currently supported.
  """
  data.ResourcePath(LOCAL_HPCCINF_FILE)
  if FLAGS['hpcc_binary'].present:
    data.ResourcePath(FLAGS.hpcc_binary)
  if FLAGS.hpcc_numa_binding and FLAGS.num_vms > 1:
    raise errors.Setup.InvalidFlagConfigurationError(
        'Numa binding with with multiple hpcc vm not supported.')
  if CONFIG_DIMENSIONS.value:
    parts = CONFIG_DIMENSIONS.value.split(',')
    if len(parts) != 2:
      raise errors.Setup.InvalidFlagConfigurationError(
          'For --hpcc_dimensions must have two values like "1,2" '
          f'not "{CONFIG_DIMENSIONS.value}"')
    if not (parts[0].isnumeric() and parts[1].isnumeric()):
      raise errors.Setup.InvalidFlagConfigurationError(
          '--hpcc_dimensions must be integers like "1,2" not '
          f'"{parts[0]},{parts[1]}"')


def _CalculateHpccDimensions(num_vms: int, num_cpus: int,
                             vm_memory_size_actual: int) -> HpccDimensions:
  """Calculates the HPCC dimensions for the run."""
  if FLAGS.memory_size_mb:
    total_memory = FLAGS.memory_size_mb * 1024 * 1024 * num_vms
  else:
    total_memory = vm_memory_size_actual * 1024 * num_vms
  total_cpus = num_cpus * num_vms
  block_size = CONFIG_BLOCK_SIZE.value or BLOCK_SIZE

  if CONFIG_PROBLEM_SIZE.value:
    problem_size = CONFIG_PROBLEM_SIZE.value
  else:
    # Finds a problem size that will fit in memory and is a multiple of the
    # block size.
    base_problem_size = math.sqrt(total_memory * .1)
    blocks = int(base_problem_size / block_size)
    blocks = blocks if (blocks % 2) == 0 else blocks - 1
    problem_size = block_size * blocks

  if CONFIG_DIMENSIONS.value:
    num_rows, num_columns = [
        int(item) for item in CONFIG_DIMENSIONS.value.split(',')
    ]
  else:
    # Makes the grid as 'square' as possible, with rows < columns
    sqrt_cpus = int(math.sqrt(total_cpus)) + 1
    num_rows = 0
    num_columns = 0
    for i in reversed(list(range(sqrt_cpus))):
      if total_cpus % i == 0:
        num_rows = i
        num_columns = total_cpus // i
        break

  return HpccDimensions(
      problem_size=problem_size,
      block_size=block_size,
      num_rows=num_rows,
      num_columns=num_columns,
      pfacts=PFACT_RFACT_MAPPING[CONFIG_PFACTS.value],
      nbmins=CONFIG_NBMINS.value,
      rfacts=PFACT_RFACT_MAPPING[CONFIG_RFACTS.value],
      bcasts=BCAST_MAPPING[CONFIG_BCASTS.value],
      depths=CONFIG_DEPTHS.value,
      swap=SWAP_MAPPING[CONFIG_SWAP.value],
      l1=L1_U_MAPPING[CONFIG_L1.value],
      u=L1_U_MAPPING[CONFIG_U.value],
      equilibration=EQUILIBRATION_MAPPING[CONFIG_EQUILIBRATION.value])


def CreateHpccinf(vm: linux_vm.BaseLinuxVirtualMachine,
                  benchmark_spec: bm_spec.BenchmarkSpec) -> HpccDimensions:
  """Creates the HPCC input file."""
  dimensions = _CalculateHpccDimensions(
      len(benchmark_spec.vms), vm.NumCpusForBenchmark(),
      vm.total_free_memory_kb)
  vm.RemoteCommand(f'rm -f {HPCCINF_FILE}')
  vm.RenderTemplate(
      data.ResourcePath(LOCAL_HPCCINF_FILE),
      remote_path=HPCCINF_FILE,
      context=dataclasses.asdict(dimensions))
  return dimensions


def PrepareHpcc(vm: linux_vm.BaseLinuxVirtualMachine) -> None:
  """Builds HPCC on a single vm."""
  logging.info('Building HPCC on %s', vm)
  vm.Install('hpcc')
  if FLAGS.hpcc_numa_binding:
    vm.Install('numactl')


def PrepareBinaries(vms: List[linux_vm.BaseLinuxVirtualMachine]) -> None:
  """Prepare binaries on all vms."""
  headnode_vm = vms[0]
  if FLAGS.hpcc_binary:
    headnode_vm.PushFile(data.ResourcePath(FLAGS.hpcc_binary), './hpcc')
  else:
    headnode_vm.RemoteCommand(f'cp {hpcc.HPCC_DIR}/hpcc hpcc')
  vm_util.RunThreaded(lambda vm: _PrepareBinaries(headnode_vm, vm), vms[1:])


def _PrepareBinaries(headnode_vm: linux_vm.BaseLinuxVirtualMachine,
                     vm: linux_vm.BaseLinuxVirtualMachine) -> None:
  """Prepares the binaries on the vm."""
  vm.Install('fortran')
  headnode_vm.MoveFile(vm, 'hpcc', 'hpcc')
  headnode_vm.MoveFile(vm, '/usr/bin/orted', 'orted')
  vm.RemoteCommand('sudo mv orted /usr/bin/orted')
  if FLAGS.hpcc_math_library == hpcc.HPCC_MATH_LIBRARY_MKL:
    intel_repo.CopyIntelFiles(headnode_vm, vm)


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Install HPCC on the target vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  vms = benchmark_spec.vms
  headnode_vm = vms[0]

  PrepareHpcc(headnode_vm)
  CreateHpccinf(headnode_vm, benchmark_spec)
  hpc_util.CreateMachineFile(vms, remote_path=MACHINEFILE)
  PrepareBinaries(vms)
  headnode_vm.AuthenticateVm()


def BaseMetadata() -> Dict[str, str]:
  """Update metadata with hpcc-related flag values."""
  metadata = {}
  metadata['memory_size_mb'] = FLAGS.memory_size_mb
  if FLAGS['hpcc_binary'].present:
    metadata['override_binary'] = FLAGS.hpcc_binary
  if FLAGS['hpcc_mpi_env'].present:
    metadata['mpi_env'] = FLAGS.hpcc_mpi_env
  metadata['hpcc_math_library'] = FLAGS.hpcc_math_library
  metadata['hpcc_version'] = hpcc.HPCC_VERSION
  if FLAGS.hpcc_benchmarks:
    metadata['hpcc_benchmarks'] = FLAGS.hpcc_benchmarks
  if FLAGS.hpcc_math_library == hpcc.HPCC_MATH_LIBRARY_MKL:
    metadata['math_library_version'] = mkl.MKL_VERSION.value
  elif FLAGS.hpcc_math_library == hpcc.HPCC_MATH_LIBRARY_OPEN_BLAS:
    metadata['math_library_version'] = openblas.GIT_TAG
  metadata['openmpi_version'] = FLAGS.openmpi_version
  if FLAGS.hpcc_numa_binding:
    metadata['hpcc_numa_binding'] = FLAGS.hpcc_numa_binding
  return metadata


def ParseOutput(hpcc_output: str) -> List[sample.Sample]:
  """Parses the output from HPCC.

  Args:
    hpcc_output: A string containing the text of hpccoutf.txt.

  Returns:
    A list of samples to be published (in the same format as Run() returns).
  """
  results = []

  # Parse all metrics from metric=value lines in the HPCC output.
  metric_values = regex_util.ExtractAllFloatMetrics(hpcc_output)

  # For each benchmark that is run, collect the metrics and metadata for that
  # benchmark from the metric_values map.
  benchmarks_run = FLAGS.hpcc_benchmarks or hpcc.HPCC_METRIC_MAP
  for benchmark in benchmarks_run:
    for metric, units in hpcc.HPCC_METRIC_MAP[benchmark].items():
      value = metric_values[metric]

      # Common metadata for all runs done in Run's call to _AddCommonMetadata
      metadata = {
          metadata_item: metric_values[metadata_item]
          for metadata_item in hpcc.HPCC_METADATA_MAP[benchmark]
      }

      results.append(sample.Sample(metric, value, units, metadata))

  return results


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Run HPCC on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  # recreate the HPL config file with each run in case parameters change
  dimensions = CreateHpccinf(benchmark_spec.vms[0], benchmark_spec)
  logging.info('HPL.dat dimensions: %s', dimensions)
  samples = RunHpccSource(benchmark_spec.vms)
  _AddCommonMetadata(samples, benchmark_spec, dataclasses.asdict(dimensions))
  return samples


def _AddCommonMetadata(samples: List[sample.Sample],
                       benchmark_spec: bm_spec.BenchmarkSpec,
                       dimensions: Dict[str, Any]) -> None:
  """Adds metadata common to all samples."""
  for item in samples:
    item.metadata.update(BaseMetadata())
    item.metadata['num_machines'] = len(benchmark_spec.vms)
    item.metadata.update(dimensions)


def RunHpccSource(
    vms: List[linux_vm.BaseLinuxVirtualMachine]) -> List[sample.Sample]:
  """Returns the parsed output from running the compiled from source HPCC."""
  headnode_vm = vms[0]
  # backup existing HPCC output, if any
  headnode_vm.RemoteCommand(('if [ -f hpccoutf.txt ]; then '
                             'mv hpccoutf.txt hpccoutf-$(date +%s).txt; '
                             'fi'))
  num_processes = len(vms) * headnode_vm.NumCpusForBenchmark()
  run_as_root = '--allow-run-as-root' if FLAGS.mpirun_allow_run_as_root else ''
  mpi_flags = (f'-machinefile {MACHINEFILE} --mca orte_rsh_agent '
               f'"ssh -o StrictHostKeyChecking=no" {run_as_root} {_MpiEnv()}')
  mpi_cmd = 'mpirun '
  hpcc_exec = './hpcc'
  if FLAGS.hpcc_math_library == hpcc.HPCC_MATH_LIBRARY_MKL:
    # Must exec HPCC wrapper script to pickup location of libiomp5.so
    vm_util.RunThreaded(_CreateHpccWrapper, vms)
    hpcc_exec = f'./{HPCC_WRAPPER}'

  if FLAGS.hpcc_numa_binding:
    numa_map = numactl.GetNuma(headnode_vm)
    numa_hpcc_cmd = []
    for node, num_cpus in numa_map.items():
      numa_hpcc_cmd.append(f'-np {num_cpus} {mpi_flags} '
                           f'numactl --cpunodebind {node} '
                           f'--membind {node} {hpcc_exec}')
    mpi_cmd += ' : '.join(numa_hpcc_cmd)
  else:
    mpi_cmd += f'-np {num_processes} {mpi_flags} {hpcc_exec}'

  headnode_vm.RobustRemoteCommand(
      mpi_cmd, timeout=int(FLAGS.hpcc_timeout_hours * SECONDS_PER_HOUR))
  logging.info('HPCC Results:')
  stdout, _ = headnode_vm.RemoteCommand('cat hpccoutf.txt', should_log=True)
  if stdout.startswith('HPL ERROR'):
    # Annoyingly the mpi_cmd will succeed when there is an HPL error
    raise errors.Benchmarks.RunError(f'Error running HPL: {stdout}')

  return ParseOutput(stdout)


def _CreateHpccWrapper(vm: linux_vm.BaseLinuxVirtualMachine) -> None:
  """Creates a bash script to run HPCC on the VM.

  This is required for when MKL is installed via the Intel repos as the
  libiomp5.so file is not in /lib but rather in one found via sourcing the
  mklvars.sh file.

  Args:
    vm: Virtual machine to put file on.
  """
  text = ['#!/bin/bash', mkl.SOURCE_MKL_INTEL64_CMD, './hpcc']
  vm_util.CreateRemoteFile(vm, '\n'.join(text), HPCC_WRAPPER)
  vm.RemoteCommand(f'chmod +x {HPCC_WRAPPER}')


def _MpiEnv(mpi_flag: str = '-x') -> str:
  """Returns the --hpcc_mpi_env flags as a string for the mpirun command."""
  return ' '.join([f'{mpi_flag} {v}' for v in FLAGS.hpcc_mpi_env])


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleanup HPCC on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  vms = benchmark_spec.vms
  headnode_vm = vms[0]
  headnode_vm.RemoveFile('hpcc*')
  headnode_vm.RemoveFile(MACHINEFILE)

  for vm in vms[1:]:
    vm.RemoveFile('hpcc')
    vm.RemoveFile('/usr/bin/orted')
