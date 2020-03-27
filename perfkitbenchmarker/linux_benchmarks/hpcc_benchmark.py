# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import hpc_util
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import hpcc
from perfkitbenchmarker.linux_packages import mkl
from perfkitbenchmarker.linux_packages import openblas
import six
from six.moves import range

FLAGS = flags.FLAGS
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
    MKL_TGZ:
        'e28d12173bef9e615b0ded2f95f59a42b3e9ad0afa713a79f8801da2bfb31936',
}

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

flags.DEFINE_integer('memory_size_mb',
                     None,
                     'The amount of memory in MB on each machine to use. By '
                     'default it will use the entire system\'s memory.')
flags.DEFINE_string('hpcc_binary', None,
                    'The path of prebuilt hpcc binary to use. If not provided, '
                    'this benchmark built its own using OpenBLAS.')
flags.DEFINE_list('hpcc_mpi_env', [],
                  'Comma separated list containing environment variables '
                  'to use with mpirun command. e.g. '
                  'MKL_DEBUG_CPU_TYPE=7,MKL_ENABLE_INSTRUCTIONS=AVX512')
flags.DEFINE_integer('hpcc_timeout_hours', 4,
                     'The number of hours to wait for the HPCC binary to '
                     'complete before timing out and assuming it failed.')


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(_):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  data.ResourcePath(HPCCINF_FILE)
  if FLAGS['hpcc_binary'].present:
    data.ResourcePath(FLAGS.hpcc_binary)


def CreateHpccinf(vm, benchmark_spec):
  """Creates the HPCC input file."""
  num_vms = len(benchmark_spec.vms)
  if FLAGS.memory_size_mb:
    total_memory = FLAGS.memory_size_mb * 1024 * 1024 * num_vms
  else:
    total_memory = vm.total_free_memory_kb * 1024 * num_vms
  total_cpus = vm.NumCpusForBenchmark() * num_vms
  block_size = BLOCK_SIZE

  # Finds a problem size that will fit in memory and is a multiple of the
  # block size.
  base_problem_size = math.sqrt(total_memory * .1)
  blocks = int(base_problem_size / block_size)
  blocks = blocks if (blocks % 2) == 0 else blocks - 1
  problem_size = block_size * blocks

  # Makes the grid as 'square' as possible, with rows < columns
  sqrt_cpus = int(math.sqrt(total_cpus)) + 1
  num_rows = 0
  num_columns = 0
  for i in reversed(list(range(sqrt_cpus))):
    if total_cpus % i == 0:
      num_rows = i
      num_columns = total_cpus / i
      break

  file_path = data.ResourcePath(HPCCINF_FILE)
  vm.PushFile(file_path, HPCCINF_FILE)
  sed_cmd = (('sed -i -e "s/problem_size/%s/" -e "s/block_size/%s/" '
              '-e "s/rows/%s/" -e "s/columns/%s/" %s') %
             (problem_size, block_size, num_rows, num_columns, HPCCINF_FILE))
  vm.RemoteCommand(sed_cmd)


def PrepareHpcc(vm):
  """Builds HPCC on a single vm."""
  logging.info('Building HPCC on %s', vm)
  vm.Install('hpcc')


def PrepareBinaries(vms):
  """Prepare binaries on all vms."""
  master_vm = vms[0]
  if FLAGS.hpcc_binary:
    master_vm.PushFile(
        data.ResourcePath(FLAGS.hpcc_binary), './hpcc')
  else:
    master_vm.RemoteCommand('cp %s/hpcc hpcc' % hpcc.HPCC_DIR)

  for vm in vms[1:]:
    vm.Install('fortran')
    master_vm.MoveFile(vm, 'hpcc', 'hpcc')
    master_vm.MoveFile(vm, '/usr/bin/orted', 'orted')
    vm.RemoteCommand('sudo mv orted /usr/bin/orted')
    if FLAGS.hpcc_math_library == hpcc.HPCC_MATH_LIBRARY_MKL:
      master_vm.MoveFile(vm, '/lib/libiomp5.so', 'libiomp5.so')
      vm.RemoteCommand('sudo mv libiomp5.so /lib/libiomp5.so')


def Prepare(benchmark_spec):
  """Install HPCC on the target vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  master_vm = vms[0]

  PrepareHpcc(master_vm)
  CreateHpccinf(master_vm, benchmark_spec)
  hpc_util.CreateMachineFile(vms, remote_path=MACHINEFILE)
  PrepareBinaries(vms)
  master_vm.AuthenticateVm()


def UpdateMetadata(metadata):
  """Update metadata with hpcc-related flag values."""
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
    metadata['math_library_version'] = mkl.MKL_VERSION
  elif FLAGS.hpcc_math_library == hpcc.HPCC_MATH_LIBRARY_OPEN_BLAS:
    metadata['math_library_version'] = openblas.GIT_TAG
  metadata['openmpi_version'] = FLAGS.openmpi_version


def ParseOutput(hpcc_output, benchmark_spec):
  """Parses the output from HPCC.

  Args:
    hpcc_output: A string containing the text of hpccoutf.txt.
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of samples to be published (in the same format as Run() returns).
  """
  results = []
  base_metadata = {
      'num_machines': len(benchmark_spec.vms)
  }
  UpdateMetadata(base_metadata)

  # Parse all metrics from metric=value lines in the HPCC output.
  metric_values = regex_util.ExtractAllFloatMetrics(hpcc_output)

  # For each benchmark that is run, collect the metrics and metadata for that
  # benchmark from the metric_values map.
  benchmarks_run = FLAGS.hpcc_benchmarks or hpcc.HPCC_METRIC_MAP
  for benchmark in benchmarks_run:
    for metric, units in six.iteritems(hpcc.HPCC_METRIC_MAP[benchmark]):
      value = metric_values[metric]

      # Copy metadata reported in the HPCC summary statistics to the metadata.
      metadata = base_metadata.copy()
      for metadata_item in hpcc.HPCC_METADATA_MAP[benchmark]:
        metadata[metadata_item] = metric_values[metadata_item]

      results.append(sample.Sample(metric, value, units, metadata))

  return results


def Run(benchmark_spec):
  """Run HPCC on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  master_vm = vms[0]
  # backup existing HPCC output, if any
  master_vm.RemoteCommand(('if [ -f hpccoutf.txt ]; then '
                           'mv hpccoutf.txt hpccoutf-$(date +%s).txt; '
                           'fi'))
  num_processes = len(vms) * master_vm.NumCpusForBenchmark()
  mpi_env = ' '.join(['-x %s' % v for v in FLAGS.hpcc_mpi_env])
  run_as_root = '--allow-run-as-root' if FLAGS.mpirun_allow_run_as_root else ''
  mpi_cmd = ('mpirun -np %s -machinefile %s --mca orte_rsh_agent '
             '"ssh -o StrictHostKeyChecking=no" %s %s ./hpcc' %
             (num_processes, MACHINEFILE, run_as_root, mpi_env))
  master_vm.RobustRemoteCommand(
      mpi_cmd, timeout=FLAGS.hpcc_timeout_hours * SECONDS_PER_HOUR)
  logging.info('HPCC Results:')
  stdout, _ = master_vm.RemoteCommand('cat hpccoutf.txt', should_log=True)

  return ParseOutput(stdout, benchmark_spec)


def Cleanup(benchmark_spec):
  """Cleanup HPCC on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  master_vm = vms[0]
  master_vm.RemoveFile('hpcc*')
  master_vm.RemoveFile(MACHINEFILE)

  for vm in vms[1:]:
    vm.RemoveFile('hpcc')
    vm.RemoveFile('/usr/bin/orted')
