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

import logging
import math
import re

from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import hpc_util
from perfkitbenchmarker.linux_packages import hpcc

FLAGS = flags.FLAGS
HPCCINF_FILE = 'hpccinf.txt'
MACHINEFILE = 'machinefile'
BLOCK_SIZE = 192
STREAM_METRICS = ['Copy', 'Scale', 'Add', 'Triad']

BENCHMARK_NAME = 'hpcc'
BENCHMARK_CONFIG = """
hpcc:
  description: Runs HPCC. Specify the number of VMs with --num_vms
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: null
"""

flags.DEFINE_integer('memory_size_mb',
                     None,
                     'The amount of memory in MB on each machine to use. By '
                     'default it will use the entire system\'s memory.')
flags.DEFINE_string('hpcc_binary', None,
                    'The path of prebuilt hpcc binary to use. If not provided, '
                    'this benchmark built its own using OpenBLAS.')
flags.DEFINE_list('hpcc_mpi_env', [],
                  'Comma seperated list containing environment variables '
                  'to use with mpirun command. e.g. '
                  'MKL_DEBUG_CPU_TYPE=7,MKL_ENABLE_INSTRUCTIONS=AVX512')



def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
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
  total_cpus = vm.num_cpus * num_vms
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
  for i in reversed(range(sqrt_cpus)):
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
  metadata = dict()
  match = re.search('HPLMaxProcs=([0-9]*)', hpcc_output)
  metadata['num_cpus'] = match.group(1)
  metadata['num_machines'] = len(benchmark_spec.vms)
  UpdateMetadata(metadata)

  # Parse all metrics from metric=value lines in the HPCC output.
  metric_values = regex_util.ExtractAllFloatMetrics(
      hpcc_output)
  for metric, value in metric_values.iteritems():
    results.append(sample.Sample(metric, value, '', metadata))

  # Parse some metrics separately and add units. Although these metrics are
  # parsed above and added to results, this handling is left so that existing
  # uses of these metric names will continue to work.
  value = regex_util.ExtractFloat('HPL_Tflops=([0-9]*\\.[0-9]*)', hpcc_output)
  results.append(sample.Sample('HPL Throughput', value, 'Tflops', metadata))

  value = regex_util.ExtractFloat('SingleRandomAccess_GUPs=([0-9]*\\.[0-9]*)',
                                  hpcc_output)
  results.append(
      sample.Sample('Random Access Throughput', value, 'GigaUpdates/sec',
                    metadata))

  for metric in STREAM_METRICS:
    regex = 'SingleSTREAM_%s=([0-9]*\\.[0-9]*)' % metric
    value = regex_util.ExtractFloat(regex, hpcc_output)
    results.append(
        sample.Sample('STREAM %s Throughput' % metric, value, 'GB/s', metadata))

  value = regex_util.ExtractFloat(r'PTRANS_GBs=([0-9]*\.[0-9]*)', hpcc_output)
  results.append(sample.Sample('PTRANS Throughput', value, 'GB/s', metadata))
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
  num_processes = len(vms) * master_vm.num_cpus
  mpi_env = ' '.join(['-x %s' % v for v in FLAGS.hpcc_mpi_env])
  mpi_cmd = ('mpirun -np %s -machinefile %s --mca orte_rsh_agent '
             '"ssh -o StrictHostKeyChecking=no" %s ./hpcc' %
             (num_processes, MACHINEFILE, mpi_env))
  master_vm.RobustRemoteCommand(mpi_cmd)
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
