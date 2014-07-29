#!/usr/bin/env python
# Copyright 2014 Google Inc. All rights reserved.
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

import math
import re
import tempfile

import gflags as flags
import logging

from perfkitbenchmarker import regex_util

FLAGS = flags.FLAGS
HPCC_TAR = 'hpcc-1.4.3.tar.gz'
HPCC_URL = 'http://icl.cs.utk.edu/projectsfiles/hpcc/download/' + HPCC_TAR
HPCC_DIR = ' hpcc-1.4.3'
MAKE_FLAVOR = 'Linux_PII_CBLAS'
HPCC_MAKEFILE = 'Make.' + MAKE_FLAVOR
HPCC_MAKEFILE_PATH = HPCC_DIR + '/hpl/' + HPCC_MAKEFILE
MPI_TAR = 'openmpi-1.6.5.tar.gz'
MPI_URL = 'http://www.open-mpi.org/software/ompi/v1.6/downloads/' + MPI_TAR
MPI_DIR = 'openmpi-1.6.5'
OPENBLAS_URL = 'git://github.com/xianyi/OpenBLAS'
OPENBLAS_COMMIT = 'v0.2.11'
OPENBLAS_DIR = 'OpenBLAS'
REQUIRED_PACKAGES = 'build-essential git gfortran'
HPCCINF_DIRECTORY = 'data/'
HPCCINF_FILE = 'hpccinf.txt'
MACHINEFILE = 'machinefile'
BLOCK_SIZE = 192
STREAM_METRICS = ['Copy', 'Scale', 'Add', 'Triad']

BENCHMARK_INFO = {'name': 'hpcc',
                  'description': 'Runs HPCC.',
                  'scratch_disk': False}

flags.DEFINE_integer('memory_size_mb',
                     None,
                     'The amount of memory in MB on each machine to use. By '
                     'default it will use the entire system\'s memory.')


def GetInfo():
  BENCHMARK_INFO['num_machines'] = FLAGS.num_vms
  return BENCHMARK_INFO


def CreateMachineFile(vms):
  """Create a file with the IP of each machine in the cluster on its own line.

  Args:
    vms: The list of vms which will be in the cluster.
  """
  with tempfile.NamedTemporaryFile() as machine_file:
    master_vm = vms[0]
    machine_file.write('localhost slots=%d\n' % master_vm.num_cpus)
    for vm in vms[1:]:
      machine_file.write('%s slots=%d\n' % (vm.internal_ip,
                                            vm.num_cpus))
    machine_file.flush()
    master_vm.PushFile(machine_file.name, MACHINEFILE)


def CreateHpccinf(vm, benchmark_spec):
  """Creates the HPCC input file."""
  num_vms = benchmark_spec.num_vms
  if FLAGS.memory_size_mb:
    total_memory = FLAGS.memory_size_mb * 1024 * 1024 * num_vms
  else:
    total_memory = vm.total_memory_kb * 1024 * num_vms
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

  file_path = HPCCINF_DIRECTORY + HPCCINF_FILE
  vm.PushFile(file_path, HPCCINF_FILE)
  sed_cmd = (('sed -i -e "s/problem_size/%s/" -e "s/block_size/%s/" '
              '-e "s/rows/%s/" -e "s/columns/%s/" %s') %
             (problem_size, block_size, num_rows, num_columns, HPCCINF_FILE))
  vm.RemoteCommand(sed_cmd)


def PrepareHpcc(vm):
  """Builds HPCC on a single vm."""
  logging.info('Building HPCC on %s', vm)
  vm.InstallPackage(REQUIRED_PACKAGES)

  vm.RemoteCommand('wget %s' % MPI_URL)
  vm.RemoteCommand('tar xvfz %s' % MPI_TAR)
  make_jobs = vm.num_cpus
  config_cmd = ('./configure --enable-static --disable-shared --disable-dlopen '
                '--prefix=/usr')
  vm.RemoteCommand('cd %s; %s; make -j %s; sudo make install' %
                   (MPI_DIR, config_cmd, make_jobs))
  vm.RemoteCommand('git clone %s' % OPENBLAS_URL)
  vm.RemoteCommand('cd %s; git checkout -q %s' % (OPENBLAS_DIR,
                                                  OPENBLAS_COMMIT))
  vm.RemoteCommand('cd %s; make' % OPENBLAS_DIR)

  vm.RemoteCommand('wget %s' % HPCC_URL)
  vm.RemoteCommand('tar xvfz %s' % HPCC_TAR)
  vm.RemoteCommand(
      'cp %s/hpl/setup/%s %s' % (HPCC_DIR, HPCC_MAKEFILE, HPCC_MAKEFILE_PATH))
  sed_cmd = ('sed -i -e "/^MP/d" -e "s/gcc/mpicc/" -e "s/g77/mpicc/" '
             '-e "s/netlib\\/ARCHIVES\\/Linux_PII/OpenBLAS/" '
             '-e "s/libcblas.*/libopenblas.a/" '
             '-e "s/\\-lm/\\-lgfortran \\-lm/" %s') % HPCC_MAKEFILE_PATH
  vm.RemoteCommand(sed_cmd)
  vm.RemoteCommand('cd %s; make arch=Linux_PII_CBLAS' % HPCC_DIR)


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
  CreateMachineFile(vms)

  for vm in vms:
    vm.InstallPackage('gfortran')
    master_vm.MoveFile(vm, '%s/hpcc' % HPCC_DIR, 'hpcc')
    master_vm.MoveFile(vm, '/usr/bin/orted', 'orted')
    vm.RemoteCommand('sudo mv orted /usr/bin/orted')


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
  metadata['num_machines'] = benchmark_spec.num_vms
  metadata['memory_size_mb'] = FLAGS.memory_size_mb
  value = regex_util.ExtractFloat('HPL_Tflops=([0-9]*\\.[0-9]*)', hpcc_output)
  results.append(('HPL Throughput', value, 'Tflops', metadata))

  value = regex_util.ExtractFloat('SingleRandomAccess_GUPs=([0-9]*\\.[0-9]*)',
                                  hpcc_output)
  results.append(('Random Access Throughput', value, 'GigaUpdates/sec'))

  for metric in STREAM_METRICS:
    regex = 'SingleSTREAM_%s=([0-9]*\\.[0-9]*)' % metric
    value = regex_util.ExtractFloat(regex, hpcc_output)
    results.append(('STREAM %s Throughput' % metric, value, 'GB/s'))

  return results


def Run(benchmark_spec):
  """Run HPCC on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  vms = benchmark_spec.vms
  master_vm = vms[0]
  num_processes = len(vms) * master_vm.num_cpus

  mpi_cmd = ('mpirun -np %s -machinefile %s --mca orte_rsh_agent '
             '"ssh -o StrictHostKeyChecking=no" ./hpcc' %
             (num_processes, MACHINEFILE))
  master_vm.LongRunningRemoteCommand(mpi_cmd)
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
  master_vm.RemoteCommand('cd %s; sudo make uninstall' % MPI_DIR)
  master_vm.RemoveFile(MPI_DIR + '*')
  master_vm.RemoveFile(OPENBLAS_DIR + '*')
  master_vm.RemoveFile('hpcc*')
  master_vm.RemoveFile(MACHINEFILE)

  for vm in vms[1:]:
    vm.RemoveFile('hpcc')
    vm.RemoveFile('/usr/bin/orted')
