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
"""Installs the Intel MPI library."""

import logging
from absl import flags
from perfkitbenchmarker.linux_packages import intel_repo

MPI_VERSION = flags.DEFINE_string('intelmpi_version', '2019.6-088',
                                  'MPI version.')
FLAGS = flags.FLAGS


def MpiVars(vm) -> str:
  """Returns the path to the mpivars.sh file.

  With different versions of Intel software installed the mpivars.sh for
  2019.6 can be under compilers_and_libraries_2020.0.166 while the symlink
  for compilers_and_libraries points to compilers_and_libraries_2018

  Args:
    vm: Virtual machine to look for mpivars.sh on.
  """
  txt, _ = vm.RemoteCommand('readlink -f /opt/intel/compilers_and_libraries*/'
                            'linux/mpi/intel64/bin/mpivars.sh | sort | uniq')
  files = txt.splitlines()
  if not files:
    raise ValueError('Could not find the mpivars.sh file')
  if len(files) > 1:
    logging.info('More than 1 mpivars.sh found, returning first: %s', files)
  return files[0]


def SourceMpiVarsCommand(vm):
  """Returns the command to source the mpivars.sh script."""
  cmd = f'. {MpiVars(vm)}'
  if FLAGS.aws_efa:  # Disable IntelMPI's libfabric to use AWS EFA's
    cmd += ' -ofi_internal=0'
  return cmd


def FixEnvironment(vm):
  """Changes system settings for optimal Intel MPI conditions.


  Sets the ptrace_scope to 0, for details see:
    https://www.kernel.org/doc/Documentation/security/Yama.txt

  Args:
    vm: The virtual machine to run on.
  """
  if not vm.TryRemoteCommand('ulimit -l | grep unlimited'):
    ulimit_fix_cmd = (f'echo "{vm.user_name} - memlock unlimited" | '
                      'sudo tee -a /etc/security/limits.conf')
    vm.RemoteCommand(ulimit_fix_cmd)
    logging.info('Rebooting to permamently set ulimit')
    vm.Reboot()
    vm.WaitForBootCompletion()
  vm.RemoteCommand('sudo sysctl -w kernel.yama.ptrace_scope=0')


def _Install(vm, mpi_version: str) -> None:
  """Installs Intel MPI."""
  vm.InstallPackages(f'intel-mpi-{mpi_version}')
  FixEnvironment(vm)
  # Log the version of MPI and other associated values for debugging
  vm.RemoteCommand(f'. {MpiVars(vm)}; mpirun -V')


def AptInstall(vm) -> None:
  """Installs the MPI library."""
  intel_repo.AptPrepare(vm)
  _Install(vm, MPI_VERSION.value)
  # Ubuntu's POSIX dash shell does not have bash's "==" comparator
  vm.RemoteCommand(f'sudo sed -i "s/==/=/" {MpiVars(vm)}')


def YumInstall(vm) -> None:
  """Installs the MPI library."""
  intel_repo.YumPrepare(vm)
  _Install(vm, MPI_VERSION.value)
