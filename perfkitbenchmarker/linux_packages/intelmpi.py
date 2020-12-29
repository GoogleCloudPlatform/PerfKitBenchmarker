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

from absl import flags
from perfkitbenchmarker.linux_packages import intel_repo

MPI_VARS = '/opt/intel/compilers_and_libraries/linux/mpi/intel64/bin/mpivars.sh'

MPI_VERSION = flags.DEFINE_string('intelmpi_version', '2019.6-088',
                                  'MPI version.')
FLAGS = flags.FLAGS


def _Install(vm, mpi_version: str) -> None:
  """Installs Intel MPI."""
  vm.InstallPackages(f'intel-mpi-{mpi_version}')


def AptInstall(vm) -> None:
  """Installs the MPI library."""
  intel_repo.AptPrepare(vm)
  _Install(vm, MPI_VERSION.value)
  # Ubuntu's POSIX dash shell does not have bash's "==" comparator
  vm.RemoteCommand(f'sudo sed -i "s/==/=/" {MPI_VARS}')


def YumInstall(vm) -> None:
  """Installs the MPI library."""
  intel_repo.YumPrepare(vm)
  _Install(vm, MPI_VERSION.value)
