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


"""Module containing OpenMPI installation and cleanup functions."""

from __future__ import print_function

import posixpath
import re

from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import INSTALL_DIR

FLAGS = flags.FLAGS

flags.DEFINE_string('openmpi_version', '3.1.2',
                    'OpenMPI version to install, such as 3.1.2 and 4.0.2.'
                    'Set to empty to ignore the intallation of OpenMPI.')
flags.DEFINE_bool('openmpi_enable_shared', False,
                  'Whether openmpi should build shared libraries '
                  'in addition to static ones.')
flags.DEFINE_bool('openmpi_with_cuda_support', False,
                  'Compile with CUDA support')
flags.DEFINE_string('openmpi_configs', None,
                    'command line options to be provided to ./configure for'
                    'OpenMPI compilation')

MPI_URL_BASE = 'https://download.open-mpi.org/release/open-mpi'
REMOVE_MPI_CMD = 'autoremove -y libopenmpi-dev openmpi-bin openmpi-common'


class MpirunParseOutputError(Exception):
  pass


def GetMpiVersion(vm):
  """Get the MPI version on the vm, based on mpirun.

  Args:
    vm: the virtual machine to query

  Returns:
    A string containing the active MPI version,
    None if mpirun could not be found
  """
  stdout, _ = vm.RemoteCommand('mpirun --version',
                               ignore_failure=True,
                               suppress_warning=True)
  if bool(stdout.rstrip()):
    regex = r'MPI\) (\S+)'
    match = re.search(regex, stdout)
    try:
      return str(match.group(1))
    except:
      raise MpirunParseOutputError('Unable to parse mpirun version output')
  else:
    return None


def _Install(vm):
  """Installs the OpenMPI package on the VM."""
  version_to_install = FLAGS.openmpi_version
  if not version_to_install:
    return
  current_version = GetMpiVersion(vm)
  if current_version == version_to_install:
    return

  first_dot_pos = version_to_install.find('.')
  second_dot_pos = version_to_install.find('.', first_dot_pos + 1)
  major_version = version_to_install[0:second_dot_pos]
  mpi_tar = ('openmpi-{version}.tar.gz'.format(version=version_to_install))
  mpi_url = ('{mpi_url_base}/v{major_version}/{mpi_tar}'.format(
      mpi_url_base=MPI_URL_BASE, major_version=major_version, mpi_tar=mpi_tar))
  install_dir = posixpath.join(INSTALL_DIR, 'openmpi-{version}'
                               .format(version=version_to_install))

  vm.Install('build_tools')
  vm.Install('wget')
  vm.RemoteCommand('wget %s -P %s' % (mpi_url, install_dir))
  vm.RemoteCommand('cd %s && tar xvfz %s' % (install_dir, mpi_tar))
  make_jobs = vm.NumCpusForBenchmark()

  config_options = []
  config_options.append('--enable-static')
  config_options.append('--prefix=/usr')
  config_options.append('--enable-shared' if FLAGS.openmpi_enable_shared
                        else '--disable-shared')
  if FLAGS.openmpi_with_cuda_support:
    config_options.append('--with-cuda=/usr/local/cuda-{version}/'
                          .format(version=FLAGS.cuda_toolkit_version))
    config_options.append('--with-cuda-libdir=/usr/local/cuda-{version}/lib64/'
                          .format(version=FLAGS.cuda_toolkit_version))
  if FLAGS.openmpi_configs:
    config_options.append(FLAGS.openmpi_configs)

  config_cmd = './configure {}'.format(' '.join(config_options))
  vm.RobustRemoteCommand(
      'cd %s/openmpi-%s && %s && make -j %s && sudo make install' %
      (install_dir, version_to_install, config_cmd, make_jobs))


def GetMpiDir():
  """Returns the installation dirtory of OpenMPI."""
  mpi_dir = posixpath.join(INSTALL_DIR, 'openmpi-{version}'
                           .format(version=FLAGS.openmpi_version))
  return mpi_dir


def YumInstall(vm):
  """Installs the OpenMPI package on the VM."""
  if not FLAGS.openmpi_version:
    return
  vm.RobustRemoteCommand('sudo yum {}'.format(REMOVE_MPI_CMD))
  _Install(vm)


def AptInstall(vm):
  """Installs the OpenMPI package on the VM."""
  if not FLAGS.openmpi_version:
    return
  vm.RobustRemoteCommand('sudo apt-get {}'.format(REMOVE_MPI_CMD))
  _Install(vm)


def _Uninstall(vm):
  """Uninstalls the OpenMPI package on the VM."""
  vm.RemoteCommand('cd {0} && sudo make uninstall'.format(GetMpiDir()))


def YumUninstall(vm):
  """Uninstalls the OpenMPI package on the VM."""
  _Uninstall(vm)


def AptUninstall(vm):
  """Uninstalls the OpenMPI package on the VM."""
  _Uninstall(vm)
