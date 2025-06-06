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

"""Module containing OpenBLAS installation and cleanup functions."""

import logging
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_packages

OPENBLAS_DIR = '%s/OpenBLAS' % linux_packages.INSTALL_DIR
GIT_REPO = 'https://github.com/xianyi/OpenBLAS'
GIT_TAG = 'v0.3.21'
LATEST_GIT_TAG = 'v0.3.29'
GIT_TAG_FILE = 'git_tag'


def _Install(vm):
  """Installs the OpenBLAS package on the VM."""
  vm.Install('build_tools')
  vm.Install('fortran')
  vm.RemoteCommand('git clone {} {}'.format(GIT_REPO, OPENBLAS_DIR))
  vm.RemoteCommand('cd {} && git checkout {}'.format(OPENBLAS_DIR, GIT_TAG))
  # TODO(user): By specifying TARGET= for the first compilation attempt we may
  # achieve better performance. A list of targets is available at
  # https://github.com/xianyi/OpenBLAS/blob/develop/TargetList.txt
  try:
    vm.RemoteCommand('cd {} && make USE_THREAD=0'.format(OPENBLAS_DIR))
  except errors.VirtualMachine.RemoteCommandError:
    try:
      logging.info(
          'Attempting to recompile OpenBLAS with TARGET=SAPPHIRERAPIDS'
      )
      vm.RemoteCommand(
          'cd {} && make TARGET=SAPPHIRERAPIDS USE_THREAD=0'.format(
              OPENBLAS_DIR
          )
      )
    except errors.VirtualMachine.RemoteCommandError:
      logging.info('Attempting to recompile OpenBLAS with %s', LATEST_GIT_TAG)
      vm.RemoteCommand(
          'cd {} && git checkout {}'.format(OPENBLAS_DIR, LATEST_GIT_TAG)
      )
      vm.RemoteCommand(
          'cd {} && make clean && make USE_THREAD=0'.format(OPENBLAS_DIR)
      )
  finally:
    # write the git tag to a file
    vm.RemoteCommand(
        'cd {} && git describe --exact-match --tags > {}'.format(
            OPENBLAS_DIR, GIT_TAG_FILE
        )
    )


def YumInstall(vm):
  """Installs the OpenBLAS package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the OpenBLAS package on the VM."""
  _Install(vm)


def GetVersion(vm):
  """Returns the version of the OpenBLAS package on the VM by reading the git_tag file."""
  return vm.RemoteCommand('cd {} && cat {}'.format(
      OPENBLAS_DIR, GIT_TAG_FILE))[0].strip()
