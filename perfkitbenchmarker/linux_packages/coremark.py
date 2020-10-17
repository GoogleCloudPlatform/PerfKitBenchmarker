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


"""Module containing coremark installation and cleanup functions."""

from perfkitbenchmarker import linux_packages

COREMARK_TAR_URL = 'https://github.com/eembc/coremark/archive/v1.01.tar.gz'
COREMARK_TAR = 'v1.01.tar.gz'


def InstallCoremark(remote_command):
  """Installs coremark on a VM.

  Args:
    remote_command: Function to run a remote command on the VM. This allows this
    function to be reused by the windows/cygwin version of the coremark test.
  """
  remote_command('wget %s -P %s' %
                 (COREMARK_TAR_URL, linux_packages.INSTALL_DIR))
  remote_command('cd %s && tar xvfz %s' %
                 (linux_packages.INSTALL_DIR, COREMARK_TAR))


def Install(vm):
  """Installs the coremark package on the VM."""
  vm.Install('build_tools')
  vm.Install('wget')
  InstallCoremark(vm.RemoteCommand)
