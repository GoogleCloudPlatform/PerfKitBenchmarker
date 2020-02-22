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


"""Module containing iperf installation and cleanup functions."""

import posixpath

from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import INSTALL_DIR

PACKAGE_NAME = 'iperf'
IPERF_TAR = 'iperf-2.0.13.tar.gz'
IPERF_URL = 'https://sourceforge.net/projects/iperf2/files/iperf-2.0.13.tar.gz'
IPERF_DIR = '%s/iperf-2.0.13' % INSTALL_DIR


def _Install(vm):
  """Installs the iperf package on the VM."""
  vm.Install('build_tools')
  vm.Install('wget')

  vm.RemoteCommand('wget -O %s/%s %s' %
                   (INSTALL_DIR, IPERF_TAR, IPERF_URL))

  vm.RemoteCommand('cd %s; tar xvf %s; cd %s; '
                   './configure; make; sudo make install' %
                   (INSTALL_DIR, IPERF_TAR, IPERF_DIR))


def YumInstall(vm):
  """Installs the iperf package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the iperf package on the VM."""
  _Install(vm)
