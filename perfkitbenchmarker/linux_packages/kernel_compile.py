# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

import os

from perfkitbenchmarker import linux_packages

LINUX_VERSION = '4.4.35'
URL = f'https://www.kernel.org/pub/linux/kernel/v4.x/linux-{LINUX_VERSION}.tar.gz'
TARBALL = f'linux-{LINUX_VERSION}.tar.gz'
UNTAR_DIR = f'linux-{LINUX_VERSION}'
KERNEL_TARBALL = os.path.join(linux_packages.INSTALL_DIR, TARBALL)


def _Install(vm):
  vm.Install('build_tools')
  vm.Install('wget')
  vm.InstallPackages('bc')
  vm.RemoteCommand(
      'mkdir -p {0} && cd {0} && wget {1}'.format(
          linux_packages.INSTALL_DIR, URL
      )
  )


def AptInstall(vm):
  _Install(vm)


def YumInstall(vm):
  _Install(vm)


def Cleanup(vm):
  vm.RemoteCommand(
      'cd {} && rm -f {}'.format(linux_packages.INSTALL_DIR, TARBALL)
  )
