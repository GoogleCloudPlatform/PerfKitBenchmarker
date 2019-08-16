# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing nginx installation functions."""

from perfkitbenchmarker import errors

RHEL_REPO = ('[nginx]\n'
             'name=nginx repo\n'
             'baseurl=https://nginx.org/packages/rhel/$releasever/$basearch/\n'
             'gpgcheck=0\n'
             'enabled=1')


def YumInstall(vm):
  """Installs nginx on the VM."""
  vm.RemoteCommand('echo \'%s\' | '
                   'sudo tee /etc/yum.repos.d/nginx.repo' % RHEL_REPO)
  try:
    vm.InstallPackages('nginx')
  except errors.VmUtil.SshConnectionError:
    # Amazon Linux does not have a releasever configured.
    vm.RemoteCommand('sudo sed -i -e "s/\\$releasever/6/" '
                     '/etc/yum.repos.d/nginx.repo')
    vm.InstallPackages('nginx')


def AptInstall(vm):
  """Installs nginx on the VM."""
  vm.InstallPackages('nginx')
