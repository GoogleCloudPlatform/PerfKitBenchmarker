# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing Nginx installation and cleanup functions."""

from perfkitbenchmarker import vm_util

NGINX_TAR_URL = 'nginx.org/download/nginx-1.8.0.tar.gz'

NGINX_PORT = 80


def _Install(vm):
  """Installs the Nginx on the VM."""
  vm.Install('pcre')
  vm.Install('openssl')
  vm.Install('zlib')
  vm.InstallPackages('gcc build-essential make')
  vm.RemoteCommand('cd {0} && '
                   'wget {1} && '
                   'tar zxf nginx-1.8.0.tar.gz && '
                   'cd nginx-1.8.0 && '
                   './configure && '
                   'make && '
                   'sudo make install'.format(
                       vm_util.VM_TMP_DIR, NGINX_TAR_URL))


def YumInstall(vm):
  """Installs the Nginx on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the Nginx on the VM."""
  _Install(vm)


def Start(vm, fw):
  """Starts Nginx server."""
  fw.AllowPort(vm, 80)
  vm.RemoteCommand('sudo /usr/local/nginx/sbin/nginx')


def Stop(vm):
  vm.RemoteCommand('sudo /usr/local/nginx/sbin/nginx -s stop')
