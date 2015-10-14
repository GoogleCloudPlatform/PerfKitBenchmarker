# Copyright 2015 Google Inc. All rights reserved.
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

"""Module containing Nginx installation and cleanup functions.

https://www.nginx.com/
"""


def _Install(vm):
  vm.InstallPackages('nginx')
  # Nginx defaults to 4 worker processes.
  # Change the configuration to use use 1 per CPU
  # This can also be specified with "auto", but not on the version that ships
  # with CentOS 6.
  vm.RemoteCommand(("sudo sed -i.bak -e "
                    "'s/worker_processes.*$/worker_processes {0};/' "
                    "/etc/nginx/nginx.conf").format(vm.num_cpus))


def YumInstall(vm):
  """Installs the Nginx package on the VM."""
  vm.InstallEpelRepo()
  _Install(vm)


def AptInstall(vm):
  """Installs the Nginx package on the VM."""
  _Install(vm)


def Start(vm):
  """Starts Nginx on "vm"."""
  # CentOS7 uses systemd as an init system
  vm.RemoteCommand('hash systemctl 2>/dev/null && '
                   'sudo systemctl start nginx || '
                   'sudo /etc/init.d/nginx start')


def Stop(vm):
  """Stops Nginx on "vm"."""
  vm.RemoteCommand('hash systemctl 2>/dev/null && '
                   'sudo systemctl stop nginx || '
                   'sudo /etc/init.d/nginx stop')
