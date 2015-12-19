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


"""Module containing docker installation and cleanup functions.

This is probably the only package that should use RemoteHostCommand instead
of RemoteCommand, since Docker has to be installed directly on the remote VM
and not within a container running on that VM.
"""

from perfkitbenchmarker import vm_util

DOCKER_RPM_URL = ('https://get.docker.com/rpm/1.7.0/centos-6/'
                  'RPMS/x86_64/docker-engine-1.7.0-1.el6.x86_64.rpm')


def YumInstall(vm):
  """Installs the docker package on the VM."""
  vm.RemoteHostCommand('curl -o %s/docker.rpm -sSL %s'
                       % (vm_util.VM_TMP_DIR, DOCKER_RPM_URL))
  vm.RemoteHostCommand('sudo yum localinstall '
                       '--nogpgcheck %s/docker.rpm -y' % vm_util.VM_TMP_DIR)
  vm.RemoteHostCommand('sudo service docker start')


def AptInstall(vm):
  """Installs the docker package on the VM."""
  vm.RemoteHostCommand('curl -sSL https://get.docker.com/ | sh')
