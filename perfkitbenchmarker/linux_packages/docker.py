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

from absl import flags
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import virtual_machine

VERSION = flags.DEFINE_string('docker_version', None,
                              'Version of docker to install.')

DOCKER_RPM_URL = ('https://get.docker.com/rpm/1.7.0/centos-6/'
                  'RPMS/x86_64/docker-engine-1.7.0-1.el6.x86_64.rpm')


# Docker images that VMs are allowed to install.
_IMAGES = [
    'cloudsuite/data-caching:client',
    'cloudsuite/data-caching:server',
    'cloudsuite/data-serving:client',
    'cloudsuite/data-serving:server',
    'cloudsuite/graph-analytics',
    'cloudsuite/in-memory-analytics',
    'cloudsuite/media-streaming:client',
    'cloudsuite/media-streaming:dataset',
    'cloudsuite/media-streaming:server',
    'cloudsuite/movielens-dataset',
    'cloudsuite/spark',
    'cloudsuite/twitter-dataset-graph',
    'cloudsuite/web-search:client',
    'cloudsuite/web-search:server',
    'cloudsuite/web-serving:db_server',
    'cloudsuite/web-serving:faban_client',
    'cloudsuite/web-serving:memcached_server',
    'cloudsuite/web-serving:web_server',
]


class _DockerImagePackage(object):
  """Facsimile of a perfkitbenchmarker.linux_packages.<name> package."""

  def __init__(self, name):
    """Creates a vm-installable package from a docker image."""
    self.name = name
    self.__name__ = name

  def Install(self, vm):
    """Installs the docker image for self.name on the VM."""
    vm.Install('docker')
    vm.RemoteCommand('sudo docker pull {}'.format(self.name))

  def Uninstall(self, vm):
    """Removes the docker image for self.name from the VM."""
    vm.RemoteCommand('sudo docker rmi {}'.format(self.name))


def CreateImagePackages():
  """Creates _DockerImagePackage objects."""
  return [(name, _DockerImagePackage(name)) for name in _IMAGES]


def YumInstall(vm):
  """Installs the docker package on the VM."""
  vm.RemoteHostCommand('curl -o %s/docker.rpm -sSL %s' %
                       (linux_packages.INSTALL_DIR, DOCKER_RPM_URL))
  vm.RemoteHostCommand('sudo yum localinstall '
                       '--nogpgcheck %s/docker.rpm -y' %
                       linux_packages.INSTALL_DIR)
  vm.RemoteHostCommand('sudo service docker start')


def AptInstall(vm):
  """Installs the docker package on the VM."""
  vm.RemoteHostCommand('curl -sSL https://get.docker.com/ | '
                       f'VERSION={VERSION.value or ""} sh')


def IsInstalled(vm):
  """Checks whether docker is installed on the VM."""
  resp, _ = vm.RemoteCommand('command -v docker',
                             ignore_failure=True,
                             suppress_warning=True)
  return bool(resp.rstrip())


def AddUser(vm: virtual_machine.BaseVirtualMachine) -> None:
  """Run Docker as a non-root user.

  https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user

  Args:
    vm: The VM to work on
  """
  # Create the docker group.
  vm.RemoteCommand('sudo groupadd docker', ignore_failure=True)
  # Add your user to the docker group.
  vm.RemoteCommand(f'sudo usermod -aG docker {vm.user_name}')
  # Log out and log back in so that your group membership is re-evaluated.
  vm.RemoteCommand(f'pkill -KILL -u {vm.user_name}', ignore_failure=True)
