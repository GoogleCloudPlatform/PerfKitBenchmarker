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


"""Module containing OpenJDK7 installation and cleanup functions."""
import logging

from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util


JAVA_HOME = '/usr'


def YumInstall(vm):
  """Installs the OpenJDK7 package on the VM."""
  vm.InstallPackages('java-1.7.0-openjdk-devel')

@vm_util.Retry(max_retries=1)
def AptInstall(vm):
  """Installs the OpenJDK7 package on the VM."""
  try:
      vm.InstallPackages('openjdk-7-jdk')
  except errors.VirtualMachine.RemoteCommandError as e:
      # On Ubuntu 16.04, we have to add the repo that contains
      # openjdk7 before we can install it.
      logging.warning('failed to install openjdk-7-jdk.')
      UpdateAptRepo(vm)
      raise e

def UpdateAptRepo(vm):
    logging.info('Adding openjdk repo.')
    vm.RemoteCommand('sudo add-apt-repository -y ppa:openjdk-r/ppa')
    vm.RemoteCommand('sudo apt-get update')

