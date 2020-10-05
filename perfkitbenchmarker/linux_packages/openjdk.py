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


"""Module containing OpenJDK installation and cleanup functions."""

from absl import flags
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

JAVA_HOME = '/usr'

flags.DEFINE_string('openjdk_version', None, 'Version of openjdk to use. '
                    'By default, the version of openjdk is automatically '
                    'detected.')


def _OpenJdkPackage(vm, format_string):
  version = FLAGS.openjdk_version
  if version is None:
    # Only install Java 7 if Java 8 is not available.
    if (vm.HasPackage(format_string.format('7'))
        and not vm.HasPackage(format_string.format('8'))):
      version = '7'
    else:
      version = '8'
  return format_string.format(version)


def YumInstall(vm):
  """Installs the OpenJDK package on the VM."""
  vm.InstallPackages(_OpenJdkPackage(vm, 'java-1.{0}.0-openjdk-devel'))


@vm_util.Retry()
def _AddRepository(vm):
  """Install could fail when Ubuntu keyservers are overloaded."""
  vm.RemoteCommand(
      'sudo add-apt-repository -y ppa:openjdk-r/ppa && sudo apt-get update')


def AptInstall(vm):
  """Installs the OpenJDK package on the VM."""
  package_name = _OpenJdkPackage(vm, 'openjdk-{0}-jdk')

  if not vm.HasPackage(package_name):
    _AddRepository(vm)
  vm.InstallPackages(package_name)

  # Populate the ca-certificates-java's trustAnchors parameter.
  vm.RemoteCommand(
      'sudo /var/lib/dpkg/info/ca-certificates-java.postinst configure')
