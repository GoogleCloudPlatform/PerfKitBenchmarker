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

from perfkitbenchmarker import flags

FLAGS = flags.FLAGS

JAVA_HOME = '/usr'

flags.DEFINE_string('openjdk_version', None, 'Version of openjdk to use. '
                    'By default, the version of openjdk is automatically '
                    'detected.')


def _OpenJdkPackage(vm, format_string):
  version = FLAGS.openjdk_version
  if version is None:
    if vm.HasPackage(format_string.format('7')):
      version = '7'
    else:
      version = '8'
  return format_string.format(version)


def YumInstall(vm):
  """Installs the OpenJDK package on the VM."""
  vm.InstallPackages(_OpenJdkPackage(vm, 'java-1.{0}.0-openjdk-devel'))


def AptInstall(vm):
  """Installs the OpenJDK package on the VM."""
  vm.InstallPackages(_OpenJdkPackage(vm, 'openjdk-{0}-jdk'))
