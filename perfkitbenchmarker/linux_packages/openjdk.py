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

from typing import Callable

from absl import flags
from perfkitbenchmarker import errors

JAVA_HOME = '/usr'

OPENJDK_VERSION = flags.DEFINE_integer(
    'openjdk_version', None,
    'Version of openjdk to use. By default, the oldest non-end-of-life LTS '
    'version of openjdk is automatically detected.')


# Earlier elements of list are preferred.
# These are the 3 LTS versions of Java.
# The default 11 is a compromise between the older most popular, but maintenance
# mode Java 8 and the newest Java 17.
KNOWN_JAVA_VERSIONS = [11, 17, 8]


def _Install(vm, get_package_name_for_version: Callable[[int], str]):
  """Installs the OpenJDK package on the VM."""
  def DetectJava():
    for version in KNOWN_JAVA_VERSIONS:
      if vm.HasPackage(get_package_name_for_version(version)):
        return version

  version = OPENJDK_VERSION.value or DetectJava()
  if not version:
    raise errors.VirtualMachine.VirtualMachineError(
        f'No OpenJDK candidate found for {vm.name}.')
  vm.InstallPackages(get_package_name_for_version(version))


def YumInstall(vm):
  """Installs the OpenJDK package on the VM."""
  def OpenJdkPackage(version: int) -> str:
    return f"java-{version > 8 and version or f'1.{version}.0'}-openjdk-devel"
  _Install(vm, OpenJdkPackage)


def AptInstall(vm):
  """Installs the OpenJDK package on the VM."""
  _Install(vm, lambda version: f'openjdk-{version}-jdk')
