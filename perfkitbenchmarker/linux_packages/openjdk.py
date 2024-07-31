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
from perfkitbenchmarker import os_types
from perfkitbenchmarker import vm_util

JAVA_HOME = '/usr'

OPENJDK_VERSION = flags.DEFINE_integer(
    'openjdk_version',
    None,
    'Version of openjdk to use. By default, the Default Java version is used '
    'on Debian based systems and the latest available version is used on '
    'Red Hat based systems.',
)


def YumInstall(vm):
  """Installs the OpenJDK package on the VM."""

  def OpenJdkPackage(version: str) -> str:
    # TODO(pclay): Remove support for Java 8
    if version == '8':
      version = '1.8.0'
    if (
        vm.OS_TYPE == os_types.AMAZONLINUX2023
        or vm.OS_TYPE == os_types.AMAZONLINUX2
    ):
      # Corretto is Amazon's build of the OpenJDK source.
      # https://aws.amazon.com/corretto/
      build_name = 'amazon-corretto'
    else:
      build_name = 'openjdk'
    return f'java-{version}-{build_name}-devel'

  @vm_util.Retry(max_retries=5)
  def DetectLatestJavaPackage() -> str:
    # Amazon Linux 2 is handled elsewhere.
    assert vm.PACKAGE_MANAGER == 'dnf'

    # Check for latest
    if vm.HasPackage(OpenJdkPackage('latest')):
      return OpenJdkPackage('latest')

    stdout, _ = vm.RemoteCommand(
        # dnf has a habit of crashing and we need to catch it before processing
        # the text (or process the text in python).
        'set -o pipefail && '
        f"sudo dnf list '{OpenJdkPackage('*')}' "
        "| cut -d'-' -f2 "
        '| sort -rn '
        '| head -1'
    )
    version = stdout.strip()
    assert int(version), f'Invalid Java version detected: {version}. Retrying.'
    return OpenJdkPackage(version)

  if OPENJDK_VERSION.value:
    package = OpenJdkPackage(str(OPENJDK_VERSION.value))
  elif vm.OS_TYPE == os_types.AMAZONLINUX2:
    # yum is worse than dnf at finding the latest version. Just hard code it.
    package = OpenJdkPackage('17')
  else:
    # TODO(pclay): Record Java version in metadata.
    package = DetectLatestJavaPackage()
  vm.InstallPackages(package)


def AptInstall(vm):
  """Installs the OpenJDK package on the VM."""
  if OPENJDK_VERSION.value:
    package = f'openjdk-{OPENJDK_VERSION.value}-jdk'
  else:
    # TODO(pclay): Record default-jdk version in metadata.
    package = 'default-jdk'
  vm.InstallPackages(package)
