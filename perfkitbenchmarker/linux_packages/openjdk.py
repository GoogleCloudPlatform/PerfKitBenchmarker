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

flags.DEFINE_string('openjdk_version', '7', 'Version of openjdk to use. '
                    'You must use this flag to specify version 8 for '
                    'ubuntu 1604 and other operating systems where '
                    'openjdk7 is not installable by default')


def YumInstall(vm):
  """Installs the OpenJDK package on the VM."""
  vm.InstallPackages('java-1.{0}.0-openjdk-devel'.format(FLAGS.openjdk_version))


def AptInstall(vm):
  """Installs the OpenJDK package on the VM."""
  vm.InstallPackages('openjdk-{0}-jdk'.format(FLAGS.openjdk_version))
