# Copyright 2014 Google Inc. All rights reserved.
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


"""Module containing iperf installation and cleanup functions."""

from perfkitbenchmarker.packages import maven

GIT_REPO = 'git://github.com/brianfrankcooper/YCSB.git'
GIT_TAG = '5659fc582c8280e1431ebcfa0891979f806c70ed'
YCSB_DIR = 'pkb/YCSB'


def _Install(vm):
  """Installs the fio package on the VM."""
  vm.Install('java7')
  vm.Install('maven')
  vm.Install('build_tools')
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, YCSB_DIR))
  vm.RemoteCommand('cd {0} && git checkout {1}'.format(YCSB_DIR, GIT_TAG))
  # TODO(user): remove this and update the commit referenced above
  #    when https://github.com/brianfrankcooper/YCSB/issues/181 is fixed.
  vm.RemoteCommand(
      'cd {0} && sed -i -e "s,<module>mapkeeper</module>,<!--&-->," '
      'pom.xml'.format(YCSB_DIR))
  vm.RemoteCommand('cd {0} && ../../{1}/bin/mvn clean package'.format(
      YCSB_DIR, maven.MVN_DIR))


def YumInstall(vm):
  """Installs the iperf package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the iperf package on the VM."""
  _Install(vm)
