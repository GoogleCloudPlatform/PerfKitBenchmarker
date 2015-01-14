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


"""Module containing maven installation and cleanup functions."""

MVN_TAR = 'apache-maven-3.2.5-bin.tar.gz'
MVN_URL = ('http://download.nextag.com/apache/maven/maven-3/3.2.5/binaries/' +
           MVN_TAR)
MVN_DIR = 'pkb/apache-maven-3.2.5'


def _Install(vm):
  """Installs the maven package on the VM."""
  vm.Install('openjdk7')
  vm.RemoteCommand('wget %s -P pkb' % MVN_URL)
  vm.RemoteCommand('cd pkb && tar xvzf %s' % MVN_TAR)


def YumInstall(vm):
  """Installs the maven package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the maven package on the VM."""
  _Install(vm)
