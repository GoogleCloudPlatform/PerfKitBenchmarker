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


"""Module containing gcs-oauth2-boto-plugin functions."""


def _Install(vm):
  """Installs the iperf package on the VM."""
  vm.Install('pip')
  vm.RemoteCommand('sudo pip install gcs-oauth2-boto-plugin==1.8')


def YumInstall(vm):
  """Installs the iperf package on the VM."""
  vm.InstallPackages('gcc openssl-devel python-devel libffi-devel')
  _Install(vm)


def AptInstall(vm):
  """Installs the iperf package on the VM."""
  vm.InstallPackages('gcc python-dev libffi-dev')
  _Install(vm)
