# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Package for installing the AWS CLI."""


def Install(vm):
  """Installs the awscli package on the VM."""
  vm.Install('pip')
  vm.RemoteCommand('sudo pip install awscli')


def Uninstall(vm):
  vm.RemoteCommand('/usr/bin/yes | sudo pip uninstall awscli')
