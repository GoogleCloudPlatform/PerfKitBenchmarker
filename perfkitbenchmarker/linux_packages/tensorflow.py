# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing TensorFlow installation and cleanup functions."""


def Install(vm):
  """Installs TensorFlow on the VM."""
  vm.Install('pip')
  vm.RemoteCommand('sudo pip install --upgrade tensorflow-gpu', should_log=True)


def Uninstall(vm):
  """Uninstalls TensorFlow on the VM."""
  vm.RemoteCommand('sudo pip uninstall --upgrade tensorflow', should_log=True)
