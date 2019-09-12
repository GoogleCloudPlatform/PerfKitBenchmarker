# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing google.cloud go package installation and cleanup.
"""


def Install(vm):
  """Install google.cloud go package on the VM."""
  vm.RemoteCommand('/usr/local/go/bin/go get -u cloud.google.com/go/...')


def Uninstall(_):
  """Uninstalls google.cloud go package on the VM."""
  # No clean way to uninstall everything. The VM will be deleted at the end
  # of the test.
  pass
