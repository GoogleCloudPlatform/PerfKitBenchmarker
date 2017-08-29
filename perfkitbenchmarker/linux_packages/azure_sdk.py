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

"""Package for installing the Azure SDK."""

from perfkitbenchmarker import flags

flags.DEFINE_string('azure_lib_version', '1.0.3',
                    'Use a particular version of azure client lib, e.g.: 1.0.2')
FLAGS = flags.FLAGS


def Install(vm):
  """Installs the azure-cli package on the VM."""
  vm.InstallPackages('python-dev')

  if FLAGS.azure_lib_version:
    version_string = '==' + FLAGS.azure_lib_version
  else:
    version_string = ''
  vm.RemoteCommand('sudo pip install azure%s' % version_string)
