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
"""Package for installing the DaCapo benchmarks."""

from perfkitbenchmarker import linux_packages

_DACAPO_URL = ('https://sourceforge.net/projects/dacapobench/files/'
               '9.12-bach-MR1/dacapo-9.12-MR1-bach.jar')


def Install(vm):
  """Installs the `dacapo` package on the VM."""
  vm.Install('openjdk')
  vm.RemoteCommand('wget %s -P %s' % (_DACAPO_URL, linux_packages.INSTALL_DIR))
