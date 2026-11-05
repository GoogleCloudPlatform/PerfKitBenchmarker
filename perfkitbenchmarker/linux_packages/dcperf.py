# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing the Meta DCPerf (benchpress) harness.

https://github.com/facebookresearch/DCPerf
"""

import posixpath

from absl import flags
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import virtual_machine

DCPERF_DIR = posixpath.join(linux_packages.INSTALL_DIR, 'DCPerf')
DCPERF_GIT_REPO = 'https://github.com/facebookresearch/DCPerf.git'

DCPERF_VERSION = flags.DEFINE_string(
    'dcperf_version', 'v1.0', 'Git branch, tag, or commit of DCPerf.'
)


def AptInstall(vm: virtual_machine.BaseVirtualMachine) -> None:
  vm.InstallPackages(
      'git lshw dmidecode netcat-traditional numactl python3-click '
      'python3-numpy python3-pandas python3-tabulate python3-yaml'
  )
  vm.RemoteCommand(
      f'git clone {DCPERF_GIT_REPO} {DCPERF_DIR} && '
      f'cd {DCPERF_DIR} && git checkout {DCPERF_VERSION.value}'
  )
  vm.RemoteCommand(
      "printf 'root soft nofile 1048576\\nroot hard nofile 1048576\\n' | "
      'sudo tee /etc/security/limits.d/dcperf.conf'
  )
