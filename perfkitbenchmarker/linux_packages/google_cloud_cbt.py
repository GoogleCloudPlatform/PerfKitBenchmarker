# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing gcloud SDK cbt CLI install function.

See https://github.com/googleapis/cloud-bigtable-cbt-cli for info about the CLI.
"""

import os
from absl import flags
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import go_lang

FLAGS = flags.FLAGS

CBT_CLI_VERSION = flags.DEFINE_string(
    'google_cloud_cbt_cli_version',
    '9c4f438e783f3af6e8900e80e834725640f1c6a9',
    'Google Cloud Bigtable `cbt` CLI version',
)
CBT_BIN = os.path.join(linux_packages.INSTALL_DIR, 'cbt/cbt')


@vm_util.Retry()
def Install(vm):
  """Installs the cbt CLI."""
  vm.Install('go_lang')
  vm.RemoteCommand(f'cd {linux_packages.INSTALL_DIR} && mkdir -p cbt')
  vm.RemoteCommand(
      f'{go_lang.GO_BIN} install '
      f'cloud.google.com/go/cbt@{CBT_CLI_VERSION.value}'
  )
  vm.RemoteCommand(f'cp go/bin/cbt {CBT_BIN}')
