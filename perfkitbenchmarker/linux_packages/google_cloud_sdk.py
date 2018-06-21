# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing google cloud sdk installation function."""

import os

from perfkitbenchmarker import vm_util


SDK_REPO = 'https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz'
SDK_DIR = '%s/google-cloud-sdk' % vm_util.VM_TMP_DIR
SDK_INSTALL_FILE = '%s/install.sh' % SDK_DIR
GCLOUD_PATH = '%s/bin/gcloud' % SDK_DIR
GSUTIL_PATH = '%s/bin/gsutil' % SDK_DIR


def RunGcloud(vm, cmd):
  return vm.RemoteCommand('export CLOUDSDK_CORE_DISABLE_PROMPTS=1 && %s %s '
                          '--project %s --format json' % (GCLOUD_PATH, cmd,
                                                          vm.project))


def Install(vm):
  """Installs google cloud sdk on the VM."""
  vm.Install('wget')
  vm.RemoteCommand('cd {0} && wget {1} && tar xzf {2} && rm {2}'.format(
      vm_util.VM_TMP_DIR, SDK_REPO, os.path.basename(SDK_REPO)))
  vm.RemoteCommand('%s --disable-installation-options --usage-report=false '
                   '--path-update=false --bash-completion=false'
                   % SDK_INSTALL_FILE)
