# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing stress-ng installation and cleanup functions."""

from absl import flags

FLAGS = flags.FLAGS

GIT_REPO = 'https://github.com/ColinIanKing/stress-ng'
GIT_TAG_MAP = {
    '0.05.23': '54722768329c9f8184c1c98db63435f201377df1',  # ubuntu1604
    '0.09.25': '2db2812edf99ec80c08edf98ee88806a3662031c',  # ubuntu1804
}
STRESS_NG_DIR = '~/stress_ng'


def AptInstall(vm):
  """Installs stress-ng."""
  vm.InstallPackages(
      'build-essential libaio-dev libapparmor-dev libattr1-dev libbsd-dev '
      'libcap-dev libgcrypt11-dev libkeyutils-dev libsctp-dev zlib1g-dev'
  )
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, STRESS_NG_DIR))
  vm.RemoteCommand('cd {0} && git checkout {1}'.format(
      STRESS_NG_DIR, GIT_TAG_MAP[FLAGS.stress_ng_version]))
  vm.RemoteCommand('cd {0} && make && sudo make install'.format(STRESS_NG_DIR))


def AptUninstall(vm):
  """Uninstalls stress-ng."""
  vm.RemoteCommand('cd {0} && sudo make uninstall'.format(STRESS_NG_DIR))
