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
"""Module containing CUDA samples installation."""

from absl import flags

FLAGS = flags.FLAGS

BANDWIDTH_TEST_PATH = '/usr/local/cuda/extras/demo_suite/bandwidthTest'


def Install(vm):
  vm.Install('cuda_toolkit')
  source_code = f'v{FLAGS.cuda_toolkit_version}.tar.gz'
  vm.RemoteCommand(
      'wget -c https://github.com/NVIDIA/cuda-samples/archive/refs/tags/'
      f'{source_code}')
  vm.RemoteCommand(f'tar -zxvf {source_code} && '
                   f'cd cuda-samples-{FLAGS.cuda_toolkit_version} && make')


def GetBandwidthTestPath(vm):
  """Get CUDA band width test binary path."""
  if vm.TryRemoteCommand(f'stat {BANDWIDTH_TEST_PATH}'):
    return BANDWIDTH_TEST_PATH

  bandwidth_test_path = (f'cuda-samples-{FLAGS.cuda_toolkit_version}/bin/x86_64'
                         '/linux/release/bandwidthTest')
  if vm.TryRemoteCommand(f'stat {bandwidth_test_path}'):
    return bandwidth_test_path

  Install(vm)
  return bandwidth_test_path
