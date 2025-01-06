# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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
"""Package for downloading DLRMv2 data and models."""

from perfkitbenchmarker import errors
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample

# "_" is not allowed in some cloud object storage services.
PACKAGE_NAME = 'dlrm'
PREPROVISIONED_DATA = {
    'day_23_dense.npy': (
        '127dadd27ec933c4fc16223fc54609478891b7a51cf6f4de3781543c36f6fcc2'
    ),
    'day_23_labels.npy': (
        'b650f2fd015b8e45461e82ada9ff5eb6e31bb918ddf4968e42aef4f7f5040aad'
    ),
    'day_23_sparse_multi_hot.npz': (
        '6d2afc30d35c16e8b98f1ef0531dd620b6a0f32072b3a1d1b4eb1badb2ac3d70'
    ),
    'weights.zip': (
        '6fe76b56c15fef16903ff4f6837e01581c8b033709286945d7bb62bf3d9ec262'
    ),
}
DLRM_DATA = (
    'day_23_dense.npy',
    'day_23_labels.npy',
    'day_23_sparse_multi_hot.npz',
)
DLRM_DOWNLOAD_TIMEOUT = 3600

MLPERF_ROOT = 'mlcommons'
MODEL_PATH = f'{MLPERF_ROOT}/model-terabyte'
DATA_PATH = f'{MLPERF_ROOT}/data-terabyte'


def Install(vm):
  """Download and configure the dlrm data on the VM."""
  vm.InstallPackages('unzip')
  vm.RemoteCommand(f'mkdir -p {DATA_PATH}')
  for file_name in DLRM_DATA:
    vm.DownloadPreprovisionedData(
        DATA_PATH, PACKAGE_NAME, file_name, DLRM_DOWNLOAD_TIMEOUT
    )
  vm.DownloadPreprovisionedData(
      MLPERF_ROOT, PACKAGE_NAME, 'weights.zip', DLRM_DOWNLOAD_TIMEOUT
  )
  vm.RemoteCommand(
      f'cd {MLPERF_ROOT}; unzip weights.zip -d .; '
      'mv model_weights model-terabyte'
  )
  vm.DownloadPreprovisionedData(
      MODEL_PATH, PACKAGE_NAME, 'dlrm_int8.pt', DLRM_DOWNLOAD_TIMEOUT
  )


def ParseDlrmSummary(summary, metadata, scenario):
  """Parse MLPerf DLRM summary."""
  metadata['valid'] = 'Result is : VALID' in summary
  if not metadata['valid']:
    raise errors.Benchmarks.RunError(
        'Result is invalid. Please check the log for details.'
    )
  samples_per_sec_field = 'Samples per second: '
  if scenario == 'server':
    samples_per_sec_field = 'Completed samples per second    : '
  samples_per_sec = regex_util.ExtractFloat(
      samples_per_sec_field + f'({regex_util.FLOAT_REGEX})', summary
  )
  for percentile in ('50.00', '90.00', '95.00', '97.00', '99.00', '99.90'):
    latency = regex_util.ExtractFloat(
        percentile + r' percentile latency \(ns\)\s*: (\d+)', summary
    )
    metadata[f'p{percentile}'] = latency
  return [
      sample.Sample(
          'Samples per second', samples_per_sec, 'Samples/sec', metadata
      )
  ]

