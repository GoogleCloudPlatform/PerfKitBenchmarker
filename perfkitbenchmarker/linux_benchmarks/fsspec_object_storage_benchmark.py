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
"""Run fsspec (gcsfs, s3fs, adlfs) AI/ML Benchmark on object storage.

Uses the official https://github.com/fsspec implementations:
  - GCP: https://github.com/fsspec/gcsfs (gcsfs.GCSFileSystem, gs://)
  - AWS: https://github.com/fsspec/s3fs (s3fs.S3FileSystem, s3://)
  - Azure: https://github.com/fsspec/adlfs (adlfs.AzureBlobFileSystem, abfs://)

Scenarios follow a splittable format (modeled after FioScenarioParser):
  <pattern>_<block_size>_<operation>_<file_size>[_files-<N>][_ops-<M>][_workers-<W>]

Examples:
  - seq_16M_read_100G                    (100 GiB sequential read, 16 MiB block)
  - rand_16M_read_100G_ops-1000_workers-16 (100 GiB random read, 16 workers)
  - seq_8K_read_8K_files-10000_workers-16  (10,000 x 8 KiB small-file reads)
  - seq_16M_write_50G                    (50 GiB checkpoint write, 16 MiB block)
"""

import json
import re
from typing import Any
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample

FLAGS = flags.FLAGS

_SCENARIOS = flags.DEFINE_list(
    'fsspec_scenarios',
    [
        'seq_5M_read_100G_workers-1',
        'rand_4K_read_100G_ops-10000_workers-1',
        'rand_4K_read_10M_files-10000_ops-10000_workers-1',
    ],
    'Splittable fsspec scenario strings of the form '
    '<pattern>_<block_size>_<operation>_<file_size>'
    '[_files-<N>][_ops-<M>][_workers-<W>] '
    '(e.g. seq_16M_read_100G, rand_16M_read_100G_ops-1000_workers-1, '
    'seq_8K_read_8K_files-10000_workers-1, seq_16M_write_50G).',
)
_CACHE_TYPE = flags.DEFINE_enum(
    'fsspec_cache_type',
    'readahead',
    ['readahead', 'background', 'none'],
    'fsspec cache type for sequential reads.',
)
_PREFILL_BLOCK_SIZE = flags.DEFINE_string(
    'fsspec_prefill_block_size',
    '16M',
    'Block size used when pre-filling datasets in Prepare.',
)
_ENABLE_METADATA_CACHE = flags.DEFINE_boolean(
    'fsspec_enable_metadata_cache',
    False,
    'Whether to pre-populate the fsspec metadata cache (dircache) before '
    'running read scenarios.',
)

_SCENARIO_REGEX = re.compile(
    r'^(seq|rand)_\d+[BKMGT]_(read|write)_\d+[BKMGT]'
    r'(?:_(?:files|ops|workers)-\d+)*$',
    re.IGNORECASE,
)
flags.register_validator(
    'fsspec_scenarios',
    lambda scenarios: bool(scenarios)
    and all(_SCENARIO_REGEX.match(s) for s in scenarios),
    message=(
        'Each scenario must match '
        '<pattern>_<block_size>_<operation>_<file_size>'
        '[_files-<N>][_ops-<M>][_workers-<W>].'
    ),
)

BENCHMARK_NAME = 'fsspec_object_storage'
BENCHMARK_CONFIG = """
fsspec_object_storage:
  description: Runs fsspec (gcsfs, s3fs, adlfs) against object storage.
  vm_groups:
    default:
      vm_spec: *default_dual_core
      disk_spec: *default_500_gb
      vm_count: 1
  flags:
    data_disk_type: object_storage
    object_storage_use_fuse: False
    gcloud_scopes: https://www.googleapis.com/auth/devstorage.full_control
"""

_CLOUD_TO_PACKAGE = {
    'GCP': 'gcsfs',
    'AWS': 's3fs',
    'Azure': 'adlfs',
}


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  disk_spec = config['vm_groups']['default']['disk_spec']
  for cloud in disk_spec:
    disk_spec[cloud]['mount_point'] = None
  return config


def CheckPrerequisites(_) -> None:
  """Verifies that the required flags are set."""
  if FLAGS.data_disk_type != 'object_storage':
    raise errors.Setup.InvalidFlagConfigurationError(
        'fsspec_object_storage requires --data_disk_type=object_storage.'
    )


def _GetRunnerEnvAndCmd(vm: Any, mode: str, scenario: str) -> str:
  """Builds the remote command to execute fsspec_runner.py."""
  disk = vm.scratch_disks[0]
  bucket_name = disk.bucket_name
  env_vars = ''
  if FLAGS.cloud == 'AWS':
    env_vars = f'AWS_DEFAULT_REGION={vm.region} '
  elif FLAGS.cloud == 'Azure':
    storage_account = disk.service.storage_account
    env_vars = (
        f'AZURE_STORAGE_ACCOUNT_NAME={storage_account.name} '
        f'AZURE_STORAGE_ACCOUNT_KEY="{storage_account.key}" '
    )
  return (
      f'{env_vars}python3 ~/fsspec_runner.py '
      f'--mode={mode} '
      f'--cloud={FLAGS.cloud} '
      f'--bucket={bucket_name} '
      f'--scenario={scenario} '
      f'--cache_type={_CACHE_TYPE.value} '
      f'--enable_metadata_cache={_ENABLE_METADATA_CACHE.value}'
  )


def Prepare(spec: benchmark_spec.BenchmarkSpec) -> None:
  """Installs fsspec backend (gcsfs, s3fs, adlfs) and prefills datasets."""
  vm = spec.vm_groups['default'][0]
  vm.PushDataFile(f'{BENCHMARK_NAME}/fsspec_runner.py', remote_path='~/')
  vm.RemoteCommand('chmod +x ~/fsspec_runner.py')

  pkg = _CLOUD_TO_PACKAGE[FLAGS.cloud]
  vm.Install('pip')
  vm.RemoteCommand(f'pip install {pkg} numpy')

  # Pre-fill bucket once per file_size using the max number of files needed.
  prepare_files = {}
  for scenario in _SCENARIOS.value:
    parts = scenario.split('_')
    if parts[2] == 'read':
      file_size = parts[3]
      extras = dict(p.split('-', 1) for p in parts[4:])
      num_files = int(extras.get('files', 1))
      prepare_files[file_size] = max(prepare_files.get(file_size, 0), num_files)
  for file_size, num_files in prepare_files.items():
    prepare_scenario = (
        f'seq_{_PREFILL_BLOCK_SIZE.value}_read_{file_size}'
        f'_files-{num_files}_workers-16'
    )
    vm.RobustRemoteCommand(
        _GetRunnerEnvAndCmd(vm, mode='prepare', scenario=prepare_scenario)
    )


def Run(spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs fsspec_runner.py against the provisioned bucket in scratch_disks."""
  vm = spec.vm_groups['default'][0]
  samples = []
  for scenario in _SCENARIOS.value:
    stdout, _ = vm.RobustRemoteCommand(
        _GetRunnerEnvAndCmd(vm, mode='run', scenario=scenario)
    )
    results = json.loads(stdout.strip().splitlines()[-1])
    samples.extend(
        sample.Sample(m['metric'], float(m['value']), m['unit'], m['metadata'])
        for m in results
    )
  return samples


def Cleanup(spec: benchmark_spec.BenchmarkSpec) -> None:
  del spec
