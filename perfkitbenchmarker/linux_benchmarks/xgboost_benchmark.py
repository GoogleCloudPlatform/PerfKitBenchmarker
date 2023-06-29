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
"""Run XGBoost benchmarks."""

import logging
from typing import Any, Dict, List
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cuda_toolkit

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'xgboost'
BENCHMARK_CONFIG = """
xgboost:
  description: Runs XGBoost Benchmark.
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
          zone: us-east1-c
          image_family: common-cu113
          image_project: deeplearning-platform-release
          boot_disk_size: 200
        AWS:
          machine_type: g4dn.xlarge
          zone: us-east-1a
          boot_disk_size: 200
        Azure:
          machine_type: Standard_NC4as_T4_v3
          zone: eastus
          image: microsoft-dsvm:ubuntu-hpc:2004:latest
          boot_disk_size: 200
"""

_USE_GPU = flags.DEFINE_boolean(
    'xgboost_use_gpu', True, 'Run XGBoost using GPU.'
)
CODE_PATH = '/scratch/xgboost_ray'


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Install and set up XGBoost on the target vm.

  Args:
    bm_spec: The benchmark specification
  """
  vm = bm_spec.vms[0]
  if _USE_GPU.value:
    vm.Install('cuda_toolkit')
  vm.RemoteCommand('git clone https://github.com/ray-project/xgboost_ray.git')
  vm.RemoteCommand(
      f'{FLAGS.xgboost_env} python3 -m pip install -r'
      ' xgboost_ray/requirements/test-requirements.txt'
  )
  vm.RemoteCommand(f'{FLAGS.xgboost_env} python3 -m pip install xgboost_ray')
  vm.RemoteCommand(
      f'{FLAGS.xgboost_env} python3'
      ' xgboost_ray/xgboost_ray/tests/release/create_test_data.py'
      ' /tmp/classification.parquet --seed 1234 --num-rows 1000000 --num-cols'
      ' 40 --num-partitions 100 --num-classes 2'
  )
  vm_util.ReplaceText(
      vm,
      r'ray.init\(address=\"auto\"\)',
      'pass',
      'xgboost_ray/xgboost_ray/tests/release/benchmark_cpu_gpu.py',
  )


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Run XGBoost on the cluster.

  Args:
    bm_spec: The benchmark specification

  Returns:
    A list of sample.Sample objects.
  """
  vm = bm_spec.vms[0]
  cmd = [
      FLAGS.xgboost_env,
      'python3',
      'xgboost_ray/xgboost_ray/tests/release/benchmark_cpu_gpu.py',
      '1',
      '10',
      '20',
      '--gpu' if _USE_GPU.value else '',
      '--file',
      '/tmp/classification.parquet',
  ]
  metadata = {}
  if _USE_GPU.value:
    metadata.update(cuda_toolkit.GetMetadata(vm))
  metadata['command'] = ' '.join(cmd)

  stdout, stderr, exit_code = vm.RemoteCommandWithReturnCode(
      metadata['command'], ignore_failure=True)
  if exit_code:
    logging.warning('Error with getting XGBoost stats: %s', stderr)
  training_time = regex_util.ExtractFloat(
      r'TRAIN TIME TAKEN: ([\d\.]+) seconds', stdout
  )
  return [sample.Sample('training_time', training_time, 'seconds', metadata)]


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Cleanup XGBoost on the cluster.

  Args:
    bm_spec: The benchmark specification. Contains all data that
      is required to run the benchmark.
  """
  del bm_spec
