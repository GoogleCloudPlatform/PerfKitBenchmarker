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
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import xgboost

_TREE_METHOD = flags.DEFINE_enum(
    'xgboost_tree_method', 'gpu_hist', ['gpu_hist', 'hist'],
    'XGBoost builtin tree methods.')
_SPARSITY = flags.DEFINE_float(
    'xgboost_sparsity', 0.0, 'XGBoost sparsity-aware split finding algorithm.')
_ROWS = flags.DEFINE_integer(
    'xgboost_rows', 1000000, 'The number of data rows.')
_COLUMNS = flags.DEFINE_integer(
    'xgboost_columns', 50, 'The number of data columns.')
_ITERATIONS = flags.DEFINE_integer(
    'xgboost_iterations', 500, 'The number of training iterations.')
_TEST_SIZE = flags.DEFINE_float(
    'xgboost_test_size', 0.25,
    'Train-test split for evaluating machine learning algorithms')
_PARAMS = flags.DEFINE_string(
    'xgboost_params', None,
    'Provide additional parameters as a Python dict string, '
    'e.g. --params \"{\'max_depth\':2}\"')


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
          gpu_type: t4
          gpu_count: 1
          zone: us-east1-c
          image_family: tf-latest-gpu-gvnic
          image_project: deeplearning-platform-release
        AWS:
          machine_type: g4dn.xlarge
          zone: us-east-1a
          image: ami-0d50576797d8d1a43
        Azure:
          machine_type: Standard_NC4as_T4_v3
          zone: eastus
          image: microsoft-dsvm:ubuntu-hpc:1804:latest
"""


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
  vm_util.RunThreaded(lambda vm: vm.Install('xgboost'), bm_spec.vms)


def _MetadataFromFlags(vm: virtual_machine.BaseVirtualMachine) -> [str, Any]:
  """Returns metadata dictionary from flag settings."""
  return {
      'tree_method': _TREE_METHOD.value,
      'sparsity': _SPARSITY.value,
      'rows': _ROWS.value,
      'columns': _COLUMNS.value,
      'iterations': _ITERATIONS.value,
      'test_size': _TEST_SIZE.value,
      'params': _PARAMS.value,
      'xgboost_version': xgboost.GetXgboostVersion(vm),
  }


def _CollectGpuSamples(
    vm: virtual_machine.BaseVirtualMachine) -> List[sample.Sample]:
  """Run XGBoost on the cluster.

  Args:
    vm: The virtual machine to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  cmd = [
      f'{FLAGS.xgboost_env}',
      'python3',
      f'{linux_packages.INSTALL_DIR}/xgboost/tests/benchmark/benchmark_tree.py',
      f'--tree_method={_TREE_METHOD.value}',
      f'--sparsity={_SPARSITY.value}',
      f'--rows={_ROWS.value}',
      f'--columns={_COLUMNS.value}',
      f'--iterations={_ITERATIONS.value}',
      f'--test_size={_TEST_SIZE.value}',
  ]
  if _PARAMS.value:
    cmd.append(f'--params="{_PARAMS.value}"')
  metadata = _MetadataFromFlags(vm)
  metadata.update(cuda_toolkit.GetMetadata(vm))
  metadata['command'] = ' '.join(cmd)

  stdout, stderr, exit_code = vm.RemoteCommandWithReturnCode(
      metadata['command'], ignore_failure=True)
  if exit_code:
    logging.warning('Error with getting XGBoost stats: %s', stderr)
  training_time = regex_util.ExtractFloat(
      r'Train Time: ([\d\.]+) seconds', stdout)
  return sample.Sample('training_time', training_time, 'seconds', metadata)


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  return vm_util.RunThreaded(_CollectGpuSamples, bm_spec.vms)


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Cleanup XGBoost on the cluster.

  Args:
    bm_spec: The benchmark specification. Contains all data that
      is required to run the benchmark.
  """
  del bm_spec
