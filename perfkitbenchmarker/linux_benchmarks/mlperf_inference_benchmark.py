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
"""Run MLPerf Inference benchmarks."""
import json
import logging
import math
import posixpath
import re
from typing import Any, Dict, List, Tuple
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import mlperf_benchmark
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import docker
from perfkitbenchmarker.linux_packages import nvidia_driver

FLAGS = flags.FLAGS
MLPERF_INFERENCE_VERSION = 'v1.1'

_MLPERF_SCRATCH_PATH = '/scratch'
_DLRM_DATA_MODULE = 'criteo'
_DLRM_DATA = 'day_23.gz'
_DLRM_PREPROCESSED_DATA = 'full_recalib.tar.gz'
_DLRM_MODEL = '40m_limit.tar.gz'
_DLRM_ROW_FREQ = 'tb00_40M.pt'
BENCHMARK_NAME = 'mlperf_inference'
BENCHMARK_CONFIG = """
mlperf_inference:
  description: Runs MLPerf Inference Benchmark.
  vm_groups:
    default:
      disk_spec: *default_500_gb
      vm_spec:
        GCP:
          machine_type: a2-highgpu-1g
          zone: us-central1-b
          boot_disk_size: 200
        AWS:
          machine_type: p4d.24xlarge
          zone: us-west-2a
          boot_disk_size: 200
        Azure:
          machine_type: Standard_ND96asr_v4
          zone: westus2
          boot_disk_size: 200
"""
_SERVER = 'server'
_SINGLESTREAM = 'singlestream'
_OFFLINE = 'offline'
_SCENARIOS = flags.DEFINE_enum('mlperf_inference_scenarios', _SERVER,
                               [_SERVER, _SINGLESTREAM, _OFFLINE],
                               'MLPerf has defined three different scenarios')
_SERVER_TARGET_QPS = flags.DEFINE_float('mlperf_server_target_qps', None,
                                        'server target qps')

_ACCURACY_METADATA = [
    'benchmark',
    'coalesced_tensor',
    'gpu_batch_size',
    'gpu_copy_streams',
    'gpu_inference_streams',
    'input_dtype',
    'input_format',
    'precision',
    'scenario',
    'server_target_qps',
    'system',
    'tensor_path',
    'use_graphs',
    'config_name',
    'config_ver',
    'accuracy_level',
    'optimization_level',
    'inference_server',
    'system_id',
    'use_cpu',
    'power_limit',
    'cpu_freq',
    'test_mode',
    'gpu_num_bundles',
    'log_dir',
]
_PERFORMANCE_METRIC = 'result_completed_samples_per_sec'
_VALID = 'Result is : VALID'
_INVALID = 'Result is : INVALID'
_PATCH = 'mlperf_inference.patch'


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Loads and returns benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Installs and sets up MLPerf Inference on the target vm.

  Args:
    bm_spec: The benchmark specification

  Raises:
    errors.Config.InvalidValue upon both GPUs and TPUs appear in the config
  """
  vm = bm_spec.vms[0]

  repository = f'inference_results_{MLPERF_INFERENCE_VERSION}'
  vm.RemoteCommand(f'git clone https://github.com/mlcommons/{repository}.git')
  vm.PushDataFile(_PATCH)
  vm.RemoteCommand(f'patch -p0 < {_PATCH}')

  makefile = f'{repository}/closed/NVIDIA/Makefile'
  vm_util.ReplaceText(vm, 'shell uname -p', 'shell uname -m', makefile)

  requirements1 = f'{repository}/closed/NVIDIA/docker/requirements.1'
  vm_util.ReplaceText(vm, 'opencv-python-headless==4.5.2.52',
                      'opencv-python-headless==4.5.3.56', requirements1)
  requirements2 = f'{repository}/closed/NVIDIA/docker/requirements.2'

  benchmark = FLAGS.mlperf_benchmark
  if _SERVER_TARGET_QPS.value:
    config = f'{repository}/closed/NVIDIA/configs/{benchmark}/Server/__init__.py'
    vm_util.ReplaceText(vm, 'server_target_qps = .*',
                        f'server_target_qps = {_SERVER_TARGET_QPS.value}',
                        config)

  for requirements in (requirements1, requirements2):
    vm_util.ReplaceText(vm, 'git:', 'https:', requirements)

  if nvidia_driver.CheckNvidiaGpuExists(vm):
    vm.Install('cuda_toolkit')
    vm.Install('nvidia_driver')
    vm.Install('nvidia_docker')

  bm_spec.env_cmd = (f'export MLPERF_SCRATCH_PATH={_MLPERF_SCRATCH_PATH} && '
                     f'cd {repository}/closed/NVIDIA')
  docker.AddUser(vm)
  vm.RobustRemoteCommand(
      f'{bm_spec.env_cmd} && '
      'make build_docker NO_BUILD=1 && '
      'make docker_add_user && '
      'make launch_docker DOCKER_COMMAND="make clean" && '
      'make launch_docker DOCKER_COMMAND="make link_dirs"',
      should_log=True)
  if benchmark == mlperf_benchmark.DLRM:
    # Download data
    data_dir = posixpath.join(_MLPERF_SCRATCH_PATH, 'data', _DLRM_DATA_MODULE)
    vm.DownloadPreprovisionedData(data_dir, _DLRM_DATA_MODULE, _DLRM_DATA)
    vm.RemoteCommand(f'cd {data_dir} && gzip -d {_DLRM_DATA}')

    # Download model
    model_dir = posixpath.join(_MLPERF_SCRATCH_PATH, 'models', benchmark)
    vm.DownloadPreprovisionedData(model_dir, benchmark, _DLRM_MODEL)
    vm.RemoteCommand(f'cd {model_dir} && '
                     f'tar -zxvf {_DLRM_MODEL} && '
                     f'rm -f {_DLRM_MODEL}')
    vm.DownloadPreprovisionedData(model_dir, benchmark, _DLRM_ROW_FREQ)

    # Preprocess Data
    preprocessed_data_dir = posixpath.join(_MLPERF_SCRATCH_PATH,
                                           'preprocessed_data',
                                           _DLRM_DATA_MODULE)
    vm.DownloadPreprovisionedData(preprocessed_data_dir, _DLRM_DATA_MODULE,
                                  _DLRM_PREPROCESSED_DATA)
    vm.RemoteCommand(f'cd {preprocessed_data_dir} && '
                     f'tar -zxvf {_DLRM_PREPROCESSED_DATA} && '
                     f'rm -f {_DLRM_PREPROCESSED_DATA}')
  elif benchmark == mlperf_benchmark.BERT:
    # Download data
    data_dir = posixpath.join(_MLPERF_SCRATCH_PATH, 'data', 'squad')
    vm.DownloadPreprovisionedData(data_dir, benchmark, 'dev-v1.1.json')

    # Download model
    model_dir = posixpath.join(_MLPERF_SCRATCH_PATH, 'models', benchmark)
    vm.DownloadPreprovisionedData(model_dir, benchmark, 'bert_large_v1_1.onnx')
    vm.DownloadPreprovisionedData(model_dir, benchmark,
                                  'bert_large_v1_1_fake_quant.onnx')
    vm.DownloadPreprovisionedData(model_dir, benchmark, 'vocab.txt')

    # Preprocess Data
    preprocessed_data_dir = posixpath.join(_MLPERF_SCRATCH_PATH,
                                           'preprocessed_data',
                                           'squad_tokenized')
    vm.DownloadPreprovisionedData(preprocessed_data_dir, benchmark,
                                  'input_ids.npy')
    vm.DownloadPreprovisionedData(preprocessed_data_dir, benchmark,
                                  'input_mask.npy')
    vm.DownloadPreprovisionedData(preprocessed_data_dir, benchmark,
                                  'segment_ids.npy')
  else:
    vm.RobustRemoteCommand(
        f'{bm_spec.env_cmd} && '
        'make launch_docker DOCKER_COMMAND='
        f'"make download_data BENCHMARKS={benchmark}"',
        should_log=True)
    vm.RobustRemoteCommand(
        f'{bm_spec.env_cmd} && '
        'make launch_docker DOCKER_COMMAND='
        f'"make download_model BENCHMARKS={benchmark}"',
        should_log=True)
    vm.RobustRemoteCommand(
        f'{bm_spec.env_cmd} && '
        'make launch_docker DOCKER_COMMAND='
        f'"make preprocess_data BENCHMARKS={benchmark}"',
        should_log=True)

  vm.RobustRemoteCommand(
      f'{bm_spec.env_cmd} && '
      'make launch_docker DOCKER_COMMAND='
      '"make build" && '
      'make launch_docker DOCKER_COMMAND='
      '"make run RUN_ARGS=\''
      f'--benchmarks={FLAGS.mlperf_benchmark} '
      f'--scenarios={_SCENARIOS.value} --fast\'"',
      should_log=True)


def _CreateMetadataDict(
    bm_spec: benchmark_spec.BenchmarkSpec) -> Dict[str, Any]:
  """Creates metadata dict to be used in run results.

  Args:
    bm_spec: The benchmark specification. Contains all data that is required to
      run the benchmark.

  Returns:
    metadata dict
  """
  metadata = {
      'model': FLAGS.mlperf_benchmark,
      'version': MLPERF_INFERENCE_VERSION,
  }
  vms = bm_spec.vms
  num_vms = len(vms)
  vm = vms[0]
  gpus_per_node = nvidia_driver.QueryNumberOfGpus(vm)
  total_gpus = gpus_per_node * num_vms
  metadata.update(cuda_toolkit.GetMetadata(vm))
  metadata['total_gpus'] = total_gpus
  return metadata


def MakePerformanceSamplesFromOutput(base_metadata: Dict[str, Any],
                                     output: str) -> List[sample.Sample]:
  """Creates performance samples containing metrics.

  Args:
    base_metadata: dict contains all the metadata that reports.
    output: string, command output
  Example output:
    perfkitbenchmarker/tests/linux_benchmarks/mlperf_inference_benchmark_test.py

  Returns:
    Samples containing training metrics.
  """
  metadata = {}
  for result in regex_util.ExtractAllMatches(r':::MLLOG (.*)', output):
    metric = json.loads(result)
    metadata[metric['key']] = metric['value']
  metadata.update(base_metadata)
  return [
      sample.Sample('throughput', metadata[_PERFORMANCE_METRIC],
                    'samples per second', metadata)
  ]


def MakeAccuracySamplesFromOutput(base_metadata: Dict[str, Any],
                                  output: str) -> List[sample.Sample]:
  """Creates accuracy samples containing metrics.

  Args:
    base_metadata: dict contains all the metadata that reports.
    output: string, command output

  Returns:
    Samples containing training metrics.
  """
  metadata = {}
  for column_name in _ACCURACY_METADATA:
    metadata[f'mlperf {column_name}'] = regex_util.ExtractExactlyOneMatch(
        fr'{re.escape(column_name)} *: *(.*)', output)
  accuracy = regex_util.ExtractFloat(
      r': Accuracy = (\d+\.\d+), Threshold = \d+\.\d+\. Accuracy test PASSED',
      output)
  metadata['Threshold'] = regex_util.ExtractFloat(
      r': Accuracy = \d+\.\d+, Threshold = (\d+\.\d+)\. Accuracy test PASSED',
      output)
  metadata.update(base_metadata)
  return [sample.Sample('accuracy', float(accuracy), '%', metadata)]


def _Run(bm_spec: benchmark_spec.BenchmarkSpec, target_qps: float) -> bool:
  """Runs MLPerf inference test under a server target QPS.

  Args:
    bm_spec: The benchmark specification. Contains all data that is required to
      run the benchmark.
    target_qps: The load to generate.

  Returns:
    Whether the system under test passes under the serer target QPS.
  """
  vm = bm_spec.vms[0]
  config = f'configs/{FLAGS.mlperf_benchmark}/Server/__init__.py'
  vm.RobustRemoteCommand(
      f'{bm_spec.env_cmd} && '
      f'make launch_docker DOCKER_COMMAND="sed -i \'s/server_target_qps = .*/server_target_qps = {target_qps}/g\' {config}"',
      should_log=True)
  # For valid log, result_validity is VALID
  # For invalid log, result_validity is INVALID
  stdout, _ = vm.RobustRemoteCommand(
      f'{bm_spec.env_cmd} && '
      'make launch_docker DOCKER_COMMAND="make run_harness RUN_ARGS=\''
      f'--benchmarks={FLAGS.mlperf_benchmark} '
      f'--scenarios={_SCENARIOS.value} --test_mode=PerformanceOnly --fast\'"',
      should_log=True)
  return _VALID in stdout


def _LastRunResults(bm_spec: benchmark_spec.BenchmarkSpec) -> str:
  """Finds the results of the last run.

  Args:
    bm_spec: The benchmark specification. Contains all data that is required to
      run the benchmark.

  Returns:
    The detail log.
  """
  vm = bm_spec.vms[0]
  stdout, _ = vm.RobustRemoteCommand(
      f'{bm_spec.env_cmd} && make launch_docker DOCKER_COMMAND='
      '"grep -l \'\\"VALID\\"\' build/logs/*/*/*/*/mlperf_log_detail.txt | '
      'xargs ls -t | head -n 1 | xargs cat"',
      should_log=True)
  return stdout


def _FindStartingQps(
    bm_spec: benchmark_spec.BenchmarkSpec) -> Tuple[float, float]:
  """Finds the QPS range to search.

  Args:
    bm_spec: The benchmark specification. Contains all data that is required to
      run the benchmark.

  Returns:
    A tuple of passing QPS and failing QPS.
  """
  # T4 QPS is greater than 256 samples per second.
  passing_qps = falling_qps = 128
  while True:
    if _Run(bm_spec, falling_qps):
      passing_qps, falling_qps = falling_qps, falling_qps * 2
    else:
      logging.info('Lower QPS is %s and upper QPS is %s', passing_qps,
                   falling_qps)
      return passing_qps, falling_qps


def _BinarySearch(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Finds the system under test QPS.

  Uses binary search between to find the max GPU QPS while meet the latency
  constraint. Stops searching when the absolute difference is less 1 samples
  per second.

  Args:
    bm_spec: The benchmark specification. Contains all data that is required to
      run the benchmark.
  """
  passing_qps, falling_qps = _FindStartingQps(bm_spec)
  # Set absolute tolerance to 1
  while not math.isclose(passing_qps, falling_qps, abs_tol=1):
    target_qps = (passing_qps + falling_qps) / 2
    if _Run(bm_spec, target_qps):
      passing_qps = target_qps
    else:
      falling_qps = target_qps
  else:
    logging.info('Target QPS is %s.', passing_qps)


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs MLPerf Inference on the cluster.

  Args:
    bm_spec: The benchmark specification. Contains all data that is required to
      run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  _BinarySearch(bm_spec)

  metadata = _CreateMetadataDict(bm_spec)
  performance_samples = MakePerformanceSamplesFromOutput(
      metadata, _LastRunResults(bm_spec))

  vm = bm_spec.vms[0]
  stdout, _ = vm.RobustRemoteCommand(
      f'{bm_spec.env_cmd} && '
      'make launch_docker DOCKER_COMMAND="make run_harness RUN_ARGS=\''
      f'--benchmarks={FLAGS.mlperf_benchmark} '
      f'--scenarios={_SCENARIOS.value} --test_mode=AccuracyOnly --fast\'"',
      should_log=True)
  accuracy_samples = MakeAccuracySamplesFromOutput(metadata, stdout)
  return performance_samples + accuracy_samples


def Cleanup(unused_bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Cleanup MLPerf Inference on the cluster."""
  pass
