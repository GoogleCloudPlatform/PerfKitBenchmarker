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
import re
from typing import Any, Dict, List
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import nvidia_driver

FLAGS = flags.FLAGS
MLPERF_INFERENCE_VERSION = 'v1.1'

BENCHMARK_NAME = 'mlperf_inference'
BENCHMARK_CONFIG = """
mlperf_inference:
  description: Runs MLPerf Inference Benchmark.
  vm_groups:
    default:
      os_type: ubuntu1804
      disk_spec: *default_500_gb
      vm_spec:
        GCP:
          machine_type: a2-highgpu-1g
          zone: us-central1-b
          boot_disk_size: 200
          image_family: tf-latest-gpu
          image_project: deeplearning-platform-release
        AWS:
          machine_type: p4d.24xlarge
          zone: us-west-2a
          boot_disk_size: 200
          image: ami-0ccc71d716eb5d6a4
        Azure:
          machine_type: Standard_ND96asr_v4
          zone: westus2
          boot_disk_size: 200
          image: microsoft-dsvm:ubuntu-hpc:1804:latest
"""

_SCENARIOS = flags.DEFINE_enum('mlperf_inference_scenarios', 'server',
                               ['server', 'singlestream', 'offline'],
                               'MLPerf has defined three different scenarios')

_METADATA_COLUMNS = [
    'active_sms',
    'benchmark',
    'bert_opt_seqlen',
    'coalesced_tensor',
    'enable_interleaved',
    'gpu_batch_size',
    'gpu_copy_streams',
    'gpu_inference_streams',
    'graphs_max_seqlen',
    'input_dtype',
    'input_format',
    'precision',
    'scenario',
    'server_num_issue_query_threads',
    'server_target_qps',
    'soft_drop',
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
    'gpu_num_bundles',
    'log_dir',
    'SUT name',
    'Scenario',
    'Mode',
    'Scheduled samples per second',
    'Result is',
    'Performance constraints satisfied',
    'Min duration satisfied',
    'Min queries satisfied',
    'Completed samples per second',
    'Min latency (ns)',
    'Max latency (ns)',
    'Mean latency (ns)',
    '50.00 percentile latency (ns)',
    '90.00 percentile latency (ns)',
    '95.00 percentile latency (ns)',
    '97.00 percentile latency (ns)',
    '99.00 percentile latency (ns)',
    '99.90 percentile latency (ns)',
    'samples_per_query',
    'target_latency (ns)',
    'max_async_queries',
    'min_duration (ms)',
    'max_duration (ms)',
    'min_query_count',
    'max_query_count',
    'qsl_rng_seed',
    'sample_index_rng_seed',
    'schedule_rng_seed',
    'accuracy_log_rng_seed',
    'accuracy_log_probability',
    'accuracy_log_sampling_target',
    'print_timestamps',
    'performance_issue_unique',
    'performance_issue_same',
    'performance_issue_same_index',
    'performance_sample_count',
]


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Install and set up MLPerf Inference on the target vm.

  Args:
    bm_spec: The benchmark specification

  Raises:
    errors.Config.InvalidValue upon both GPUs and TPUs appear in the config
  """
  vm = bm_spec.vms[0]

  repository = f'inference_results_{MLPERF_INFERENCE_VERSION}'
  vm.RemoteCommand(
      f'git clone https://github.com/mlcommons/{repository}.git',
      should_log=True)

  makefile = f'{repository}/closed/NVIDIA/Makefile'
  vm_util.ReplaceText(vm, 'shell uname -p', 'shell uname -m', makefile)

  requirements = f'{repository}/closed/NVIDIA/docker/requirements.1'
  vm_util.ReplaceText(vm, 'opencv-python-headless==4.5.2.52',
                      'opencv-python-headless==4.5.3.56', requirements)

  if nvidia_driver.CheckNvidiaGpuExists(vm):
    vm.Install('cuda_toolkit')
    vm.Install('nvidia_driver')
    vm.Install('nvidia_docker')

  benchmark = FLAGS.mlperf_benchmark
  bm_spec.env_cmd = ('export MLPERF_SCRATCH_PATH=/scratch && '
                     f'cd {repository}/closed/NVIDIA')
  vm.RobustRemoteCommand(
      f'{bm_spec.env_cmd} && '
      'make build_docker NO_BUILD=1 && '
      'make docker_add_user && '
      'make launch_docker DOCKER_COMMAND="echo $MLPERF_SCRATCH_PATH" && '
      'make launch_docker DOCKER_COMMAND="ls -al $MLPERF_SCRATCH_PATH" && '
      'make launch_docker DOCKER_COMMAND="make clean" && '
      'make launch_docker DOCKER_COMMAND="make link_dirs" && '
      'make launch_docker DOCKER_COMMAND="ls -al build/"',
      should_log=True)
  vm.RobustRemoteCommand(
      f'{bm_spec.env_cmd} && '
      'make launch_docker DOCKER_COMMAND='
      f'"make download_data BENCHMARKS={benchmark}"',
      should_log=True)
  vm.RobustRemoteCommand(
      f'{bm_spec.env_cmd} && '
      'make launch_docker DOCKER_COMMAND='
      f'"make download_model BENCHMARKS={benchmark}" && '
      'make launch_docker DOCKER_COMMAND='
      f'"make preprocess_data BENCHMARKS={benchmark}" && '
      f'make launch_docker DOCKER_COMMAND="make build"',
      should_log=True)


def _CreateMetadataDict(
    bm_spec: benchmark_spec.BenchmarkSpec) -> Dict[str, Any]:
  """Create metadata dict to be used in run results.

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


def MakeSamplesFromOutput(metadata: Dict[str, Any],
                          output: str) -> List[sample.Sample]:
  """Create samples containing metrics.

  Args:
    metadata: dict contains all the metadata that reports.
    output: string, command output
  Example output:
    perfkitbenchmarker/tests/linux_benchmarks/mlperf_inference_benchmark_test.py

  Returns:
    Samples containing training metrics.
  """
  for column_name in _METADATA_COLUMNS:
    metadata[f'mlperf {column_name}'] = regex_util.ExtractExactlyOneMatch(
        fr'{re.escape(column_name)} *: *(.*)', output)
  throughput = regex_util.ExtractFloat(
      r': result_scheduled_samples_per_sec *: *(.*), Result is VALID', output)
  return [sample.Sample('throughput', float(throughput), 'samples/s', metadata)]


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Run MLPerf Inference on the cluster.

  Args:
    bm_spec: The benchmark specification. Contains all data that is required to
      run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vm = bm_spec.vms[0]

  metadata = _CreateMetadataDict(bm_spec)
  stdout, _ = vm.RobustRemoteCommand(
      f'{bm_spec.env_cmd} && '
      'make launch_docker DOCKER_COMMAND="make run RUN_ARGS=\'--benchmarks='
      f'{FLAGS.mlperf_benchmark} --scenarios={_SCENARIOS.value}\'"',
      should_log=True)
  return MakeSamplesFromOutput(metadata, stdout)


def Cleanup(unused_bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Cleanup MLPerf Inference on the cluster."""
  pass
