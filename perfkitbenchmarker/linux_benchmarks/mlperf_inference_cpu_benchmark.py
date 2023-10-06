# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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
"""Run MLPerf Inference CPU benchmarks.

This benchmark measures the MLPerf inference performance of the CPU.
"""
from typing import Any, Dict, List
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample

FLAGS = flags.FLAGS
MLPERF_INFERENCE_VERSION = 'v3.0'

_MLPERF_SCRATCH_PATH = '/scratch'
_DLRM_DATA_MODULE = 'criteo'
_DLRM_DATA = 'day_23.gz'
_DLRM_PREPROCESSED_DATA = 'full_recalib.tar.gz'
_DLRM_MODEL = '40m_limit.tar.gz'
_DLRM_ROW_FREQ = 'tb00_40M.pt'
BENCHMARK_NAME = 'mlperf_inference_cpu'
BENCHMARK_CONFIG = """
mlperf_inference_cpu:
  description: Runs MLPerf Inference Benchmark on CPU.
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n2-standard-16
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
_BACKEND = flags.DEFINE_enum(
    'mlperf_inference_cpu_backend',
    'onnxruntime',
    ['onnxruntime', 'tvm-onnx', 'tf', 'deepsparse'],
    'backend',
)
_TVM_PIP_INSTALL = flags.DEFINE_bool(
    'mlperf_inference_cpu_tvm_pip_install', False, 'TVM pip install'
)
_MODEL = flags.DEFINE_enum(
    'mlperf_inference_cpu_model',
    'resnet50',
    [
        'resnet50',
        'bert-99',
        'bert-99.9',
        '3d-unet-99',
        '3d-unet-99.9',
        'retinanet',
        'rnnt',
    ],
    'model',
)
_MODE = flags.DEFINE_enum(
    'mlperf_inference_cpu_mode',
    'performance',
    ['performance', 'accuracy'],
    'mode',
)
_DIVISION = flags.DEFINE_enum(
    'mlperf_inference_cpu_division',
    'open',
    ['closed', 'open'],
    'division',
)
_CATEGORY = flags.DEFINE_enum(
    'mlperf_inference_cpu_category',
    'datacenter',
    ['datacenter', 'edge'],
    'category',
)
_DEVICE = flags.DEFINE_enum(
    'mlperf_inference_cpu_device', 'cpu', ['cpu', 'cuda', 'tensorrt'], 'device'
)
_IMPLEMENTATION = flags.DEFINE_enum(
    'mlperf_inference_cpu_implementation',
    'python',
    ['reference', 'python', 'nvidia-original'],
    'implementation',
)
_TARGET_LATENCY = flags.DEFINE_integer(
    'mlperf_inference_latency', None, 'target latency'
)


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
  vm.Install('pip3')
  vm.RemoteCommand('python3 -m pip install cmind -U')
  cm = 'PATH=~/.local/bin:$PATH cm'
  bm_spec.cm = cm
  vm.RemoteCommand(f'{cm} pull repo mlcommons@ck --checkout=master')
  vm.RemoteCommand(f'{cm} run script "get sys-utils-cm" --quiet')
  vm.RemoteCommand(
      f'{cm} run script "install python-venv" --version=3.10.8 --name=mlperf'
  )


def ParseFromOutput(output: str) -> Dict[str, str]:
  """Creates samples containing metrics.

  Args:
    output: string, command output

  Example output:
    perfkitbenchmarker/tests/linux_benchmarks/mlperf_inference_cpu_benchmark_test.py

  Returns:
    Samples containing training metrics.
  """
  result = regex_util.ExtractAllMatches(r'(.*):(.*)', output)
  return {key.strip(): value.strip() for key, value in result}


def MakeSamplesFromOutput(
    base_metadata: Dict[str, Any], output: str
) -> sample.Sample:
  """Creates samples containing metrics.

  Args:
    base_metadata: dict contains all the metadata that reports.
    output: string, command output

  Example output:
    perfkitbenchmarker/tests/linux_benchmarks/mlperf_inference_cpu_benchmark_test.py

  Returns:
    Sample containing training metrics.
  """
  metadata = ParseFromOutput(output)
  metadata.update(base_metadata)
  return sample.Sample(
      'throughput',
      metadata['Samples per second'],
      'samples per second',
      metadata,
  )


def _IsValid(output: str) -> List[sample.Sample]:
  """Creates samples containing metrics.

  Args:
    output: string, command output

  Returns:
    whether the result is valid or invalid.
  """
  results = regex_util.ExtractAllMatches(r'Result is : (\S+)', output)
  return results[0] == 'VALID'


def _Run(
    bm_spec: benchmark_spec.BenchmarkSpec, target_qps: float = 1.01
) -> str:
  """Runs MLPerf Inference on the cluster.

  Args:
    bm_spec: The benchmark specification. Contains all data that is required to
      run the benchmark.
    target_qps: float, the scheduled samples per second.

  Returns:
    mlperf inference output.
  """
  # TODO(tohaowu) Add a full Run function test.
  vm = bm_spec.vms[0]
  stdout, _ = vm.RemoteCommand(
      f'{bm_spec.cm} run script'
      ' --tags=run,mlperf,inference,generate-run-cmds,_find-performance'
      ' --adr.python.name=mlperf --adr.python.version_min=3.8'
      ' --submitter="Community" --hw_name=default --quiet --clean'
      ' --results_dir=$HOME/logs --execution-mode=valid --test_query_count=5'
      f' --implementation={_IMPLEMENTATION.value} --model={_MODEL.value}'
      f' --backend={_BACKEND.value} --device={_DEVICE.value}'
      f' --scenario={FLAGS.mlperf_inference_scenarios} --mode={_MODE.value}'
      f' --category={_CATEGORY.value} --target_qps={target_qps} --count=1'
      f' {"--adr.tvm.tags=_pip-install" if _TVM_PIP_INSTALL.value else ""}'
  )
  return stdout


def _SearchQps(bm_spec: benchmark_spec.BenchmarkSpec) -> str:
  """Finds the system under test QPS.

  Uses binary search between to find the max GPU QPS while meet the latency
  constraint. Stops searching when the absolute difference is less 1 samples
  per second.

  Args:
    bm_spec: The benchmark specification. Contains all data that is required to
      run the benchmark.

  Returns:
    The best performance test result.
  """
  target_qps = float(ParseFromOutput(_Run(bm_spec))['Samples per second'])
  return _Run(bm_spec, target_qps)


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs MLPerf Inference on the cluster.

  Args:
    bm_spec: The benchmark specification. Contains all data that is required to
      run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vm = bm_spec.vms[0]
  vm.RemoteCommand(f'{bm_spec.cm} rm cache -f')
  stdout = _SearchQps(bm_spec)

  metadata = {
      'implementation': _IMPLEMENTATION.value,
      'model': _MODEL.value,
      'backend': _BACKEND.value,
      'device': _DEVICE.value,
      'scenario': FLAGS.mlperf_inference_scenarios,
      'mode': _MODE.value,
      'category': _CATEGORY.value,
  }
  return [MakeSamplesFromOutput(metadata, stdout)]


def Cleanup(unused_bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Cleanup MLPerf Inference on the cluster."""
  pass
