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
"""Run MLPerf Inference CPU benchmarks.

This benchmark measures the MLPerf inference performance of the CPU.
"""
from typing import Any, Dict
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker.linux_packages import dlrm

FLAGS = flags.FLAGS

_DNNL_MAX_CPU_ISA = flags.DEFINE_string(
    'dnnl_max_cpu_isa',
    'AVX512_CORE_AMX',
    'This limits the processor features ONEDNN is able to detect. Refer to'
    ' https://oneapi-src.github.io/oneDNN/v2/dev_guide_cpu_dispatcher_control.html'
    ' for more details. The default options i AVX512_CORE_AMX. Other options'
    ' include AVX512_CORE, AVX512_CORE_VNNI, and AVX512_CORE_BF16.',
)
_BENCHMARK_SCENARIO = flags.DEFINE_string(
    'dlrm_intel_cpu_benchmark_scenario',
    'offline',
    'The benchmark scenario to run. The default value is offline. The other'
    ' option is server.',
)

_SERVER_TARGET_QPS = flags.DEFINE_float(
    'dlrm_intel_cpu_server_target_qps',
    9750.0,
    'The Target QPS for the server scenario. The default value is 9750.0.',
)

BENCHMARK_NAME = 'dlrm_intel_cpu_inference'
BENCHMARK_CONFIG = """
dlrm_intel_cpu_inference:
  description: Runs MLPerf Inference with DLRM reference implementation on CPU.
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n4-highmem-80
          zone: us-central1-b
          boot_disk_size: 3000
        AWS:
          machine_type: m7i.48xlarge
          zone: us-east-1a
          boot_disk_size: 3000
        Azure:
          machine_type: Standard_E96s_v3
          zone: eastus
          boot_disk_size: 3000
  flags:
    disable_smt: True
"""
BENCHMARK_DATA = {
    'dlrm_int8.pt': (  # for intel dlrm inference only
        'c6a4580c396c5440d5e667cc6b9726735f583cfe37e48fce82e91c4e0ea0d4e5'
    ),
}


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Loads and returns benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Installs and sets up MLPerf Inference on the target vm.

  Args:
    bm_spec: The benchmark specification

  Raises:
    errors.Config.InvalidValue upon both GPUs and TPUs appear in the config
  """
  vm = bm_spec.vms[0]
  vm.Install('dlrm')
  vm.DownloadPreprovisionedData(
      dlrm.MODEL_PATH, 'dlrm', 'dlrm_int8.pt', dlrm.DLRM_DOWNLOAD_TIMEOUT
  )
  vm.Install('docker')
  vm.RemoteCommand('sudo chmod 666 /var/run/docker.sock')
  vm.RemoteCommand(
      'cd mlcommons/ && '
      'git clone https://github.com/mlcommons/inference_results_v4.0.git && '
      'cd inference_results_v4.0/closed/Intel/code/'
      'dlrm-v2-99.9/pytorch-cpu-int8/docker && '
      'sed -i "s|docker build|docker build --network=host|g" '
      'build_dlrm-v2-99_int8_container.sh && '
      'sed -i "s|conda install -y -c intel|conda install -y -c '
      r'https\:\/\/software.repos.intel.com\/python\/conda\/|g" Dockerfile && '
      'sed -i "s|conda config --add channels intel|conda config --add channels '
      r'https\:\/\/software.repos.intel.com\/python\/conda\/|g" Dockerfile && '
      'bash build_dlrm-v2-99_int8_container.sh'
  )
  # physical cores since we turn off SMT
  cpus_per_socket = vm.CheckLsCpu().cores_per_socket
  vm.RemoteCommand(
      'cd mlcommons && docker run -td --privileged --net=host '
      '-v ./model-terabyte:/root/model '
      '-v ./data-terabyte:/root/data '
      '-e DATA_DIR=/root/data '
      '-e MODEL_DIR=/root/model '
      f'-e NUM_SOCKETS={vm.CheckLsCpu().socket_count} '
      f'-e CPUS_PER_SOCKET={cpus_per_socket} '
      f'-e CPUS_PER_PROCESS={cpus_per_socket} '
      '-e CPUS_PER_INSTANCE=2 '
      '-e CPUS_FOR_LOADGEN=1 '
      '-e BATCH_SIZE=400 '
      # AMX ignored if not supported
      f'-e DNNL_MAX_CPU_ISA={_DNNL_MAX_CPU_ISA.value} '
      '--name=pkb-dlrm '
      'mlperf_inference_dlrm2:4.0'
  )
  # The target qps is set with the assumption running on EMR hosts.
  # Increase to make sure we always generate enough load.
  vm.RemoteCommand(
      "docker exec pkb-dlrm bash -c 'cd"
      ' /opt/workdir/code/dlrm-v2-99.9/pytorch-cpu-int8; ln -s'
      ' /root/model/dlrm_int8.pt dlrm_int8.pt; sed -i'
      ' "s/dlrm.Offline.target_qps = 8600.0/dlrm.Offline.target_qps ='
      ' 16000.0/g" user_default.conf; sed -i "s/dlrm.Server.target_qps ='
      f' 8200.0/dlrm.Server.target_qps = {_SERVER_TARGET_QPS.value}/g"'
      " user_default.conf'"
  )


def Run(bm_spec):
  """Runs DLRM inference intel implementation."""
  vm = bm_spec.vms[0]
  cpus_per_socket = vm.CheckLsCpu().cores_per_socket
  metadata = {
      'scenario': f'{_BENCHMARK_SCENARIO.value}',
      'num_sockets': vm.CheckLsCpu().socket_count,
      'cpus_per_socket': cpus_per_socket,
      'cpus_per_process': cpus_per_socket,
      'cpus_for_loadgen': 1,
      'batch_size': 400,
      'cpus_per_instance': 2,
  }
  stdout, _ = vm.RemoteCommand(
      'docker exec pkb-dlrm '
      'bash -c '
      "'cd /opt/workdir/code/dlrm-v2-99.9/pytorch-cpu-int8; "
      f'bash run_main.sh {_BENCHMARK_SCENARIO.value} int8; '
      f'cat output/pytorch-cpu/dlrm/{_BENCHMARK_SCENARIO.value.capitalize()}/performance/'
      "run_1/mlperf_log_summary.txt'"
  )
  return dlrm.ParseDlrmSummary(stdout, metadata, _BENCHMARK_SCENARIO.value)


def Cleanup(bm_spec):
  del bm_spec
