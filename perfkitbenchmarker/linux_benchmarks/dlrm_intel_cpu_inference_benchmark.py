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
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample

FLAGS = flags.FLAGS
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

FLAGS = flags.FLAGS


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
  vm.Install('dlrm_v2')
  vm.Install('docker')
  vm.RemoteCommand('sudo chmod 666 /var/run/docker.sock')
  vm.RemoteCommand(
      'cd mlcommons/ && '
      'git clone https://github.com/mlcommons/inference_results_v4.0.git && '
      'cd inference_results_v4.0/closed/Intel/code/'
      'dlrm-v2-99.9/pytorch-cpu-int8/docker && '
      'sed -i "s|docker build|docker build --network=host|g" '
      'build_dlrm-v2-99_int8_container.sh && '
      'bash build_dlrm-v2-99_int8_container.sh'
  )
  num_cpus = vm.NumCpusForBenchmark()
  # CPUS_PER_SOCKET needs to be dividable by CPUS_PER_INSTANCE
  cpus_per_socket = num_cpus // vm.numa_node_count // 2 * 2
  vm.RemoteCommand(
      'cd mlcommons && docker run -td --privileged --net=host '
      '-v ./model-terabyte:/root/model '
      '-v ./data-terabyte:/root/data '
      '-e DATA_DIR=/root/data '
      '-e MODEL_DIR=/root/model '
      f'-e NUM_SOCKETS={vm.numa_node_count} '
      f'-e CPUS_PER_SOCKET={cpus_per_socket} '
      f'-e CPUS_PER_PROCESS={cpus_per_socket} '
      '-e CPUS_PER_INSTANCE=2 '
      '-e CPUS_FOR_LOADGEN=1 '
      '-e BATCH_SIZE=400 '
      '-e DNNL_MAX_CPU_ISA=AVX512_CORE_AMX '  # AMX ignored if not supported
      '--name=pkb-dlrm '
      'mlperf_inference_dlrm2:4.0'
  )
  # The target qps is set with the assumption running on EMR hosts.
  # Increase to make sure we always generate enough load.
  vm.RemoteCommand(
      'docker exec pkb-dlrm '
      'bash -c '
      "'cd /opt/workdir/code/dlrm-v2-99.9/pytorch-cpu-int8; "
      'ln -s /root/model/dlrm_int8.pt dlrm_int8.pt; '
      'sed -i "s/dlrm.Offline.target_qps = 8600.0/'
      'dlrm.Offline.target_qps = 16000.0/g" user_default.conf\''
  )


def Run(bm_spec):
  """Runs DLRM inference intel implementation."""
  vm = bm_spec.vms[0]
  cpus_for_benchmark = vm.NumCpusForBenchmark() // vm.numa_node_count // 2 * 2
  metadata = {
      'scenario': 'offline',
      'num_sockets': vm.numa_node_count,
      'cpus_per_socket': cpus_for_benchmark,
      'cpus_per_process': cpus_for_benchmark,
      'cpus_for_loadgen': 1,
      'batch_size': 400,
      'cpus_per_instance': 2,
  }
  stdout, _ = vm.RemoteCommand(
      'docker exec pkb-dlrm '
      'bash -c '
      "'cd /opt/workdir/code/dlrm-v2-99.9/pytorch-cpu-int8; "
      'bash run_main.sh offline int8; '
      'cat output/pytorch-cpu/dlrm/Offline/performance/'
      "run_1/mlperf_log_summary.txt'"
  )
  samples_per_sec = regex_util.ExtractFloat(
      r'Samples per second: ' + f'({regex_util.FLOAT_REGEX})', stdout
  )
  metadata['valid'] = 'Result is : VALID' in stdout
  for percentile in ('50.00', '90.00', '95.00', '97.00', '99.00', '99.90'):
    latency = regex_util.ExtractFloat(
        percentile + r' percentile latency \(ns\)\s*: (\d+)', stdout
    )
    metadata[f'p{percentile}'] = latency
  return [
      sample.Sample(
          'Samples per second', samples_per_sec, 'Samples/sec', metadata
      )
  ]


def Cleanup(bm_spec):
  del bm_spec
