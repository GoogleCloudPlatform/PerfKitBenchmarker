# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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

This benchmark measures the MLPerf inference performance of the AMD CPU.
"""
import os
from typing import Any
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import dlrm


_DLRM_NAME = 'dlrmv2-mlperf'
_ZIP = 'dlrmv2-mlperf-binary-updated.zip'
_INSTALL_DIR = os.path.join(
    linux_packages.INSTALL_DIR, 'dlrmv2-mlperf-binary', f'{_DLRM_NAME}'
)
BENCHMARK_DATA = {
    # https://www.amd.com/en/developer/zendnn.html#downloads
    _ZIP: 'a41b3c8d91b78cf88c67ed70ca25543dc457de6739ef79b03e2870d080042b17',
    'dlrm_int8-pace-4bit.pt': (
        '4b0bcfa4b9e5e1c1825bd30262496cb20a7d1cf6f2e715e6cc5f03475e4e94ef'
    ),
}
_SET_ENV = (
    f'cd {_INSTALL_DIR}; source ~/miniconda3/bin/activate pace-env-py3.9; '
)

BENCHMARK_NAME = 'dlrm_amd_cpu_inference'
BENCHMARK_CONFIG = """
dlrm_amd_cpu_inference:
  description: Runs MLPerf Inference with DLRM reference implementation on CPU.
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: c3d-highmem-360
          zone: us-central1-b
          boot_disk_size: 3000
        AWS:
          machine_type: r7a.24xlarge
          zone: us-east-1a
          boot_disk_size: 3000
        Azure:
          machine_type: Standard_F64ams_v6
          zone: eastus
          boot_disk_size: 3000
  flags:
    disable_smt: True
    sar: True
"""


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
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
  vm.InstallPreprovisionedBenchmarkData(
      BENCHMARK_NAME, [_ZIP], linux_packages.INSTALL_DIR
  )
  vm.InstallPreprovisionedBenchmarkData(
      BENCHMARK_NAME, ['dlrm_int8-pace-4bit.pt'], dlrm.MLPERF_ROOT
  )
  vm.RemoteCommand(f'cd {linux_packages.INSTALL_DIR}; unzip {_ZIP}')
  # Install MiniConda3
  vm.InstallPackages('gcc-12 g++-12')
  vm.RemoteCommand(
      f'sudo ln -s /home/{vm.user_name}/miniconda3/bin/conda /usr/bin/conda; '
      f'sudo ln -s /home/{vm.user_name}/miniconda3/envs/pace-env-py3.9/'
      'bin/python /usr/bin/python; '
      'sudo ln -s /usr/bin/g++-12 /usr/bin/g++; '
      'sudo ln -s /usr/bin/g++-12 /usr/bin/c++; '
      'sudo ln -s /usr/bin/gcc-12 /usr/bin/gcc',
      ignore_failure=True,
  )
  vm.RemoteCommand(
      'curl -O https://repo.anaconda.com/miniconda/'
      'Miniconda3-latest-Linux-x86_64.sh; '
      'bash Miniconda3-latest-Linux-x86_64.sh -b'
  )
  vm.RemoteCommand(
      './miniconda3/bin/conda tos accept --override-channels --channel'
      ' https://repo.anaconda.com/pkgs/main; ./miniconda3/bin/conda tos accept'
      ' --override-channels --channel https://repo.anaconda.com/pkgs/r;'
      ' ./miniconda3/bin/conda create -n pace-env-py3.9 python=3.9 -y;'
      ' ./miniconda3/bin/conda init'
  )
  vm.RemoteCommand(f'{_SET_ENV} conda install -c conda-forge gcc=12.1.0 -y')
  vm.RemoteCommand(f'{_SET_ENV} bash prepare_env.sh')


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs DLRM inference intel implementation."""
  # Refactor with Intel benchmarks.
  vm = bm_spec.vms[0]
  # physical cores since we turn off SMT
  cpus_per_socket = vm.CheckLsCpu().cores_per_socket
  metadata = {
      'scenario': 'offline',
      'num_sockets': vm.CheckLsCpu().socket_count,
      'cpus_per_socket': cpus_per_socket,
      'cpus_per_process': cpus_per_socket,
      'cpus_for_loadgen': 1,
      'batch_size': 400,
      'cpus_per_instance': 2,
      'target': dlrm.TARGET.value,
  }
  cmd_prefix = (
      f'{_SET_ENV} '
      'source ~/miniconda3/bin/activate pace-env-py3.9; conda env list;  '
      'chmod 755 run_*.sh; '
      f'export DATA_DIR=/home/{vm.user_name}/mlcommons/data-terabyte; '
      f'export MODEL_DIR=/home/{vm.user_name}/mlcommons; '
      f'export NUM_SOCKETS={vm.numa_node_count}; '
      f'export CPUS_PER_SOCKET={cpus_per_socket}; '
      f'export CPUS_PER_PROCESS={cpus_per_socket}; '
      'export CPUS_PER_INSTANCE=2; '
      'export CPUS_FOR_LOADGEN=1; '
      'export BATCH_SIZE=400; '
  )
  dlrm.CheckAccuracy(vm.RemoteCommand(
      f'{cmd_prefix}'
      'bash run_main.sh offline accuracy int8', login_shell=True,
  )[0], dlrm.TARGET.value)
  stdout, _ = vm.RemoteCommand(
      f'{cmd_prefix} '
      './run_main.sh offline int8; '
      'cat output/pytorch-cpu/dlrm/Offline/performance/run_1/'
      'mlperf_log_summary.txt',
      login_shell=True,
  )
  return dlrm.ParseDlrmSummary(stdout, metadata, 'offline')


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  del bm_spec
