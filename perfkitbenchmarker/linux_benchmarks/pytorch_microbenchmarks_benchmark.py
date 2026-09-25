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
"""Runs PyTorch microbenchmarks to measure native memory bandwidth scaling."""

import json

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import sample

FLAGS = flags.FLAGS

flags.DEFINE_enum(
    'pytorch_microbenchmark_mode',
    'bandwidth',
    ['bandwidth', 'einsum'],
    'Mode classifying the metric outputs.',
)
flags.DEFINE_list(
    'pytorch_microbenchmark_device_counts',
    ['1', '2', '4', '8'],
    'Active device counts to evaluate during the sweep.',
)

BENCHMARK_NAME = 'pytorch_microbenchmarks'
BENCHMARK_CONFIG = """
pytorch_microbenchmarks:
  description: Measures PyTorch memory bandwidth scaling and derate.
  vm_groups:
    default:
      os_type: ubuntu2204
      vm_spec:
        GCP:
          machine_type: ct6e-standard-8t
"""

_RUNNER_TEMPLATE = 'pytorch_microbenchmarks_runner.py.j2'
_REMOTE_RUNNER_PATH = '~/pytorch_microbenchmarks_runner.py'


def GetConfig(user_config):
  """Loads the benchmark configuration."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Checks prerequisites for running the benchmark."""
  del benchmark_config


def _GetPipCmd(vm):
  """Returns the pip install command matching the VM OS."""
  if 'ubuntu24' in getattr(vm, 'OS_TYPE', ''):
    return 'pip3 install --break-system-packages'
  return 'pip3 install'


def _InstallTpuDependencies(vm):
  """Installs PyTorch and PyTorch-XLA dependencies on Cloud TPU VMs."""
  pip_cmd = _GetPipCmd(vm)
  vm.RemoteCommand(
      f'{pip_cmd} --no-cache-dir torch~=2.5.0 --index-url'
      ' https://download.pytorch.org/whl/cpu && '
      f'{pip_cmd} --no-cache-dir torch_xla[tpu]~=2.5.0 -f'
      ' https://storage.googleapis.com/libtpu-releases/index.html'
  )


def _InstallNvidiaDependencies(vm):
  """Installs NVIDIA drivers and PyTorch packages on GPU VMs."""
  pip_cmd = _GetPipCmd(vm)
  out, _ = vm.RemoteCommand('lspci | grep -i nvidia || true')
  if '2901' in out or 'B200' in out:
    vm.RemoteCommand(
        'sudo apt-get update && sudo apt-get install -y'
        ' nvidia-headless-580-server-open nvidia-utils-580-server && sudo'
        f' modprobe nvidia && {pip_cmd} --no-cache-dir numpy torch torchvision'
        ' torchaudio --index-url https://download.pytorch.org/whl/cu130'
    )
    return
  vm.RemoteCommand(
      f'{pip_cmd} --no-cache-dir --upgrade torch torchvision torchaudio numpy'
  )


def Prepare(benchmark_spec):
  """Prepares the VM by installing dependencies and rendering template."""
  vm = benchmark_spec.vms[0]
  vm.Install('pip')
  vm.InstallPackages('python3-dev zlib1g-dev git python3-pip')

  if FLAGS.tpu_type:
    _InstallTpuDependencies(vm)
  else:
    _InstallNvidiaDependencies(vm)

  template_path = data.ResourcePath(_RUNNER_TEMPLATE)
  context = {
      'device_counts': ','.join(FLAGS.pytorch_microbenchmark_device_counts)
  }
  vm.RenderTemplate(template_path, _REMOTE_RUNNER_PATH, context)


_PEAK_BANDWIDTH_GBPS = {
    'b200': 8000.0,
    'h100': 3350.0,
    'a100': 1935.0,
    'v6e': 1638.0,
    'tpu': 1638.0,
}


def _GetPeakBandwidth(device):
  """Returns the theoretical peak memory bandwidth in GB/s per chip."""
  candidates = [device, FLAGS.gpu_type, FLAGS.tpu_type]
  for candidate in candidates:
    if not candidate:
      continue
    candidate_lower = candidate.lower()
    for key, bandwidth in _PEAK_BANDWIDTH_GBPS.items():
      if key in candidate_lower:
        return bandwidth
  return None


def _ParseBenchmarkOutput(stdout_or_metrics):
  """Parses benchmark JSON output into PKB sample objects."""
  if isinstance(stdout_or_metrics, str):
    raw_metrics = []
    for line in stdout_or_metrics.splitlines():
      if line.startswith('{'):
        raw_metrics.append(json.loads(line))
  else:
    raw_metrics = stdout_or_metrics

  samples = []
  for metric in raw_metrics:
    bandwidth = metric.pop('bandwidth_gbps')

    metadata = {
        'op_name': metric['op_name'],
        'test_name': metric['test_name'],
        'dtype': metric['dtype'],
        'array_size_bytes_per_chip': metric['array_size_bytes_per_chip'],
        'message_size_bytes': metric['message_size_bytes'],
        'device': metric['device'],
        'num_chips': metric['num_chips'],
    }

    samples.append(
        sample.Sample('Memory_Bandwidth', bandwidth, 'GB/s', metadata)
    )

    peak_bandwidth = _GetPeakBandwidth(metadata['device'])
    if peak_bandwidth:
      attained_pct = (bandwidth / peak_bandwidth) * 100.0
      samples.append(
          sample.Sample('Efficiency', attained_pct, '% of Peak', metadata)
      )
      samples.append(
          sample.Sample('Derate', 100.0 - attained_pct, '%', metadata)
      )
  return samples


def Run(benchmark_spec):
  """Runs the microbenchmark across specified device counts and collects metrics."""
  vm = benchmark_spec.vms[0]
  env_vars = []
  if FLAGS.tpu_type:
    tpu_accel = (
        FLAGS.tpu_type if '-' in FLAGS.tpu_type else f'{FLAGS.tpu_type}-8'
    )
    env_vars.extend([
        'TPU_SKIP_MDS_QUERY=1',
        f'TPU_ACCELERATOR_TYPE={tpu_accel}',
        'PJRT_DEVICE=TPU',
        'TPU_WORKER_HOSTNAMES=localhost',
    ])
  env_prefix = f'{" ".join(env_vars)} ' if env_vars else ''

  raw_metrics = []
  for count_str in FLAGS.pytorch_microbenchmark_device_counts:
    count = int(count_str)
    cmd = f'{env_prefix}python3 {_REMOTE_RUNNER_PATH} --num_chips={count}'
    if FLAGS.tpu_type:
      cmd += f' --accelerator={FLAGS.tpu_type}'
    stdout, _ = vm.RemoteCommand(cmd)
    for line in stdout.splitlines():
      if line.startswith('{'):
        raw_metrics.append(json.loads(line))

  samples = _ParseBenchmarkOutput(raw_metrics)
  if not samples:
    raise ValueError('No metric results found from microbenchmark runs.')

  return samples


def Cleanup(benchmark_spec):
  """Cleans up the remote benchmark payload."""
  benchmark_spec.vms[0].RemoteCommand(f'rm -f {_REMOTE_RUNNER_PATH}')
