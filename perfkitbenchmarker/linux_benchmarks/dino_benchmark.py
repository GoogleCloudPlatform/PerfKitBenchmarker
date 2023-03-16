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
"""Run Dino benchmarks."""

import posixpath
from typing import Any, Dict, List
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

_ENV = flags.DEFINE_string('dino_env', 'PATH=/opt/conda/bin:$PATH', 'Env')

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'dino'
BENCHMARK_CONFIG = """
dino:
  description: Runs Dino Benchmark.
  vm_groups:
    default:
      disk_spec: *default_500_gb
      vm_spec:
        GCP:
          machine_type: a2-ultragpu-8g
          zone: us-central1-c
          boot_disk_size: 130
        AWS:
          machine_type: p4d.24xlarge
          zone: us-east-1a
          boot_disk_size: 130
        Azure:
          machine_type: Standard_ND96amsr_A100_v4
          zone: eastus
          boot_disk_size: 130
"""
_IMAGENET = 'ILSVRC2012_img_train.tar'


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Install and set up Dino benchmark on the target vm.

  Args:
    bm_spec: The benchmark specification
  """
  vm = bm_spec.vms[0]
  vm.Install('cuda_toolkit')
  data_dir = vm.scratch_disks[0].mount_point
  vm.DownloadPreprovisionedData(data_dir, 'imagenet', _IMAGENET, 3600)
  vm.RemoteCommand(f'cd {data_dir} && tar -xvf {_IMAGENET}')
  imagenet_dir = posixpath.join(data_dir, 'imagenet')
  vm.RemoteCommand(f'mkdir -p {imagenet_dir}')
  stdout, _ = vm.RemoteCommand(f'cd {data_dir} && ls n*.tar')
  for file in stdout.splitlines():
    category = file.split('.')[0]
    vm.RemoteCommand(
        f'cd {imagenet_dir} && mkdir {category} && cd {category} && tar -xvf '
        f'{posixpath.join(data_dir, file)}'
    )
  vm.RemoteCommand('git clone https://github.com/facebookresearch/dino.git')
  vm.Install('pytorch')
  vm.RemoteCommand('sudo pip3 install timm')
  bm_spec.imagenet_dir = imagenet_dir
  bm_spec.data_dir = data_dir


def MakeSamplesFromOutput(output: str) -> List[sample.Sample]:
  """Create samples containing metrics.

  Args:
    output: string, command output

  Returns:
    Samples containing training metrics, and the bandwidth
  """
  samples = []
  results = regex_util.ExtractAllMatches(
      (
          r'Epoch: \[(\d+)/(\d+)\] Total time: (\d+):(\d+):(\d+) \((\d+.\d+) s'
          r' / it\)'
      ),
      output,
  )
  for epoch, total_epochs, hours, minutes, seconds, iterable_time in results:
    total_seconds = int(hours) * 60 * 60 + int(minutes) * 60 + int(seconds)
    metadata = {
        'epoch': int(epoch),
        'total_epochs': int(total_epochs),
        'total_time': total_seconds,
        'iterable_time': float(iterable_time),
    }
    samples.append(sample.Sample('total_time', total_seconds, 's', metadata))
  day, hours, minutes, seconds = regex_util.ExtractExactlyOneMatch(
      r'Training time (\d+) day, (\d+):(\d+):(\d+)', output
  )
  print(day, hours, minutes, seconds)
  training_time = (
      int(day) * 24 * 60 * 60
      + int(hours) * 60 * 60
      + int(minutes) * 60
      + int(seconds)
  )
  samples.append(sample.Sample('training_time', training_time, 's', {}))
  return samples


@vm_util.Retry()
def _Run(bm_spec: benchmark_spec.BenchmarkSpec) -> str:
  vm = bm_spec.vms[0]
  stdout, _ = vm.RobustRemoteCommand(
      'cd dino && python3 -m torch.distributed.launch --nproc_per_node=8'
      f' main_dino.py --arch vit_small --data_path {bm_spec.imagenet_dir}'
      f' --output_dir {bm_spec.data_dir}'
  )
  return stdout


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Run Dino benchmark.

  Args:
    bm_spec: The benchmark specification

  Returns:
    A list of sample.Sample objects.
  """
  return MakeSamplesFromOutput(_Run(bm_spec))


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Cleanup the cluster.

  Args:
    bm_spec: The benchmark specification. Contains all data that is required to
      run the benchmark.
  """
  del bm_spec
