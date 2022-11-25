# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Run Hugging Face BERT Pretraining Benchmark."""

import posixpath
import time
from typing import Any, Dict, List
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import neuron

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'huggingface_bert_pretraining'
BENCHMARK_CONFIG = """
huggingface_bert_pretraining:
  description: Runs Hugging Face BERT Pretraining Benchmark.
  vm_groups:
    default:
      disk_spec: *default_500_gb
      vm_spec:
        AWS:
          machine_type: trn1.2xlarge
          zone: us-west-2d
          boot_disk_size: 200
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
  """Install and set up HuggingFace BERT pretraining on the target vm.

  Args:
    bm_spec: The benchmark specification
  """
  vm = bm_spec.vms[0]
  vm.Install('neuron')
  run_dir = 'aws-neuron-samples/torch-neuronx/training/dp_bert_hf_pretrain'
  bm_spec.run_dir = run_dir
  vm.RemoteCommand(
      'git clone https://github.com/aws-neuron/aws-neuron-samples.git')
  path = f'PATH={posixpath.join(neuron.ENV.value, "bin")}:$PATH'
  bm_spec.path = path
  vm.RemoteCommand(
      f'{path} python3 -m pip install -r {posixpath.join(run_dir, "requirements.txt")}'
  )

  data_dir = '/scratch/examples_datasets'
  dataset = 'bert_pretrain_wikicorpus_tokenized_hdf5_seqlen512'
  data_file = f'{dataset}.tar'
  vm.DownloadPreprovisionedData(data_dir, BENCHMARK_NAME, data_file)
  vm.RemoteCommand(f'cd {data_dir} && tar -xf {data_file} && rm {data_file}')
  bm_spec.data_dir = posixpath.join(data_dir, dataset)

  ckpt_file = 'ckpt_28125.pt'
  ckpt_dir = posixpath.join(run_dir, 'output')
  bm_spec.ckpt_dir = ckpt_dir
  vm.DownloadPreprovisionedData(ckpt_dir, BENCHMARK_NAME, ckpt_file)


def MakeSamplesFromOutput(output: str) -> List[sample.Sample]:
  """Create samples containing metrics.

  Args:
    output: string, command output Example output:
      perfkitbenchmarker/tests/linux_benchmarks/nccl_benchmark_test.py

  Returns:
    Samples containing training metrics, and the bandwidth
  """
  samples = []
  results = regex_util.ExtractAllMatches(
      r'LOG (.*) - \((\d+), (\d+)\) step_loss : (\S+)\s+learning_rate : (\S+)\s+throughput : (\S+)',
      output)
  for time_now, epoch, step, step_loss, learning_rate, throughput in results:
    metadata = {
        'epoch': int(epoch),
        'step': int(step),
        'step_loss': float(step_loss),
        'learning_rate': float(learning_rate),
    }
    timestamp = time.mktime(time.strptime(time_now))
    samples.append(
        sample.Sample('throughput', float(throughput), 'i/s', metadata,
                      timestamp))
  return samples


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Run HuggingFace BRET pretaining test.

  Args:
    bm_spec: The benchmark specification

  Returns:
    A list of sample.Sample objects.
  """
  vm = bm_spec.vms[0]
  stdout, _ = vm.RobustRemoteCommand(
      f'XLA_USE_BF16=1 {bm_spec.path} '
      f'python3 {bm_spec.run_dir}/dp_bert_large_hf_pretrain_hdf5.py '
      f'--data_dir {bm_spec.data_dir} '
      '--lr 2.8e-4 '
      '--phase2 '
      '--resume_ckpt '
      '--phase1_end_step 28125 '
      '--batch_size 2 '
      '--grad_accum_usteps 512 '
      '--seq_len 512 '
      '--max_pred_len 80 '
      '--warmup_steps 781 '
      '--max_steps 782 '
      f'--output_dir {bm_spec.ckpt_dir}')
  return MakeSamplesFromOutput(stdout)


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Cleanup HuggingFace BERT pretraining on the cluster.

  Args:
    bm_spec: The benchmark specification. Contains all data that is required to
      run the benchmark.
  """
  del bm_spec
