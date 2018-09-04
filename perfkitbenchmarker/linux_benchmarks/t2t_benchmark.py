# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Run a Tensor2Tensor benchmark.

Code:
https://github.com/tensorflow/tensor2tensor
This benchmark can run any tensor2tensor model, including ones that target TPU's
"""

import datetime
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import mnist_benchmark
from perfkitbenchmarker.linux_packages import cloud_tpu_models
from perfkitbenchmarker.linux_packages import tensorflow

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'tensor2tensor'
BENCHMARK_CONFIG = """
tensor2tensor:
  description: Runs a benchmark using the Tensor2Tensor framework.
  vm_groups:
    default:
      os_type: ubuntu1604
      vm_spec:
        GCP:
          machine_type: n1-standard-8
          zone: us-east1-d
          boot_disk_size: 200
        AWS:
          machine_type: p2.xlarge
          zone: us-east-1
          boot_disk_size: 200
        Azure:
          machine_type: Standard_NC6
          zone: eastus
"""

flags.DEFINE_string('t2t_model', None, 'Tensor2Tensor model to run')
flags.DEFINE_string('t2t_problem', None, 'Tensor2Tensor problem to run')
flags.DEFINE_string('t2t_hparams_set', None,
                    'Tensor2Tensor hyperparameters set')
flags.DEFINE_integer('t2t_train_steps', 1000, 'Number of train steps')
flags.DEFINE_integer('t2t_eval_steps', 1, 'Number of eval steps')


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _UpdateBenchmarkSpecWithFlags(benchmark_spec):
  """Update the benchmark_spec with supplied command line flags.

  Args:
    benchmark_spec: benchmark specification to update
  """
  benchmark_spec.model = FLAGS.t2t_model
  benchmark_spec.problem = FLAGS.t2t_problem
  benchmark_spec.train_steps = FLAGS.t2t_train_steps
  benchmark_spec.eval_steps = FLAGS.t2t_eval_steps
  benchmark_spec.data_dir = FLAGS.t2t_data_dir
  benchmark_spec.hparams_set = FLAGS.t2t_hparams_set


def Prepare(benchmark_spec):
  """Install and set up the Tensor2Tensor benchmark on the target vm.

  Args:
    benchmark_spec: The benchmark specification
  """
  mnist_benchmark.Prepare(benchmark_spec)
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)


def _CreateMetadataDict(benchmark_spec):
  """Create metadata dict to be used in run results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    metadata dict
  """
  metadata = {
      'use_tpu': benchmark_spec.use_tpu,
      'tpu': benchmark_spec.tpu,
      'model': benchmark_spec.model,
      'problem': benchmark_spec.problem,
      'hparams_set': benchmark_spec.hparams_set,
      'data_dir': benchmark_spec.data_dir,
      'model_dir': benchmark_spec.model_dir,
      'train_steps': benchmark_spec.train_steps,
      'eval_steps': benchmark_spec.eval_steps,
  }
  return metadata


def _MakeSamplesFromOutput(metadata, output):

  samples = []

  samples.extend(
      mnist_benchmark.ExtractThroughput(r'global_step/sec: (\S+)', output,
                                        metadata, 'Global Steps Per Second',
                                        'global_steps/sec'))
  samples.extend(
      mnist_benchmark.ExtractThroughput(r'examples/sec: (\S+)', output,
                                        metadata, 'Examples Per Second',
                                        'examples/sec'))
  pattern = (r'Saving dict for global step \d+: .*global_step = (\d+), '
             r'.*loss = (\d+\.\d+), '
             r'.*accuracy = (\d+\.\d+), '
             r'.*accuracy_per_sequence = (\d+\.\d+), '
             r'.*accuracy_top5 = (\d+\.\d+), '
             r'.*neg_log_perplexity = (-?\d+\.\d+)')
  for step, loss, accuracy, accuracy_per_sequence, accuracy_top5, neg_log_perplexity in (
      regex_util.ExtractAllMatches(pattern, output)):
    metadata_copy = metadata.copy()
    metadata_copy['step'] = int(step)
    samples.append(sample.Sample('Eval Loss', float(loss), '', metadata_copy))
    samples.append(
        sample.Sample('Accuracy',
                      float(accuracy) * 100, '%', metadata_copy))
    samples.append(
        sample.Sample('Accuracy Per Sequence',
                      float(accuracy_per_sequence) * 100, '%', metadata_copy))
    samples.append(
        sample.Sample('Negative Log Perplexity', float(neg_log_perplexity),
                      'perplexity', metadata_copy))
    samples.append(
        sample.Sample('Top 5 Accuracy',
                      float(accuracy_top5) * 100, '%', metadata_copy))

  return samples


def Run(benchmark_spec):
  """Run the Tensor2Tensor model on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vm = benchmark_spec.vms[0]

  t2t_benchmark_cmd = ('t2t-trainer '
                       '--model={model} '
                       '--problem={problem} '
                       '--hparams_set={hparams_set} '
                       '--train_steps={train_steps} --eval_steps={eval_steps} '
                       '--data_dir={data_dir} --output_dir={model_dir}'.format(
                           model=benchmark_spec.model,
                           problem=benchmark_spec.problem,
                           train_steps=benchmark_spec.train_steps,
                           eval_steps=benchmark_spec.eval_steps,
                           data_dir=benchmark_spec.data_dir,
                           model_dir=benchmark_spec.model_dir,
                           hparams_set=benchmark_spec.hparams_set,
                       ))

  if benchmark_spec.use_tpu:
    t2t_benchmark_cmd += (
        ' --use_tpu=True '
        '--master={master}'.format(
            master=benchmark_spec.tpu_groups['train'].GetMasterGrpcAddress()))

  stdout, stderr = vm.RobustRemoteCommand(t2t_benchmark_cmd, should_log=True)

  # TODO(user) Add timestamp to tensor2tensor output to enable samples like
  # resnet_benchmark
  return _MakeSamplesFromOutput(
      _CreateMetadataDict(benchmark_spec), stdout + stderr)


def Cleanup(benchmark_spec):
  """Cleanup the Tensor2Tensor workload on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  mnist_benchmark.Cleanup(benchmark_spec)
