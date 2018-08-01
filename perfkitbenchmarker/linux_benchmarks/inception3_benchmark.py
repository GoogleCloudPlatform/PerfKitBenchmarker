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

"""Run Inception V3 benchmarks.

Tutorials: https://cloud.google.com/tpu/docs/tutorials/inception
Code: https://github.com/tensorflow/tpu/blob/master/models/experimental/inception/inception_v3.py
This benchmark is equivalent to tensorflow_benchmark with the inception3 model
except that this can target TPU.
"""
# TODO(tohaowu): We only measure image processing speed for now, and we will
# measure the other metrics in the future.

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_benchmarks import mnist_benchmark
from perfkitbenchmarker.linux_packages import cloud_tpu_models
from perfkitbenchmarker.linux_packages import tensorflow

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'inception3'
BENCHMARK_CONFIG = """
inception3:
  description: Runs Inception V3 Benchmark.
  vm_groups:
    default:
      os_type: ubuntu1604
      vm_spec:
        GCP:
          machine_type: n1-standard-4
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

flags.DEFINE_float('inception3_learning_rate', 0.165, 'Learning rate.')
flags.DEFINE_integer('inception3_train_steps', 250000,
                     'Number of steps use for training.')
flags.DEFINE_enum('inception3_use_data', 'real', ['real', 'fake'],
                  'Whether to use real or fake data. If real, the data is '
                  'downloaded from imagenet_data_dir. Otherwise, synthetic '
                  'data is generated.')
flags.DEFINE_enum('inception3_mode', 'train',
                  ['train', 'eval', 'train_and_eval'],
                  'Mode to run: train, eval, train_and_eval')
flags.DEFINE_integer('inception3_train_steps_per_eval', 2000,
                     'Number of training steps to run between evaluations.')
flags.DEFINE_integer('inception3_save_checkpoints_secs', 0, 'Interval (in '
                     'seconds) at which the model data should be checkpointed. '
                     'Set to 0 to disable.')
flags.DEFINE_integer('inception3_train_batch_size', 1024,
                     'Global (not per-shard) batch size for training')
flags.DEFINE_integer('inception3_eval_batch_size', 1024,
                     'Global (not per-shard) batch size for evaluation')


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
  benchmark_spec.learning_rate = FLAGS.inception3_learning_rate
  benchmark_spec.train_steps = FLAGS.inception3_train_steps
  benchmark_spec.use_data = FLAGS.inception3_use_data
  benchmark_spec.mode = FLAGS.inception3_mode
  benchmark_spec.train_steps_per_eval = FLAGS.inception3_train_steps_per_eval
  benchmark_spec.save_checkpoints_secs = FLAGS.inception3_save_checkpoints_secs
  benchmark_spec.train_batch_size = FLAGS.inception3_train_batch_size
  benchmark_spec.eval_batch_size = FLAGS.inception3_eval_batch_size
  benchmark_spec.commit = cloud_tpu_models.GetCommit(benchmark_spec.vms[0])


def Prepare(benchmark_spec):
  """Install and set up Inception V3 on the target vm.

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
      'master': benchmark_spec.master,
      'tpu': benchmark_spec.tpu,
      'learning_rate': benchmark_spec.learning_rate,
      'train_steps': benchmark_spec.train_steps,
      'iterations': benchmark_spec.iterations,
      'use_tpu': benchmark_spec.use_tpu,
      'use_data': benchmark_spec.use_data,
      'mode': benchmark_spec.mode,
      'train_steps_per_eval': benchmark_spec.train_steps_per_eval,
      'data_dir': benchmark_spec.data_dir,
      'model_dir': benchmark_spec.model_dir,
      'save_checkpoints_secs': benchmark_spec.save_checkpoints_secs,
      'train_batch_size': benchmark_spec.train_batch_size,
      'eval_batch_size': benchmark_spec.eval_batch_size,
      'commit': benchmark_spec.commit,
      'num_shards': benchmark_spec.num_shards
  }
  return metadata


def Run(benchmark_spec):
  """Run Inception V3 on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vm = benchmark_spec.vms[0]
  inception3_benchmark_script = (
      'tpu/models/experimental/inception/inception_v3.py')
  inception3_benchmark_cmd = (
      'python {script} '
      '--tpu={tpu} '
      '--learning_rate={learning_rate} '
      '--train_steps={train_steps} '
      '--iterations={iterations} '
      '--use_tpu={use_tpu} '
      '--use_data={use_data} '
      '--mode={mode} '
      '--train_steps_per_eval={train_steps_per_eval} '
      '--data_dir={data_dir} '
      '--model_dir={model_dir} '
      '--save_checkpoints_secs={save_checkpoints_secs} '
      '--train_batch_size={train_batch_size} '
      '--eval_batch_size={eval_batch_size} '
      '--num_shards={num_shards}'.format(
          script=inception3_benchmark_script,
          tpu=benchmark_spec.tpu,
          learning_rate=benchmark_spec.learning_rate,
          train_steps=benchmark_spec.train_steps,
          iterations=benchmark_spec.iterations,
          use_tpu=benchmark_spec.use_tpu,
          use_data=benchmark_spec.use_data,
          mode=benchmark_spec.mode,
          train_steps_per_eval=benchmark_spec.train_steps_per_eval,
          data_dir=benchmark_spec.data_dir,
          model_dir=benchmark_spec.model_dir,
          save_checkpoints_secs=benchmark_spec.save_checkpoints_secs,
          train_batch_size=benchmark_spec.train_batch_size,
          eval_batch_size=benchmark_spec.eval_batch_size,
          num_shards=benchmark_spec.num_shards))
  if FLAGS.tf_device == 'gpu':
    inception3_benchmark_cmd = '{env} {cmd}'.format(
        env=tensorflow.GetEnvironmentVars(vm), cmd=inception3_benchmark_cmd)
  stdout, stderr = vm.RobustRemoteCommand(inception3_benchmark_cmd,
                                          should_log=True)
  return mnist_benchmark.MakeSamplesFromOutput(
      _CreateMetadataDict(benchmark_spec), stdout + stderr)


def Cleanup(benchmark_spec):
  """Cleanup Inception V3 on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  mnist_benchmark.Cleanup(benchmark_spec)
