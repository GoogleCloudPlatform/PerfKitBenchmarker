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

"""Run ResNet benchmarks.

Tutorials: https://cloud.google.com/tpu/docs/tutorials/resnet
Code: https://github.com/tensorflow/tpu/tree/master/models/official/resnet
This benchmark is equivalent to tensorflow_benchmark with the resnet model
except that this can target TPU.
"""

import datetime
import time
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import mnist_benchmark
from perfkitbenchmarker.linux_packages import cloud_tpu_models
from perfkitbenchmarker.linux_packages import tensorflow

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'resnet'
BENCHMARK_CONFIG = """
resnet:
  description: Runs ResNet Benchmark.
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

flags.DEFINE_enum('resnet_depth', '50', ['18', '34', '50', '101', '152', '200'],
                  'Depth of ResNet model to use. Deeper models require more '
                  'training time and more memory and may require reducing '
                  '--resnet_train_batch_size to prevent running out of memory.')
flags.DEFINE_enum('resnet_mode', 'train_and_eval',
                  ['train', 'eval', 'train_and_eval'],
                  'Mode to run: train, eval, train_and_eval')
flags.DEFINE_integer('resnet_train_epochs', 90,
                     'The Number of epochs to use for training.', lower_bound=1)
flags.DEFINE_integer('resnet_train_batch_size', 1024,
                     'Global (not per-shard) batch size for training')
flags.DEFINE_integer('resnet_eval_batch_size', 1024,
                     'Global (not per-shard) batch size for evaluation')
flags.DEFINE_enum('resnet_data_format', 'channels_last',
                  ['channels_first', 'channels_last'],
                  'A flag to override the data format used in the model. The '
                  'value is either channels_first or channels_last. To run the '
                  'network on CPU or TPU, channels_last should be used. For GPU'
                  ', channels_first will improve performance.')
flags.DEFINE_bool('resnet_skip_host_call', False, 'Skip the host_call which is '
                  'executed every training step. This is generally used for '
                  'generating training summaries (train loss, learning rate, '
                  'etc...). When --skip_host_call=false, there could be a '
                  'performance drop if host_call function is slow and cannot '
                  'keep up with the TPU-side computation.')
flags.DEFINE_integer(
    'resnet_epochs_per_eval', 2, 'Controls how often evaluation is performed.'
    ' Since evaluation is fairly expensive, it is advised to evaluate as '
    'infrequently as possible (i.e. up to --train_steps, which evaluates the '
    'model only after finishing the entire training regime).', lower_bound=2)


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
  benchmark_spec.depth = FLAGS.resnet_depth
  benchmark_spec.mode = FLAGS.resnet_mode
  benchmark_spec.train_batch_size = FLAGS.resnet_train_batch_size
  benchmark_spec.eval_batch_size = FLAGS.resnet_eval_batch_size
  benchmark_spec.data_format = FLAGS.resnet_data_format
  benchmark_spec.commit = cloud_tpu_models.GetCommit(benchmark_spec.vms[0])
  benchmark_spec.skip_host_call = FLAGS.resnet_skip_host_call
  benchmark_spec.data_dir = FLAGS.imagenet_data_dir
  benchmark_spec.num_train_images = FLAGS.imagenet_num_train_images
  benchmark_spec.num_eval_images = FLAGS.imagenet_num_eval_images
  benchmark_spec.num_examples_per_epoch = (
      float(benchmark_spec.num_train_images) / benchmark_spec.train_batch_size)
  benchmark_spec.train_epochs = FLAGS.resnet_train_epochs
  benchmark_spec.train_steps = int(
      benchmark_spec.train_epochs * benchmark_spec.num_examples_per_epoch)
  benchmark_spec.epochs_per_eval = FLAGS.resnet_epochs_per_eval
  benchmark_spec.steps_per_eval = int(
      benchmark_spec.epochs_per_eval * benchmark_spec.num_examples_per_epoch)


def Prepare(benchmark_spec):
  """Install and set up ResNet on the target vm.

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
      'tpu_train': benchmark_spec.tpu_train,
      'tpu_eval': benchmark_spec.tpu_eval,
      'data_dir': benchmark_spec.data_dir,
      'model_dir': benchmark_spec.model_dir,
      'depth': benchmark_spec.depth,
      'mode': benchmark_spec.mode,
      'train_steps': benchmark_spec.train_steps,
      'train_batch_size': benchmark_spec.train_batch_size,
      'eval_batch_size': benchmark_spec.eval_batch_size,
      'iterations': benchmark_spec.iterations,
      'num_shards': benchmark_spec.num_shards,
      'num_shards_train': benchmark_spec.num_shards_train,
      'num_shards_eval': benchmark_spec.num_shards_eval,
      'data_format': benchmark_spec.data_format,
      'precision': benchmark_spec.precision,
      'commit': benchmark_spec.commit,
      'skip_host_call': benchmark_spec.skip_host_call,
      'num_train_images': benchmark_spec.num_train_images,
      'num_eval_images': benchmark_spec.num_eval_images,
      'train_epochs': benchmark_spec.train_epochs,
      'num_examples_per_epoch': benchmark_spec.num_examples_per_epoch,
      'epochs_per_eval': benchmark_spec.epochs_per_eval,
      'steps_per_eval': benchmark_spec.steps_per_eval
  }
  return metadata


def _ParseDateTime(wall_time):
  """Parse date and time from output log.

  Args:
    wall_time: date and time from output log

  Example: 0626 15:10:23.018357

  Returns:
    datetime
  """
  if wall_time:
    current_date = datetime.datetime.now()
    current_month = current_date.month
    run_month = wall_time[0:2]
    if run_month == '12' and current_month == '01':
      year = current_date.year - 1
    else:
      year = current_date.year
    return datetime.datetime.strptime(
        '{year}{datetime}'.format(year=year, datetime=wall_time),
        '%Y%m%d %H:%M:%S.%f')


def MakeSamplesFromEvalOutput(metadata, output, elapsed_seconds):
  """Create a sample containing evaluation metrics.

  Args:
    metadata: dict contains all the metadata that reports.
    output: string, command output
    elapsed_seconds: float, elapsed seconds from saved checkpoint.

  Example output:
    perfkitbenchmarker/tests/linux_benchmarks/resnet_benchmark_test.py

  Returns:
    a Sample containing evaluation metrics
  """
  pattern = (r'Saving dict for global step \d+: global_step = (\d+), '
             r'loss = (\d+\.\d+), top_1_accuracy = (\d+\.\d+), '
             r'top_5_accuracy = (\d+\.\d+)')
  step, loss, top_1_accuracy, top_5_accuracy = (
      regex_util.ExtractExactlyOneMatch(pattern, output))
  metadata_copy = metadata.copy()
  step = int(step)
  metadata_copy['step'] = step
  num_examples_per_epoch = metadata['num_examples_per_epoch']
  metadata_copy['epoch'] = step / num_examples_per_epoch
  metadata_copy['elapsed_seconds'] = elapsed_seconds
  return [sample.Sample('Eval Loss', float(loss), '', metadata_copy),
          # In the case of top-1 score, the trained model checks if the top
          # class (the one having the highest probability) is the same as the
          # target label. In the case of top-5 score, the trained model checks
          # if the target label is one of your top 5 predictions (the 5 ones
          # with the highest probabilities).
          sample.Sample('Top 1 Accuracy', float(top_1_accuracy) * 100, '%',
                        metadata_copy),
          sample.Sample('Top 5 Accuracy', float(top_5_accuracy) * 100, '%',
                        metadata_copy)]


def Run(benchmark_spec):
  """Run ResNet on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vm = benchmark_spec.vms[0]
  resnet_benchmark_script = 'resnet_main.py'
  resnet_benchmark_cmd = (
      '{env_cmd} && cd tpu/models/official/resnet && '
      'python {script} '
      '--use_tpu={use_tpu} '
      '--data_dir={data_dir} '
      '--model_dir={model_dir} '
      '--resnet_depth={depth} '
      '--train_batch_size={train_batch_size} '
      '--eval_batch_size={eval_batch_size} '
      '--iterations_per_loop={iterations} '
      '--data_format={data_format} '
      '--precision={precision} '
      '--skip_host_call={skip_host_call} '
      '--num_train_images={num_train_images} '
      '--num_eval_images={num_eval_images}'.format(
          env_cmd=benchmark_spec.env_cmd,
          script=resnet_benchmark_script,
          use_tpu=benchmark_spec.use_tpu,
          data_dir=benchmark_spec.data_dir,
          model_dir=benchmark_spec.model_dir,
          depth=benchmark_spec.depth,
          train_batch_size=benchmark_spec.train_batch_size,
          eval_batch_size=benchmark_spec.eval_batch_size,
          iterations=benchmark_spec.iterations,
          data_format=benchmark_spec.data_format,
          precision=benchmark_spec.precision,
          skip_host_call=benchmark_spec.skip_host_call,
          num_train_images=benchmark_spec.num_train_images,
          num_eval_images=benchmark_spec.num_eval_images
      ))
  if FLAGS.tf_device == 'gpu':
    resnet_benchmark_cmd = '{env} {cmd}'.format(
        env=tensorflow.GetEnvironmentVars(vm), cmd=resnet_benchmark_cmd)
  samples = []
  metadata = _CreateMetadataDict(benchmark_spec)
  elapsed_seconds = 0
  steps_per_eval = benchmark_spec.steps_per_eval
  train_steps = benchmark_spec.train_steps
  for step in range(steps_per_eval, train_steps + steps_per_eval,
                    steps_per_eval):
    step = min(step, train_steps)
    resnet_benchmark_cmd_step = '{cmd} --train_steps={step}'.format(
        cmd=resnet_benchmark_cmd, step=step)
    if benchmark_spec.mode in ('train', 'train_and_eval'):
      resnet_benchmark_train_cmd = (
          '{cmd} --tpu={tpu} --mode=train --num_cores={num_cores}'.format(
              cmd=resnet_benchmark_cmd_step,
              tpu=benchmark_spec.tpu_train,
              num_cores=benchmark_spec.num_shards_train))
      start = time.time()
      stdout, stderr = vm.RobustRemoteCommand(resnet_benchmark_train_cmd,
                                              should_log=True)
      elapsed_seconds += (time.time() - start)
      samples.extend(mnist_benchmark.MakeSamplesFromTrainOutput(
          metadata, stdout + stderr, elapsed_seconds, step))
    if benchmark_spec.mode in ('train_and_eval', 'eval'):
      resnet_benchmark_eval_cmd = (
          '{cmd} --tpu={tpu} --mode=eval --num_cores={num_cores}'.format(
              cmd=resnet_benchmark_cmd_step,
              tpu=benchmark_spec.tpu_eval,
              num_cores=benchmark_spec.num_shards_eval))
      stdout, stderr = vm.RobustRemoteCommand(resnet_benchmark_eval_cmd,
                                              should_log=True)
      samples.extend(MakeSamplesFromEvalOutput(
          metadata, stdout + stderr, elapsed_seconds))
  return samples


def Cleanup(benchmark_spec):
  """Cleanup ResNet on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  mnist_benchmark.Cleanup(benchmark_spec)
