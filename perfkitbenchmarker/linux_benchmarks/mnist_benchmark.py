# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Run MNIST benchmarks."""

import copy
import os
import time
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import cloud_tpu_models
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import tensorflow
from perfkitbenchmarker.providers.gcp import gcs
from perfkitbenchmarker.providers.gcp import util


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'mnist'
BENCHMARK_CONFIG = """
mnist:
  description: Runs MNIST Benchmark.
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
GCP_ENV = 'PATH=/tmp/pkb/google-cloud-sdk/bin:$PATH'

flags.DEFINE_string('mnist_data_dir', None, 'mnist train file for tensorflow')
flags.DEFINE_string('imagenet_data_dir',
                    'gs://cloud-tpu-test-datasets/fake_imagenet',
                    'Directory where the input data is stored')
flags.DEFINE_string(
    't2t_data_dir', None,
    'Directory where the input data is stored for tensor2tensor')
flags.DEFINE_integer('imagenet_num_train_images', 1281167,
                     'Size of ImageNet training data set.')
flags.DEFINE_integer('imagenet_num_eval_images', 50000,
                     'Size of ImageNet validation data set.')
flags.DEFINE_integer('mnist_num_train_images', 55000,
                     'Size of MNIST training data set.')
flags.DEFINE_integer('mnist_num_eval_images', 5000,
                     'Size of MNIST validation data set.')
flags.DEFINE_integer('mnist_train_epochs', 37,
                     'Total number of training echos', lower_bound=1)
flags.DEFINE_integer('mnist_eval_epochs', 1,
                     'Total number of evaluation epochs. If `0`, evaluation '
                     'after training is skipped.')
flags.DEFINE_integer('tpu_iterations', 500,
                     'Number of iterations per TPU training loop.')
flags.DEFINE_integer('mnist_batch_size', 1024,
                     'Mini-batch size for the training. Note that this '
                     'is the global batch size and not the per-shard batch.')
flags.DEFINE_enum('tpu_precision', 'bfloat16', ['bfloat16', 'float32'],
                  'Precision to use')

EXAMPLES_PER_SECOND_PRECISION = 0.01


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
  benchmark_spec.data_dir = FLAGS.mnist_data_dir
  benchmark_spec.iterations = FLAGS.tpu_iterations
  benchmark_spec.gcp_service_account = FLAGS.gcp_service_account
  benchmark_spec.batch_size = FLAGS.mnist_batch_size
  benchmark_spec.num_train_images = FLAGS.mnist_num_train_images
  benchmark_spec.num_eval_images = FLAGS.mnist_num_eval_images
  benchmark_spec.num_examples_per_epoch = (
      float(benchmark_spec.num_train_images) / benchmark_spec.batch_size)
  benchmark_spec.train_epochs = FLAGS.mnist_train_epochs
  benchmark_spec.train_steps = int(
      benchmark_spec.train_epochs * benchmark_spec.num_examples_per_epoch)
  benchmark_spec.eval_epochs = FLAGS.mnist_eval_epochs
  benchmark_spec.eval_steps = int(
      benchmark_spec.eval_epochs * benchmark_spec.num_examples_per_epoch)
  benchmark_spec.precision = FLAGS.tpu_precision
  benchmark_spec.env_cmd = 'export PYTHONPATH=$PYTHONPATH:$PWD/tpu/models'


def Prepare(benchmark_spec):
  """Install and set up MNIST on the target vm.

  Args:
    benchmark_spec: The benchmark specification
  """
  benchmark_spec.always_call_cleanup = True
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vm = benchmark_spec.vms[0]
  if not benchmark_spec.tpus:
    vm.Install('tensorflow')
  vm.Install('cloud_tpu_models')
  vm.RemoteCommand('git clone https://github.com/tensorflow/models.git',
                   should_log=True)
  if benchmark_spec.tpus:
    storage_service = gcs.GoogleCloudStorageService()
    storage_service.PrepareVM(vm)
    benchmark_spec.storage_service = storage_service
    model_dir = 'gs://{}'.format(FLAGS.run_uri)
    benchmark_spec.model_dir = model_dir
    vm.RemoteCommand(
        '{gsutil} mb -c regional -l {location} {model_dir}'.format(
            gsutil=vm.gsutil_path,
            location=util.GetRegionFromZone(
                benchmark_spec.tpu_groups['train'].GetZone()),
            model_dir=benchmark_spec.model_dir), should_log=True)
    vm.RemoteCommand(
        '{gsutil} acl ch -u {service_account}:W {model_dir}'.format(
            gsutil=vm.gsutil_path,
            service_account=benchmark_spec.gcp_service_account,
            model_dir=benchmark_spec.model_dir), should_log=True)
  else:
    benchmark_spec.model_dir = '/tmp'

  if (FLAGS.imagenet_data_dir or FLAGS.t2t_data_dir) and FLAGS.cloud != 'GCP':
    vm.Install('google_cloud_sdk')
    vm.RemoteCommand('echo "export {}" >> ~/.bashrc'.format(GCP_ENV),
                     login_shell=True)
    credential_path = os.path.join('~', '.config', 'gcloud')
    vm.RemoteCommand('mkdir -p {}'.format(credential_path), login_shell=True)
    credential_file = os.path.join(credential_path,
                                   'application_default_credentials.json')
    vm.PushFile(FLAGS.gcp_credential, credential_file)
    vm.RemoteCommand('{env} gcloud auth '
                     'activate-service-account --key-file {key_file}'.format(
                         env=GCP_ENV, key_file=credential_file),
                     login_shell=True)


def CreateMetadataDict(benchmark_spec):
  """Create metadata dict to be used in run results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    metadata dict
  """
  return {
      'data_dir': benchmark_spec.data_dir,
      'use_tpu': bool(benchmark_spec.tpus),
      'model_dir': benchmark_spec.model_dir,
      'train_steps': benchmark_spec.train_steps,
      'eval_steps': benchmark_spec.eval_steps,
      'commit': cloud_tpu_models.GetCommit(benchmark_spec.vms[0]),
      'iterations': benchmark_spec.iterations,
      'train_tpu_num_shards': (
          benchmark_spec.tpu_groups['train'].GetNumShards() if
          benchmark_spec.tpus else ''),
      'train_tpu_accelerator_type': (
          benchmark_spec.tpu_groups['train'].GetAcceleratorType() if
          benchmark_spec.tpus else ''),
      'num_train_images': benchmark_spec.num_train_images,
      'num_eval_images': benchmark_spec.num_eval_images,
      'train_epochs': benchmark_spec.train_epochs,
      'eval_epochs': benchmark_spec.eval_epochs,
      'num_examples_per_epoch': benchmark_spec.num_examples_per_epoch,
      'train_batch_size': benchmark_spec.batch_size,
      'eval_batch_size': benchmark_spec.batch_size
  }


def ExtractThroughput(regex, output, metadata, metric, unit):
  """Extract throughput from MNIST output.

  Args:
    regex: string. Regular expression.
    output: MNIST output
    metadata: dict. Additional metadata to include with the sample.
    metric: string. Name of the metric within the benchmark.
    unit: string. Units for 'value'.

  Returns:
    samples containing the throughput
  """
  matches = regex_util.ExtractAllMatches(regex, output)
  samples = []
  for index, value in enumerate(matches):
    metadata_with_index = copy.deepcopy(metadata)
    metadata_with_index['index'] = index
    samples.append(sample.Sample(metric, float(value), unit,
                                 metadata_with_index))
  return samples


def MakeSamplesFromTrainOutput(metadata, output, elapsed_seconds, step):
  """Create a sample containing training metrics.

  Args:
    metadata: dict contains all the metadata that reports.
    output: string, command output
    elapsed_seconds: float, elapsed seconds from saved checkpoint.
    step: int, the global steps in the training process.

  Example output:
    perfkitbenchmarker/tests/linux_benchmarks/mnist_benchmark_test.py

  Returns:
    a Sample containing training metrics, current step, elapsed seconds
  """
  samples = []
  metadata_copy = metadata.copy()
  metadata_copy['step'] = int(step)
  metadata_copy['epoch'] = step / metadata['num_examples_per_epoch']
  metadata_copy['elapsed_seconds'] = elapsed_seconds

  get_mean = lambda matches: sum(float(x) for x in matches) / len(matches)
  loss = get_mean(regex_util.ExtractAllMatches(
      r'Loss for final step: (\d+\.\d+)', output))
  samples.append(sample.Sample('Loss', float(loss), '', metadata_copy))

  if 'global_step/sec: ' in output:
    global_step_sec = get_mean(regex_util.ExtractAllMatches(
        r'global_step/sec: (\S+)', output))
    samples.append(sample.Sample(
        'Global Steps Per Second', global_step_sec,
        'global_steps/sec', metadata_copy))
    examples_sec = global_step_sec * metadata['train_batch_size']
    if 'examples/sec: ' in output:
      examples_sec_log = get_mean(regex_util.ExtractAllMatches(
          r'examples/sec: (\S+)', output))
      precision = abs(examples_sec_log - examples_sec) / examples_sec_log
      assert precision < EXAMPLES_PER_SECOND_PRECISION, 'examples/sec is wrong.'
      examples_sec = examples_sec_log
    samples.append(sample.Sample('Examples Per Second', examples_sec,
                                 'examples/sec', metadata_copy))
  return samples


def MakeSamplesFromEvalOutput(metadata, output, elapsed_seconds):
  """Create a sample containing evaluation metrics.

  Args:
    metadata: dict contains all the metadata that reports.
    output: string, command output
    elapsed_seconds: float, elapsed seconds from saved checkpoint.

  Example output:
    perfkitbenchmarker/tests/linux_benchmarks/mnist_benchmark_test.py

  Returns:
    a Sample containing evaluation metrics
  """
  pattern = (r'Saving dict for global step \d+: accuracy = (\d+\.\d+), '
             r'global_step = (\d+), loss = (\d+\.\d+)')
  accuracy, step, loss = regex_util.ExtractAllMatches(pattern, output).pop()
  metadata_copy = metadata.copy()
  step = int(step)
  metadata_copy['step'] = step
  num_examples_per_epoch = metadata['num_examples_per_epoch']
  metadata_copy['epoch'] = step / num_examples_per_epoch
  metadata_copy['elapsed_seconds'] = elapsed_seconds
  return [sample.Sample('Eval Loss', float(loss), '', metadata_copy),
          sample.Sample('Accuracy', float(accuracy) * 100, '%', metadata_copy)]


def Run(benchmark_spec):
  """Run MNIST on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vm = benchmark_spec.vms[0]
  mnist_benchmark_script = 'mnist_tpu.py'
  mnist_benchmark_cmd = (
      'cd models/official/mnist && '
      'python {script} '
      '--data_dir={data_dir} '
      '--iterations={iterations} '
      '--model_dir={model_dir} '
      '--batch_size={batch_size}'.format(
          script=mnist_benchmark_script,
          data_dir=benchmark_spec.data_dir,
          iterations=benchmark_spec.iterations,
          model_dir=benchmark_spec.model_dir,
          batch_size=benchmark_spec.batch_size))
  if cuda_toolkit.CheckNvidiaGpuExists(vm):
    mnist_benchmark_cmd = '{env} {cmd}'.format(
        env=tensorflow.GetEnvironmentVars(vm), cmd=mnist_benchmark_cmd)
  samples = []
  metadata = CreateMetadataDict(benchmark_spec)
  if benchmark_spec.train_steps:
    mnist_benchmark_train_cmd = (
        '{cmd} --tpu={tpu} --use_tpu={use_tpu} --train_steps={train_steps} '
        '--num_shards={num_shards} --noenable_predict'.format(
            cmd=mnist_benchmark_cmd,
            tpu=(benchmark_spec.tpu_groups['train'].GetName() if
                 benchmark_spec.tpus else ''),
            use_tpu=True if benchmark_spec.tpus else False,
            train_steps=benchmark_spec.train_steps,
            num_shards=(benchmark_spec.tpu_groups['train'].GetNumShards() if
                        benchmark_spec.tpus else 0)))
    start = time.time()
    stdout, stderr = vm.RobustRemoteCommand(mnist_benchmark_train_cmd,
                                            should_log=True)
    elapsed_seconds = (time.time() - start)
    samples.extend(MakeSamplesFromTrainOutput(
        metadata, stdout + stderr, elapsed_seconds, benchmark_spec.train_steps))
  if benchmark_spec.eval_steps:
    mnist_benchmark_eval_cmd = (
        '{cmd} --tpu="" --use_tpu=False --eval_steps={eval_steps}'.format(
            cmd=mnist_benchmark_cmd, eval_steps=benchmark_spec.eval_steps))
    stdout, stderr = vm.RobustRemoteCommand(mnist_benchmark_eval_cmd,
                                            should_log=True)
    samples.extend(MakeSamplesFromEvalOutput(metadata, stdout + stderr,
                                             elapsed_seconds))
  return samples


def Cleanup(benchmark_spec):
  """Cleanup MNIST on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  if benchmark_spec.tpus:
    vm = benchmark_spec.vms[0]
    vm.RemoteCommand(
        '{gsutil} rm -r {model_dir}'.format(
            gsutil=vm.gsutil_path,
            model_dir=benchmark_spec.model_dir), should_log=True)
    benchmark_spec.storage_service.CleanupVM(vm)
