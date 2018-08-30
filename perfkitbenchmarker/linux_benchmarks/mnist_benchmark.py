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
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import cloud_tpu_models
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import tensorflow
from perfkitbenchmarker.providers.gcp import gcs

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
flags.DEFINE_integer('mnist_train_steps', 2000,
                     'Total number of training steps')
flags.DEFINE_integer('tpu_iterations', 500,
                     'Number of iterations per TPU training loop.')


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
  benchmark_spec.use_tpu = True if benchmark_spec.tpus else False
  benchmark_spec.train_steps = FLAGS.mnist_train_steps
  benchmark_spec.tpu_train = ''
  benchmark_spec.tpu_eval = ''
  benchmark_spec.num_shards_train = FLAGS.tpu_cores_per_donut
  benchmark_spec.num_shards_eval = FLAGS.tpu_cores_per_donut
  if benchmark_spec.use_tpu:
    tpu_groups = benchmark_spec.tpu_groups
    if 'train' in tpu_groups:
      tpu_train = tpu_groups['train']
      benchmark_spec.tpu_train = tpu_train.GetName()
      benchmark_spec.num_shards_train = tpu_train.GetNumShards()
    if 'eval' in tpu_groups:
      tpu_eval = tpu_groups['eval']
      benchmark_spec.tpu_eval = tpu_eval.GetName()
      benchmark_spec.num_shards_eval = tpu_eval.GetNumShards()
  benchmark_spec.tpu = benchmark_spec.tpu_train
  benchmark_spec.iterations = FLAGS.tpu_iterations
  benchmark_spec.gcp_service_account = FLAGS.gcp_service_account
  benchmark_spec.num_shards = benchmark_spec.num_shards_train


def Prepare(benchmark_spec):
  """Install and set up MNIST on the target vm.

  Args:
    benchmark_spec: The benchmark specification
  """
  benchmark_spec.always_call_cleanup = True
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vm = benchmark_spec.vms[0]
  if not benchmark_spec.use_tpu:
    vm.Install('tensorflow')
  vm.Install('cloud_tpu_models')
  vm.RemoteCommand('git clone https://github.com/tensorflow/models.git',
                   should_log=True)
  if benchmark_spec.use_tpu:
    storage_service = gcs.GoogleCloudStorageService()
    storage_service.PrepareVM(vm)
    benchmark_spec.storage_service = storage_service
    model_dir = 'gs://{}'.format(FLAGS.run_uri)
    benchmark_spec.model_dir = model_dir
    vm.RemoteCommand(
        '{gsutil} mb {model_dir}'.format(
            gsutil=vm.gsutil_path,
            model_dir=benchmark_spec.model_dir), should_log=True)
    vm.RemoteCommand(
        '{gsutil} acl ch -u {service_account}:W {model_dir}'.format(
            gsutil=vm.gsutil_path,
            service_account=benchmark_spec.gcp_service_account,
            model_dir=benchmark_spec.model_dir), should_log=True)
  else:
    benchmark_spec.model_dir = '/tmp'

  if FLAGS.imagenet_data_dir and FLAGS.cloud != 'GCP':
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


def _CreateMetadataDict(benchmark_spec):
  """Create metadata dict to be used in run results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    metadata dict
  """
  return {
      'data_dir': benchmark_spec.data_dir,
      'use_tpu': benchmark_spec.use_tpu,
      'model_dir': benchmark_spec.model_dir,
      'train_steps': benchmark_spec.train_steps,
      'tpu': benchmark_spec.tpu,
      'tpu_train': benchmark_spec.tpu_train,
      'tpu_eval': benchmark_spec.tpu_eval,
      'commit': cloud_tpu_models.GetCommit(benchmark_spec.vms[0]),
      'iterations': benchmark_spec.iterations,
      'num_shards': benchmark_spec.num_shards
  }


def _ExtractThroughput(regex, output, metadata, metric, unit):
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


def MakeSamplesFromOutput(metadata, output):
  """Create a sample continaing the measured MNIST throughput.

  Args:
    metadata: dict contains all the metadata that reports.
    output: MNIST output

  Returns:
    a Sample containing the MNIST throughput
  """
  samples = _ExtractThroughput(r'global_step/sec: (\S+)', output, metadata,
                               'Global Steps Per Second', 'global_steps/sec')
  if metadata['use_tpu']:
    samples.extend(_ExtractThroughput(r'examples/sec: (\S+)', output, metadata,
                                      'Examples Per Second', 'examples/sec'))
  return samples


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
      '--tpu={tpu} '
      '--data_dir={data_dir} '
      '--use_tpu={use_tpu} '
      '--train_steps={train_steps} '
      '--iterations={iterations} '
      '--model_dir={model_dir} '
      '--num_shards={num_shards}'.format(
          script=mnist_benchmark_script,
          tpu=benchmark_spec.tpu,
          data_dir=benchmark_spec.data_dir,
          use_tpu=benchmark_spec.use_tpu,
          train_steps=benchmark_spec.train_steps,
          iterations=benchmark_spec.iterations,
          model_dir=benchmark_spec.model_dir,
          num_shards=benchmark_spec.num_shards))
  if cuda_toolkit.CheckNvidiaGpuExists(vm):
    mnist_benchmark_cmd = '{env} {cmd}'.format(
        env=tensorflow.GetEnvironmentVars(vm), cmd=mnist_benchmark_cmd)
  stdout, stderr = vm.RobustRemoteCommand(mnist_benchmark_cmd,
                                          should_log=True)
  return MakeSamplesFromOutput(_CreateMetadataDict(benchmark_spec),
                               stdout + stderr)


def Cleanup(benchmark_spec):
  """Cleanup MNIST on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  if benchmark_spec.use_tpu:
    vm = benchmark_spec.vms[0]
    vm.RemoteCommand(
        '{gsutil} rm -r {model_dir}'.format(
            gsutil=vm.gsutil_path,
            model_dir=benchmark_spec.model_dir), should_log=True)
    benchmark_spec.storage_service.CleanupVM(vm)
