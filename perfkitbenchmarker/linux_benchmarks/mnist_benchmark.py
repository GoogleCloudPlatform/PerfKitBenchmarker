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

flags.DEFINE_string('mnist_train_file',
                    'gs://tfrc-test-bucket/mnist-records/train.tfrecords',
                    'mnist train file for tensorflow')
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
  benchmark_spec.train_file = FLAGS.mnist_train_file
  benchmark_spec.use_tpu = benchmark_spec.cloud_tpu is not None
  benchmark_spec.train_steps = FLAGS.mnist_train_steps
  if benchmark_spec.use_tpu:
    benchmark_spec.master = 'grpc://{ip}:{port}'.format(
        ip=benchmark_spec.cloud_tpu.GetCloudTpuIp(),
        port=benchmark_spec.cloud_tpu.GetCloudTpuPort())
  else:
    benchmark_spec.master = ''
  benchmark_spec.tpu = benchmark_spec.master
  benchmark_spec.iterations = FLAGS.tpu_iterations
  benchmark_spec.gcp_service_account = FLAGS.gcp_service_account


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


def _CreateMetadataDict(benchmark_spec):
  """Create metadata dict to be used in run results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    metadata dict
  """
  return {
      'train_file': benchmark_spec.train_file,
      'use_tpu': benchmark_spec.use_tpu,
      'model_dir': benchmark_spec.model_dir,
      'train_steps': benchmark_spec.train_steps,
      'master': benchmark_spec.master,
      'tpu': benchmark_spec.tpu,
      'commit': cloud_tpu_models.GetCommit(benchmark_spec.vms[0]),
      'iterations': benchmark_spec.iterations
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
  mnist_benchmark_script = 'tpu/cloud_tpu/models/mnist/mnist.py'
  mnist_benchmark_cmd = (
      'python {script} '
      '--master={master} '
      '--train_file={train_file} '
      '--use_tpu={use_tpu} '
      '--train_steps={train_steps} '
      '--iterations={iterations} '
      '--model_dir={model_dir}'.format(
          script=mnist_benchmark_script,
          master=benchmark_spec.master,
          train_file=benchmark_spec.train_file,
          use_tpu=benchmark_spec.use_tpu,
          train_steps=benchmark_spec.train_steps,
          iterations=benchmark_spec.iterations,
          model_dir=benchmark_spec.model_dir))
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
