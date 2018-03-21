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
flags.DEFINE_string('mnist_model_dir', None, 'Estimator model directory')
flags.DEFINE_integer('mnist_train_steps', 2000,
                     'Total number of training steps')


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
  benchmark_spec.model_dir = FLAGS.mnist_model_dir and os.path.join(
      FLAGS.mnist_model_dir, FLAGS.run_uri)
  benchmark_spec.train_steps = FLAGS.mnist_train_steps
  benchmark_spec.master = 'grpc://{ip}:{port}'.format(
      ip=benchmark_spec.cloud_tpu.GetCloudTpuIp(),
      port=benchmark_spec.cloud_tpu.GetCloudTpuPort()
  ) if benchmark_spec.use_tpu else ''


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


def _CreateMetadataDict(benchmark_spec):
  """Create metadata dict to be used in run results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    metadata dict
  """
  metadata = dict()
  metadata['train_file'] = benchmark_spec.train_file
  metadata['use_tpu'] = benchmark_spec.use_tpu
  metadata['model_dir'] = benchmark_spec.model_dir
  metadata['train_steps'] = benchmark_spec.train_steps
  metadata['master'] = benchmark_spec.master
  vm = benchmark_spec.vms[0]
  metadata['commit'] = cloud_tpu_models.GetCommit(vm)
  return metadata


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
  mnist_benchmark_dir = 'tpu/cloud_tpu/models/mnist'
  mnist_benchmark_cmd = (
      'python mnist.py --master={master} --train_file={train_file} '
      '--use_tpu={use_tpu} '
      '--train_steps={train_steps}'.format(
          master=benchmark_spec.master,
          train_file=benchmark_spec.train_file,
          use_tpu=benchmark_spec.use_tpu,
          train_steps=benchmark_spec.train_steps))
  if benchmark_spec.model_dir:
    mnist_benchmark_cmd = '{cmd} --model_dir {model_dir}'.format(
        cmd=mnist_benchmark_cmd, model_dir=benchmark_spec.model_dir)
  if cuda_toolkit.CheckNvidiaGpuExists(vm):
    mnist_benchmark_cmd = '%s %s' % (tensorflow.GetEnvironmentVars(vm),
                                     mnist_benchmark_cmd)
  run_command = 'cd %s && %s' % (mnist_benchmark_dir, mnist_benchmark_cmd)
  stdout, stderr = vm.RobustRemoteCommand(run_command, should_log=True)
  return MakeSamplesFromOutput(_CreateMetadataDict(benchmark_spec),
                               stdout + stderr)


def Cleanup(unused_benchmark_spec):
  """Cleanup MNIST on the cluster."""
  pass
