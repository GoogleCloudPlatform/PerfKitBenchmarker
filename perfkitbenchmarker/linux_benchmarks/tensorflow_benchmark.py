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

"""Run Tensorflow benchmarks (https://github.com/tensorflow/benchmarks)."""

import logging
import os
import re
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import cuda_toolkit_8

FLAGS = flags.FLAGS

CUDA_TOOLKIT_INSTALL_DIR = cuda_toolkit_8.CUDA_TOOLKIT_INSTALL_DIR

BENCHMARK_NAME = 'tensorflow'
BENCHMARK_CONFIG = """
tensorflow:
  description: Runs Tensorflow Benchmark.
  vm_groups:
    default:
      vm_spec:
        GCP:
          image: ubuntu-1604-xenial-v20170307
          image_project: ubuntu-os-cloud
          machine_type: n1-standard-4
          gpu_type: k80
          gpu_count: 1
          zone: us-east1-d
          boot_disk_size: 200
        AWS:
          image: ami-a9d276c9
          machine_type: p2.xlarge
          zone: us-west-2b
          boot_disk_size: 200
        Azure:
          image: Canonical:UbuntuServer:16.04.0-LTS:latest
          machine_type: Standard_NC6
          zone: eastus
"""

flags.DEFINE_enum('tf_model', 'vgg16',
                  ['vgg11', 'vgg16', 'vgg19', 'lenet', 'googlenet', 'overfeat',
                   'alexnet', 'trivial', 'inception3', 'inception4', 'resnet50',
                   'resnet101', 'resnet152'], 'name of the model to run')
flags.DEFINE_enum('tf_data_name', 'imagenet', ['imagenet', 'flowers'],
                  'Name of dataset: imagenet or flowers.')
flags.DEFINE_integer('tf_batch_size', 64, 'batch size per compute device')
flags.DEFINE_enum('tf_variable_update', 'parameter_server',
                  ['parameter_server', 'replicated',
                   'distributed_replicated', 'independent'],
                  '''The method for managing variables: parameter_server,
                  replicated, distributed_replicated, independent''')
flags.DEFINE_enum('tf_local_parameter_device', 'gpu', ['cpu', 'gpu'],
                  '''Device to use as parameter server: cpu or gpu. For
                  distributed training, it can affect where caching of
                  variables happens.''')
flags.DEFINE_boolean('tf_use_nccl', True,
                     'Whether to use nccl all-reduce primitives where possible')
flags.DEFINE_boolean('tf_distortions', True,
                     '''Enable/disable distortions during image preprocessing.
                     These include bbox and color distortions.''')


class TFParseOutputException(Exception):
  pass


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
  benchmark_spec.model = FLAGS.tf_model
  benchmark_spec.data_name = FLAGS.tf_data_name
  benchmark_spec.batch_size = FLAGS.tf_batch_size
  benchmark_spec.variable_update = FLAGS.tf_variable_update
  benchmark_spec.local_parameter_device = FLAGS.tf_local_parameter_device
  benchmark_spec.use_nccl = FLAGS.tf_use_nccl
  benchmark_spec.distortions = FLAGS.tf_distortions


def Prepare(benchmark_spec):
  """Install and set up TensorFlow on the target vm.

  Args:
    benchmark_spec: The benchmark specification
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vms = benchmark_spec.vms
  master_vm = vms[0]
  logging.info('Installing CUDA Toolkit 8.0 on %s', master_vm)
  master_vm.Install('cuda_toolkit_8')
  benchmark_spec.num_gpus = cuda_toolkit_8.QueryNumberOfGpus(master_vm)
  master_vm.Install('cudnn')
  master_vm.Install('tensorflow')


def _CreateMetadataDict(benchmark_spec):
  """Create metadata dict to be used in run results.

  Args:
    benchmark_spec: benchmark spec

  Returns:
    metadata dict
  """
  metadata = dict()
  metadata.update(cuda_toolkit_8.GetMetadataFromFlags())
  metadata['model'] = benchmark_spec.model
  metadata['num_gpus'] = benchmark_spec.num_gpus
  metadata['data_name'] = benchmark_spec.data_name
  metadata['batch_size'] = benchmark_spec.batch_size
  metadata['variable_update'] = benchmark_spec.variable_update
  metadata['local_parameter_device'] = benchmark_spec.local_parameter_device
  metadata['use_nccl'] = benchmark_spec.use_nccl
  metadata['distortions'] = benchmark_spec.distortions
  return metadata


def _GetEnvironmentVars(vm):
  """Return a string containing TensorFlow-related environment variables.

  Args:
    vm: vm to get environment varibles

  Returns:
    string of environment variables
  """

  output, _ = vm.RemoteCommand('getconf LONG_BIT', should_log=True)
  long_bit = output.strip()
  lib_name = 'lib' if long_bit == '32' else 'lib64'
  return ' '.join([
      'PATH=%s${PATH:+:${PATH}}' %
      os.path.join(CUDA_TOOLKIT_INSTALL_DIR, 'bin'),
      'CUDA_HOME=%s' % CUDA_TOOLKIT_INSTALL_DIR,
      'LD_LIBRARY_PATH=%s${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}' %
      os.path.join(CUDA_TOOLKIT_INSTALL_DIR, lib_name),
  ])


def _ExtractThroughput(output):
  """Extract throughput from TensorFlow output.

  Args:
    output: TensorFlow output

  Returns:
    throuput (float)
  """
  regex = r'total images/sec: (\S+)'
  match = re.search(regex, output)
  try:
    return float(match.group(1))
  except:
    raise TFParseOutputException('Unable to parse TensorFlow output')


def _MakeSamplesFromOutput(benchmark_spec, output):
  """Create a sample continaing the measured TensorFlow throughput.

  Args:
    benchmark_spec: benchmark spec
    output: TensorFlow output

  Returns:
    a Sample containing the TensorFlow throughput in Gflops
  """
  metadata = _CreateMetadataDict(benchmark_spec)
  tensorflow_throughput = _ExtractThroughput(output)
  return [sample.Sample('Training synthetic data', tensorflow_throughput,
                        'images/sec', metadata)]


def Run(benchmark_spec):
  """Run TensorFlow on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  master_vm = vms[0]
  master_vm.RemoteCommand(
      'git clone https://github.com/tensorflow/benchmarks.git', should_log=True)
  tf_cnn_benchmark_dir = 'benchmarks/scripts/tf_cnn_benchmarks'
  tf_cnn_benchmark_cmd = (
      'python tf_cnn_benchmarks.py --local_parameter_device=%s --num_gpus=%s '
      '--batch_size=%s --model=%s --data_name=%s --variable_update=%s '
      '--nccl=%s --distortions=%s') % (
          benchmark_spec.local_parameter_device,
          benchmark_spec.num_gpus,
          benchmark_spec.batch_size,
          benchmark_spec.model,
          benchmark_spec.data_name,
          benchmark_spec.variable_update,
          benchmark_spec.use_nccl,
          benchmark_spec.distortions)
  run_command = 'cd %s && %s %s' % (tf_cnn_benchmark_dir,
                                    _GetEnvironmentVars(master_vm),
                                    tf_cnn_benchmark_cmd)
  output, _ = master_vm.RobustRemoteCommand(run_command, should_log=True)
  return _MakeSamplesFromOutput(benchmark_spec, output)


def Cleanup(benchmark_spec):
  """Cleanup TensorFlow on the cluster."""
  pass
