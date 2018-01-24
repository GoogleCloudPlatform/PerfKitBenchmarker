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

import re
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cuda_toolkit_8
from perfkitbenchmarker.linux_packages import tensorflow

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'tensorflow'
BENCHMARK_CONFIG = """
tensorflow:
  description: Runs Tensorflow Benchmark.
  vm_groups:
    default:
      vm_spec:
        GCP:
          image: ubuntu-1604-xenial-v20180122
          image_project: ubuntu-os-cloud
          machine_type: n1-standard-4
          zone: us-east1-d
          boot_disk_size: 200
        AWS:
          image: ami-41e0b93b
          machine_type: p2.xlarge
          zone: us-east-1
          boot_disk_size: 200
        Azure:
          image: Canonical:UbuntuServer:16.04.0-LTS:latest
          machine_type: Standard_NC6
          zone: eastus
"""

GPU = 'gpu'
CPU = 'cpu'

MODELS = ['vgg11', 'vgg16', 'vgg19', 'lenet', 'googlenet', 'overfeat',
          'alexnet', 'trivial', 'inception3', 'inception4', 'resnet50',
          'resnet101', 'resnet152']

flags.DEFINE_boolean('tf_forward_only', False, '''whether use forward-only or
                     training for benchmarking''')
flags.DEFINE_list('tf_models', ['inception3', 'vgg16', 'alexnet', 'resnet50'],
                  'name of the models to run')
flags.register_validator('tf_models',
                         lambda models: models and set(models).issubset(MODELS),
                         'Invalid models list. tf_models must be a subset of '
                         + ', '.join(MODELS))
flags.DEFINE_enum('tf_data_name', 'imagenet', ['imagenet', 'flowers'],
                  'Name of dataset: imagenet or flowers.')
flags.DEFINE_integer('tf_batch_size', None, 'batch size per compute device. '
                     'If not provided, the suggested batch size is used for '
                     'the given model')
flags.DEFINE_enum('tf_variable_update', 'parameter_server',
                  ['parameter_server', 'replicated',
                   'distributed_replicated', 'independent'],
                  '''The method for managing variables: parameter_server,
                  replicated, distributed_replicated, independent''')
flags.DEFINE_enum('tf_local_parameter_device', CPU, [CPU, GPU],
                  '''Device to use as parameter server: cpu or gpu. For
                  distributed training, it can affect where caching of
                  variables happens.''')
flags.DEFINE_enum('tf_device', GPU, [CPU, GPU],
                  'Device to use for computation: cpu or gpu')
flags.DEFINE_enum('tf_data_format', 'NCHW', ['NCHW', 'NHWC'], '''Data layout to
                  use: NHWC (TF native) or NCHW (cuDNN native).''')
flags.DEFINE_boolean('tf_use_nccl', True,
                     'Whether to use nccl all-reduce primitives where possible')
flags.DEFINE_boolean('tf_distortions', True,
                     '''Enable/disable distortions during image preprocessing.
                     These include bbox and color distortions.''')
flags.DEFINE_string('tf_benchmarks_commit_hash',
                    'abe3c808933c85e6db1719cdb92fcbbd9eac6dec',
                    'git commit hash of desired tensorflow benchmark commit.')


def LocalParameterDeviceValidator(value):
  if FLAGS.tf_device == CPU:
    return value == CPU
  return True

flags.register_validator('tf_local_parameter_device',
                         LocalParameterDeviceValidator)


DEFAULT_BATCH_SIZE = 64
DEFAULT_BATCH_SIZES_BY_MODEL = {
    'vgg16': 32,
    'alexnet': 512,
    'resnet152': 32,
}


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


def _GetDefaultBatchSizeByModel(model):
  return DEFAULT_BATCH_SIZES_BY_MODEL.get(model, DEFAULT_BATCH_SIZE)


def _GetBatchSize(model):
  return FLAGS.tf_batch_size or _GetDefaultBatchSizeByModel(model)


def _UpdateBenchmarkSpecWithFlags(benchmark_spec):
  """Update the benchmark_spec with supplied command line flags.

  Args:
    benchmark_spec: benchmark specification to update
  """
  benchmark_spec.forward_only = FLAGS.tf_forward_only
  benchmark_spec.data_name = FLAGS.tf_data_name
  benchmark_spec.variable_update = FLAGS.tf_variable_update
  benchmark_spec.local_parameter_device = FLAGS.tf_local_parameter_device
  benchmark_spec.device = FLAGS.tf_device
  benchmark_spec.data_format = FLAGS.tf_data_format
  benchmark_spec.use_nccl = FLAGS.tf_use_nccl
  benchmark_spec.distortions = FLAGS.tf_distortions
  benchmark_spec.benchmarks_commit_hash = FLAGS.tf_benchmarks_commit_hash
  benchmark_spec.tensorflow_pip_package = FLAGS.tf_pip_package


def _PrepareVm(vm):
  """Install and set up TensorFlow on the target vm.

  The TensorFlow benchmarks are also installed.
  A specific commit of the benchmarks which works best with TensorFlow
  1.3 is used and can be overridden with the flag tf_benchmarks_commit_hash.

  Args:
    vm: virtual machine on which to install TensorFlow
  """
  commit_hash = FLAGS.tf_benchmarks_commit_hash
  vm.Install('tensorflow')
  vm.InstallPackages('git')
  vm.RemoteCommand(
      'git clone https://github.com/tensorflow/benchmarks.git', should_log=True)
  vm.RemoteCommand(
      'cd benchmarks && git checkout {}'.format(commit_hash)
  )


def Prepare(benchmark_spec):
  """Install and set up TensorFlow on the target vm.

  Args:
    benchmark_spec: The benchmark specification
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vms = benchmark_spec.vms
  vm_util.RunThreaded(_PrepareVm, vms)
  benchmark_spec.tensorflow_version = tensorflow.GetTensorFlowVersion(vms[0])


def _CreateMetadataDict(benchmark_spec, model, batch_size, num_gpus):
  """Create metadata dict to be used in run results.

  Args:
    benchmark_spec: benchmark spec
    model: model which was run
    batch_size: batch sized used
    num_gpus: number of GPUs used

  Returns:
    metadata dict
  """
  vm = benchmark_spec.vms[0]
  metadata = dict()
  if benchmark_spec.device == GPU:
    metadata.update(cuda_toolkit_8.GetMetadata(vm))
    metadata['num_gpus'] = num_gpus
  metadata['model'] = model
  metadata['batch_size'] = batch_size
  metadata['forward_only'] = benchmark_spec.forward_only
  metadata['data_name'] = benchmark_spec.data_name
  metadata['variable_update'] = benchmark_spec.variable_update
  metadata['local_parameter_device'] = benchmark_spec.local_parameter_device
  metadata['device'] = benchmark_spec.device
  metadata['data_format'] = benchmark_spec.data_format
  metadata['use_nccl'] = benchmark_spec.use_nccl
  metadata['distortions'] = benchmark_spec.distortions
  metadata['benchmarks_commit_hash'] = benchmark_spec.benchmarks_commit_hash
  metadata['tensorflow_version'] = benchmark_spec.tensorflow_version
  metadata['tensorflow_pip_package'] = benchmark_spec.tensorflow_pip_package
  return metadata


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


def _MakeSamplesFromOutput(benchmark_spec, output, model, batch_size, num_gpus):
  """Create a sample containing the measured TensorFlow throughput.

  Args:
    benchmark_spec: benchmark spec
    output: TensorFlow output
    model: model which was run
    batch_size: batch sized used
    num_gpus: number of GPUs used

  Returns:
    a Sample containing the TensorFlow throughput in Gflops
  """
  metadata = _CreateMetadataDict(benchmark_spec, model, batch_size, num_gpus)
  tensorflow_throughput = _ExtractThroughput(output)
  return [sample.Sample('Training synthetic data', tensorflow_throughput,
                        'images/sec', metadata)]


def _RunOnVm(vm, benchmark_spec):
  """Runs a TensorFlow benchmark on a single VM.

  Args:
    vm: VM to run on
    benchmark_spec: benchmark_spec object

  Returns:
    A list of samples
  """
  tf_cnn_benchmark_dir = 'benchmarks/scripts/tf_cnn_benchmarks'

  results = []
  for model in FLAGS.tf_models:
    batch_size = _GetBatchSize(model)
    tf_cnn_benchmark_cmd = (
        'python tf_cnn_benchmarks.py --local_parameter_device=%s '
        '--batch_size=%s --model=%s --data_name=%s --variable_update=%s '
        '--use_nccl=%s --distortions=%s --device=%s --data_format=%s '
        '--forward_only=%s') % (
            benchmark_spec.local_parameter_device,
            batch_size,
            model,
            benchmark_spec.data_name,
            benchmark_spec.variable_update,
            benchmark_spec.use_nccl,
            benchmark_spec.distortions,
            benchmark_spec.device,
            benchmark_spec.data_format,
            benchmark_spec.forward_only)
    if benchmark_spec.device == GPU:
      num_gpus = cuda_toolkit_8.QueryNumberOfGpus(vm)
      tf_cnn_benchmark_cmd = '%s %s --num_gpus=%s' % (
          tensorflow.GetEnvironmentVars(vm), tf_cnn_benchmark_cmd,
          num_gpus)
    else:
      num_gpus = 0
    run_command = 'cd %s && %s' % (tf_cnn_benchmark_dir,
                                   tf_cnn_benchmark_cmd)
    output, _ = vm.RobustRemoteCommand(run_command, should_log=True)
    results.extend(_MakeSamplesFromOutput(
        benchmark_spec, output, model, batch_size, num_gpus))

  return results


def Run(benchmark_spec):
  """Run TensorFlow on the cluster for each model specified.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vms = benchmark_spec.vms
  args = [((vm, benchmark_spec), {}) for vm in vms]
  run_results = vm_util.RunThreaded(_RunOnVm, args)

  # Add vm index to results metadata
  for idx, vm_result in enumerate(run_results):
    for result_sample in vm_result:
      result_sample.metadata['vm_index'] = idx

  # Flatten the list
  flattened_results = (
      [samples for vm_results in run_results for samples in vm_results])

  return flattened_results


def Cleanup(unused_benchmark_spec):
  """Cleanup TensorFlow on the cluster."""
  pass
