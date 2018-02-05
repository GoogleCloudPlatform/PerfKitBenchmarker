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


"""Run MXnet benchmarks.

(https://github.com/apache/incubator-mxnet/tree/master/example/
image-classification).
"""

import re
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import mxnet
from perfkitbenchmarker.linux_packages import mxnet_cnn


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'mxnet'
BENCHMARK_CONFIG = """
mxnet:
  description: Runs MXNet Benchmark.
  vm_groups:
    default:
      os_type: ubuntu1604
      vm_spec:
        GCP:
          machine_type: n1-highmem-4
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

GPU = 'gpu'
CPU = 'cpu'
IMAGENET_SHAPE = '3,299,299'

MODELS = ['alexnet', 'googlenet', 'inception-bn', 'inception-resnet-v2',
          'inception-v3', 'inception-v4', 'lenet', 'mlp', 'mobilenet',
          'resnet-v1', 'resnet', 'resnext', 'vgg']
flags.DEFINE_list('mx_models', ['inception-v3', 'vgg', 'alexnet', 'resnet'],
                  'The network to train')
flags.register_validator('mx_models',
                         lambda models: models and set(models).issubset(MODELS),
                         'Invalid models list. mx_models must be a subset of '
                         + ', '.join(MODELS))
flags.DEFINE_integer('mx_batch_size', None, 'The batch size for SGD training.')
flags.DEFINE_integer('mx_num_epochs', 80,
                     'The maximal number of epochs to train.')
flags.DEFINE_enum('mx_device', GPU, [CPU, GPU],
                  'Device to use for computation: cpu or gpu')
flags.DEFINE_integer('mx_num_layers', None, 'Number of layers in the neural '
                     'network, required by some networks such as resnet')

DEFAULT_BATCH_SIZE = 64
DEFAULT = 'default'
DEFAULT_BATCH_SIZES_BY_MODEL = {
    'vgg': {
        16: 32
    },
    'alexnet': {
        DEFAULT: 512
    },
    'resnet': {
        152: 32
    }
}

DEFAULT_NUM_LAYERS_BY_MODEL = {
    'vgg': 16,
    'resnet': 50
}


class MXParseOutputException(Exception):
  pass


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _GetDefaultBatchSize(model, num_layers=None):
  return DEFAULT_BATCH_SIZES_BY_MODEL.get(model, {}).get(num_layers or DEFAULT,
                                                         DEFAULT_BATCH_SIZE)


def _GetBatchSize(model, num_layers=None):
  return FLAGS.tf_batch_size or _GetDefaultBatchSize(model, num_layers)


def _GetDefaultNumLayersByModel(model):
  return DEFAULT_NUM_LAYERS_BY_MODEL.get(model)


def _GetNumLayers(model):
  return FLAGS.mx_num_layers or _GetDefaultNumLayersByModel(model)


def _UpdateBenchmarkSpecWithFlags(benchmark_spec):
  """Update the benchmark_spec with supplied command line flags.

  Args:
    benchmark_spec: benchmark specification to update
  """
  benchmark_spec.models = FLAGS.mx_models
  benchmark_spec.batch_size = FLAGS.mx_batch_size
  benchmark_spec.num_epochs = FLAGS.mx_num_epochs
  benchmark_spec.device = FLAGS.mx_device
  benchmark_spec.num_layers = FLAGS.mx_num_layers


def Prepare(benchmark_spec):
  """Install and set up MXNet on the target vm.

  Args:
    benchmark_spec: The benchmark specification
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vm = benchmark_spec.vms[0]
  vm.Install('mxnet')
  vm.Install('mxnet_cnn')
  benchmark_spec.mxnet_version = mxnet.GetMXNetVersion(vm)


def _CreateMetadataDict(benchmark_spec):
  """Create metadata dict to be used in run results.

  Args:
    benchmark_spec: benchmark spec

  Returns:
    metadata dict
  """
  vm = benchmark_spec.vms[0]
  metadata = dict()
  if benchmark_spec.device == GPU:
    metadata.update(cuda_toolkit.GetMetadata(vm))
  metadata['batch_size'] = benchmark_spec.batch_size
  metadata['num_epochs'] = benchmark_spec.num_epochs
  metadata['device'] = benchmark_spec.device
  metadata['num_layers'] = benchmark_spec.num_layers
  metadata['model'] = benchmark_spec.model
  metadata['mxnet_version'] = benchmark_spec.mxnet_version
  metadata['commit'] = mxnet_cnn.GetCommit(vm)
  return metadata


def _ExtractThroughput(output):
  """Extract throughput from MXNet output.

  Sample output:
  INFO:root:Epoch[0] Batch [460]	Speed: 50.42 samples/sec	accuracy=1.000000
  INFO:root:Epoch[0] Batch [480]	Speed: 50.47 samples/sec	accuracy=1.000000
  INFO:root:Epoch[0] Train-accuracy=1.000000
  INFO:root:Epoch[0] Time cost=634.243

  Args:
    output: MXNet output

  Returns:
    throughput (float)
  """
  regex = r'Batch\s+\[\d+\]\tSpeed:\s+(\d+.\d+)\s+'
  match = re.findall(regex, output)
  try:
    return sum(float(step) for step in match) / len(match)
  except:
    raise MXParseOutputException('Unable to parse MXNet output')


def _MakeSamplesFromOutput(benchmark_spec, output):
  """Create a sample continaing the measured MXNet throughput.

  Args:
    benchmark_spec: benchmark spec
    output: MXNet output

  Returns:
    a Sample containing the MXNet throughput in samples/sec
  """
  metadata = _CreateMetadataDict(benchmark_spec)
  mx_throughput = _ExtractThroughput(output)
  return sample.Sample('Training synthetic data', mx_throughput,
                       'samples/sec', metadata)


def Run(benchmark_spec):
  """Run MXNet on the cluster for each model specified.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vm = benchmark_spec.vms[0]
  mx_benchmark_dir = 'incubator-mxnet/example/image-classification'
  results = []
  for model in FLAGS.mx_models:
    num_layers = _GetNumLayers(model)
    batch_size = _GetBatchSize(model, num_layers)
    benchmark_spec.model = model
    benchmark_spec.batch_size = batch_size
    benchmark_spec.num_layers = num_layers
    mx_benchmark_cmd = (
        'python train_imagenet.py --benchmark 1 --network %s --batch-size %s '
        '--image-shape %s --num-epochs %s --kv-store device') % (
            model,
            batch_size,
            IMAGENET_SHAPE,
            benchmark_spec.num_epochs)
    if benchmark_spec.device == GPU:
      gpus = cuda_toolkit.QueryNumberOfGpus(vm)
      mx_benchmark_cmd = '%s %s --gpus %s' % (
          mxnet.GetEnvironmentVars(vm), mx_benchmark_cmd,
          ','.join(str(n) for n in range(gpus)))
    if num_layers:
      mx_benchmark_cmd = '%s --num-layers %s' % (mx_benchmark_cmd, num_layers)
    run_command = 'cd %s && %s' % (mx_benchmark_dir,
                                   mx_benchmark_cmd)
    stdout, stderr = vm.RobustRemoteCommand(run_command, should_log=True)

    results.append(_MakeSamplesFromOutput(benchmark_spec, stdout or stderr))

  return results


def Cleanup(unused_benchmark_spec):
  """Cleanup MXNet on the cluster."""
  pass
