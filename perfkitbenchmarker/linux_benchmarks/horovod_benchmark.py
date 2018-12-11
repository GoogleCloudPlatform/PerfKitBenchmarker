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

"""Run Horovod distributed Tensorflow Training benchmark."""

import logging
import posixpath
import re
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import hpc_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cuda_toolkit

FLAGS = flags.FLAGS


MACHINEFILE = 'HOSTFILE'

BENCHMARK_VERSION = 0.31
BENCHMARK_NAME = 'horovod'
BENCHMARK_CONFIG = """
horovod:
  description: Runs Horovod. Specify the number of VMs with --num_vms
  vm_groups:
    default:
      os_type: ubuntu1604
      vm_spec:
        GCP:
          machine_type: n1-standard-4
          zone: us-east1-d
          boot_disk_size: 200
          gpu_type: k80
          gpu_count: 1
        AWS:
          machine_type: p2.xlarge
          zone: us-east-1
          boot_disk_size: 200
        Azure:
          machine_type: Standard_NC6
          zone: eastus
      vm_count: null
"""

flags.DEFINE_enum(
    'horovod_model', 'resnet152',
    ['trivial', 'lenet', 'alexnet', 'googlenet', 'vgg11', 'vgg13', 'vgg16',
     'vgg19', 'inception3', 'inception4', 'resnet18', 'resnet34', 'resnet50',
     'resnet101', 'resnet152', 'resnext50', 'resnext101', 'resnext152',
     'inception-resnet2'], 'name of the model to run.')

flags.DEFINE_integer(
    'horovod_batch_size', 128, 'batch size per compute device.')

flags.DEFINE_string(
    'horovod_deep_learning_examples_commit',
    '147c37049e76daeeda8b6a313b040ac51aeabe4b',
    'Commit hash of the NVIDIA deep learning examples github repo '
    'to use for the benchmark')


class HorovodParseOutputError(errors.Benchmarks.RunError):
  pass


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(_):
  """Verify that the required prerequisites are met.

  Raises:
    perfkitbenchmarker.errors.Setup.InvalidFlagConfigurationError:
      On invalid flag configuration.
  """
  if not FLAGS.openmpi_enable_shared:
    raise errors.Setup.InvalidFlagConfigurationError(
        'The flag openmpi_enable_shared must be True in order to run Horovod.')


def _UpdateBenchmarkSpecWithFlags(benchmark_spec):
  """Update the benchmark_spec with supplied command line flags.

  Args:
    benchmark_spec: benchmark specification to update
  """
  gpus_per_node = cuda_toolkit.QueryNumberOfGpus(benchmark_spec.vms[0])
  num_vms = len(benchmark_spec.vms)
  total_gpus = gpus_per_node * num_vms

  benchmark_spec.gpus_per_node = gpus_per_node
  benchmark_spec.num_vms = num_vms
  benchmark_spec.total_gpus = total_gpus
  benchmark_spec.model = FLAGS.horovod_model
  benchmark_spec.batch_size = FLAGS.horovod_batch_size
  benchmark_spec.deep_learning_examples_commit = (
      FLAGS.horovod_deep_learning_examples_commit)


def _CopyAndUpdateRunScripts(vm):
  """Copy and update all necessary run scripts on the given vm.

  Args:
    vm: vm to place and update run scripts on
  """
  vm.InstallPackages('git')
  vm.RemoteCommand('rm -rf DeepLearningExamples')
  vm.RemoteCommand(
      'git clone https://github.com/NVIDIA/DeepLearningExamples.git')
  vm.RemoteCommand(
      'cd DeepLearningExamples && git checkout {}'.format(
          FLAGS.horovod_deep_learning_examples_commit)
  )
  # Copy the benchmark script from the github repo to the home directory.
  vm.RemoteCommand(
      'cp %s .' % posixpath.join('DeepLearningExamples',
                                 'TensorFlow',
                                 'Classification',
                                 'imagenet',
                                 'nvcnn_hvd.py'))

  # Run sed to remove the 'choices' from the models flag.
  # This has the effect of enabling all models for testing.
  vm.RemoteCommand('sed -i -e \"s/choices=\\[.*\\]//g\" %s' % 'nvcnn_hvd.py')


def _PrepareHorovod(vm):
  """Install Horovod on a single vm.

  Args:
    vm: vm to operate on
  """
  # TODO(ferneyhough): Consider moving horovod installation to a package.
  logging.info('Installing Horovod on %s', vm)
  vm.Install('tensorflow')
  vm.Install('openmpi')
  vm.RemoteCommand('sudo HOROVOD_GPU_ALLREDUCE=NCCL pip install '
                   '--no-cache-dir horovod')
  vm.AuthenticateVm()


def Prepare(benchmark_spec):
  """Install and set up Horovod on the target vms.

  Args:
    benchmark_spec: The benchmark specification
  """
  vms = benchmark_spec.vms
  vm_util.RunThreaded(_PrepareHorovod, vms)
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  for vm in vms:
    _CopyAndUpdateRunScripts(vm)
  hpc_util.CreateMachineFile(vms,
                             lambda _: benchmark_spec.gpus_per_node,
                             MACHINEFILE)


def _CreateMetadataDict(benchmark_spec):
  """Create metadata dict to be used in run results.

  Args:
    benchmark_spec: benchmark spec

  Returns:
    metadata dict
  """
  vm = benchmark_spec.vms[0]
  metadata = dict()
  metadata.update(cuda_toolkit.GetMetadata(vm))
  metadata['benchmark_version'] = BENCHMARK_VERSION
  metadata['num_nodes'] = len(benchmark_spec.vms)
  metadata['total_gpus'] = int(benchmark_spec.total_gpus)
  metadata['model'] = benchmark_spec.model
  metadata['batch_size'] = benchmark_spec.batch_size
  metadata['deep_learning_examples_commit'] = (
      benchmark_spec.deep_learning_examples_commit)
  return metadata


def _ExtractThroughput(output):
  """Extract throughput from Horovod output.

  Args:
    output: Horvod output

  Returns:
    throuput in images per second (float)
  """
  regex = r'Images\/sec\: (\d+\.\d+)'
  match = re.search(regex, output)
  try:
    return float(match.group(1))
  except:
    raise HorovodParseOutputError('Unable to parse Horovod output')


def _MakeSamplesFromOutput(benchmark_spec, output):
  """Create a sample continaing the measured Horovod throughput.

  Args:
    benchmark_spec: benchmark spec
    output: Horovod output

  Returns:
    a Sample containing the Horovod throughput in images/sec
  """
  metadata = _CreateMetadataDict(benchmark_spec)
  images_sec = _ExtractThroughput(output)
  return [sample.Sample('Training synthetic data', images_sec,
                        'images/sec', metadata)]


def Run(benchmark_spec):
  """Run Horovod on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  master_vm = vms[0]
  master_vm.RemoteCommand('rm -rf results')
  master_vm.RemoteCommand('mkdir results')

  run_command = ('mpirun -np {num_gpus} '
                 '--hostfile HOSTFILE --bind-to none --map-by slot -x '
                 'NCCL_DEBUG=INFO -x NCCL_MIN_NRINGS=4 -x '
                 'LD_LIBRARY_PATH -x PATH -mca pml ob1 -mca btl ^openib '
                 'python nvcnn_hvd.py --batch_size={batch_size} --fp16 '
                 '--model {model} --log_dir results --display_every 100'
                 ).format(num_gpus=benchmark_spec.total_gpus,
                          batch_size=benchmark_spec.batch_size,
                          model=benchmark_spec.model)

  stdout, _ = master_vm.RobustRemoteCommand(run_command, should_log=True)
  return _MakeSamplesFromOutput(benchmark_spec, stdout)


def Cleanup(benchmark_spec):
  """Cleanup Horovod on the cluster."""
  del benchmark_spec
