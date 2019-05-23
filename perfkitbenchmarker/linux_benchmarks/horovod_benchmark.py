# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import hpc_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cuda_toolkit

FLAGS = flags.FLAGS
MACHINEFILE = 'HOSTFILE'

BENCHMARK_VERSION = 0.33
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
          boot_disk_size: 500
          gpu_type: k80
          gpu_count: 1
        AWS:
          machine_type: p2.xlarge
          zone: us-east-1
          boot_disk_size: 500
        Azure:
          machine_type: Standard_NC6
          zone: eastus
      vm_count: null
"""
# The data is downloaded from http://image-net.org/
# For data preprocessing, please check
# https://github.com/mlperf/training/tree/master/image_classification#3-datasetenvironment
BENCHMARK_DATA = {'ILSVRC2012.tar': 'cd2de079dc2e18fc9a9f598b5a38969b'}

GITHUB_MODELS_URL = 'https://github.com/aws-samples/deep-learning-models.git'

flags.DEFINE_enum(
    'horovod_model', 'resnet50',
    ['resnet18', 'resnet34', 'resnet50', 'resnet101', 'resnet152'],
    'name of the model to run.')

flags.DEFINE_integer(
    'horovod_batch_size', 64, 'Batch size per compute device.')

# AWS script trained for 90 epochs when using real data.
# Note that using too small a batch size (1 for example) with
# 8 GPUs will cause the run script to fail for reasons not understood.
flags.DEFINE_integer(
    'horovod_num_epochs', 10,
    'Number of epochs to train for.')

flags.DEFINE_enum(
    'horovod_precision', 'fp16', ['fp16', 'fp32'], 'Precision.')

flags.DEFINE_boolean(
    'horovod_synthetic', True, 'Whether to use synthetic data.')

flags.DEFINE_string(
    'horovod_deep_learning_examples_commit',
    '599adf2',
    'Commit hash of the AWS deep learning samples github repo '
    'to use for the benchmark.')

flags.DEFINE_boolean(
    'horovod_using_deep_learning_image', False,
    'Whether the VM under test is using a deep learning image. '
    'This will case PKB to skip the installation of Horovod '
    'and its dependencies.')


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
  benchmark_spec.num_epochs = FLAGS.horovod_num_epochs
  benchmark_spec.precision = FLAGS.horovod_precision
  benchmark_spec.synthetic = FLAGS.horovod_synthetic
  benchmark_spec.deep_learning_examples_commit = (
      FLAGS.horovod_deep_learning_examples_commit)


def _CopyAndUpdateRunScripts(vm):
  """Copy and update all necessary run scripts on the given vm.

  Args:
    vm: vm to place and update run scripts on
  """
  vm.InstallPackages('git')
  vm.RemoteCommand('rm -rf deep-learning-models')
  vm.RemoteCommand('git clone %s' % GITHUB_MODELS_URL)
  vm.RemoteCommand(
      'cd deep-learning-models && git checkout {}'.format(
          FLAGS.horovod_deep_learning_examples_commit)
  )
  # Copy the benchmark script from the github repo to the home directory.
  vm.RemoteCommand(
      'cp %s .' % posixpath.join('deep-learning-models',
                                 'models',
                                 'resnet',
                                 'tensorflow',
                                 'train_imagenet_resnet_hvd.py'))


def _PrepareHorovod(vm):
  """Install Horovod on a single vm.

  Args:
    vm: vm to operate on
  """
  # TODO(ferneyhough): Consider moving horovod installation to a package.
  logging.info('Installing Horovod on %s', vm)
  vm.AuthenticateVm()
  if not FLAGS.horovod_synthetic:
    vm.InstallPreprovisionedBenchmarkData(BENCHMARK_NAME, BENCHMARK_DATA,
                                          vm_util.VM_TMP_DIR)
    vm.RemoteCommand('tar xvf %s' % posixpath.join(vm_util.VM_TMP_DIR,
                                                   'ILSVRC2012.tar'))
  if FLAGS.horovod_using_deep_learning_image:
    return
  vm.Install('tensorflow')
  vm.Install('openmpi')
  vm.RemoteCommand('sudo HOROVOD_GPU_ALLREDUCE=NCCL pip install '
                   '--no-cache-dir horovod')


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
  metadata['num_epochs'] = benchmark_spec.num_epochs
  metadata['precision'] = benchmark_spec.precision
  metadata['synthetic'] = benchmark_spec.synthetic
  metadata['deep_learning_examples_commit'] = (
      benchmark_spec.deep_learning_examples_commit)
  return metadata


def _ExtractThroughputAndRuntime(output):
  """Extract throughput and runtime from Horovod output.

  Args:
    output: Horvod output

  Returns:
    Tuple of:
      Average throuput in images per second (float),
      Runtime in seconds (float).
  """
  # Start from last line and iterate backwards.
  throughput_samples = []
  runtime = 0
  for line in output.splitlines()[::-1]:
    split_line = line.split()
    if split_line[0].startswith('Finished'):
      runtime = float(split_line[2])
      continue
    split_line = line.split()
    if split_line[0] == '1':  # We are done parsing.
      break
    throughput_samples.append(float(split_line[2]))
  avg_throughput = sum(throughput_samples) / len(throughput_samples)
  return round(avg_throughput, 1), round(runtime, 1)


def _MakeSamplesFromOutput(benchmark_spec, output):
  """Create a sample continaing the measured Horovod throughput.

  Args:
    benchmark_spec: benchmark spec
    output: Horovod output

  Returns:
    a Sample containing the Horovod throughput in images/sec
  """
  metadata = _CreateMetadataDict(benchmark_spec)
  images_sec, runtime = _ExtractThroughputAndRuntime(output)
  samples = []
  samples.append(sample.Sample('Training thoughput', images_sec,
                               'images/second', metadata))
  samples.append(sample.Sample('Runtime', runtime,
                               'seconds', metadata))
  return samples


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

  # GCP should work out of the box with the deep learning image but the AWS
  # image requires us to use the correct Tensorflow Python environment.
  if FLAGS.cloud == 'AWS' and FLAGS.horovod_using_deep_learning_image:
    master_vm.RobustRemoteCommand('. anaconda3/bin/activate tensorflow_p36')
    python_interpreter = 'anaconda3/envs/tensorflow_p36/bin/python'
  else:
    python_interpreter = 'python'

  # This mpirun command was copied from the horovod example training script
  # in the tensorflow_p36 environment on the AWS DLAMI.
  # https://aws.amazon.com/releasenotes/deep-learning-ami-ubuntu-version-21-2
  run_command = (
      'mpirun -np {num_gpus} -hostfile HOSTFILE -mca plm_rsh_no_tree_spawn 1 '
      '-bind-to socket -map-by slot -x HOROVOD_HIERARCHICAL_ALLREDUCE=1 '
      '-x HOROVOD_FUSION_THRESHOLD=16777216 -x NCCL_MIN_NRINGS=4 '
      '-x LD_LIBRARY_PATH -x PATH -mca pml ob1 -mca btl ^openib '
      '-mca btl_tcp_if_exclude lo,docker0 -x TF_CPP_MIN_LOG_LEVEL=0 '
      '{python} -W ignore {training_script} --num_epochs {num_epochs} '
      '-b {batch_size} --model {model} {fp16} --clear_log'
  ).format(
      num_gpus=benchmark_spec.total_gpus,
      python=python_interpreter,
      training_script='train_imagenet_resnet_hvd.py',
      num_epochs=benchmark_spec.num_epochs,
      batch_size=benchmark_spec.batch_size,
      model=benchmark_spec.model,
      fp16='--fp16' if benchmark_spec.precision == 'fp16' else '--nofp16'
  )

  if benchmark_spec.synthetic:
    run_command += ' --synthetic'
    # The use of larc and loss scale is taken from the AWS DLAMI training
    # script (see comment above).
    if benchmark_spec.total_gpus >= 128:
      run_command += ' --use_larc --loss_scale 256'
  else:
    run_command += ' --data_dir ~/ILSVRC2012/ILSVRC2012 --warmup_epochs 10'

  _, stderr = master_vm.RobustRemoteCommand(run_command, should_log=True)
  return _MakeSamplesFromOutput(benchmark_spec, stderr)


def Cleanup(benchmark_spec):
  """Cleanup Horovod on the cluster."""
  del benchmark_spec
