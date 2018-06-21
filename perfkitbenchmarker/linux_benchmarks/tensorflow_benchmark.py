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

"""Run Tensorflow benchmarks (https://github.com/tensorflow/benchmarks).

This benchmark suports distributed and non-distributed runs. Distributed
TensorFlow involves splitting the job to different vms/nodes. To train a dataset
using hundreds of GPUs, use distributed TensorFlow. In Distributed TensorFlow,
there is communication between the parameter servers and the workers, and also
between the workers. Each worker process runs the same model. When a worker
needs a variable, it accesses it from the parameter server directly.
"""

import collections
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import tensorflow

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'tensorflow'
BENCHMARK_CONFIG = """
tensorflow:
  description: Runs Tensorflow Benchmark.
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

GPU = 'gpu'
CPU = 'cpu'
NCHW = 'NCHW'
NHWC = 'NHWC'
PID_PREFIX = 'TF_PS_PID'
MODELS = ['vgg11', 'vgg16', 'vgg19', 'lenet', 'googlenet', 'overfeat',
          'alexnet', 'trivial', 'inception3', 'inception4', 'resnet50',
          'resnet101', 'resnet152']
FP16 = 'float16'
FP32 = 'float32'

flags.DEFINE_boolean('tf_forward_only', False, '''whether use forward-only or
                     training for benchmarking''')
flags.DEFINE_list('tf_models', ['inception3', 'vgg16', 'alexnet', 'resnet50',
                                'resnet152'], 'name of the models to run')
flags.register_validator('tf_models',
                         lambda models: models and set(models).issubset(MODELS),
                         'Invalid models list. tf_models must be a subset of '
                         + ', '.join(MODELS))
flags.DEFINE_enum('tf_data_name', 'imagenet', ['imagenet', 'flowers'],
                  'Name of dataset: imagenet or flowers.')
flags.DEFINE_list('tf_batch_sizes', None, 'batch sizes per compute device. '
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
flags.DEFINE_enum('tf_data_format', NCHW, [NCHW, NHWC], '''Data layout to
                  use: NHWC (TF native) or NCHW (cuDNN native).''')
flags.DEFINE_boolean('tf_distortions', True,
                     '''Enable/disable distortions during image preprocessing.
                     These include bbox and color distortions.''')
flags.DEFINE_boolean('tf_distributed', False, 'Run TensorFlow distributed')
flags.DEFINE_string('tf_distributed_port', '2222',
                    'The port to use in TensorFlow distributed job')
flags.DEFINE_enum('tf_precision', FP32, [FP16, FP32],
                  'Use 16-bit floats for certain tensors instead of 32-bit '
                  'floats. This is currently experimental.')
flags.DEFINE_string('tf_benchmark_args', None,
                    'Arguments (as a string) to pass to tf_cnn_benchmarks. '
                    'This can be used to run a benchmark with arbitrary '
                    'parameters. Arguments will be parsed and added to the '
                    'sample metadata. For example, '
                    '--tf_benchmark_args="--nodistortions --optimizer=sgd '
                    'will run tf_cnn_benchmarks.py '
                    '--nodistortions --optimizer=sgd '
                    'and put the following in the metadata: '
                    '{\'nodistortions\': \'True\', \'optimizer\': \'sgd\'}. '
                    'All arguments must be in the form --arg_name=value. '
                    'If there are GPUs on the VM and no \'num_gpus\' flag in '
                    'the tf_benchmarks_args flag, the num_gpus flag will '
                    'automatically be populated with the number of available '
                    'GPUs.')


def LocalParameterDeviceValidator(value):
  if FLAGS.tf_device == CPU:
    return value == CPU
  return True

flags.register_validator('tf_local_parameter_device',
                         LocalParameterDeviceValidator)



NVIDIA_TESLA_P4 = cuda_toolkit.NVIDIA_TESLA_P4
NVIDIA_TESLA_K80 = cuda_toolkit.NVIDIA_TESLA_K80
NVIDIA_TESLA_P100 = cuda_toolkit.NVIDIA_TESLA_P100
NVIDIA_TESLA_V100 = cuda_toolkit.NVIDIA_TESLA_V100

DEFAULT_BATCH_SIZE = 64
DEFAULT_BATCH_SIZES = {
    CPU: {
        'alexnet': 512,
        'inception3': 64,
        'resnet50': 64,
        'resnet152': 32,
        'vgg16': 32,
    },
    NVIDIA_TESLA_K80: {
        'alexnet': 512,
        'inception3': 64,
        'resnet50': 64,
        'resnet152': 32,
        'vgg16': 32,
    },
    NVIDIA_TESLA_P4: {
        'alexnet': 512,
        'inception3': 128,
        'resnet50': 128,
        'resnet152': 64,
        'vgg16': 64,
    },
    NVIDIA_TESLA_P100: {
        'alexnet': 512,
        'inception3': 256,
        'resnet50': 256,
        'resnet152': 128,
        'vgg16': 128,
    },
    NVIDIA_TESLA_V100: {
        'alexnet': 512,
        'inception3': 256,
        'resnet50': 256,
        'resnet152': 128,
        'vgg16': 128,
    },
}


class TFParseOutputException(Exception):
  pass


class TFParsePsPidException(Exception):
  pass


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _GetDefaultBatchSizeByModel(model, gpu_type):
  """Return the default batch size for a given model and gpu / cpu type.

  If gpu_type is none, it is assumed that the model will be running on the CPU.
  If there is no default for the given model and gpu_type, a default batch
  size will be returned as defined by DEFAULT_BATCH_SIZE.

  Args:
    model: name of the Tensorflow model
    gpu_type: type of the GPU, or None

  Returns:
    default batch size for the given model / gpu_type,
    or the default batch size.
  """
  computation_device = gpu_type or CPU
  try:
    return DEFAULT_BATCH_SIZES[computation_device][model]
  except KeyError:
    return DEFAULT_BATCH_SIZE


def _GetBatchSizes(model, gpu_type):
  """Return the batch_size flag if specified, or the appropriate default if not.

  Args:
    model: name of the Tensorflow model
    gpu_type: type of the GPU, or None

  Returns:
    value of the batch_size flag if specified, or the default batch size for the
    given model / gpu_type.
  """
  return FLAGS.tf_batch_sizes or [_GetDefaultBatchSizeByModel(model, gpu_type)]


def _UpdateBenchmarkSpecWithFlags(benchmark_spec):
  """Update the benchmark_spec with supplied command line flags.

  Args:
    benchmark_spec: benchmark specification to update
  """
  benchmark_spec.forward_only = FLAGS.tf_forward_only
  benchmark_spec.data_name = FLAGS.tf_data_name
  benchmark_spec.variable_update = FLAGS.tf_variable_update
  benchmark_spec.distortions = FLAGS.tf_distortions
  benchmark_spec.benchmarks_commit_hash = FLAGS.tf_benchmarks_commit_hash
  benchmark_spec.tensorflow_cpu_pip_package = FLAGS.tf_cpu_pip_package
  benchmark_spec.tensorflow_gpu_pip_package = FLAGS.tf_gpu_pip_package
  benchmark_spec.distributed = FLAGS.tf_distributed
  benchmark_spec.precision = FLAGS.tf_precision
  benchmark_spec.benchmark_args = FLAGS.tf_benchmark_args


def _PrepareVm(vm):
  """Install and set up TensorFlow on the target vm.

  The TensorFlow benchmarks are also installed.
  A specific commit of the benchmarks which works best with TensorFlow
  1.3 is used and can be overridden with the flag tf_benchmarks_commit_hash.

  Args:
    vm: virtual machine on which to install TensorFlow
  """
  vm.Install('tensorflow')
  vm.InstallPackages('git')


def Prepare(benchmark_spec):
  """Install and set up TensorFlow on the target vm.

  Args:
    benchmark_spec: The benchmark specification
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vms = benchmark_spec.vms
  vm_util.RunThreaded(_PrepareVm, vms)
  benchmark_spec.tensorflow_version = tensorflow.GetTensorFlowVersion(vms[0])

  if cuda_toolkit.CheckNvidiaGpuExists(vms[0]):
    benchmark_spec.gpu_type = cuda_toolkit.GetGpuType(vms[0])


def _GetMetadataFromBenchmarkArgs(tf_cnn_benchmark_args):
  """Return a dictionary of arg names and values.

  Only supports arguments in the following format:
    --arg_name=arg_value

  The above string will result in this function returning a dictionary
  like so: {'arg_name': 'arg_value'}

  Because this and other PKB benchmarks use the 'precision' flag to specify
  fp16 or fp32, this function will convert the Tensorflow-specific precision
  flag ('use_fp16') to 'precision' to keep results consistent. All other command
  line arguments are extracted as is without being renamed.

  Args:
    tf_cnn_benchmark_args: string. The command line args to parse into a dict.

  Returns:
    A dictionary mapping argument names to their values.
  """
  args = tf_cnn_benchmark_args.split(' ')
  args_dict = {arg.split('=')[0].replace('--', ''): arg.split('=')[1]
               for arg in args}
  if 'use_fp16' in args_dict:
    if args_dict['use_fp16'].lower() == 'true':
      args_dict['precision'] = FP16
    else:
      args_dict['precision'] = FP32
  return args_dict


def _CreateMetadataDict(benchmark_spec, model, batch_size):
  """Create metadata dict to be used in run results.

  Args:
    benchmark_spec: benchmark spec
    model: model which was run
    batch_size: batch sized used

  Returns:
    metadata dict
  """
  vm = benchmark_spec.vms[0]
  metadata = {}
  if cuda_toolkit.CheckNvidiaGpuExists(vm):
    metadata.update(cuda_toolkit.GetMetadata(vm))

  metadata['command_line'] = benchmark_spec.tf_cnn_benchmark_cmd
  metadata['benchmarks_commit_hash'] = benchmark_spec.benchmarks_commit_hash
  metadata['tensorflow_version'] = benchmark_spec.tensorflow_version
  metadata['tensorflow_cpu_pip_package'] = (
      benchmark_spec.tensorflow_cpu_pip_package)
  metadata['tensorflow_gpu_pip_package'] = (
      benchmark_spec.tensorflow_gpu_pip_package)
  # If we ran a custom command-line through the benchmark_args flag,
  # add the metadata from that command and return. We don't need anymore
  # metadata from this function as it is likely invalid.
  if getattr(benchmark_spec, 'benchmark_args', None):
    metadata.update(
        _GetMetadataFromBenchmarkArgs(benchmark_spec.benchmark_args))
    return metadata

  metadata['model'] = model
  metadata['batch_size'] = batch_size
  metadata['forward_only'] = benchmark_spec.forward_only
  metadata['data_name'] = benchmark_spec.data_name
  metadata['variable_update'] = benchmark_spec.variable_update
  metadata['local_parameter_device'] = benchmark_spec.local_parameter_device
  metadata['device'] = benchmark_spec.device
  metadata['data_format'] = benchmark_spec.data_format
  metadata['distortions'] = benchmark_spec.distortions
  metadata['distributed'] = benchmark_spec.distributed
  metadata['precision'] = benchmark_spec.precision
  return metadata


def _ExtractThroughput(output):
  """Extract throughput from TensorFlow output.

  Args:
    output: TensorFlow output

  Returns:
    throuput (float)
  """
  regex = r'total images/sec: (\S+)'
  try:
    return regex_util.ExtractFloat(regex, output)
  except:
    raise TFParseOutputException('Unable to parse TensorFlow output')


def _ExtractTfParameterServerPid(output):
  """Extract the process identification number from TensorFlow parameter server.

  Args:
    output: string, Remote command output

  Returns:
    string, process identification number from TensorFlow parameter server

  Raises:
    TFParsePsPidException
  """
  regex = r'{pid} (\S+)'.format(pid=PID_PREFIX)
  try:
    return regex_util.ExtractExactlyOneMatch(regex, output)
  except:
    raise TFParsePsPidException('Unable to parse process identification number '
                                'of TensorFlow parameter server from remote '
                                'command output.')


def _MakeSamplesFromOutput(benchmark_spec, output, model, batch_size):
  """Create a sample containing the measured TensorFlow throughput.

  Args:
    benchmark_spec: benchmark spec
    output: TensorFlow output
    model: model which was run
    batch_size: batch sized used

  Returns:
    a Sample containing the TensorFlow throughput in Gflops
  """
  metadata = _CreateMetadataDict(benchmark_spec, model, batch_size)
  tensorflow_throughput = _ExtractThroughput(output)
  return sample.Sample('Training synthetic data', tensorflow_throughput,
                       'images/sec', metadata)


def _GetTfCnnBenchmarkCommand(vm, model, batch_size, benchmark_spec,
                              args='', job_name=''):
  """Create the command used to run the tf_cnn_benchmarks script.

  The command is either formulated using flag values stored on the
  benchmark_spec, or is essentially provided outright through the
  benchmark_args flag.

  Args:
    vm: the VM to run on.
    model: name of the model to run.
    batch_size: batch size to use for training.
    benchmark_spec: the benchmark spec object.
    args: string, distributed arguments
    job_name: string, distributed job name

  Returns:
    A string that runs the tf_cnn_benchmarks.py script
    with the desired arguments.
  """
  num_gpus = (cuda_toolkit.QueryNumberOfGpus(vm) if
              cuda_toolkit.CheckNvidiaGpuExists(vm) else 0)

  if benchmark_spec.benchmark_args is not None:
    cmd = 'python tf_cnn_benchmarks.py ' + benchmark_spec.benchmark_args
    # If the user didn't specify num_gpus in the benchmark_args string,
    # use all the GPUs on the system.
    if '--num_gpus' not in benchmark_spec.benchmark_args and num_gpus:
      cmd = '{cmd} --num_gpus={num_gpus}'.format(cmd=cmd, num_gpus=num_gpus)
    return cmd

  benchmark_spec.local_parameter_device = FLAGS.tf_local_parameter_device
  benchmark_spec.device = FLAGS.tf_device
  benchmark_spec.data_format = FLAGS.tf_data_format
  if num_gpus == 0:
    benchmark_spec.local_parameter_device = CPU
    benchmark_spec.device = CPU
    benchmark_spec.data_format = NHWC

  cmd = (
      'python tf_cnn_benchmarks.py '
      '--local_parameter_device={local_parameter_device} '
      '--batch_size={batch_size} '
      '--model={model} '
      '--data_name={data_name} '
      '--variable_update={variable_update} '
      '--distortions={distortions} '
      '--device={device} '
      '--data_format={data_format} '
      '--forward_only={forward_only} '
      '--use_fp16={use_fp16}'.format(
          local_parameter_device=benchmark_spec.local_parameter_device,
          batch_size=batch_size,
          model=model,
          data_name=benchmark_spec.data_name,
          variable_update=benchmark_spec.variable_update,
          distortions=benchmark_spec.distortions,
          device=benchmark_spec.device,
          data_format=benchmark_spec.data_format,
          forward_only=benchmark_spec.forward_only,
          use_fp16=(benchmark_spec.precision == FP16)))
  if benchmark_spec.device == GPU:
    cmd = '{env} {cmd} --num_gpus={gpus}'.format(
        env=tensorflow.GetEnvironmentVars(vm),
        cmd=cmd,
        gpus=num_gpus)
  if args:
    cmd = '{cmd} --job_name={job} {args}'.format(
        cmd=cmd, job=job_name, args=args)
  return cmd


def _RunModelOnVm(vm, model, batch_size, benchmark_spec, args='', job_name=''):
  """Runs a TensorFlow benchmark on a single VM.

  Args:
    vm: VM to run on
    model: string, the name of model to run
    batch_size: int, training batch size
    benchmark_spec: BenchmarkSpec object
    args: string, distributed arguments
    job_name: string, distributed job name

  Returns:
    a Sample containing the TensorFlow throughput or the process
    identification number from TensorFlow parameter server.
  """
  tf_cnn_benchmark_cmd = _GetTfCnnBenchmarkCommand(
      vm, model, batch_size, benchmark_spec, args, job_name)
  benchmark_spec.tf_cnn_benchmark_cmd = tf_cnn_benchmark_cmd
  tf_cnn_benchmark_dir = 'benchmarks/scripts/tf_cnn_benchmarks'
  run_command = 'cd {path} ; {cmd}'.format(path=tf_cnn_benchmark_dir,
                                           cmd=tf_cnn_benchmark_cmd)
  output, _ = vm.RobustRemoteCommand(run_command, should_log=True)
  if job_name == 'ps':
    return _ExtractTfParameterServerPid(output)
  else:
    return _MakeSamplesFromOutput(benchmark_spec, output, model, batch_size)


def _RunOnVm(vm, benchmark_spec):
  """Runs a TensorFlow benchmark on a single VM.

  Args:
    vm: VM to run on
    benchmark_spec: benchmark_spec object

  Returns:
    A list of samples containing the TensorFlow throughput from different models
  """
  samples = []
  if FLAGS.tf_benchmark_args:
    return [_RunModelOnVm(vm, None, None, benchmark_spec)]

  gpu_type = getattr(benchmark_spec, 'gpu_type', None)
  for model in FLAGS.tf_models:
    for batch_size in _GetBatchSizes(model, gpu_type):
      samples.append(_RunModelOnVm(vm, model, batch_size, benchmark_spec))
  return samples


def _GetHostsArgs(hosts):
  return ','.join('{ip}:{port}'.format(ip=vm.internal_ip,
                                       port=FLAGS.tf_distributed_port)
                  for vm in hosts)


def _RunDistributedTf(benchmark_spec):
  """Run distributed TensorFlow for each model specified.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """

  ps_hosts = benchmark_spec.vm_groups['parameter_server_hosts']
  worker_hosts = benchmark_spec.vm_groups['worker_hosts']
  dist_args = '--ps_hosts={ps_args} --worker_hosts={worker_args}'.format(
      ps_args=_GetHostsArgs(ps_hosts), worker_args=_GetHostsArgs(worker_hosts))
  flattened_results = []
  vm_pid = collections.namedtuple('vm_pid', 'vm pid')
  gpu_type = getattr(benchmark_spec, 'gpu_type', None)
  for model in FLAGS.tf_models:
    for batch_size in _GetBatchSizes(model, gpu_type):
      ps_pids = []
      for task_index, vm in enumerate(ps_hosts):
        dist_ps_args = ('{args} --task_index={index} &\n'
                        'echo {pid} $!').format(args=dist_args,
                                                index=task_index,
                                                pid=PID_PREFIX)
        pid = _RunModelOnVm(vm, model, batch_size, benchmark_spec, dist_ps_args,
                            'ps')
        ps_pids.append(vm_pid(vm=vm, pid=pid))
      args = []
      for task_index, vm in enumerate(worker_hosts):
        dist_worker_args = ('{args} --job_name=worker '
                            '--task_index={index}').format(args=dist_args,
                                                           index=task_index)
        args.append(((vm, model, batch_size, benchmark_spec, dist_worker_args,
                      'worker'), {}))
      result = vm_util.RunThreaded(_RunModelOnVm, args)
      for ps_pid in ps_pids:
        ps_pid.vm.RemoteCommand('kill -9 %s' % ps_pid.pid)
      flattened_results.extend(vm_result for vm_result in result)
  return flattened_results


def _RunTf(benchmark_spec):
  """Run TensorFlow for each model specified.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  args = [((vm, benchmark_spec), {}) for vm in vms]
  run_results = vm_util.RunThreaded(_RunOnVm, args)

  # Add vm index to results metadata
  for idx, vm_result in enumerate(run_results):
    for result_sample in vm_result:
      result_sample.metadata['vm_index'] = idx

  # Flatten the list
  return [samples for vm_results in run_results for samples in vm_results]


def Run(benchmark_spec):
  """Run TensorFlow on the cluster for each model specified.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  if benchmark_spec.distributed:
    return _RunDistributedTf(benchmark_spec)
  else:
    return _RunTf(benchmark_spec)


def Cleanup(unused_benchmark_spec):
  """Cleanup TensorFlow on the cluster."""
  pass
