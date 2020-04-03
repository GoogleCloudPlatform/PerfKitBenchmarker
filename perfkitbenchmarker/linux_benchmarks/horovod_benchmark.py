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
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import hpc_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import google_cloud_sdk

FLAGS = flags.FLAGS
MACHINEFILE = 'HOSTFILE'

BENCHMARK_VERSION = 0.34
BENCHMARK_NAME = 'horovod'
BENCHMARK_CONFIG = """
horovod:
  description: Runs Horovod. Specify the number of VMs with --num_vms
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n1-highmem-96
          zone: us-central1-a
          image_family: tf-latest-gpu-gvnic
          image_project: deeplearning-platform-release
          boot_disk_size: 500
          gpu_type: v100
          gpu_count: 8
        AWS:
          machine_type: p3dn.24xlarge
          zone: us-west-2a
          image: ami-07728e9e2742b0662
          boot_disk_size: 500
        Azure:
          machine_type: Standard_NC24rs_v3
          image: microsoft-dsvm:aml-workstation:ubuntu:19.11.13
          zone: eastus
          boot_disk_size: 500
      vm_count: null
"""

# TODO(user): Use NVIDIA's repo after
# https://github.com/NVIDIA/DeepLearningExamples/pull/386 is merged
GITHUB_MODELS_URL = 'https://github.com/changlan/DeepLearningExamples.git'

flags.DEFINE_enum('horovod_model', 'resnet-50',
                  ['resnet-50', 'bert-base', 'bert-large'],
                  'name of the model to run.')

flags.DEFINE_integer('horovod_batch_size', 64, 'Batch size per compute device.')

flags.DEFINE_integer(
    'horovod_num_epochs', 10, 'Number of epochs to train for. ')

flags.DEFINE_enum('horovod_max_seq_len', '128', ['128', '384'],
                  'Max sequence length for BERT.')

flags.DEFINE_enum('horovod_precision', 'fp16', ['fp16', 'fp32'], 'Precision.')

flags.DEFINE_string(
    'horovod_cuda_visible_devices', None,
    'GPU identifiers are given as integer indices or as UUID strings.')

flags.DEFINE_bool('horovod_bert_finetune', True,
                  'Pretrain or finetune a BERT model.')

flags.DEFINE_bool('horovod_timelime', False, 'Enable timeline in Horovod.')


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
  benchmark_spec.max_seq_len = int(FLAGS.horovod_max_seq_len)
  benchmark_spec.bert_finetune = FLAGS.horovod_bert_finetune
  benchmark_spec.timeline = FLAGS.horovod_timelime
  benchmark_spec.cuda_visible_devices = FLAGS.horovod_cuda_visible_devices
  benchmark_spec.nccl_net_plugin = FLAGS.nccl_net_plugin
  benchmark_spec.nccl_extra_params = FLAGS.nccl_extra_params


def _CopyAndUpdateRunScripts(model, vm):
  """Copy and update all necessary run scripts on the given vm.

  Args:
    model: name of the model
    vm: vm to place and update run scripts on
  """
  vm.RemoteCommand('rm -rf DeepLearningExamples')
  vm.RemoteCommand('git clone --branch clan-dev %s' % GITHUB_MODELS_URL)

  resnet_base_dir = 'DeepLearningExamples/TensorFlow/Classification/RN50v1.5'
  bert_base_dir = 'DeepLearningExamples/TensorFlow/LanguageModeling/BERT'

  if model.startswith('resnet'):
    vm.Install('google_cloud_sdk')
    vm.RemoteCommand('sed -i "/from utils import dali_utils/d" '
                     '{}/utils/data_utils.py'.format(resnet_base_dir))
    vm.RemoteCommand('sed -i "/from utils import dali_utils/d" '
                     '{}/utils/__init__.py'.format(resnet_base_dir))
    vm.RemoteCommand(
        'mkdir -p {base}/imagenet && '
        '{gsutil_path} -m rsync gs://pkb-sgpyc-us-central1/perfzero_dataset/imagenet '
        '{base}/imagenet'.format(gsutil_path=google_cloud_sdk.GSUTIL_PATH,
                                 base=resnet_base_dir))

  if model.startswith('bert'):
    vm.RemoteCommand(
        'mkdir -p {bert}/data/download/google_pretrained_weights &&'
        'mkdir -p {bert}/data/download/squad/v1.1 && '
        'cd {bert}/data/download/squad/v1.1 && '
        'wget https://rajpurkar.github.io/SQuAD-explorer/dataset/train-v1.1.json'
        .format(bert=bert_base_dir))

    if model == 'bert-base':
      vm.RemoteCommand(
          'cd {bert}/data/download/google_pretrained_weights/ && '
          'wget https://storage.googleapis.com/bert_models/2018_10_18/uncased_L-12_H-768_A-12.zip && '
          'unzip uncased_L-12_H-768_A-12.zip'.format(bert=bert_base_dir))
    else:  # 'bert-large':
      vm.RemoteCommand(
          'cd {bert}/data/download/google_pretrained_weights/ && '
          'wget https://storage.googleapis.com/bert_models/2018_10_18/uncased_L-24_H-1024_A-16.zip && '
          'unzip uncased_L-24_H-1024_A-16.zip'.format(bert=bert_base_dir))


def _PrepareHorovod(vm):
  """Install dependencies on a single vm.

  Args:
    vm: vm to operate on
  """
  logging.info('Installing Horovod on %s', vm)
  vm.AuthenticateVm()

  vm.Install('cuda_toolkit')
  vm.Install('nccl')
  vm.InstallPackages('wget git unzip')


def Prepare(benchmark_spec):
  """Install and set up Horovod on the target vms.

  Args:
    benchmark_spec: The benchmark specification
  """
  vms = benchmark_spec.vms
  vm_util.RunThreaded(_PrepareHorovod, vms)
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vm_util.RunThreaded(
      lambda vm: _CopyAndUpdateRunScripts(benchmark_spec.model, vm), vms)
  hpc_util.CreateMachineFile(vms, lambda _: benchmark_spec.gpus_per_node,
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
  metadata['max_seq_len'] = benchmark_spec.max_seq_len
  metadata['nccl_net_plugin'] = benchmark_spec.nccl_net_plugin
  metadata['cuda_visible_devices'] = benchmark_spec.cuda_visible_devices
  metadata['nccl_extra_params'] = benchmark_spec.nccl_extra_params
  return metadata


def _ExtractResNetThroughput(output):
  """Extract throughput from Horovod output.

  Args:
    output: Horovod output

  Returns:
    A tuple of:
      Average throuput in images per second (float)
      Unit of the throughput metric (str)
  """
  # Start from last line and iterate backwards.
  avg_throughput = 0
  for line in output.splitlines()[::-1]:
    if 'Average total_ips' in line:
      split_line = line.split()
      avg_throughput = float(split_line[-1])
      break
  return round(avg_throughput, 1), 'images/second'


def _ExtractBertThroughput(output):
  """Extract throughput from Horovod output.

  Args:
    output: Horovod output

  Returns:
    A tuple of:
      Average throughput in sentences per second (float)
      Unit of the throughput metric (str)
  """
  # Start from last line and iterate backwards.
  avg_throughput = 0
  for line in output.splitlines()[::-1]:
    if 'Throughput Average (sentences/sec) =' in line:
      split_line = line.split()
      avg_throughput = float(split_line[-1])
      break
  return round(avg_throughput, 1), 'sentences/second'


def _MakeSamplesFromOutput(benchmark_spec, stdout, stderr):
  """Create a sample continaing the measured Horovod throughput.

  Args:
    benchmark_spec: benchmark spec
    stdout: stdout
    stderr: stderr

  Returns:
    list of a Sample containing the Horovod throughput
  """
  metadata = _CreateMetadataDict(benchmark_spec)
  output = stdout + stderr

  extractor = {
      'resnet-50': _ExtractResNetThroughput,
      'bert-base': _ExtractBertThroughput,
      'bert-large': _ExtractBertThroughput,
  }

  throughput, unit = extractor[benchmark_spec.model](output)

  samples = []
  samples.append(
      sample.Sample('Training throughput', throughput, unit, metadata))
  return samples


def Run(benchmark_spec):
  """Run Horovod on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vms = benchmark_spec.vms
  vm_util.RunThreaded(lambda vm: vm.RemoteCommand('rm -rf /tmp/models'), vms)
  master_vm = vms[0]

  # GCP should work out of the box with the deep learning image but the AWS
  # image requires us to use the correct Tensorflow Python environment.
  if FLAGS.cloud == 'AWS':
    master_vm.RobustRemoteCommand('. anaconda3/bin/activate tensorflow_p36')
    python_interpreter = 'anaconda3/envs/tensorflow_p36/bin/python'
  else:
    python_interpreter = 'python3'

  nccl_params = [
      'TF_CPP_MIN_LOG_LEVEL=0',
      'NCCL_SOCKET_IFNAME=^lo,docker0',
  ]

  if benchmark_spec.timeline:
    nccl_params.extend([
        'HOROVOD_TIMELINE={}/timeline.json'.format(vm_util.VM_TMP_DIR),
        'HOROVOD_TIMELINE_MARK_CYCLES=1',
    ])

  if benchmark_spec.cuda_visible_devices:
    nccl_params.append('CUDA_VISIBLE_DEVICES={}'.format(
        benchmark_spec.cuda_visible_devices))

  if FLAGS.nccl_extra_params:
    for extra_param in FLAGS.nccl_extra_params:
      nccl_params.append(extra_param)

  run_command = ('mpirun -np {num_gpus} -hostfile {host_file} '
                 '-mca plm_rsh_no_tree_spawn 1 '
                 '--allow-run-as-root '
                 '-bind-to socket -map-by slot '
                 '{nccl_params} '
                 '-mca pml ob1 -mca btl ^openib '
                 '-mca btl_tcp_if_exclude lo,docker0 '
                 '{python} ').format(
                     num_gpus=benchmark_spec.total_gpus,
                     host_file=MACHINEFILE,
                     python=python_interpreter,
                     nccl_params=' '.join(
                         ['-x {}'.format(param) for param in nccl_params]))

  if benchmark_spec.model == 'resnet-50':
    resnet_dir = 'DeepLearningExamples/TensorFlow/Classification/RN50v1.5/'
    run_command += (
        'DeepLearningExamples/TensorFlow/Classification/RN50v1.5/main.py '
        '--mode=training_benchmark '
        '--warmup_steps 50 '
        '--precision {precision} '
        '--batch_size {batch_size} '
        '--results_dir /tmp/models '
        '--data_dir {data_dir} '
        '--iter_unit epoch '
        '--data_format NHWC '
        '--num_iter {num_epochs} ').format(
            precision=benchmark_spec.precision,
            batch_size=benchmark_spec.batch_size,
            num_epochs=benchmark_spec.num_epochs,
            data_dir='{}/imagenet'.format(resnet_dir))
  else:  # bert
    if not benchmark_spec.bert_finetune:
      raise NotImplementedError('BERT pretraining is not supported.')
    bert_dir = (
        'DeepLearningExamples/TensorFlow/LanguageModeling/BERT/'
        'data/download/google_pretrained_weights/{}').format(
            'uncased_L-12_H-768_A-12' if benchmark_spec.model ==
            'bert-base' else 'uncased_L-24_H-1024_A-16')
    run_command += (
        'DeepLearningExamples/TensorFlow/LanguageModeling/BERT/run_squad.py '
        '--vocab_file={vocab_file} '
        '--bert_config_file={bert_config} '
        '--init_checkpoint={init_ckpt} '
        '--do_train=True '
        '--train_file={train_file} '
        '--train_batch_size={batch_size} '
        '--learning_rate=5e-6 '
        '--num_train_epochs={num_epochs} '
        '--max_seq_length={max_seq_len} '
        '--doc_stride={doc_stride} '
        '--output_dir=/tmp/models '
        '--horovod '
        '{fp16} '
    ).format(
        batch_size=benchmark_spec.batch_size,
        num_epochs=benchmark_spec.num_epochs,
        fp16='--use_fp16' if benchmark_spec.precision == 'fp16' else '',
        vocab_file='{}/vocab.txt'.format(bert_dir),
        bert_config='{}/bert_config.json'.format(bert_dir),
        init_ckpt='{}/bert_model.ckpt'.format(bert_dir),
        max_seq_len=benchmark_spec.max_seq_len,
        doc_stride=64 if benchmark_spec.max_seq_len == 128 else 128,
        train_file='DeepLearningExamples/TensorFlow/LanguageModeling/BERT/data/download/squad/v1.1/train-v1.1.json',
    )
  stdout, stderr = master_vm.RobustRemoteCommand(run_command, should_log=True)

  if benchmark_spec.timeline:
    master_vm.PullFile(vm_util.GetTempDir(),
                       '{}/timeline.json'.format(vm_util.VM_TMP_DIR))
  return _MakeSamplesFromOutput(benchmark_spec, stdout, stderr)


def Cleanup(benchmark_spec):
  """Cleanup Horovod on the cluster."""
  del benchmark_spec
