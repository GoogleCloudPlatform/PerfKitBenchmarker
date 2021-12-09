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
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import hpc_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import nvidia_driver

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
          image_family: tf-latest-gpu-gvnic-debian-10
          image_project: deeplearning-platform-release
          boot_disk_size: 300
          gpu_type: v100
          gpu_count: 8
        AWS:
          machine_type: p3dn.24xlarge
          zone: us-east-1a
          image: ami-02e86b825fe559330
          boot_disk_size: 300
        Azure:
          machine_type: Standard_NC24rs_v3
          image: microsoft-dsvm:aml-workstation:ubuntu:19.11.13
          zone: eastus
          boot_disk_size: 300
      vm_count: null
"""

# TODO(user): Use NVIDIA's repo after
# https://github.com/NVIDIA/DeepLearningExamples/pull/386 is merged
GITHUB_MODELS_URL = 'https://github.com/changlan/DeepLearningExamples.git'
BERT_BASE_URL = 'https://storage.googleapis.com/bert_models/2018_10_18/uncased_L-12_H-768_A-12.zip'
BERT_LARGE_URL = 'https://storage.googleapis.com/bert_models/2018_10_18/uncased_L-24_H-1024_A-16.zip'

flags.DEFINE_enum(
    'horovod_model', 'resnet-50',
    ['resnet-50', 'bert-base', 'bert-large', 'maskrcnn', 'resnext-101'],
    'name of the model to run.')

flags.DEFINE_integer('horovod_batch_size', 64, 'Batch size per compute device.')

flags.DEFINE_integer('horovod_num_steps', 10,
                     'Number of steps (epochs for BERT) to train for. ')

flags.DEFINE_bool('horovod_synthetic', False,
                  'Whether to train with synthetic data.')

flags.DEFINE_enum('horovod_max_seq_len', '128', ['128', '384'],
                  'Max sequence length for BERT.')

flags.DEFINE_enum('horovod_precision', 'fp16', ['fp16', 'fp32'], 'Precision.')

flags.DEFINE_bool('horovod_bert_finetune', True,
                  'Pretrain or finetune a BERT model.')

flags.DEFINE_bool('horovod_timeline', False, 'Enable timeline in Horovod.')


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


def _CopyAndUpdateRunScripts(model, vm):
  """Copy and update all necessary run scripts on the given vm.

  Args:
    model: name of the model
    vm: vm to place and update run scripts on
  """
  vm.RemoteCommand(
      '[ -d "DeepLearningExamples" ] || git clone --branch clan-dev %s' %
      GITHUB_MODELS_URL)

  # MaskRCNN
  if model == 'maskrcnn':
    vm.RemoteCommand(
        'wget -q -N http://models.tensorpack.com/FasterRCNN/ImageNet-R50-AlignPadding.npz'
    )
    vm.RemoteCommand(
        'mkdir -p coco && cd coco && '
        'wget -q -N http://images.cocodataset.org/zips/train2017.zip && '
        'wget -q -N http://images.cocodataset.org/zips/val2017.zip && '
        'wget -q -N http://images.cocodataset.org/annotations/annotations_trainval2017.zip && '
        'unzip -q -o train2017.zip && unzip -q -o val2017.zip && '
        'unzip -q -o annotations_trainval2017.zip && rm *.zip')

  # BERT
  bert_base_dir = 'DeepLearningExamples/TensorFlow/LanguageModeling/BERT'
  if model == 'bert-base' or model == 'bert-large':
    vm.RemoteCommand(
        'mkdir -p {bert}/data/download/google_pretrained_weights &&'
        'mkdir -p {bert}/data/download/squad/v1.1 && '
        'cd {bert}/data/download/squad/v1.1 && '
        'wget -q https://rajpurkar.github.io/SQuAD-explorer/dataset/train-v1.1.json'
        .format(bert=bert_base_dir))

  get_bert_data_cmd = ('cd {bert}/data/download/google_pretrained_weights/ && '
                       'wget -q {url} && unzip -o $(basename {url})')
  if model == 'bert-base':
    vm.RemoteCommand(
        get_bert_data_cmd.format(bert=bert_base_dir, url=BERT_BASE_URL))

  if model == 'bert-large':
    vm.RemoteCommand(
        get_bert_data_cmd.format(bert=bert_base_dir, url=BERT_LARGE_URL))


def PrepareHorovod(vm):
  """Install dependencies on a single vm.

  Args:
    vm: vm to operate on
  """
  logging.info('Installing Horovod on %s', vm)
  vm.AuthenticateVm()

  vm.Install('google_cloud_sdk')
  vm.Install('openmpi')
  vm.InstallPackages('wget git unzip')
  vm.Install('nccl')

  pip = 'pip'
  if FLAGS.cloud == 'GCP':
    pip = '/opt/conda/bin/pip'
    vm.RemoteCommand(f'sudo {pip} install --force-reinstall pyarrow')
  elif FLAGS.cloud == 'AWS':
    vm.RobustRemoteCommand('. anaconda3/bin/activate tensorflow_p37')
    pip = 'anaconda3/envs/tensorflow_p37/bin/pip'

  # 10.0 -> 110
  cuda_version = cuda_toolkit.GetCudaToolkitVersion(vm).replace('.', '')
  vm.RemoteCommand(
      f'sudo {pip} install '
      '--extra-index-url https://developer.download.nvidia.com/compute/redist/ '
      'git+https://github.com/NVIDIA/dllogger.git '
      f'nvidia-dali-cuda{cuda_version}')

  vm.RemoteCommand(
      f'sudo {pip} install '
      '--extra-index-url https://developer.download.nvidia.com/compute/redist/ '
      f'nvidia-dali-tf-plugin-cuda{cuda_version}')
  vm.RemoteCommand(f'sudo {pip} uninstall -y horovod')
  vm.RemoteCommand(
      f'sudo HOROVOD_GPU_OPERATIONS=NCCL HOROVOD_WITH_TENSORFLOW=1 HOROVOD_WITH_MPI=1 {pip} install -U --no-cache horovod'
  )
  vm.RemoteCommand(
      f'sudo {pip} install pynvml cython scipy \'opencv-python==3.4.2.17\'')
  vm.RemoteCommand(
      f'sudo {pip} install \'git+https://github.com/cocodataset/cocoapi.git#subdirectory=PythonAPI\''
  )
  vm.RemoteCommand(
      f'[ -d "tensorpack" ] || git clone https://github.com/tensorpack/tensorpack.git && sudo {pip} install ./tensorpack'
  )

  _CopyAndUpdateRunScripts(FLAGS.horovod_model, vm)


def Prepare(benchmark_spec):
  """Install and set up Horovod on the target vms.

  Args:
    benchmark_spec: The benchmark specification
  """
  vms = benchmark_spec.vms
  vm_util.RunThreaded(PrepareHorovod, vms)
  hpc_util.CreateMachineFile(vms, nvidia_driver.QueryNumberOfGpus, MACHINEFILE)


def _CreateMetadataDict(vms):
  """Create metadata dict to be used in run results.

  Args:
    vms: A list of worker VMs.

  Returns:
    metadata dict
  """
  vm = vms[0]
  gpus_per_node = nvidia_driver.QueryNumberOfGpus(vm)
  num_vms = len(vms)
  total_gpus = gpus_per_node * num_vms

  metadata = dict()
  metadata.update(cuda_toolkit.GetMetadata(vm))
  metadata['benchmark_version'] = BENCHMARK_VERSION
  metadata['num_nodes'] = len(vms)
  metadata['total_gpus'] = int(total_gpus)
  metadata['model'] = FLAGS.horovod_model
  metadata['batch_size'] = FLAGS.horovod_batch_size
  metadata['num_steps'] = FLAGS.horovod_num_steps
  metadata['synthetic'] = FLAGS.horovod_synthetic
  metadata['precision'] = FLAGS.horovod_precision
  metadata['max_seq_len'] = int(FLAGS.horovod_max_seq_len)
  metadata['nccl_version'] = FLAGS.nccl_version
  metadata['nccl_net_plugin'] = FLAGS.nccl_net_plugin
  metadata['cuda_visible_devices'] = FLAGS.nccl_cuda_visible_devices
  metadata['nccl_extra_params'] = FLAGS.nccl_extra_params
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
    if 'train_throughput' in line:
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


def _ExtractMaskRCNNThroughput(output):
  """Extract throughput from Horovod output.

  Args:
    output: Horovod output

  Returns:
    A tuple of:
      Average throughput in sentences per second (float)
      Unit of the throughput metric (str)
  """
  total_xput, unit = [], None
  for line in output.splitlines()[::-1]:
    if 'Throughput' in line:
      split_line = line.split()
      xput, unit = float(split_line[-1]), split_line[-2][1:-2]
      total_xput.append(xput)
  if not total_xput:
    raise ValueError('No "Throughput" found in {}'.format(output))
  return round(sum(total_xput) / len(total_xput), 1), unit


def _MakeSamplesFromOutput(vms, stdout, stderr):
  """Create a sample continaing the measured Horovod throughput.

  Args:
    vms: a list of worker VMs
    stdout: stdout
    stderr: stderr

  Returns:
    list of a Sample containing the Horovod throughput
  """
  metadata = _CreateMetadataDict(vms)
  output = stdout + stderr

  extractor = {
      'resnet-50': _ExtractResNetThroughput,
      'resnext-101': _ExtractResNetThroughput,
      'bert-base': _ExtractBertThroughput,
      'bert-large': _ExtractBertThroughput,
      'maskrcnn': _ExtractMaskRCNNThroughput,
  }

  throughput, unit = extractor[FLAGS.horovod_model](output)

  samples = []
  samples.append(
      sample.Sample('Training throughput', throughput, unit, metadata))
  return samples


def Run(benchmark_spec):
  """Wrapper of RunWithVMs.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """

  return RunWithVMs(benchmark_spec.vms)


def RunWithVMs(vms, extra_envs=None):
  """Run Horovod on the cluster.

  Args:
    vms: A list of worker VMs.
    extra_envs: A dictionary of environment variables.

  Returns:
    A list of sample.Sample objects.
  """
  vm_util.RunThreaded(lambda vm: vm.RemoteCommand('rm -rf /tmp/models'), vms)
  master_vm = vms[0]

  gpus_per_node = nvidia_driver.QueryNumberOfGpus(master_vm)
  num_vms = len(vms)
  total_gpus = gpus_per_node * num_vms

  # GCP should work out of the box with the deep learning image but the AWS
  # image requires us to use the correct Tensorflow Python environment.
  if FLAGS.cloud == 'AWS':
    master_vm.RobustRemoteCommand('. anaconda3/bin/activate tensorflow_p37')
    python_interpreter = 'anaconda3/envs/tensorflow_p37/bin/python'
  else:
    python_interpreter = '/opt/conda/bin/python'

  nccl_params = {
      'TF_CPP_MIN_LOG_LEVEL': 0,
      'NCCL_SOCKET_IFNAME': '^lo,docker0',
      'NCCL_DEBUG': 'INFO',
  }

  if FLAGS.horovod_timeline:
    nccl_params['HOROVOD_TIMELINE_MARK_CYCLES'] = 1
    nccl_params['HOROVOD_TIMELINE'] = f'{vm_util.VM_TMP_DIR}/timeline.json'

  if FLAGS.nccl_cuda_visible_devices:
    nccl_params['CUDA_VISIBLE_DEVICES'] = FLAGS.nccl_cuda_visible_devices

  if FLAGS.nccl_extra_params:
    for extra_param in FLAGS.nccl_extra_params:
      k, v = extra_param.split('=', 1)
      nccl_params[k] = v

  if extra_envs:
    nccl_params.update(extra_envs)

  run_command = (
      '{mpi} -np {num_gpus} -hostfile {host_file} '
      '-mca plm_rsh_no_tree_spawn 1 '
      '--allow-run-as-root '
      '-bind-to socket -map-by slot '
      '{nccl_params} '
      '-mca pml ob1 -mca btl ^openib '
      '-mca btl_tcp_if_exclude lo,docker0 '
      '{python} ').format(
          mpi=FLAGS.nccl_mpi,
          num_gpus=total_gpus,
          host_file=MACHINEFILE,
          python=python_interpreter,
          nccl_params=' '.join(
              [f'-x {key}={value}' for key, value in nccl_params.items()]))

  if FLAGS.horovod_model == 'resnet-50':
    run_flags = {
        'arch': 'resnet50',
        'mode': 'training_benchmark',
        'warmup_steps': 101,
        'results_dir': '/tmp/models',
        'gpu_memory_fraction': 0.95,
        'static_loss_scale': 128,
        'lr_init': 0.016,
        'lr_warmup_epochs': 8,
        'momentum': 0.875,
        'weight_decay': 3.0517578125e-05,
        'iter_unit': 'batch'
    }
    run_flags.update({
        'batch_size': FLAGS.horovod_batch_size,
        'num_iter': FLAGS.horovod_num_steps,
    })
    if FLAGS.horovod_precision == 'fp16':
      run_flags['amp'] = None

    # Load ImageNet training data from GCS if benchmark is not in synthetic mode
    if not FLAGS.horovod_synthetic:
      run_flags['data_dir'] = 'gs://cloud-ml-nas-public/classification/imagenet'

    run_command += 'DeepLearningExamples/TensorFlow/Classification/ConvNets/main.py '
    run_command += ' '.join([
        '--{}'.format(key) if value is None else '--{}={}'.format(key, value)
        for key, value in sorted(run_flags.items())
    ])
  elif FLAGS.horovod_model == 'resnext-101':
    run_flags = {
        'arch': 'resnext101-32x4d',
        'mode': 'training_benchmark',
        'warmup_steps': 101,
        'results_dir': '/tmp/models',
        'gpu_memory_fraction': 0.95,
        'use_static_loss_scaling': None,
        'loss_scale': 128,
        'lr_init': 0.016,
        'lr_warmup_epochs': 8,
        'momentum': 0.875,
        'weight_decay': 3.0517578125e-05,
        'weight_init': 'fan_in',
        'iter_unit': 'batch'
    }
    run_flags.update({
        'precision': FLAGS.horovod_precision,
        'batch_size': FLAGS.horovod_batch_size,
        'num_iter': FLAGS.horovod_num_steps,
    })

    # Load ImageNet training data from GCS if benchmark is not in synthetic mode
    if not FLAGS.horovod_synthetic:
      run_flags['data_dir'] = 'gs://cloud-ml-nas-public/classification/imagenet'

    run_command += 'DeepLearningExamples/TensorFlow/Classification/ConvNets/main.py '
    run_command += ' '.join([
        '--{}'.format(key) if value is None else '--{}={}'.format(key, value)
        for key, value in sorted(run_flags.items())
    ])
  elif FLAGS.horovod_model.startswith('bert'):  # bert
    if not FLAGS.horovod_bert_finetune:
      raise NotImplementedError('BERT pretraining is not supported.')
    bert_dir = 'DeepLearningExamples/TensorFlow/LanguageModeling/BERT/data/download/google_pretrained_weights/{}'.format(
        'uncased_L-12_H-768_A-12' if FLAGS.horovod_model ==
        'bert-base' else 'uncased_L-24_H-1024_A-16')
    squad_train_file = 'DeepLearningExamples/TensorFlow/LanguageModeling/BERT/data/download/squad/v1.1/train-v1.1.json'
    run_flags = {
        'vocab_file': '{}/vocab.txt'.format(bert_dir),
        'bert_config_file': '{}/bert_config.json'.format(bert_dir),
        'init_checkpoint': '{}/bert_model.ckpt'.format(bert_dir),
        'do_train': None,
        'train_file': squad_train_file,
        'learning_rate': 5e-6,
        'output_dir': '/tmp/models',
        'horovod': None,
        'dllog_path': '/tmp/bert_dllog.json',
        'save_checkpoints_steps': 0,
    }
    run_flags.update({
        'precision': FLAGS.horovod_precision,
        'train_batch_size': FLAGS.horovod_batch_size,
        'num_train_epochs': FLAGS.horovod_num_steps,
        'max_seq_length': FLAGS.horovod_max_seq_len,
        'doc_stride': 64 if FLAGS.horovod_max_seq_len == 128 else 128,
        'amp': FLAGS.horovod_precision == 'fp16'
    })
    run_command += 'DeepLearningExamples/TensorFlow/LanguageModeling/BERT/run_squad.py '
    run_command += ' '.join([
        '--{}'.format(key) if value is None else '--{}={}'.format(key, value)
        for key, value in sorted(run_flags.items())
    ])
  else:
    run_command += (
        'tensorpack/examples/FasterRCNN/train.py --config '
        'BACKBONE.WEIGHTS=ImageNet-R50-AlignPadding.npz '
        'DATA.BASEDIR=coco '
        'TRAINER=horovod '
        'TRAIN.EVAL_PERIOD=0 '
        # LR_SCHEDULE means equivalent steps when the total batch size is 8.
        'TRAIN.LR_SCHEDULE="[{step}, {step}, {step}]" '
        '--logdir {log_dir}/maskrcnn ').format(
            log_dir=vm_util.VM_TMP_DIR,
            step=FLAGS.horovod_num_steps * total_gpus // 8)
  stdout, stderr = master_vm.RobustRemoteCommand(run_command, should_log=True)

  if FLAGS.horovod_timeline:
    master_vm.PullFile(vm_util.GetTempDir(),
                       '{}/timeline.json'.format(vm_util.VM_TMP_DIR))
  return _MakeSamplesFromOutput(vms, stdout, stderr)


def Cleanup(benchmark_spec):
  """Cleanup Horovod on the cluster."""
  del benchmark_spec
