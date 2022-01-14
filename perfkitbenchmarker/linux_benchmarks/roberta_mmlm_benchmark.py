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
"""Runs FairSeq Roberta Masked Multilingual LM benchmark."""

import posixpath
import re
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import hpc_util
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import nvidia_driver

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'robertammlm'
BENCHMARK_CONFIG = """
robertammlm:
  description: Runs FairSeq Roberta Masked Multilingual LM benchmark'
  vm_groups:
    default:
      disk_spec: *default_500_gb
      vm_count: 2
      vm_spec:
        GCP:
          machine_type: n1-highmem-96
          zone: us-west1-b
          image_family: tf-latest-gpu-gvnic
          image_project: deeplearning-platform-release
          boot_disk_size: 105
          boot_disk_type: pd-ssd
          gpu_type: v100
          gpu_count: 1
        AWS:
          machine_type: p3dn.24xlarge
          boot_disk_size: 105
          zone: us-east-1a
          image: ami-0a4a0d42e3b855a2c
        Azure:
          machine_type: Standard_ND40s_v2
          zone: eastus
          boot_disk_size: 105
"""
NVPROF = 'nvprof'
TFPROF = 'tfprof'
NONE = 'none'
DATA_PATH = '/tmp/data'
HOSTFILE = 'HOSTFILE'
# Facebook AI Research Sequence-to-Sequence Toolkit written in Python
FAIRSEQ_GIT = 'https://github.com/taylanbil/fairseq.git '
FAIRSEQ_BRANCH = 'synth-data-roberta'
# The raw WikiText103 dataset
WIKI_TEXT = 'https://s3.amazonaws.com/research.metamind.io/wikitext/wikitext-103-raw-v1.zip'
ENCODER_JSON = 'https://dl.fbaipublicfiles.com/fairseq/gpt2_bpe/encoder.json'
VOCAB_BPE = 'https://dl.fbaipublicfiles.com/fairseq/gpt2_bpe/vocab.bpe'
FAIRSEQ_DICT = 'https://dl.fbaipublicfiles.com/fairseq/gpt2_bpe/dict.txt'
WORD_COUNT = 249997
METADATA_COLUMNS = ('epoch', 'step', 'steps per epoch', 'loss', 'nll_loss',
                    'ppl', 'wps', 'ups', 'wpb', 'bsz', 'num_updates', 'lr',
                    'gnorm', 'clip', 'oom', 'loss_scale', 'wall', 'train_wall')


flags.DEFINE_integer('robertammlm_max_sentences', 2, 'max sentences')
flags.DEFINE_integer('robertammlm_log_interval', 10, 'log interval')
flags.DEFINE_integer('robertammlm_nproc_per_node', 8, 'nproc per node')
flags.DEFINE_integer('robertammlm_update_freq', None, 'update frequence')
flags.DEFINE_integer('robertammlm_num_copies', None,
                     'num of training data copies.')
flags.DEFINE_integer('robertammlm_global_batch_size', 8192, 'global batch size')
flags.DEFINE_integer('robertammlm_max_epoch', 1, 'max number of epoch')
flags.DEFINE_enum('robertammlm_profiler', None, [NVPROF, TFPROF],
                  'profiler used to analysis GPU training')


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


def _UpdateBenchmarkSpecWithFlags(benchmark_spec):
  """Update the benchmark_spec with supplied command line flags.

  Args:
    benchmark_spec: benchmark specification to update
  """
  benchmark_spec.max_sentences = FLAGS.robertammlm_max_sentences
  benchmark_spec.nproc_per_node = FLAGS.robertammlm_nproc_per_node
  benchmark_spec.log_interval = FLAGS.robertammlm_log_interval
  benchmark_spec.profiler = FLAGS.robertammlm_profiler
  benchmark_spec.max_epoch = FLAGS.robertammlm_max_epoch
  vms = benchmark_spec.vms
  vm = vms[0]
  num_vms = len(vms)
  benchmark_spec.num_vms = num_vms
  benchmark_spec.global_batch_size = FLAGS.robertammlm_global_batch_size
  num_accelerators = nvidia_driver.QueryNumberOfGpus(vm) * num_vms
  benchmark_spec.num_accelerators = num_accelerators
  if FLAGS.robertammlm_update_freq:
    benchmark_spec.update_freq = FLAGS.robertammlm_update_freq
  else:
    benchmark_spec.update_freq = (benchmark_spec.global_batch_size // (
        benchmark_spec.max_sentences * num_accelerators))
  if FLAGS.robertammlm_num_copies:
    benchmark_spec.num_copies = FLAGS.robertammlm_num_copies
  else:
    benchmark_spec.num_copies = max(1, num_accelerators // 32)


def _DownloadData(benchmark_spec, rank):
  """Downloads train valid and test on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
    rank: integer, the node rank in distributed training.
  """
  vm = benchmark_spec.vms[rank]
  vm.Install('pip3')
  vm.Install('wget')
  vm.RemoteCommand('[ -d $HOME/fairseq ] || git clone {git} -b {branch}'
                   .format(git=FAIRSEQ_GIT, branch=FAIRSEQ_BRANCH))
  vm.RemoteCommand(f'{FLAGS.torch_env} python3 -m pip install pyarrow')
  vm.RemoteCommand(f'cd fairseq && {FLAGS.torch_env} python3 -m pip install '
                   '--editable .')
  vm.RemoteCommand('mkdir -p {}'.format(DATA_PATH))
  text_zip = posixpath.join(DATA_PATH, posixpath.basename(WIKI_TEXT))
  vm.RemoteCommand('wget -O {des} {src}'.format(des=text_zip, src=WIKI_TEXT))
  vm.RemoteCommand('unzip {text_zip} -d {data_path}'
                   .format(data_path=DATA_PATH, text_zip=text_zip))
  bpe_dir = posixpath.join(DATA_PATH, 'gpt2_bpe')
  vm.RemoteCommand('mkdir -p {}'.format(bpe_dir))
  vm.RemoteCommand('wget -O {des}/encoder.json {src}'
                   .format(des=bpe_dir, src=ENCODER_JSON))
  vm.RemoteCommand('wget -O {des}/vocab.bpe {src}'
                   .format(des=bpe_dir, src=VOCAB_BPE))
  for phase in ('train', 'valid', 'test'):
    vm.RemoteCommand(
        f'cd {DATA_PATH} && {FLAGS.torch_env} python3 -m '
        'examples.roberta.multiprocessing_bpe_encoder '
        '--encoder-json gpt2_bpe/encoder.json '
        '--vocab-bpe gpt2_bpe/vocab.bpe '
        f'--inputs wikitext-103-raw/wiki.{phase}.raw '
        f'--outputs wikitext-103-raw/wiki.{phase}.bpe '
        '--keep-empty '
        '--workers 60 ')

  vm.RemoteCommand('wget -O {des}/dict.txt {src}'
                   .format(des=bpe_dir, src=FAIRSEQ_DICT))
  vm.RemoteCommand(
      f'cd {DATA_PATH} && {FLAGS.torch_env} fairseq-preprocess '
      '--only-source  --srcdict gpt2_bpe/dict.txt '
      '--trainpref wikitext-103-raw/wiki.train.bpe '
      '--validpref wikitext-103-raw/wiki.valid.bpe '
      '--testpref wikitext-103-raw/wiki.test.bpe '
      '--destdir data-bin/wikitext-103 '
      '--workers 60')
  data_bin = posixpath.join(DATA_PATH, 'data-bin')
  vm.RemoteCommand('mkdir -p {}/mlm-w103'.format(data_bin))
  vm.RemoteCommand('for x in `seq 1 {word_count}`;'
                   'do echo "$x 1" >> {data_bin}/mlm-w103/dict.txt;'
                   'done'.format(word_count=WORD_COUNT, data_bin=data_bin))

  for copy in range(benchmark_spec.num_copies):
    vm.RemoteCommand('cp -r {data_bin}/wikitext-103 {data_bin}/mlm-w103/{copy}'
                     .format(data_bin=data_bin, copy=copy))
    vm.RemoteCommand('cp {data_bin}/mlm-w103/dict.txt {data_bin}/mlm-w103/'
                     '{copy}'.format(data_bin=data_bin, copy=copy))


def _PrepareVm(benchmark_spec, rank):
  vm = benchmark_spec.vms[rank]
  if nvidia_driver.CheckNvidiaGpuExists(vm):
    vm.Install('cuda_toolkit')
    vm.AuthenticateVm()
    vm.Install('openmpi')
    vm.Install('nccl')
  _DownloadData(benchmark_spec, rank)
  vm.Install('pytorch')


def Prepare(benchmark_spec):
  """Install and set up RoBERTa mmlm on the target vm..

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vms = benchmark_spec.vms
  benchmark_spec.always_call_cleanup = True
  list_params = [((benchmark_spec, rank), {})
                 for rank in range(benchmark_spec.num_vms)]
  vm_util.RunThreaded(_PrepareVm, list_params)
  master = vms[0]
  if nvidia_driver.CheckNvidiaGpuExists(master):
    gpus_per_vm = nvidia_driver.QueryNumberOfGpus(master)
    hpc_util.CreateMachineFile(vms, lambda _: gpus_per_vm, HOSTFILE)


def _CreateMetadataDict(benchmark_spec):
  """Create metadata dict to be used in run results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    metadata dict
  """
  return {
      'model': 'roberta-mmlm',
      'log-interval': benchmark_spec.log_interval,
      'max-sentences': benchmark_spec.max_sentences,
      'nproc-per-node': benchmark_spec.nproc_per_node,
      'update-freq': benchmark_spec.update_freq,
      'global-batch-size': benchmark_spec.global_batch_size,
      'profiler': benchmark_spec.profiler,
      'max-epoch': benchmark_spec.max_epoch,
      'num_accelerators': benchmark_spec.num_accelerators,
      'nccl_version': FLAGS.nccl_version,
      'nccl_net_plugin': FLAGS.nccl_net_plugin,
      'nccl_extra_params': FLAGS.nccl_extra_params,
  }


def MakeSamplesFromOutput(metadata, output):
  """Create samples containing metrics.

  Args:
    metadata: dict contains all the metadata that reports.
    output: string, command output

  Returns:
    Samples containing training metrics.
  """
  results = regex_util.ExtractAllMatches(
      r'^\| epoch (\d+):\s+'
      r'(\d+) / (\d+) '
      r'loss=(\S+), '
      r'nll_loss=(\S+), '
      r'ppl=(\S+), '
      r'wps=(\S+), '
      r'ups=(\S+), '
      r'wpb=(\S+), '
      r'bsz=(\S+), '
      r'num_updates=(\S+), '
      r'lr=(\S+), '
      r'gnorm=(\S+), '
      r'clip=(\S+), '
      r'oom=(\S+), '
      r'loss_scale=(\S+), '
      r'wall=(\S+), '
      r'train_wall=(\S+)$',
      output, re.MULTILINE)
  samples = []
  for row in results:
    metadata_copy = metadata.copy()
    metadata_copy.update(zip(METADATA_COLUMNS, row))
    wps = float(metadata_copy['wps'])
    samples.append(sample.Sample('wps', wps, 'wps', metadata_copy))
    samples.append(sample.Sample('wps per accelerator',
                                 wps / metadata['num_accelerators'],
                                 'wps', metadata_copy))
  return samples


def _Run(benchmark_spec, rank):
  """Run the benchmark on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
    rank: integer, the node rank in distributed training.

  Returns:
    A list of sample.Sample objects.
  """
  vm = benchmark_spec.vms[rank]
  master = benchmark_spec.vms[0]
  nccl_env = []
  if FLAGS.nccl_cuda_visible_devices:
    nccl_env.append('CUDA_VISIBLE_DEVICES={}'
                    .format(FLAGS.nccl_cuda_visible_devices))
  nccl_env.extend(FLAGS.nccl_extra_params)

  prof_cmd = ''
  if benchmark_spec.profiler:
    prof_cmd = (r'{}/bin/nvprof --profile-child-processes '
                r'-o /tmp/pkb/%h.%p.nvprof'.format(cuda_toolkit.CUDA_HOME))

  distributed_cmd = (
      'torch.distributed.launch '
      '--nproc_per_node={nproc_per_node} '
      '--nnodes={num_vms} '
      '--node_rank={rank} '
      '--master_addr={addr} '
      '--master_port=2222'
      .format(num_vms=benchmark_spec.num_vms,
              rank=rank,
              addr=master.internal_ip,
              nproc_per_node=benchmark_spec.nproc_per_node))

  cmd_flags = {
      'adam-betas': "'(0.9, 0.98)'",
      'adam-eps': 1e-06,
      'arch': 'roberta_large',
      'attention-dropout': 0.1,
      'clip-norm': 1.0,
      'criterion': 'masked_lm',
      'disable-validation': '',
      'distributed-no-spawn': '',
      'dropout': 0.1,
      'fast-stat-sync': '',
      'log-format': 'simple',
      'lr': 0.0004,
      'lr-scheduler': 'polynomial_decay',
      'max-tokens': 6000,
      'max-update': 1500000,
      'memory-efficient-fp16': '',
      'multilang-sampling-alpha': 0.7,
      'num-workers': 4,
      'no-epoch-checkpoints': '',
      'no-save': '',
      'optimizer': 'adam',
      'sample-break-mode': 'complete',
      'save-interval-updates': 3000,
      'task': 'multilingual_masked_lm',
      'tokens-per-sample': 512,
      'total-num-update': 1500000,
      'train-subset': 'train',
      'valid-subset': 'valid',
      'warmup-updates': 15000,
      'weight-decay': 0.01,
  }

  cmd_flags.update({
      'log-interval': benchmark_spec.log_interval,
      'max-sentences': benchmark_spec.max_sentences,
      'update-freq': benchmark_spec.update_freq,
      'max-epoch': benchmark_spec.max_epoch,
  })
  roberta_benchmark_flags = ' '.join(
      f'--{key}={value}' if value else f'--{key}'
      for key, value in sorted(cmd_flags.items()))
  roberta_benchmark_cmd = (
      f'{FLAGS.torch_env} DGXSYSTEM=DGX1 NEXP=1 PULL=0 LOGDIR=/tmp/robertammlm '
      f'{" ".join(nccl_env)} {prof_cmd} python3 -m {distributed_cmd} '
      f'$HOME/fairseq/train.py {DATA_PATH}/data-bin/mlm-w103 '
      f'{roberta_benchmark_flags}')
  metadata = _CreateMetadataDict(benchmark_spec)
  stdout, _ = vm.RobustRemoteCommand(roberta_benchmark_cmd, should_log=True)
  return MakeSamplesFromOutput(metadata, stdout) if master == vm else []


def Run(benchmark_spec):
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  samples = []
  list_params = [((benchmark_spec, rank), {})
                 for rank in range(benchmark_spec.num_vms)]
  for results in vm_util.RunThreaded(_Run, list_params):
    samples.extend(results)
  return samples


def Cleanup(_):
  """Cleanup on the cluster."""
  pass
