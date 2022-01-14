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
"""Run MLPerf benchmarks."""

import posixpath
import re
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import google_cloud_sdk
from perfkitbenchmarker.linux_packages import nvidia_driver
from perfkitbenchmarker.linux_packages import tensorflow
from perfkitbenchmarker.providers.gcp import gcs
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS
MLPERF_VERSION = 'v1.0'

BENCHMARK_NAME = 'mlperf'
BENCHMARK_CONFIG = """
mlperf:
  description: Runs MLPerf Benchmark.
  vm_groups:
    default:
      disk_spec: *default_500_gb
      vm_spec:
        GCP:
          machine_type: a2-highgpu-8g
          zone: us-central1-b
          boot_disk_size: 200
        AWS:
          machine_type: p4d.24xlarge
          zone: us-west-2a
          boot_disk_size: 200
          image: ami-0ccc71d716eb5d6a4
        Azure:
          machine_type: Standard_ND96asr_v4
          zone: westus2
          boot_disk_size: 200
          image: microsoft-dsvm:ubuntu-hpc:1804:latest
"""

DGXSYSTEM = 'DGXA100_singlenode'
CONFIG = f'config_{DGXSYSTEM}.sh'
TRANSFORMER = 'transformer'
RESNET = 'resnet'
MASK = 'mask'
GNMT = 'gnmt'
SSD = 'ssd'
MINIGO = 'minigo'
BERT = 'bert'
DLRM = 'dlrm'

flags.DEFINE_enum('mlperf_benchmark', RESNET,
                  [RESNET, TRANSFORMER, MASK, GNMT, SSD, MINIGO, BERT, DLRM],
                  'MLPerf benchmark test to run.')

NVPROF = 'nvprof'
TFPROF = 'tfprof'
NONE = 'none'

flags.DEFINE_enum('mlperf_profiler', NONE, [NVPROF, TFPROF, NONE],
                  'profiler used to analysis GPU training')
flags.DEFINE_integer('mlperf_profile_steps', 20, 'number of steps to profile')
flags.DEFINE_string('mlperf_bucket', None, 'GCS bucket for mlperf results; '
                    'only used for TPU runs.')

flags.DEFINE_string(
    'mlperf_gcs_resnet_checkpoint',
    'gs://p3rf-mlperf/resnet50-checkpoint-2018-02-07/model.ckpt-112603',
    'A ResNet backbone trained on the ImageNet dataset.')
flags.DEFINE_string(
    'mlperf_transformer_decode_dir', '', 'Transformer decode directory')
flags.DEFINE_string('wmt_data_dir',
                    f'gs://p3rf-mlperf/mlperf_{MLPERF_VERSION}_nv_transformer',
                    'Directory where the wmt dataset is stored')
flags.DEFINE_string('coco_data_dir', 'gs://p3rf-mlperf/coco2017',
                    'Directory where the coco dataset is stored')
flags.DEFINE_string('gnmt_data_dir',
                    f'gs://p3rf-mlperf/mlperf_{MLPERF_VERSION}_nv_gnmt',
                    'Directory where the nv 1.0 WMT dataset is stored')
flags.DEFINE_string('bert_data_dir', 'gs://p3rf-mlperf/bert_data',
                    'Directory where the nv bert dataset is stored.')
flags.DEFINE_string('minigo_model_dir', '',
                    'Directory on GCS to copy minigo source data from. Files '
                    'will be copied from subdirectories of src_dir '
                    'corresponding to the board size.')
RESNET_EPOCHS = flags.DEFINE_integer(
    'mlperf_resnet_epochs', 48,
    'The Number of epochs to use for training ResNet.', lower_bound=4)
MASK_ITERATION = flags.DEFINE_integer(
    'mlperf_mask_iteration', 20000,
    'The Number of iteration to use for training Mask R-CNN.', lower_bound=1)
BERT_STEPS = flags.DEFINE_integer(
    'mlperf_bert_steps', 1271,
    'The Number of steps to use for training BERT.', lower_bound=1)

BERT_BATCH_SIZE = flags.DEFINE_integer(
    'mlperf_bert_batch_size', 46, 'The batch size to use for training BERT.')

HYPERTHREADS = flags.DEFINE_bool('mlperf_hyperthreads', True,
                                 'enable or disable binding to hyperthreads')

RE_FLOAT = r'\d+\.\d+'


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
  benchmark_spec.imagenet_data_dir = FLAGS.imagenet_data_dir
  benchmark_spec.benchmark = FLAGS.mlperf_benchmark
  benchmark_spec.wmt_data_dir = FLAGS.wmt_data_dir
  benchmark_spec.coco_data_dir = FLAGS.coco_data_dir
  benchmark_spec.gnmt_data_dir = FLAGS.gnmt_data_dir
  benchmark_spec.bert_data_dir = FLAGS.bert_data_dir
  benchmark_spec.gcp_service_account = FLAGS.gcp_service_account


def _DownloadData(data_dir, data_path, vm):
  """Download remote benchmark data to local.

  Args:
    data_dir: remote benchmark location
    data_path: local benchmark location
    vm: vm to download the data
  """

  vm.Install('google_cloud_sdk')
  vm.RemoteCommand(
      'if [ ! -d \"{data_path}\" ]; then '
      '  sudo mkdir -p {data_path} && '
      '  sudo chmod a+w {data_path} && '
      '  {gsutil_path} -m cp -r {data_dir}/* {data_path} ;'
      'fi'.format(
          data_dir=data_dir,
          gsutil_path=google_cloud_sdk.GSUTIL_PATH,
          data_path=data_path))


def PrepareBenchmark(benchmark_spec, vm=None):
  """Install and set up MLPerf on the target vm.

  Args:
    benchmark_spec: The benchmark specification
    vm: The VM to work on

  Raises:
    errors.Config.InvalidValue upon both GPUs and TPUs appear in the config
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vm = vm or benchmark_spec.vms[0]

  if (bool(benchmark_spec.tpus) and nvidia_driver.CheckNvidiaGpuExists(vm)):
    raise errors.Config.InvalidValue(
        'Invalid configuration. GPUs and TPUs can not both present in the config.'
    )

  vm.RemoteCommand(
      f'if [ ! -d "$HOME/training_results_{MLPERF_VERSION}" ]; then '
      f'  git clone https://github.com/mlcommons/training_results_{MLPERF_VERSION}.git ; '
      'fi',
      should_log=True)
  vm.Install('pip3')
  if not HYPERTHREADS.value:
    if BERT in benchmark_spec.benchmark:
      vm_util.ReplaceText(
          vm, "'bind_pyt'", "'bind_pyt' '--no_hyperthreads'",
          f'training_results_{MLPERF_VERSION}/NVIDIA/benchmarks/bert/'
          'implementations/pytorch/run_with_docker.sh')
    elif MASK in benchmark_spec.benchmark:
      vm_util.ReplaceText(
          vm, "'bind_launch'", "'bind_launch' '--no_hyperthreads'",
          f'training_results_{MLPERF_VERSION}/NVIDIA/benchmarks/maskrcnn/'
          'implementations/pytorch/run_and_time.sh')
    elif RESNET in benchmark_spec.benchmark:
      vm_util.ReplaceText(
          vm, '--cpu=exclusive', '--cpu=exclusive,nosmt',
          f'training_results_{MLPERF_VERSION}/NVIDIA/benchmarks/resnet/'
          'implementations/mxnet/run_and_time.sh')


def PrepareRunner(benchmark_spec, vm=None):
  """Install and set up MLPerf on the target vm.

  Args:
    benchmark_spec: The benchmark specification
    vm: The VM to work on

  Raises:
    errors.Config.InvalidValue upon both GPUs and TPUs appear in the config
  """
  vm = vm or benchmark_spec.vms[0]
  if benchmark_spec.tpus:
    if vm == benchmark_spec.vms[0]:
      storage_service = gcs.GoogleCloudStorageService()
      benchmark_spec.storage_service = storage_service
      if FLAGS.mlperf_bucket:
        bucket = FLAGS.mlperf_bucket
        benchmark_spec.model_dir = f'gs://{bucket}/pkb-{FLAGS.run_uri}'
      else:
        bucket = f'pkb-{FLAGS.run_uri}'.format(uri=FLAGS.run_uri)
        benchmark_spec.model_dir = f'gs://{bucket}'

      benchmark_spec.bucket = bucket
      location = benchmark_spec.tpu_groups['train'].GetZone()
      storage_service.PrepareService(util.GetRegionFromZone(location))
      storage_service.MakeBucket(bucket)
      storage_service.AclBucket(benchmark_spec.gcp_service_account, gcs.WRITER,
                                bucket)

    # For MLPerf 1.0, the benchmake code of different hardware are different.
    if (benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-32' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-128' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-256' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-512' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-1024' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-2048'):
      run_path = (
          '$HOME/training_results_{version}/Google/benchmarks/{model}/tpu-{tpus}'
          .format(
              version=MLPERF_VERSION,
              model=benchmark_spec.benchmark,
              tpus=benchmark_spec.tpu_groups['train'].GetAcceleratorType()))
    else:
      raise ValueError(
          'MLPerf configurations do not support the hardware in PKB. PKB may '
          'need to be updated if this is a new TPU type.')

    if MASK in benchmark_spec.benchmark:
      model = 'mask_rcnn'
    elif GNMT in benchmark_spec.benchmark:
      model = 'nmt'
    else:
      model = benchmark_spec.benchmark

    code_path = (
        '$HOME/training_results_{version}/Google/benchmarks/{model}/implementations/tpu-{tpus}-{model}'
        .format(
            version=MLPERF_VERSION,
            model=benchmark_spec.benchmark,
            tpus=benchmark_spec.tpu_groups['train'].GetAcceleratorType()))

    vm.RemoteCommand('pip3 install --upgrade pyyaml==3.13 ')
    vm.RemoteCommand('pip3 install cloud-tpu-profiler==1.12')
    if (MASK in benchmark_spec.benchmark or
        SSD in benchmark_spec.benchmark):
      # Install the coco package, to load the coco dataset for Mask-RCNN
      # and SSD benchmarks.
      # TODO(user): coco whl package for python 3.5
      vm.RemoteCommand(
          'cd /tmp && '
          f'wget https://storage.cloud.google.com/mlperf_artifcats/{MLPERF_VERSION}_training/coco-1.1-cp36-cp36m-linux_x86_64.whl'
      )

    setup_script = posixpath.join(run_path, 'setup.sh')
    vm_util.ReplaceText(vm, '--progress-bar off', ' ', setup_script)
    vm_util.ReplaceText(vm, 'pip ', 'pip3 ', setup_script)
    vm.RemoteCommand(
        'chmod 755 {script} && {script}'.format(script=setup_script))

    if MASK not in benchmark_spec.benchmark:
      vm.RemoteCommand(
          'pip3 uninstall -y tf-estimator-nightly && '
          'pip3 install tf-estimator-nightly==1.14.0.dev2019051801')

    if RESNET in benchmark_spec.benchmark:
      data_dir = benchmark_spec.imagenet_data_dir
    elif TRANSFORMER in benchmark_spec.benchmark:
      data_dir = benchmark_spec.wmt_data_dir
    elif MASK in benchmark_spec.benchmark:
      data_dir = benchmark_spec.coco_data_dir
    elif GNMT in benchmark_spec.benchmark:
      data_dir = benchmark_spec.gnmt_data_dir
    elif SSD in benchmark_spec.benchmark:
      data_dir = benchmark_spec.coco_data_dir
    elif BERT in benchmark_spec.benchmark:
      data_dir = benchmark_spec.bert_data_dir
    else:
      raise ValueError('Unknown operation, cannot find {} in benchmark'.format(
          benchmark_spec.benchmark))

    run_script = posixpath.join(run_path, 'run_and_time.sh')
    data_dir = data_dir.replace('/', r'\/')
    checkpoint = FLAGS.mlperf_gcs_resnet_checkpoint.replace('/', r'\/')
    decode_dir = FLAGS.mlperf_transformer_decode_dir.replace('/', r'\/')
    tpu = benchmark_spec.tpu_groups['train'].GetName()
    vm_util.ReplaceText(vm, '--model_dir=.*', r'--model_dir=gs:\/\/{} \\\\'
                        .format(bucket), run_script)
    vm_util.ReplaceText(vm, '--data_dir=.*',
                        r'--data_dir={} \\\\'.format(data_dir), run_script)
    vm_util.ReplaceText(vm, '--training_file_pattern=.*',
                        r'--training_file_pattern={}\/train-* \\\\'
                        .format(data_dir), run_script)
    vm_util.ReplaceText(vm, '--validation_file_pattern=.*',
                        r'--validation_file_pattern={}\/val-* \\\\'
                        .format(data_dir), run_script)
    vm_util.ReplaceText(vm, '--val_json_file=.*',
                        r'--val_json_file={}\/instances_val2017.json \\\\'
                        .format(data_dir), run_script)
    vm_util.ReplaceText(vm, '--resnet_checkpoint=.*',
                        r'--resnet_checkpoint={} \\\\'.format(checkpoint),
                        run_script)
    vm_util.ReplaceText(vm, '--decode_from_file=.*',
                        r'--decode_from_file={}\/wmt14-en-de.src \\\\'
                        .format(decode_dir), run_script)
    vm_util.ReplaceText(vm, '--decode_reference=.*',
                        r'--decode_reference={}\/wmt14-en-de.ref \\\\'
                        .format(decode_dir), run_script)
    vm_util.ReplaceText(vm, '--decode_to_file=.*',
                        r'--decode_to_file={}\/decode.transformer_mlperf_tpu.'
                        r'translate_ende_wmt32k_packed.2x2_log_1018_2 \\\\'
                        .format(bucket), run_script)
    vm_util.ReplaceText(vm, '--tpu=.*', r'--tpu={} \\\\'.format(tpu),
                        run_script)
    vm_util.ReplaceText(vm, '--output_dir=.*', r'--output_dir=gs:\/\/{} \\\\'
                        .format(bucket), run_script)
    vm_util.ReplaceText(vm, '--cloud_tpu_name=.*',
                        r'--cloud_tpu_name={} \\\\'.format(tpu), run_script)
    vm_util.ReplaceText(vm, '--out_dir=.*',
                        r'--out_dir=gs:\/\/{} \\\\'.format(bucket), run_script)
    vm_util.ReplaceText(vm, '--tpu_name=.*', r'--tpu_name={} \\\\'.format(tpu),
                        run_script)
    vm.RemoteCommand('chmod 755 {}'.format(run_script))

    if GNMT in benchmark_spec.benchmark:
      metric_script = posixpath.join(code_path, model, 'metric.py')
      vm_util.ReplaceText(vm, ' sacrebleu -t', ' python3 -m sacrebleu -t',
                          metric_script)
  else:
    benchmark_spec.model_dir = '/tmp'

    has_gpu = nvidia_driver.CheckNvidiaGpuExists(vm)
    if has_gpu:
      vm.Install('cuda_toolkit')

    vm.Install('nvidia_docker')
    vm.RemoteCommand('if [ ! -d "/data" ]; then sudo ln -s /scratch /data; fi')

    if RESNET in benchmark_spec.benchmark:
      run_script = f'training_results_{MLPERF_VERSION}/NVIDIA/benchmarks/resnet/implementations/mxnet/run_and_time.sh'
      vm.RemoteCommand(
          f'cd training_results_{MLPERF_VERSION}/NVIDIA/benchmarks/resnet/implementations/mxnet &&'
          ' sudo docker build --network=host . -t mlperf-nvidia:image_classification',
          should_log=True)
      _DownloadData(benchmark_spec.imagenet_data_dir,
                    posixpath.join('/data', 'imagenet'), vm)

    if TRANSFORMER in benchmark_spec.benchmark:
      vm.RemoteCommand(
          f'cd training_results_{MLPERF_VERSION}/NVIDIA/benchmarks/transformer/implementations/pytorch &&'
          ' sudo docker build --network=host . -t mlperf-nvidia:translation',
          should_log=True)
      _DownloadData(benchmark_spec.wmt_data_dir, posixpath.join('/data', 'wmt'),
                    vm)

    if MINIGO in benchmark_spec.benchmark:
      build_path = f'training_results_{MLPERF_VERSION}/NVIDIA/benchmarks/minigo/implementations/tensorflow'
      run_script = posixpath.join(build_path, 'run_and_time.sh')
      vm_util.ReplaceText(vm, 'get_data.py', 'get_data.py --src_dir={}'.format(
          FLAGS.minigo_model_dir.replace('/', r'\/')), run_script)
      vm.RemoteCommand('cd {} && sudo docker build --network=host -t '
                       'mlperf-nvidia:minigo .'.format(build_path),
                       should_log=True)

    if MASK in benchmark_spec.benchmark:
      vm.RemoteCommand(
          f'cd training_results_{MLPERF_VERSION}/NVIDIA/benchmarks/maskrcnn/implementations/pytorch && '
          'sudo docker build --network=host -t mlperf-nvidia:object_detection . ',
          should_log=True)
      _DownloadData(benchmark_spec.coco_data_dir,
                    posixpath.join('/data', 'coco2017'), vm)

    if GNMT in benchmark_spec.benchmark:
      vm.RemoteCommand(
          f'cd training_results_{MLPERF_VERSION}/NVIDIA/benchmarks/gnmt/implementations/pytorch && '
          'sudo docker build --network=host -t mlperf-nvidia:rnn_translator . ',
          should_log=True)
      _DownloadData(benchmark_spec.gnmt_data_dir,
                    posixpath.join('/data', 'gnmt'), vm)

    if SSD in benchmark_spec.benchmark:
      vm.RemoteCommand(
          f'cd training_results_{MLPERF_VERSION}/NVIDIA/benchmarks/ssd/implementations/pytorch && '
          'sudo docker build --network=host -t mlperf-nvidia:single_stage_detector . ',
          should_log=True)
      _DownloadData(benchmark_spec.coco_data_dir,
                    posixpath.join('/data', 'coco2017'), vm)

    if BERT in benchmark_spec.benchmark:
      vm.RemoteCommand(
          f'cd training_results_{MLPERF_VERSION}/NVIDIA/benchmarks/bert/implementations/pytorch && '
          'sudo docker build --network=host -t mlperf-nvidia:language_model . ',
          should_log=True)
      _DownloadData(benchmark_spec.bert_data_dir,
                    posixpath.join('/data', 'bert_data'), vm)


def _GetChangesForMask(config_sed_input):
  """Get changes to config and run scripts for MaskRCNN.

  Also update train_mlperf.py if nvprof is used.

  Args:
    config_sed_input: Input list of sed pairs for config_DGXA100.sh.

  Returns:
    config_sed_output: Output list of sed pairs for config_DGXA100.sh.
  """
  config_sed = config_sed_input
  config_sed += [('SOLVER_MAX_ITER=.*',
                  f'SOLVER_MAX_ITER={MASK_ITERATION.value}')]
  config_sed += [(r'WALLTIME_MINUTES=100',
                  r'WALLTIME_MINUTES=100\n'
                  r'export CONT=mlperf-nvidia:object_detection\n'
                  r'export DATADIR=\/data\n'
                  r'export PKLDIR=\/data\/coco2017\/pkl_coco\n'
                  r'export NEXP=1')]
  return config_sed


def _GetChangesForResnet(config_sed_input):
  """Get changes to config and run scripts for Resnet.

  Args:
    config_sed_input: Input list of sed pairs for config_DGXA100.sh.

  Returns:
    config_sed_output: Output list of sed pairs for config_DGXA100.sh.
  """
  config_sed = config_sed_input
  config_sed += [(r'NUMEPOCHS=.*',
                  fr'NUMEPOCHS={RESNET_EPOCHS.value}\n'
                  r'export CONT=mlperf-nvidia:image_classification\n'
                  r'export DATADIR=\/data\/imagenet')]

  return config_sed


def _GetChangesForBert(config_sed_input):
  """Get changes to config and run scripts for BERT.

  Args:
    config_sed_input: Input list of sed pairs for config_DGXA100.sh.

  Returns:
    config_sed_output: Output list of sed pairs for config_DGXA100.sh.
  """
  config_sed = config_sed_input

  config_sed += [(r'source .*',
                  r'export CONT=mlperf-nvidia:language_model\n'
                  r'export NEXP=1')]
  config_sed += [(r'DATADIR=.*',
                  r'DATADIR=\/data\/bert_data\/2048_shards_uncompressed')]
  config_sed += [(r'MAX_STEPS=.*', f'MAX_STEPS={BERT_STEPS.value}')]
  config_sed += [(r'DATADIR_PHASE2=.*',
                  r'DATADIR_PHASE2=\/data\/bert_data\/'
                  r'2048_shards_uncompressed')]
  config_sed += [(r'EVALDIR=.*',
                  r'EVALDIR=\/data\/bert_data\/eval_set_uncompressed')]
  config_sed += [(r'CHECKPOINTDIR=.*',
                  r'CHECKPOINTDIR=\/data\/bert_data\/tf1_ckpt')]
  config_sed += [(r'CHECKPOINTDIR_PHASE1=.*',
                  r'CHECKPOINTDIR_PHASE1=\/data\/bert_data\/tf1_ckpt')]
  config_sed += [(r'BATCHSIZE=.*', fr'BATCHSIZE={BERT_BATCH_SIZE.value}')]

  return config_sed


def SedPairsToString(pairs):
  """Convert a list of sed pairs to a string for the sed command.

  Args:
    pairs: a list of pairs, indicating the replacement requests

  Returns:
    a string to supply to the sed command
  """
  sed_str = '; '.join(['s/%s/%s/g' % pair for pair in pairs])
  if pairs:
    sed_str += ';'
  return sed_str


def _UpdateScripts(benchmark_spec, vm):
  """Update the running scripts on the target vm.

  Args:
    benchmark_spec: The benchmark specification.
    vm: The VM to work on
  """
  benchmark = benchmark_spec.benchmark
  vm = vm or benchmark_spec.vms[0]

  config_sed = []
  config_sed += [(r'DGXSYSTEM=.*', fr'DGXSYSTEM=\"{DGXSYSTEM}\"')]
  config_sed += [(r'DGXNGPU=.*',
                  fr'DGXNGPU={nvidia_driver.QueryNumberOfGpus(vm)}')]
  config_sed += [(r'DGXNSOCKET=.*',
                  fr'DGXNSOCKET={vm.CheckLsCpu().socket_count}')]
  config_sed += [(r'DGXSOCKETCORES=.*',
                  fr'DGXSOCKETCORES={vm.CheckLsCpu().cores_per_socket}')]

  model = 'maskrcnn' if MASK in benchmark else benchmark
  framework = 'mxnet' if RESNET in benchmark else 'pytorch'
  script_path = (
      fr'$HOME/training_results_{MLPERF_VERSION}/NVIDIA/benchmarks/{model}/'
      fr'implementations/{framework}')

  config_files = [CONFIG]

  if MASK in benchmark:
    config_sed = _GetChangesForMask(config_sed)
    config_files = ['config_DGXA100.sh']

  elif RESNET in benchmark:
    config_sed = _GetChangesForResnet(config_sed)
    config_files = ['config_DGXA100_common.sh', 'config_DGXA100.sh']

  elif BERT in benchmark:
    config_sed = _GetChangesForBert(config_sed)
    config_files = ['config_DGXA100_common.sh', 'config_DGXA100_1x8x56x1.sh']

  vm.RemoteCommand(
      f'cd {script_path} && '
      f'sed "{SedPairsToString(config_sed)}" '
      f'{" ".join(config_files)} > {CONFIG} && '
      f'chmod 755 {CONFIG} && '
      f'sed -i "2 i source {CONFIG}" run_and_time.sh && '
      f'sed -i "2 i source {CONFIG}" run_with_docker.sh')


def Prepare(benchmark_spec, vm=None):
  """Install and set up MLPerf on the target vm.

  Args:
    benchmark_spec: The benchmark specification
    vm: The VM to work on

  Raises:
    errors.Config.InvalidValue upon both GPUs and TPUs appear in the config
  """
  PrepareBenchmark(benchmark_spec, vm)
  _UpdateScripts(benchmark_spec, vm)
  PrepareRunner(benchmark_spec, vm)


def _CreateMetadataDict(benchmark_spec):
  """Create metadata dict to be used in run results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    metadata dict
  """
  metadata = {
      'use_tpu': bool(benchmark_spec.tpus),
      'model_dir': benchmark_spec.model_dir,
      'model': benchmark_spec.benchmark,
      'version': MLPERF_VERSION,
  }
  vms = benchmark_spec.vms
  num_vms = len(vms)
  vm = vms[0]
  gpus_per_node = nvidia_driver.QueryNumberOfGpus(vm)
  total_gpus = gpus_per_node * num_vms
  metadata.update(cuda_toolkit.GetMetadata(vm))
  metadata['total_gpus'] = total_gpus
  if benchmark_spec.tpus:
    metadata.update({
        'train_tpu_num_shards':
            benchmark_spec.tpu_groups['train'].GetNumShards(),
        'train_tpu_accelerator_type':
            benchmark_spec.tpu_groups['train'].GetAcceleratorType()
    })
  return metadata


def MakeSamplesFromOutput(metadata, output, use_tpu=False, model=RESNET):
  """Create samples containing metrics.

  Args:
    metadata: dict contains all the metadata that reports.
    output: string, command output
    use_tpu: bool, whether tpu is in use
    model: string, model name
  Example output:
    perfkitbenchmarker/tests/linux_benchmarks/mlperf_benchmark_test.py

  Returns:
    Samples containing training metrics.
  """
  samples = []
  if RESNET in model:
    results = regex_util.ExtractAllMatches(
        f'Speed: ({RE_FLOAT}) samples/sec', output)
    results.extend(regex_util.ExtractAllMatches(
        f'"imgs_sec": ({RE_FLOAT})', output))
    results.extend(regex_util.ExtractAllMatches(
        f'"key": "throughput", "value": ({RE_FLOAT})', output))
  elif TRANSFORMER in model:
    results = re.findall(r'wps=(\S+),', output)
  elif GNMT in model:
    results = re.findall(r'Tok/s (\S+)', output)
  elif SSD in model:
    results = re.findall(r'avg. samples / sec: (\S+)', output)
  elif MASK in model:
    results = regex_util.ExtractAllMatches(
        f'"throughput": ({RE_FLOAT})', output)
    results.extend(regex_util.ExtractAllMatches(
        f'"key": "throughput", "value": ({RE_FLOAT})', output))
    results.extend(regex_util.ExtractAllMatches(
        f'MLPERF METRIC THROUGHPUT=({RE_FLOAT}) iterations / s', output))
  elif BERT in model:
    results = regex_util.ExtractAllMatches(
        f"'training_sequences_per_second': ({RE_FLOAT})", output)
  for speed in results:
    samples.append(sample.Sample('speed', float(speed), 'samples/sec',
                                 metadata))

  if not use_tpu:
    if MINIGO in model:
      times = regex_util.ExtractAllMatches(r'RESULT,.*,(\d+),.*,.*', output)
    else:
      times = regex_util.ExtractAllMatches(r'RESULT,.*,.*,(\d+),.*,.*', output)
    samples.append(sample.Sample('Time', int(times[0]), 'seconds', metadata))

  return samples


def Run(benchmark_spec):
  """Run MLPerf on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vm = benchmark_spec.vms[0]
  if benchmark_spec.tpus:
    # For MLPerf 1.0, the benchmake code of different hardware are different.
    if (benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-32' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-128' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-256' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-512' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-1024' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-2048'):
      run_path = (
          '$HOME/training_results_{version}/Google/benchmarks/{model}/tpu-{tpus}'
          .format(
              version=MLPERF_VERSION,
              model=benchmark_spec.benchmark,
              tpus=benchmark_spec.tpu_groups['train'].GetAcceleratorType()))
      code_path = (
          '$HOME/training_results_{version}/Google/benchmarks/{model}/implementations/tpu-{tpus}-{model}'
          .format(
              version=MLPERF_VERSION,
              model=benchmark_spec.benchmark,
              tpus=benchmark_spec.tpu_groups['train'].GetAcceleratorType()))

      if MASK in benchmark_spec.benchmark:
        model = 'mask_rcnn'
      elif GNMT in benchmark_spec.benchmark:
        model = 'nmt'
      else:
        model = benchmark_spec.benchmark

      mlperf_benchmark_cmd = (
          'cd {code_path} && '
          'export PYTHONPATH=$(pwd):$(pwd)/{model} && '
          'cd {model} && '
          '{run_path}/run_and_time.sh'.format(
              code_path=code_path,
              model=model,
              run_path=run_path))

      if SSD in benchmark_spec.benchmark:
        mlperf_benchmark_cmd = (
            'export '
            'MLP_GCS_RESNET_CHECKPOINT={checkpoint}'
            ' && {cmd}'.format(
                checkpoint=FLAGS.mlperf_gcs_resnet_checkpoint,
                cmd=mlperf_benchmark_cmd))
    else:
      raise ValueError(
          'MLPerf configurations do not support the hardware in PKB. PKB may '
          'need to be updated if this is a new TPU type.')

  else:
    run_sub_paths = {RESNET: 'resnet/implementations/mxnet',
                     TRANSFORMER: 'transformer/implementations/pytorch',
                     MINIGO: 'minigo/implementations/tensorflow',
                     MASK: 'maskrcnn/implementations/pytorch',
                     GNMT: 'gnmt/implementations/pytorch',
                     SSD: 'ssd/implementations/pytorch',
                     BERT: 'bert/implementations/pytorch',}
    benchmark_path = f'$HOME/training_results_{MLPERF_VERSION}/NVIDIA/benchmarks'
    run_path = posixpath.join(benchmark_path,
                              run_sub_paths[benchmark_spec.benchmark])
    env = {
        'DGXSYSTEM': DGXSYSTEM,
        'NEXP': 1,
        'PULL': 0,
        'LOGDIR': f'/tmp/{benchmark_spec.benchmark}',
    }
    envs = {
        RESNET: {},
        TRANSFORMER: {'DATADIR': '/data/wmt/utf8'},
        MINIGO: {'CONT': 'mlperf-nvidia:minigo'},
        MASK: {},
        GNMT: {'DATADIR': '/data/gnmt'},
        SSD: {'DATADIR': '/data'},
        BERT: {}
    }
    env.update(envs[benchmark_spec.benchmark])

    run_script = posixpath.join(run_path, 'run_with_docker.sh')
    vm_util.ReplaceText(vm, 'SYSLOGGING=1', 'SYSLOGGING=0', run_script)
    vm_util.ReplaceText(vm, 'docker exec -it', 'docker exec -t', run_script)
    vm_util.ReplaceText(vm, 'nvidia-docker', 'sudo nvidia-docker', run_script)
    vm_util.ReplaceText(vm, 'docker exec', 'sudo docker exec', run_script)
    vm_util.ReplaceText(vm, 'docker container', 'sudo docker container',
                        run_script)
    if benchmark_spec.benchmark == MASK:
      vm_util.ReplaceText(vm, r'_cont_mounts=\(',
                          r'_cont_mounts=\(\"--volume=\${PKLDIR}:\/pkl_coco\" ',
                          run_script)

    env = ' '.join(f'{key}={value}' for key, value in env.items())
    if nvidia_driver.CheckNvidiaGpuExists(vm):
      env = f'{tensorflow.GetEnvironmentVars(vm)} {env}'

    mlperf_benchmark_cmd = (
        f'chmod 755 {run_script} && '
        f'cd {run_path} && '
        f'{env} {run_script}')

  samples = []
  metadata = _CreateMetadataDict(benchmark_spec)
  stdout, _ = vm.RobustRemoteCommand(mlperf_benchmark_cmd, should_log=True)
  if NONE in FLAGS.mlperf_profiler:
    samples.extend(
        MakeSamplesFromOutput(
            metadata,
            stdout,
            use_tpu=bool(benchmark_spec.tpus),
            model=benchmark_spec.benchmark))
  return samples


def Cleanup(benchmark_spec):
  """Cleanup MLPerf on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  if benchmark_spec.tpus and FLAGS.mlperf_bucket is None:
    benchmark_spec.storage_service.DeleteBucket(benchmark_spec.bucket)
