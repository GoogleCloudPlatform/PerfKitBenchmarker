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
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import google_cloud_sdk
from perfkitbenchmarker.linux_packages import tensorflow
from perfkitbenchmarker.providers.gcp import gcs
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'mlperf'
BENCHMARK_CONFIG = """
mlperf:
  description: Runs MLPerf Benchmark.
  vm_groups:
    default:
      os_type: ubuntu1604
      disk_spec: *default_500_gb
      vm_spec:
        GCP:
          machine_type: n1-highmem-96
          zone: us-west1-b
          boot_disk_size: 100
          boot_disk_type: pd-ssd
          min_cpu_platform: skylake
        AWS:
          machine_type: p3dn.24xlarge
          zone: us-east-1
          boot_disk_size: 100
          image: ami-07728e9e2742b0662
        Azure:
          machine_type: Standard_ND40s_v2
          zone: eastus
"""

flags.DEFINE_enum('mlperf_benchmark', 'resnet',
                  ['resnet', 'transformer', 'mask', 'gnmt', 'ssd', 'minigo'],
                  'MLPerf benchmark test to run.')
flags.DEFINE_string(
    'mlperf_gcs_resnet_checkpoint',
    'gs://cloud-tpu-artifacts/resnet/resnet-nhwc-2018-02-07/model.ckpt-112603',
    'A ResNet backbone trained on the ImageNet dataset.')
flags.DEFINE_string(
    'mlperf_transformer_decode_dir', '', 'Transformer decode directory')
flags.DEFINE_string('wmt_data_dir',
                    'gs://pkb-sgpyc-us-west1/mlperf_v0.6_nv_transformer/',
                    'Directory where the wmt dataset is stored')
flags.DEFINE_string('coco_data_dir', 'gs://pkb-sgpyc-us-west1/coco2017/',
                    'Directory where the coco dataset is stored')
flags.DEFINE_string('gnmt_data_dir',
                    'gs://pkb-sgpyc-us-west1/mlperf_v0.6_nv_gnmt/',
                    'Directory where the nv v0.6 WMT dataset is stored')
flags.DEFINE_string('minigo_model_dir', None,
                    'Directory on GCS to copy minigo source data from. Files '
                    'will be copied from subdirectories of src_dir '
                    'corresponding to the board size.')


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
  benchmark_spec.gcp_service_account = FLAGS.gcp_service_account


def _DownloadData(data_dir, data_path, vm):
  """Download remote benchmark data to local.

  Args:
    data_dir: remote benchmark location
    data_path: local benchmark location
    vm: vm to download the data
  """

  vm.Install('google_cloud_sdk')
  vm.RemoteCommand('if [ ! -d \"{data_path}\" ]; then '
                   '  sudo mkdir -p {data_path} && '
                   '  sudo chmod a+w {data_path} && '
                   '  {gsutil_path} -m cp -r {data_dir}/* {data_path} ;'
                   'fi'.format(
                       data_dir=data_dir,
                       gsutil_path=google_cloud_sdk.GSUTIL_PATH,
                       data_path=data_path))


def Prepare(benchmark_spec, vm=None):
  """Install and set up MLPerf on the target vm.

  Args:
    benchmark_spec: The benchmark specification
    vm: The VM to work on

  Raises:
    errors.Config.InvalidValue upon both GPUs and TPUs appear in the config
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  if vm is None:
    vm = benchmark_spec.vms[0]

  if (bool(benchmark_spec.tpus) and cuda_toolkit.CheckNvidiaGpuExists(vm)):
    raise errors.Config.InvalidValue(
        'Invalid configuration. GPUs and TPUs can not both present in the config.'
    )

  vm.RemoteCommand(
      'if [ ! -d "$HOME/training_results_v0.6" ]; then '
      '  git clone https://github.com/mlperf/training_results_v0.6.git ; '
      'fi',
      should_log=True)
  vm.InstallPackages('python3-pip')

  if benchmark_spec.tpus:
    if vm == benchmark_spec.vms[0]:
      storage_service = gcs.GoogleCloudStorageService()
      benchmark_spec.storage_service = storage_service
      bucket = 'pkb{}'.format(FLAGS.run_uri)
      benchmark_spec.bucket = bucket
      benchmark_spec.model_dir = 'gs://{}'.format(bucket)
      location = benchmark_spec.tpu_groups['train'].GetZone()
      storage_service.PrepareService(util.GetRegionFromZone(location))
      storage_service.MakeBucket(bucket)
      storage_service.ChmodBucket(benchmark_spec.gcp_service_account, 'W',
                                  bucket)

    # For MLPerf v0.6, the benchmake code of different hardware are different.
    if (benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-32' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-128' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-256' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-512' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-1024' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-2048'):
      run_path = (
          '$HOME/training_results_v0.6/Google/benchmarks/{model}/tpu-{tpus}'
          .format(
              model=benchmark_spec.benchmark,
              tpus=benchmark_spec.tpu_groups['train'].GetAcceleratorType()))
    else:
      raise ValueError(
          'MLPerf configurations do not support the hardware in PKB. PKB may '
          'need to be updated if this is a new TPU type.')

    if 'mask' in benchmark_spec.benchmark:
      model = 'mask_rcnn'
    elif 'gnmt' in benchmark_spec.benchmark:
      model = 'nmt'
    else:
      model = benchmark_spec.benchmark

    code_path = (
        '$HOME/training_results_v0.6/Google/benchmarks/{model}/implementations/tpu-{tpus}-{model}'
        .format(
            model=benchmark_spec.benchmark,
            tpus=benchmark_spec.tpu_groups['train'].GetAcceleratorType()))

    vm.RemoteCommand('pip3 install --upgrade pyyaml==3.13 ')
    vm.RemoteCommand('pip3 install cloud-tpu-profiler==1.12')
    if ('mask' in benchmark_spec.benchmark or
        'ssd' in benchmark_spec.benchmark):
      # TODO(b/141876878): coco whl package for python 3.5
      vm.RemoteCommand(
          'cd /tmp && '
          'wget https://storage.cloud.google.com/mlperf_artifcats/v0.6_training/coco-1.1-cp36-cp36m-linux_x86_64.whl'
      )

    run_script = posixpath.join(run_path, 'setup.sh')
    vm_util.ReplaceText(vm, '--progress-bar off', ' ', run_script)
    vm_util.ReplaceText(vm, 'pip ', 'pip3 ', run_script)
    vm.RemoteCommand('chmod 755 {script} && {script}'.format(script=run_script))

    if 'mask' not in benchmark_spec.benchmark:
      vm.RemoteCommand(
          'pip3 uninstall -y tf-estimator-nightly && '
          'pip3 install tf-estimator-nightly==1.14.0.dev2019051801')

    if 'resnet' in benchmark_spec.benchmark:
      data_dir = benchmark_spec.imagenet_data_dir
    elif 'transformer' in benchmark_spec.benchmark:
      data_dir = benchmark_spec.wmt_data_dir
    elif 'mask' in benchmark_spec.benchmark:
      data_dir = benchmark_spec.coco_data_dir
    elif 'gnmt' in benchmark_spec.benchmark:
      data_dir = benchmark_spec.gnmt_data_dir
    elif 'ssd' in benchmark_spec.benchmark:
      data_dir = benchmark_spec.coco_data_dir
    else:
      raise ValueError('Unknown operation, cannot find {} in benchmark'.format(
          benchmark_spec.benchmark))

    run_script = posixpath.join(run_path, 'run_and_time.sh')
    data_dir = data_dir.replace('/', r'\/')
    checkpoint = FLAGS.mlperf_gcs_resnet_checkpoint.replace('/', r'\/'),
    decode_dir = FLAGS.mlperf_transformer_decode_dir.replace('/', r'\/'),
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

    if 'gnmt' in benchmark_spec.benchmark:
      run_script = posixpath.join(code_path, model, 'metric.py')
      vm_util.ReplaceText(vm, ' sacrebleu -t', ' python3 -m sacrebleu -t',
                          run_script)
  else:
    benchmark_spec.model_dir = '/tmp'

    has_gpu = cuda_toolkit.CheckNvidiaGpuExists(vm)
    if has_gpu:
      vm.Install('cuda_toolkit')

    vm.Install('nvidia_docker')
    vm.RemoteCommand('if [ ! -d "/data" ]; then sudo ln -s /scratch /data; fi')

    if 'resnet' in benchmark_spec.benchmark:
      vm.RemoteCommand(
          'cd training_results_v0.6/NVIDIA/benchmarks/resnet/implementations/mxnet &&'
          ' sudo docker build --pull --network=host . -t mlperf-nvidia:image_classification',
          should_log=True)
      _DownloadData(benchmark_spec.imagenet_data_dir,
                    posixpath.join('/data', 'imagenet'), vm)

    if 'transformer' in benchmark_spec.benchmark:
      vm.RemoteCommand(
          'cd training_results_v0.6/NVIDIA/benchmarks/transformer/implementations/pytorch &&'
          ' sudo docker build --pull --network=host . -t mlperf-nvidia:translation',
          should_log=True)
      _DownloadData(benchmark_spec.wmt_data_dir, posixpath.join('/data', 'wmt'),
                    vm)

    if 'minigo' in benchmark_spec.benchmark:
      build_path = 'training_results_v0.6/NVIDIA/benchmarks/minigo/implementations/tensorflow'
      run_script = posixpath.join(build_path, 'run_and_time.sh')
      vm_util.ReplaceText(vm, 'get_data.py', 'get_data.py --src_dir={}'.format(
          FLAGS.minigo_model_dir.replace('/', r'\/')), run_script)
      vm.RemoteCommand('cd {} && sudo docker build --pull --network=host -t '
                       'mlperf-nvidia:minigo .'.format(build_path),
                       should_log=True)

    if 'mask' in benchmark_spec.benchmark:
      vm.RemoteCommand(
          'cd training_results_v0.6/NVIDIA/benchmarks/maskrcnn/implementations/pytorch && '
          'sudo docker build --pull --network=host -t mlperf-nvidia:object_detection . ',
          should_log=True)
      _DownloadData(benchmark_spec.coco_data_dir,
                    posixpath.join('/data', 'coco'), vm)

    if 'gnmt' in benchmark_spec.benchmark:
      vm.RemoteCommand(
          'cd training_results_v0.6/NVIDIA/benchmarks/gnmt/implementations/pytorch && '
          'sudo docker build --pull --network=host -t mlperf-nvidia:rnn_translator . ',
          should_log=True)
      _DownloadData(benchmark_spec.gnmt_data_dir,
                    posixpath.join('/data', 'gnmt'), vm)

    if 'ssd' in benchmark_spec.benchmark:
      vm.RemoteCommand(
          'cd training_results_v0.6/NVIDIA/benchmarks/ssd/implementations/pytorch && '
          'sudo docker build --pull --network=host -t mlperf-nvidia:single_stage_detector . ',
          should_log=True)
      _DownloadData(benchmark_spec.coco_data_dir,
                    posixpath.join('/data', 'coco'), vm)


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
      'version': 'v0.6.0',
  }
  if benchmark_spec.tpus:
    metadata.update({
        'train_tpu_num_shards':
            benchmark_spec.tpu_groups['train'].GetNumShards(),
        'train_tpu_accelerator_type':
            benchmark_spec.tpu_groups['train'].GetAcceleratorType()
    })
  return metadata


def MakeSamplesFromOutput(metadata, output, use_tpu=False, model='resnet'):
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

  results = regex_util.ExtractAllMatches(
      r':::MLL (\d+\.\d+) eval_accuracy: {(.*)}', output)

  start = None
  for wall_time, result in results:
    wall_time = float(wall_time)
    if not start:
      start = wall_time
    metadata_copy = metadata.copy()
    epoch = regex_util.ExtractExactlyOneMatch(r'"epoch_num": (\d+)', result)
    if ('transformer' in model and (not use_tpu)):
      value = regex_util.ExtractExactlyOneMatch(r'"value": "(\d+\.\d+)"',
                                                result)
    elif 'mask' in model:
      # TODO(b/141889167): Add support for two accuracy values
      value = 0
    else:
      value = regex_util.ExtractExactlyOneMatch(r'"value": (\d+\.\d+)', result)
    metadata_copy['times'] = wall_time - start
    metadata_copy['epoch'] = int(epoch)
    samples.append(
        sample.Sample('Eval Accuracy',
                      float(value) * 100, '%', metadata_copy))

  if not use_tpu:
    if 'minigo' in model:
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
    # For MLPerf v0.6, the benchmake code of different hardware are different.
    if (benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-32' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-128' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-256' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-512' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-1024' or
        benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-2048'):
      run_path = (
          '$HOME/training_results_v0.6/Google/benchmarks/{model}/tpu-{tpus}'
          .format(
              model=benchmark_spec.benchmark,
              tpus=benchmark_spec.tpu_groups['train'].GetAcceleratorType()))
      code_path = (
          '$HOME/training_results_v0.6/Google/benchmarks/{model}/implementations/tpu-{tpus}-{model}'
          .format(
              model=benchmark_spec.benchmark,
              tpus=benchmark_spec.tpu_groups['train'].GetAcceleratorType()))

      if 'mask' in benchmark_spec.benchmark:
        model = 'mask_rcnn'
      elif 'gnmt' in benchmark_spec.benchmark:
        model = 'nmt'
      else:
        model = benchmark_spec.benchmark

      mlperf_benchmark_cmd = ('cd {code_path} && '
                              'export PYTHONPATH=$(pwd):$(pwd)/{model} && '
                              'cd {model} && '
                              '{run_path}/run_and_time.sh'.format(
                                  code_path=code_path,
                                  model=model,
                                  run_path=run_path))

      if 'ssd' in benchmark_spec.benchmark:
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
    benchmark_path = '$HOME/training_results_v0.6/NVIDIA/benchmarks'
    common_env = 'DGXSYSTEM=DGX1 NEXP=1'
    if 'resnet' in benchmark_spec.benchmark:
      run_path = posixpath.join(benchmark_path, 'resnet/implementations/mxnet')
      env = 'DATADIR=/data/imagenet LOGDIR=/tmp/resnet PULL=0 '
    elif 'transformer' in benchmark_spec.benchmark:
      run_path = posixpath.join(benchmark_path,
                                'transformer/implementations/pytorch')
      env = 'DATADIR=/data/wmt/utf8 LOGDIR=/tmp/transformer PULL=0 '
    elif 'minigo' in benchmark_spec.benchmark:
      run_path = posixpath.join(benchmark_path,
                                'minigo/implementations/tensorflow')
      env = 'LOGDIR=/tmp/minigo CONT=mlperf-nvidia:minigo '
    elif 'mask' in benchmark_spec.benchmark:
      run_path = posixpath.join(benchmark_path,
                                'maskrcnn/implementations/pytorch')
      env = 'LOGDIR=/tmp/mask DATADIR=/data PULL=0 '
    elif 'gnmt' in benchmark_spec.benchmark:
      run_path = posixpath.join(benchmark_path, 'gnmt/implementations/pytorch')
      env = 'LOGDIR=/tmp/gnmt DATADIR=/data/gnmt PULL=0 '
    elif 'ssd' in benchmark_spec.benchmark:
      run_path = posixpath.join(benchmark_path, 'ssd/implementations/pytorch')
      env = 'LOGDIR=/tmp/ssd DATADIR=/data PULL=0 '

    run_script = posixpath.join(run_path, 'run.sub')
    vm_util.ReplaceText(vm, 'SYSLOGGING=1', 'SYSLOGGING=0', run_script)
    mlperf_benchmark_cmd = (
        'chmod 755 {run_script} && sudo {common_env} {env} {run_script} '
        .format(run_script=run_script, common_env=common_env, env=env))

  if cuda_toolkit.CheckNvidiaGpuExists(vm):
    mlperf_benchmark_cmd = '{env} {cmd}'.format(
        env=tensorflow.GetEnvironmentVars(vm), cmd=mlperf_benchmark_cmd)

  samples = []
  metadata = _CreateMetadataDict(benchmark_spec)
  stdout, _ = vm.RobustRemoteCommand(mlperf_benchmark_cmd, should_log=True)
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
  if benchmark_spec.tpus:
    benchmark_spec.storage_service.DeleteBucket(benchmark_spec.bucket)
