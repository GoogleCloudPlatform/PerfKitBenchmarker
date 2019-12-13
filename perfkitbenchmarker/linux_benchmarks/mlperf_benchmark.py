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
        AWS:
          machine_type: p3dn.24xlarge
          zone: us-east-1
          boot_disk_size: 100
        Azure:
          machine_type: Standard_ND40s_v2
          zone: eastus
"""

flags.DEFINE_enum('mlperf_benchmark', 'resnet',
                  ['resnet', 'transformer', 'mask', 'gnmt', 'ssd', 'minigo'],
                  'MLPerf benchmark test to run.')
flags.DEFINE_string('wmt_data_dir',
                    'gs://pkb-sgpyc-europe-west4/mlperf_v0.6_nv_transformer',
                    'Directory where the wmt dataset is stored')
flags.DEFINE_string('coco_data_dir', 'gs://pkb-sgpyc-europe-west4/coco',
                    'Directory where the coco dataset is stored')
flags.DEFINE_string('gnmt_data_dir',
                    'gs://pkb-sgpyc-europe-west4/mlperf_v0.6_nv_gnmt',
                    'Directory where the nv v0.6 WMT dataset is stored')
flags.DEFINE_string('coco2017_data_dir', 'gs://pkb-sgpyc-europe-west4/coco2017',
                    'Directory where the coco2017 dataset is stored')


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
  benchmark_spec.coco2017_data_dir = FLAGS.coco2017_data_dir
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

  # vm.InstallPreprovisionedBenchmarkData(BENCHMARK_NAME, [_ILSVRC2012_TAR],
  #                                       local_imagenet_data_path)
  # vm.RemoteCommand('sudo tar -xvf {tar} -C {data_dir}'.format(
  #     tar=posixpath.join(imagenet_data_dir, _ILSVRC2012_TAR),
  #     data_dir=imagenet_data_dir))
  # Some of the data are in the sub directory. Copy all the data to current
  # directory.
  # vm.RemoteCommand('find {data_dir} -name "*-*-of-*" -exec mv {{}} {data_dir}'
  #                 ' \\;'.format(data_dir=imagenet_data_dir))


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

      vm.RemoteCommand('cd {path} && '
                       'sed "s/--progress-bar off/ /g" ./setup.sh | '
                       'sed "s/pip /pip3 /g" > ./setup1.sh && '
                       'chmod 755 ./setup1.sh && '
                       './setup1.sh'.format(path=run_path))
    else:
      vm.RemoteCommand(
          'cd {path} && '
          'sed "s/--progress-bar off/ /g" ./setup.sh > ./setup1.sh && '
          'chmod 755 ./setup1.sh && '
          './setup1.sh'.format(path=run_path))

    if 'mask' not in benchmark_spec.benchmark:
      vm.RemoteCommand(
          'pip3 uninstall -y tf-estimator-nightly && '
          'pip3 install tf-estimator-nightly==1.14.0.dev2019051801')

    vm.RemoteCommand(
        r'cd {path} && '
        r'sed "s/--model_dir=.*/--model_dir=gs:\/\/{bucket} \\\/g" run_and_time.sh | '
        r'sed "s/--tpu=.*/--tpu={tpu} \\\/g" | '
        r'sed "s/--output_dir=.*/--output_dir=gs:\/\/{bucket} \\\/g" | '
        r'sed "s/--cloud_tpu_name=.*/--cloud_tpu_name={tpu} \\\/g" | '
        r'sed "s/--out_dir=.*/--out_dir=gs:\/\/{bucket} \\\/g" | '
        r'sed "s/--tpu_name=.*/--tpu_name={tpu} \\\/g" > run_and_time1.sh && '
        r'chmod 755 run_and_time1.sh '.format(
            path=run_path,
            bucket=bucket,
            tpu=benchmark_spec.tpu_groups['train'].GetName()))

    if 'gnmt' in benchmark_spec.benchmark:
      vm.RemoteCommand(
          'cd {code_path}/{model} && '
          'cp metric.py metric0.py && '
          'sed "s/ sacrebleu -t/ python3 -m sacrebleu -t/g" metric0.py > metric.py'
          .format(code_path=code_path, model=model))

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
      vm.RemoteCommand(
          'cd training_results_v0.6/NVIDIA/benchmarks/minigo/implementations/tensorflow && '
          'sudo docker build --pull --network=host -t mlperf-nvidia:minigo .',
          should_log=True)

    if 'mask' in benchmark_spec.benchmark:
      vm.RemoteCommand(
          'cd training_results_v0.6/NVIDIA/benchmarks/maskrcnn/implementations/pytorch && '
          'sudo docker build --pull --network=host -t mlperf-nvidia:object_detection . ',
          should_log=True)
      _DownloadData(benchmark_spec.coco2017_data_dir,
                    posixpath.join('/data', 'coco2017'), vm)

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
      _DownloadData(benchmark_spec.coco2017_data_dir,
                    posixpath.join('/data', 'coco2017'), vm)


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
  version = 'v0.6.0'
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
    metadata_copy['version'] = version
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
                              '{run_path}/run_and_time1.sh'.format(
                                  code_path=code_path,
                                  model=model,
                                  run_path=run_path))

      if 'ssd' in benchmark_spec.benchmark:
        mlperf_benchmark_cmd = (
            'export '
            'MLP_GCS_RESNET_CHECKPOINT=gs://download.tensorflow.org/models/mlperf/v0.5.0/resnet34_ssd_checkpoint'
            ' && {cmd}'.format(cmd=mlperf_benchmark_cmd))

    else:
      raise ValueError(
          'MLPerf configurations do not support the hardware in PKB. PKB may '
          'need to be updated if this is a new TPU type.')

  else:
    if 'resnet' in benchmark_spec.benchmark:
      mlperf_benchmark_cmd = (
          'cd '
          'training_results_v0.6/NVIDIA/benchmarks/resnet/implementations/mxnet'
          ' && sed \'s/SYSLOGGING=1/SYSLOGGING=0/g\' ./run.sub > ./run1.sub &&'
          ' chmod 755 ./run1.sub && sudo DATADIR=/data/imagenet '
          'LOGDIR=/tmp/resnet PULL=0 DGXSYSTEM=DGX1 NEXP=1 ./run1.sub ')

    if 'transformer' in benchmark_spec.benchmark:
      mlperf_benchmark_cmd = (
          'cd '
          'training_results_v0.6/NVIDIA/benchmarks/transformer/implementations/pytorch'
          ' && sed \'s/SYSLOGGING=1/SYSLOGGING=0/g\' ./run.sub > ./run1.sub &&'
          ' chmod 755 ./run1.sub && sudo DATADIR=/data/wmt/utf8 '
          'LOGDIR=/tmp/transformer PULL=0 DGXSYSTEM=DGX1 NEXP=1 ./run1.sub ')

    if 'minigo' in benchmark_spec.benchmark:
      mlperf_benchmark_cmd = (
          'cd '
          '$HOME/training_results_v0.6/NVIDIA/benchmarks/minigo/implementations/tensorflow'
          ' && sed \'s/SYSLOGGING=1/SYSLOGGING=0/g\' ./run.sub > run1.sub && '
          'chmod 755 ./run1.sub && sudo LOGDIR=/tmp/minigo '
          'CONT=mlperf-nvidia:minigo DGXSYSTEM=DGX1 NEXP=1 ./run1.sub ')

    if 'mask' in benchmark_spec.benchmark:
      mlperf_benchmark_cmd = (
          'cd '
          '$HOME/training_results_v0.6/NVIDIA/benchmarks/maskrcnn/implementations/pytorch'
          ' && sed "s/SYSLOGGING=1/SYSLOGGING=0/g" ./run.sub > ./run1.sub && '
          'chmod 755 ./run1.sub && sudo LOGDIR=/tmp/mask DATADIR=/data PULL=0 '
          'DGXSYSTEM=DGX1 NEXP=1 ./run1.sub ')

    if 'gnmt' in benchmark_spec.benchmark:
      mlperf_benchmark_cmd = (
          'cd '
          '$HOME/training_results_v0.6/NVIDIA/benchmarks/gnmt/implementations/pytorch'
          ' && sed "s/SYSLOGGING=1/SYSLOGGING=0/g" ./run.sub > ./run1.sub && '
          'chmod 755 ./run1.sub && sudo LOGDIR=/tmp/gnmt DATADIR=/data/gnmt '
          'PULL=0 DGXSYSTEM=DGX1 NEXP=1 ./run1.sub ')

    if 'ssd' in benchmark_spec.benchmark:
      mlperf_benchmark_cmd = (
          'cd '
          '$HOME/training_results_v0.6/NVIDIA/benchmarks/ssd/implementations/pytorch'
          ' && sed "s/SYSLOGGING=1/SYSLOGGING=0/g" ./run.sub > ./run1.sub && '
          'chmod 755 ./run1.sub && sudo LOGDIR=/tmp/ssd DATADIR=/data PULL=0 '
          'DGXSYSTEM=DGX1 NEXP=1 ./run1.sub ')

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
