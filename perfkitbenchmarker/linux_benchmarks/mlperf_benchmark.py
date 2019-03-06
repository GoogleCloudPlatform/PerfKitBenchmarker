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
from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import mnist_benchmark
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import tensorflow

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
          gpu_type: v100
          gpu_count: 8
        AWS:
          machine_type: p3dn.24xlarge
          zone: us-east-1
          boot_disk_size: 100
        Azure:
          machine_type: Standard_ND40s_v2
          zone: eastus
"""
# Prepare the ImageNet data in preprovisioned data.
# The data is downloaded from http://image-net.org/
# For data preprocessing, please check
# https://github.com/mlperf/training/tree/master/image_classification#3-datasetenvironment
_ILSVRC2012_TAR = 'ILSVRC2012.tar'
BENCHMARK_DATA = {_ILSVRC2012_TAR: 'cd2de079dc2e18fc9a9f598b5a38969b'}

flags.DEFINE_enum('mlperf_benchmark', 'resnet', ['resnet'],
                  'MLPerf benchmark test to run.')


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if 'tpu_groups' in config:
    for vm_spec in config['vm_groups']['default']['vm_spec'].values():
      vm_spec.pop('gpu_type', None)
      vm_spec.pop('gpu_count', None)
  return config


def _UpdateBenchmarkSpecWithFlags(benchmark_spec):
  """Update the benchmark_spec with supplied command line flags.

  Args:
    benchmark_spec: benchmark specification to update
  """
  benchmark_spec.data_dir = FLAGS.imagenet_data_dir
  benchmark_spec.benchmark = FLAGS.mlperf_benchmark


def Prepare(benchmark_spec):
  """Install and set up MLPerf on the target vm.

  Args:
    benchmark_spec: The benchmark specification
  """
  mnist_benchmark.Prepare(benchmark_spec)
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vm = benchmark_spec.vms[0]

  vm.RemoteCommand('git clone https://github.com/mlperf/results.git',
                   should_log=True)
  vm.InstallPackages('python3-pip')
  vm.RemoteCommand('pip3 install mlperf_compliance==0.0.10')

  if benchmark_spec.tpus:
    vm.RemoteCommand('pip3 install --upgrade '
                     'pyyaml==3.13 '
                     'oauth2client==4.1.3 '
                     'google-api-python-client==1.7.4 '
                     'google-cloud==0.34.0'
                    )
    vm.RemoteCommand('pip3 install cloud-tpu-profiler==1.12')
  else:
    vm.Install('nvidia_docker')
    vm.RemoteCommand('sudo ln -s /scratch /data')
    imagenet_data_dir = posixpath.join('/data', 'imagenet', 'combined')
    vm.RemoteCommand('sudo mkdir -p {}'.format(imagenet_data_dir))
    vm.RemoteCommand('sudo chmod a+w /data/imagenet/combined')
    vm.InstallPreprovisionedBenchmarkData(
        BENCHMARK_NAME, [_ILSVRC2012_TAR], imagenet_data_dir)
    vm.RemoteCommand('sudo tar -xvf {tar} -C {data_dir}'.format(
        tar=posixpath.join(imagenet_data_dir, _ILSVRC2012_TAR),
        data_dir=imagenet_data_dir))
    # Some of the data are in the sub directory. Copy all the data to current
    # directory.
    vm.RemoteCommand('find {data_dir} -name "*-*-of-*" -exec mv {{}} {data_dir}'
                     ' \\;'.format(data_dir=imagenet_data_dir))
  # Clearing caches.
  # https://github.com/mlperf/results/blob/master/v0.5.0/google/cloud_v2.8/resnet-tpuv2-8/code/resnet/model/main.sh#L133
  vm.RemoteCommand('sync && echo 3 | sudo tee /proc/sys/vm/drop_caches')
  vm.RemoteCommand('python3 -c "import mlperf_compliance;mlperf_compliance.'
                   'mlperf_log.{}_print(key=\'run_clear_caches\')"'.format(
                       benchmark_spec.benchmark))


def _CreateMetadataDict(benchmark_spec):
  """Create metadata dict to be used in run results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    metadata dict
  """
  return mnist_benchmark.CreateMetadataDict(benchmark_spec)


def MakeSamplesFromOutput(metadata, output):
  """Create samples containing metrics.

  Args:
    metadata: dict contains all the metadata that reports.
    output: string, command output

  Example output:
    perfkitbenchmarker/tests/linux_benchmarks/mlperf_benchmark_test.py

  Returns:
    Samples containing training metrics.
  """
  samples = []
  results = regex_util.ExtractAllMatches(
      r':::MLPv(\S+) resnet (\d+\.\d+) .* eval_accuracy: {(.*)}', output)
  start = None
  for version, wall_time, result in results:
    wall_time = float(wall_time)
    if not start:
      start = wall_time
    metadata_copy = metadata.copy()
    epoch = regex_util.ExtractExactlyOneMatch(r'"epoch": (\d+)', result)
    value = regex_util.ExtractExactlyOneMatch(r'"value": (0\.\d+)', result)
    metadata_copy['times'] = wall_time - start
    metadata_copy['epoch'] = int(epoch)
    metadata_copy['version'] = version
    samples.append(sample.Sample('Eval Accuracy', float(value) * 100,
                                 '%', metadata_copy))
  times = regex_util.ExtractExactlyOneMatch(r'RESULT,resnet,.*,(\d+),.*,.*',
                                            output)
  samples.append(sample.Sample('Times', int(times), 'seconds', metadata))
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
    # For MLPerf v0.5, the benchmake code of different hardware are different.
    if benchmark_spec.tpu_groups['train'].GetNumShards() > 8:
      code_path = 'cloud_v2.512/resnet-tpuv2-512/code/resnet/model'
    elif benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v2-8':
      code_path = 'cloud_v2.8/resnet-tpuv2-8/code/resnet/model'
    elif benchmark_spec.tpu_groups['train'].GetAcceleratorType() == 'v3-8':
      code_path = 'cloud_v3.8/resnet-tpuv3-8/code/resnet/model'
    else:
      raise ValueError(
          'MLPerf configurations do not support the hardware in PKB. PKB may '
          'need to be updated if this is a new TPU type.')
    cmd = 'bash run_helper.sh 2>&1 | tee output.txt'
  else:
    code_path = 'cloud_v100x8/code/resnet'
    cmd = (
        'sudo nvidia-docker build . -t foo && '
        'sudo nvidia-docker run -v $MLP_HOST_DATA_DIR:/data -v '
        '$MLP_HOST_OUTPUT_DIR:/output -v /proc:/host_proc -t '
        'foo:latest run_helper_8xV100.sh 2>&1 | tee output.txt')
  mlperf_benchmark_cmd = (
      'export MLP_GCS_MODEL_DIR={model_dir} && '
      'export MLP_PATH_GCS_IMAGENET={data_dir} && '
      'export MLP_TPU_NAME={tpu_train} && '
      'export MLP_PATH_GCS_EUW_IMAGENET={data_dir} && '
      'export MLP_GCS_EUW_MODEL_DIR={model_dir} && '
      'export MLP_TPU_SIDECAR_NAME={tpu_eval} && '
      'export MLP_HOST_DATA_DIR=/data && '
      'export MLP_HOST_OUTPUT_DIR=`pwd`/output && '
      'export PYTHONPATH=$PYTHONPATH:$PWD/tpu/models && '
      'cd results/v0.5.0/google/{code_path} && '
      'sed -i "s/python /python3 /g" run_helper*.sh && '
      'mkdir -p $MLP_HOST_OUTPUT_DIR && '
      '{cmd}'.format(
          model_dir=benchmark_spec.model_dir,
          data_dir=benchmark_spec.data_dir,
          tpu_train=(benchmark_spec.tpu_groups['train'].GetName() if
                     benchmark_spec.tpus else ''),
          tpu_eval=(benchmark_spec.tpu_groups['eval'].GetName() if
                    benchmark_spec.tpus else ''),
          code_path=code_path,
          cmd=cmd))
  if cuda_toolkit.CheckNvidiaGpuExists(vm):
    mlperf_benchmark_cmd = '{env} {cmd}'.format(
        env=tensorflow.GetEnvironmentVars(vm), cmd=mlperf_benchmark_cmd)
  samples = []
  metadata = _CreateMetadataDict(benchmark_spec)
  stdout, _ = vm.RobustRemoteCommand(mlperf_benchmark_cmd, should_log=True)
  samples.extend(MakeSamplesFromOutput(metadata, stdout))
  return samples


def Cleanup(benchmark_spec):
  """Cleanup MLPerf on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  mnist_benchmark.Cleanup(benchmark_spec)
