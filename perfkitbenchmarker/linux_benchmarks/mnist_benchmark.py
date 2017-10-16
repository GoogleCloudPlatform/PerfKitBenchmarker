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

"""Run MNIST benchmarks."""

import re
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'mnist'
BENCHMARK_CONFIG = """
mnist:
  description: Runs MNIST Benchmark.
  vm_groups:
    default:
      vm_spec:
        GCP:
          image: debian-9-tf-v20171009
          image_project: ml-images
          machine_type: n1-standard-8
          zone: us-central1-c
  cloud_tpu:
    tpu_tf_version: nightly
    tpu_zone: us-central1-c
    tpu_cidr_range: 10.240.0.0/29
  flags:
    gcloud_scopes: https://www.googleapis.com/auth/cloud-platform
"""

flags.DEFINE_string('mnist_train_file',
                    'gs://tfrc-test-bucket/mnist-records/train.tfrecords',
                    'mnist train file for tensorflow')
flags.DEFINE_bool('mnist_use_tpu', True, 'Use TPUs rather than plain CPUs')


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _UpdateBenchmarkSpecWithFlags(benchmark_spec):
  """Update the benchmark_spec with supplied command line flags.

  Args:
    benchmark_spec: benchmark specification to update
  """
  benchmark_spec.train_file = FLAGS.mnist_train_file
  benchmark_spec.use_tpu = FLAGS.mnist_use_tpu


def Prepare(benchmark_spec):
  """Install and set up MNIST on the target vm.

  Args:
    benchmark_spec: The benchmark specification
  """
  benchmark_spec.always_call_cleanup = True
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vms = benchmark_spec.vms
  master_vm = vms[0]
  master_vm.RemoteCommand(
      'git clone https://github.com/tensorflow/tpu-demos.git', should_log=True)


def _CreateMetadataDict(benchmark_spec):
  """Create metadata dict to be used in run results.

  Args:
    benchmark_spec: benchmark spec

  Returns:
    metadata dict
  """
  metadata = dict()
  metadata['train_file'] = benchmark_spec.train_file
  metadata['use_tpu'] = benchmark_spec.use_tpu
  return metadata


def ExtractThroughput(output):
  """Extract throughput from MNIST output.

  Args:
    output: MNIST output

  Returns:
    throuput float
  """
  regex = r'INFO:tensorflow:global_step/sec: (\S+)'
  match = re.findall(regex, str(output))
  return sum(float(step) for step in match) / len(match)


def _MakeSamplesFromOutput(benchmark_spec, output):
  """Create a sample continaing the measured MNIST throughput.

  Args:
    benchmark_spec: benchmark spec
    output: MNIST output

  Returns:
    a Sample containing the MNIST throughput
  """
  metadata = _CreateMetadataDict(benchmark_spec)
  global_step_sec = ExtractThroughput(output)
  return [sample.Sample('tensorflow', global_step_sec,
                        'global_step/sec', metadata)]


def Run(benchmark_spec):
  """Run MNIST on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  _UpdateBenchmarkSpecWithFlags(benchmark_spec)
  vms = benchmark_spec.vms
  master_vm = vms[0]
  mnist_benchmark_dir = 'tpu-demos/cloud_tpu/models/mnist'
  tpu_master = 'grpc://{}:8470'.format(benchmark_spec.cloud_tpu.GetCloudTpuIp())
  mnist_benchmark_cmd = (
      'python mnist.py --master={0} --train_file={1} --use_tpu={2}'.format(
          tpu_master, benchmark_spec.train_file, benchmark_spec.use_tpu))
  run_command = 'cd %s && %s' % (mnist_benchmark_dir, mnist_benchmark_cmd)
  stdout, stderr = master_vm.RobustRemoteCommand(run_command, should_log=True)
  return _MakeSamplesFromOutput(benchmark_spec, stdout or stderr)


def Cleanup(benchmark_spec):
  """Cleanup MNIST on the cluster."""
  pass
