# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Run NCCL benchmarks."""

import collections
import posixpath

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker.linux_benchmarks import nccl_benchmark
from perfkitbenchmarker.linux_packages import nvidia_driver


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'nccl_cluster'
BENCHMARK_CONFIG = """
nccl_cluster:
  description: Runs NCCL Benchmark.
  cluster:
    headnode:
      vm_spec:
        GCP:
          machine_type: c3-standard-88
          zone: us-central1-a
          boot_disk_size: 2000
        AWS:
          machine_type: m7i.16xlarge
          zone: us-east-2a
          boot_disk_size: 2000
      vm_count: 1
    workers:
      vm_count: null
      os_type: ubuntu2004
      vm_spec:
        GCP:
          machine_type: a4-highgpu-8g
          zone: us-central1-a
        AWS:
          machine_type: p6.48xlarge
          zone: us-east-2a
  flags:
    placement_group_style: closest_supported
    preprovision_ignore_checksum: True
"""

BENCHMARK_DATA = {
    'a4.sqsh':
        '97c207f163519982227a639461bbc1e8ee3bcbe5db342d3e16ab0238cb8a6f27',
    'p6.sqsh':
        '74bcde564591789fc6d3200db6b39d4e7dc08bd471fca6bb0b8b44e9ccbd1d4d'
}


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install and set up NCCL on the target vm.

  Args:
    benchmark_spec: The benchmark specification
  """
  benchmark_spec.always_call_cleanup = True
  benchmark_spec.cluster.AuthenticateVM()
  benchmark_spec.cluster.InstallSquashImage(
      BENCHMARK_NAME,
      f'{benchmark_spec.cluster.TYPE}.sqsh',
      benchmark_spec.cluster.nfs_path,
      posixpath.join(BENCHMARK_NAME,
                     f'{benchmark_spec.cluster.TYPE}.dockerfile')
  )


def Run(benchmark_spec):
  """Run NCCL on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  image = posixpath.join(
      benchmark_spec.cluster.nfs_path, f'{benchmark_spec.cluster.TYPE}.sqsh')
  sample_results = []
  ngpus = nvidia_driver.QueryNumberOfGpus(benchmark_spec.cluster.worker_vms[0])
  extra_params = collections.defaultdict(list)
  for extra_param in (
      FLAGS.nccl_extra_params + FLAGS.nccl_extra_params_spaceseplist
  ):
    param_key, param_value = extra_param.split('=', 1)
    extra_params[param_key].append(param_value)

  for extra_param in nccl_benchmark.TuningParameters(
      list(extra_params.items())):
    param_metadata = {
        param_key: param_value for param_key, param_value in extra_param
    }

    for operation in FLAGS.nccl_operations:
      metadata = nccl_benchmark.CreateMetadataDict()
      metadata['operation'] = operation
      metadata.update(param_metadata)
      nccl_cmd = (
          f'./{operation}_perf '
          f'--minbytes {FLAGS.nccl_minbytes} '
          f'--maxbytes {FLAGS.nccl_maxbytes} '
          f'--stepfactor {FLAGS.nccl_stepfactor} '
          f'--ngpus {FLAGS.nccl_ngpus} '
          f'--check {FLAGS.nccl_check} '
          f'--nthreads {FLAGS.nccl_nthreads} '
          f'--iters {FLAGS.nccl_iters}')
      stdout, _ = benchmark_spec.cluster.RemoteCommand(
          '--mpi=pmix --cpu-bind=none '
          f'--gpus-per-node={ngpus} --tasks-per-node={ngpus} '
          '--container-writable --wait=60 --kill-on-bad-exit=1 '
          f'--container-image {image} {nccl_cmd}',
          login_shell=True,
          env=' '.join(f'export {k}={v};' for k, v in extra_param)
      )
      samples, _ = nccl_benchmark.MakeSamplesFromOutput(
          metadata, stdout
      )
      sample_results.extend(samples)
  return sample_results


def Cleanup(unused_benchmark_spec):
  """Cleanup NCCL on the cluster.

  Args:
    unused_benchmark_spec: The benchmark specification. Contains all data that
      is required to run the benchmark.
  """
  pass
