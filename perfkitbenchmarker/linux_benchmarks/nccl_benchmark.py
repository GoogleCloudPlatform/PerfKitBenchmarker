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
"""Run NCCL benchmarks."""

import re
import time
import numpy as np
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

flags.DEFINE_integer('nccl_np', 16, 'Number of processes to run')
flags.DEFINE_integer('nccl_slots', 8,
                     'Launch n processes per node on all allocated nodes')
flags.DEFINE_string('nccl_cuda_visible_devices', None, 'GPU identifiers are '
                    'given as integer indices or as UUID strings.')
flags.DEFINE_list('nccl_extra_params', None, 'Export an environment variable')
flags.DEFINE_string('nccl_minbytes', '8', 'Minimum size to start with')
flags.DEFINE_string('nccl_maxbytes', '256M', 'Maximum size to start with')
flags.DEFINE_integer('nccl_stepfactor', 2,
                     'Multiplication factor between sizes')
flags.DEFINE_integer('nccl_ngpus', 1, 'Number of gpus per thread.')
flags.DEFINE_boolean('nccl_check', False, 'Check correctness of results.')
flags.DEFINE_integer('nccl_nthreads', 1, 'Number of threads per process')
flags.DEFINE_integer('nccl_num_runs', 10, 'The number of consecutive run.',
                     lower_bound=1)
flags.DEFINE_integer('nccl_seconds_between_runs', 10,
                     'Sleep between consecutive run.')
flags.DEFINE_boolean('nccl_install_openmpi', False, 'Install Open MPI')
flags.DEFINE_boolean('nccl_install_nccl', False, 'Install NCCL')
flags.DEFINE_integer('nccl_iters', 20, 'Number of iterations')
flags.DEFINE_string('nccl_mpi', '/usr/bin/mpirun', 'MPI binary path')
flags.DEFINE_string('nccl_mpi_home', '/usr/lib/x86_64-linux-gnu/openmpi',
                    'MPI home')
flags.DEFINE_string('nccl_nccl_home', '/usr/local/nccl2', 'NCCL home')


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'nccl'
BENCHMARK_CONFIG = """
nccl:
  description: Runs NCCL Benchmark.
  vm_groups:
    default:
      vm_count: 2
      vm_spec:
        GCP:
          machine_type: n1-highmem-96
          zone: us-central1-a
          image_family: common-cu101
          image_project: deeplearning-platform-release
          boot_disk_size: 100
          gpu_type: v100
          gpu_count: 8
        AWS:
          machine_type: p3dn.24xlarge
          zone: us-west-2a
          image: ami-04121e1f9d541d468
          boot_disk_size: 100
        Azure:
          machine_type: Standard_NC24rs_v3
          image: microsoft-dsvm:aml-workstation:ubuntu:19.11.13
          zone: eastus
"""

_HOSTFILE = 'HOSTFILE'
_SAMPLE_LINE_RE = re.compile(r'# nThread (?P<nThread>\d+) '
                             r'nGpus (?P<nGpus>\d+) '
                             r'minBytes (?P<minBytes>\d+) '
                             r'maxBytes (?P<maxBytes>\d+) '
                             r'step: (?P<step>\S+) '
                             r'warmup iters: (?P<warmup_iters>\d+) '
                             r'iters: (?P<iters>\d+) '
                             r'validation: (?P<validation>\d+)\s*')

# Withoud '--mca btl_tcp_if_exclude docker0,lo', it stuck forever
# This is caused by Docker network in DLVM, use mca btl_tcp_if_exclude to skip
# docker network.

_RUN_CMD = ('{mpi} '
            '--hostfile {hostfile} '
            '--mca btl tcp,self '
            '--mca btl_tcp_if_exclude docker0,lo '
            '--bind-to none '
            '-np {np} '
            '-N {slots} '
            '{env} '
            'nccl-tests/build/all_reduce_perf '
            '--minbytes {minbytes} '
            '--maxbytes {maxbytes} '
            '--stepfactor {stepfactor} '
            '--ngpus {ngpus} '
            '--check {check} '
            '--nthreads {nthreads} '
            '--iters {iters}')

_METADATA_COLUMNS = ('size', 'count', 'nccl_type', 'redop', 'out_of_place_time',
                     'out_of_place_algbw', 'out_of_place_busbw',
                     'out_of_place_error', 'in_place_time', 'in_place_algbw',
                     'in_place_busbw', 'in_place_error')

_SAMPLE_NAMES = {'Out of place algorithm bandwidth': 'out_of_place_algbw',
                 'Out of place bus bandwidth': 'out_of_place_busbw',
                 'In place algorithm bandwidth': 'in_place_algbw',
                 'In place bus bandwidth': 'in_place_busbw'}


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _PrepareVm(vm):
  """Install and set up NCCL on the target vm.

  Args:
    vm: virtual machine on which to install NCCL
  """
  vm.AuthenticateVm()
  vm.Install('cuda_toolkit')
  if FLAGS.nccl_install_nccl:
    vm.Install('nccl')
  if FLAGS.nccl_install_openmpi:
    vm.Install('openmpi')
  env = ''
  if FLAGS.aws_efa:
    env = ('export LD_LIBRARY_PATH=/opt/amazon/efa/lib:/opt/amazon/efa/lib64:'
           '$LD_LIBRARY_PATH &&')
    vm.InstallPackages('libudev-dev libtool autoconf')
    vm.RemoteCommand('git clone https://github.com/aws/aws-ofi-nccl.git -b aws')
    vm.RemoteCommand('cd aws-ofi-nccl && ./autogen.sh && ./configure '
                     '--with-mpi={mpi} '
                     '--with-libfabric=/opt/amazon/efa '
                     '--with-nccl={nccl} '
                     '--with-cuda={cuda} && sudo make && '
                     'sudo make install'.format(
                         mpi=FLAGS.nccl_mpi_home,
                         nccl=FLAGS.nccl_nccl_home,
                         cuda='/usr/local/cuda-{}'.format(
                             FLAGS.cuda_toolkit_version)))
  vm.RemoteCommand('git clone https://github.com/NVIDIA/nccl-tests.git')
  vm.RemoteCommand('cd nccl-tests && {env} make MPI=1 MPI_HOME={mpi} '
                   'NCCL_HOME={nccl} CUDA_HOME={cuda}'.format(
                       env=env, mpi=FLAGS.nccl_mpi_home,
                       nccl=FLAGS.nccl_nccl_home,
                       cuda='/usr/local/cuda-{}'.format(
                           FLAGS.cuda_toolkit_version)))


def Prepare(benchmark_spec):
  """Install and set up NCCL on the target vm.

  Args:
    benchmark_spec: The benchmark specification
  """
  benchmark_spec.always_call_cleanup = True
  vm_util.RunThreaded(_PrepareVm, benchmark_spec.vms)
  host = benchmark_spec.vms[0]
  for vm in benchmark_spec.vms:
    cmd = 'echo "{ip} slots={slots}" >> {hostfile}'.format(
        ip=vm.internal_ip, hostfile=_HOSTFILE, slots=FLAGS.nccl_slots)
    host.RemoteCommand(cmd)


def _CreateMetadataDict():
  """Create metadata dict to be used in run results.

  Returns:
    metadata dict
  """
  metadata = {'np': FLAGS.nccl_np,
              'slots': FLAGS.nccl_slots,
              'minbytes': FLAGS.nccl_minbytes,
              'maxbytes': FLAGS.nccl_maxbytes,
              'stepfactor': FLAGS.nccl_stepfactor,
              'ngpus': FLAGS.nccl_ngpus,
              'check': FLAGS.nccl_check,
              'nthreads': FLAGS.nccl_nthreads,
              'iters': FLAGS.nccl_iters,
              'cuda_visible_devices': FLAGS.nccl_cuda_visible_devices}
  if FLAGS.nccl_extra_params:
    for extra_param in FLAGS.nccl_extra_params:
      param_key, param_value = extra_param.split('=', 1)
      metadata[param_key] = param_value
  return metadata


def MakeSamplesFromOutput(metadata, output):
  """Create samples containing metrics.

  Args:
    metadata: dict contains all the metadata that reports.
    output: string, command output
  Example output:
    perfkitbenchmarker/tests/linux_benchmarks/nccl_benchmark_test.py

  Returns:
    Samples containing training metrics, and the bandwidth
  """
  samples = []
  metadata.update(_SAMPLE_LINE_RE.match(output).groupdict())
  results = regex_util.ExtractAllMatches(
      r'(Rank\s+\d+) (.*)', output)
  for rank, device in results:
    metadata[rank] = device
  results = regex_util.ExtractAllMatches(
      r'(\d+)\s+'
      r'(\d+)\s+'
      r'(\w+)\s+'
      r'(\w+)\s+'
      r'(\d+(?:\.\d+)?)\s+'
      r'(\d+(?:\.\d+)?)\s+'
      r'(\d+(?:\.\d+)?)\s+'
      r'(\S+)\s+'
      r'(\d+(?:\.\d+)?)\s+'
      r'(\d+(?:\.\d+)?)\s+'
      r'(\d+(?:\.\d+)?)\s+'
      r'(\S+)', output)
  bandwidth = 0
  for row in results:
    metadata_copy = metadata.copy()
    metadata_copy.update(zip(_METADATA_COLUMNS, row))
    for metric, metadata_key in sorted(_SAMPLE_NAMES.items()):
      samples.append(sample.Sample(metric, float(metadata_copy[metadata_key]),
                                   'GB/s', metadata_copy))
    # Gbps is gigaBIT per second and GB/s is gigaBYTE per second
    bandwidth = max(bandwidth, float(metadata_copy['out_of_place_algbw']))

  avg_bus_bandwidth = regex_util.ExtractExactlyOneMatch(
      r'Avg bus bandwidth\s+: ([0-9\.]+)', output)
  samples.append(sample.Sample('Avg bus bandwidth', float(avg_bus_bandwidth),
                               'GB/s', metadata))
  samples.append(sample.Sample('bandwidth', bandwidth * 8, 'Gbps', metadata))
  return samples, bandwidth


def Run(benchmark_spec):
  """Run NCCL on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  master = benchmark_spec.vms[0]
  env = []
  if FLAGS.nccl_cuda_visible_devices:
    env.append('CUDA_VISIBLE_DEVICES={}'.format(
        FLAGS.nccl_cuda_visible_devices))
  if FLAGS.nccl_extra_params:
    for extra_param in FLAGS.nccl_extra_params:
      env.append('{}'.format(extra_param))

  cmd = _RUN_CMD.format(mpi=FLAGS.nccl_mpi,
                        hostfile=_HOSTFILE,
                        np=FLAGS.nccl_np,
                        slots=FLAGS.nccl_slots,
                        env=' '.join(['-x {}'.format(x) for x in env]),
                        minbytes=FLAGS.nccl_minbytes,
                        maxbytes=FLAGS.nccl_maxbytes,
                        stepfactor=FLAGS.nccl_stepfactor,
                        ngpus=FLAGS.nccl_ngpus,
                        check=int(FLAGS.nccl_check),
                        nthreads=FLAGS.nccl_nthreads,
                        iters=FLAGS.nccl_iters)
  metadata = _CreateMetadataDict()
  sample_results = []
  bandwidth_results = []

  for _ in range(FLAGS.nccl_num_runs):
    stdout, _ = master.RobustRemoteCommand(cmd)
    samples, bandwidth = MakeSamplesFromOutput(metadata, stdout)
    sample_results.extend(samples)
    bandwidth_results.append(bandwidth)
    time.sleep(FLAGS.nccl_seconds_between_runs)
  samples.append(sample.Sample('bandwidth_average',
                               np.mean(bandwidth_results), 'Gbps', metadata))
  samples.append(sample.Sample('bandwidth_variance', np.var(bandwidth_results),
                               '', metadata))
  return samples


def Cleanup(unused_benchmark_spec):
  """Cleanup NCCL on the cluster.

  Args:
    unused_benchmark_spec: The benchmark specification. Contains all data that
      is required to run the benchmark.
  """
  pass
