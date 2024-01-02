# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs fio benchmarks.

The default FIO job is inspired from
https://cloud.google.com/compute/docs/disks/benchmarking-pd-performance.
Alternatively, run with --fio_generate_scenarios.
"""

import logging
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks import fio_benchmark as linux_fio
from perfkitbenchmarker.windows_packages import fio


FLAGS = flags.FLAGS


flags.DEFINE_integer(
    'fio_file_size', 10, '"size" field of the fio config in GB.'
)

BENCHMARK_NAME = 'fio'
BENCHMARK_CONFIG = """
fio:
  description: Runs fio in sequential, random, read and write modes.
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n2-standard-8
          zone: us-central1-a
        AWS:
          machine_type: m6i.2xlarge
          zone: us-east-1a
        Azure:
          machine_type: Standard_D8s_v5
          zone: eastus-1
      disk_spec: *default_500_gb
  flags:
    fio_ioengine: windowsaio
    fio_runtime: 30
    fio_fill_size: 500GB
"""

DEFAULT_JOB = """
[global]
size={filesize}g
filename=fio_test_file
ioengine=windowsaio
ramp_time=5s
runtime={runtime}s
time_based
stonewall
direct=1
invalidate=1
group_reporting=1
verify=0

[random_write_iops]
rw=randwrite
blocksize=4k
iodepth=256

[random_read_iops]
rw=randread
blocksize=4k
iodepth=256

[random_write_iops_multi]
rw=randwrite
blocksize=4k
iodepth=256
numjobs=4

[random_read_iops_multi]
rw=randread
blocksize=4k
iodepth=256
numjobs=4

[sequential_write_throughput]
rw=write
blocksize=1M
iodepth=64
end_fsync=1
numjobs=8

[sequential_read_throughput]
rw=read
blocksize=1M
iodepth=64
numjobs=8
"""


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS.fio_hist_log:
    logging.warning('--fio_hist_log is unsupported on Windows; ignoring.')
    FLAGS.fio_hist_log = False
  if FLAGS.fio_target_mode != linux_fio.AGAINST_FILE_WITHOUT_FILL_MODE:
    raise errors.Setup.InvalidFlagConfigurationError(
        'PKB only supports FIO against file without fill on Windows'
    )
  return config


def Prepare(benchmark_spec):
  vm = benchmark_spec.vms[0]
  vm.Install('fio')
  # TODO(user): Support other FIO target modes e.g. with fill, against device


def Run(benchmark_spec):
  """Runs the fio benchmark test.

  Args:
    benchmark_spec: specification of the benchmark.

  Returns:
    List of samples produced by the test.
  """
  vm = benchmark_spec.vms[0]
  fio_exec = fio.GetFioExec(vm)
  remote_job_file_path = fio.GetRemoteJobFilePath(vm)

  # Ignored when --fio_generate_scenarios is used.
  job_file_contents = DEFAULT_JOB.format(
      filesize=FLAGS.fio_file_size, runtime=FLAGS.fio_runtime
  )

  return linux_fio.RunWithExec(
      vm, fio_exec, remote_job_file_path, job_file_contents
  )


def Cleanup(unused_benchmark_spec):
  pass
