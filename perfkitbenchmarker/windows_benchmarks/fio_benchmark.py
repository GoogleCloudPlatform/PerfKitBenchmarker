# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""


from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_benchmarks import fio_benchmark as linux_fio
from perfkitbenchmarker.windows_packages import fio

FLAGS = flags.FLAGS

_FIO_MEDIUM_SIZE = 2
_FIO_LARGE_SIZE = 2 * _FIO_MEDIUM_SIZE
_MAX_SIZE = 100000

_SIZE_EXPLANATION = ('This is the size of I/O for this job. fio will run until'
                     ' this many bytes have been transferred. The default is '
                     '{size} * (System Memory) or {max_size}GB, whichever is'
                     ' smaller.')

_DEFAULT_SIZE_LARGE_EXPLANATION = _SIZE_EXPLANATION.format(
    size=_FIO_LARGE_SIZE, max_size=(_MAX_SIZE / 1000))

_DEFAULT_SIZE_MEDIUM_EXPLANATION = _SIZE_EXPLANATION.format(
    size=_FIO_MEDIUM_SIZE, max_size=(_MAX_SIZE / 1000))

flags.DEFINE_integer('fio_file_size', None,
                     ('"filesize" field of the global section of the '
                      'fio config. This is the size of the individual files. '
                      'Default is {large} * (System Memory) or {max_size}GB, '
                      'whichever is smaller.').format(
                          large=_FIO_LARGE_SIZE, max_size=_MAX_SIZE / 1000))

flags.DEFINE_integer('fio_sequential_write_size', None,
                     ('"size" field of the sequential_write section of the '
                      'fio config. {explanation}').format(
                          explanation=_DEFAULT_SIZE_LARGE_EXPLANATION))

flags.DEFINE_integer('fio_sequential_read_size', None,
                     ('"size" field of the sequential_read section of the '
                      'fio config. {explanation}').format(
                          explanation=_DEFAULT_SIZE_LARGE_EXPLANATION))

flags.DEFINE_integer('fio_random_write_size', None,
                     ('"size" field of the random_write section of the '
                      'fio config. {explanation}').format(
                          explanation=_DEFAULT_SIZE_MEDIUM_EXPLANATION))

flags.DEFINE_integer('fio_random_read_size', None,
                     ('"size" field of the random_read section of the '
                      'fio config. {explanation}').format(
                          explanation=_DEFAULT_SIZE_MEDIUM_EXPLANATION))

flags.DEFINE_integer('fio_random_read_parallel_size', None,
                     ('"size" field of the random_read_parallel section of the '
                      'fio config. {explanation}').format(
                          explanation=_DEFAULT_SIZE_MEDIUM_EXPLANATION))

BENCHMARK_NAME = 'fio'
BENCHMARK_CONFIG = """
fio:
  description: Runs fio in sequential, random, read and write modes.
  vm_groups:
    default:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
"""

DEFAULT_JOB = """
[global]
filesize={filesize}m
filename=fio_test_file
ioengine=windowsaio

[sequential_write]
overwrite=0
rw=write
blocksize=512k
size={seq_write_size}m
iodepth=64
direct=1
end_fsync=1

[sequential_read]
stonewall
invalidate=1
overwrite=0
rw=read
blocksize=512k
size={seq_read_size}m
iodepth=64
direct=1

[random_write_test]
stonewall
overwrite=1
rw=randwrite
blocksize=4k
iodepth=1
size={rand_write_size}m
direct=1

[random_read_test]
invalidate=1
stonewall
rw=randread
blocksize=4k
iodepth=1
size={rand_read_size}m
direct=1

[random_read_test_parallel]
invalidate=1
stonewall
rw=randread
blocksize=4k
iodepth=64
size={rand_read_parallel_size}m
direct=1
"""


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS.fio_target_mode != linux_fio.AGAINST_FILE_WITHOUT_FILL_MODE:
    disk_spec = config['vm_groups']['default']['disk_spec']
    for cloud in disk_spec:
      disk_spec[cloud]['mount_point'] = None
  return config


def Prepare(benchmark_spec):
  vm = benchmark_spec.vms[0]
  exec_path = fio.GetFioExec(vm)
  linux_fio.PrepareWithExec(benchmark_spec, exec_path)


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
  total_memory_mb = vm.GetTotalMemoryMb()

  size_medium = min(_MAX_SIZE, total_memory_mb * _FIO_MEDIUM_SIZE)
  size_large = min(_MAX_SIZE, total_memory_mb * _FIO_LARGE_SIZE)

  filesize = FLAGS.fio_file_size or size_large
  seq_write_size = FLAGS.fio_sequential_write_size or size_large
  seq_read_size = FLAGS.fio_sequential_read_size or size_large
  rand_write_size = FLAGS.fio_random_write_size or size_medium
  rand_read_size = FLAGS.fio_random_read_size or size_medium
  rand_read_parallel_size = FLAGS.fio_random_read_parallel_size or size_medium

  job_file_contents = DEFAULT_JOB.format(
      filesize=filesize,
      seq_write_size=seq_write_size,
      seq_read_size=seq_read_size,
      rand_write_size=rand_write_size,
      rand_read_size=rand_read_size,
      rand_read_parallel_size=rand_read_parallel_size)
  return linux_fio.RunWithExec(benchmark_spec, fio_exec, remote_job_file_path,
                               job_file_contents)


def Cleanup(unused_benchmark_spec):
  pass
