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

"""Runs stress-ng.

From the stress-ng ubuntu documentation:
stress-ng will stress test a computer system in various selectable ways.
It was designed to exercise various physical subsystems of a computer as
well as the various operating system kernel interfaces. stress-ng also has
a wide range of CPU specific stress tests that exercise floating point,
integer, bit manipulation and control flow.

stress-ng manpage:
http://manpages.ubuntu.com/manpages/xenial/man1/stress-ng.1.html
"""

import logging
import numpy

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample


FLAGS = flags.FLAGS


BENCHMARK_NAME = 'stress_ng'
BENCHMARK_CONFIG = """
stress_ng:
  description: Runs stress-ng
  vm_groups:
    default:
      vm_spec: *default_single_core
      disk_spec: *default_50_gb
"""

flags.DEFINE_integer('stress_ng_duration', 10,
                     'Number of seconds to run the test.')
flags.DEFINE_boolean('stress_ng_calc_geomean', True,
                     'Whether to calculate geomean or not.')
flags.DEFINE_list('stress_ng_custom_stressors', [],
                  'List of stressors to run against. Default combines cpu,'
                  'cpu-cache, and memory suites')


def _GeoMeanOverflow(iterable):
  """Returns the geometric mean.

  See https://en.wikipedia.org/wiki/Geometric_mean#Relationship_with_logarithms

  Args:
    iterable: a list of positive floats to take the geometric mean of.

  Returns: The geometric mean of the list.
  """
  a = numpy.log(iterable)
  return numpy.exp(a.sum() / len(a))


def StressngCustomStressorsValidator(stressors):
  """Returns whether or not the list of custom stressors is valid."""
  valid_stressors = {
      'affinity', 'af-alg', 'aio', 'aio-linux', 'apparmor', 'bigheap', 'brk',
      'bsearch', 'cache', 'chdir', 'chmod', 'clock', 'clone', 'context', 'cpu',
      'cpu-online', 'crypt', 'daemon', 'dentry', 'dir', 'dup', 'epoll',
      'eventfd', 'exec', 'fallocate', 'fault', 'fcntl', 'fiemap', 'fifo',
      'filename', 'flock', 'fork', 'fp-error', 'fstat', 'futex', 'get',
      'getrandom', 'getdent', 'handle', 'hdd', 'heapsort', 'hsearch', 'icache',
      'iosync', 'inotify', 'itimer', 'kcmp', 'key', 'kill', 'klog', 'lease',
      'link', 'lockbus', 'lockf', 'longjmp', 'lsearch', 'malloc', 'matrix',
      'membarrier', 'memcpy', 'memfd', 'mergesort', 'mincore', 'mknod', 'mlock',
      'mmap', 'mmapfork', 'mmapmany', 'mremap', 'msg', 'mq', 'nice', 'null',
      'numa', 'oom-pipe', 'open', 'personality', 'pipe', 'poll', 'procfs',
      'pthread', 'ptrace', 'qsort', 'quota', 'rdrand', 'readahead',
      'remap-file-pages', 'rename', 'rlimit', 'seccomp', 'seek', 'sem-posix',
      'sem-sysv', 'shm-posix', 'shm-sysv', 'sendfile', 'sigfd', 'sigfpe',
      'sigpending', 'sigq', 'sigsegv', 'sigsuspend', 'sleep', 'socket',
      'socket-fd', 'socket-pair', 'spawn', 'splice', 'stack', 'str', 'stream',
      'switch', 'symlink', 'sync-file', 'sysinfo', 'sysfs', 'tee', 'timer',
      'timerfd', 'tsc', 'tsearch', 'udp', 'udp-flood', 'unshare', 'urandom',
      'userfaultfd', 'utime', 'vecmath', 'vfork', 'vm', 'vm-rw', 'vm-splice',
      'wait', 'wcs', 'xattr', 'yield', 'zero', 'zlib', 'zombie'
  }
  return valid_stressors.issuperset(set(stressors))


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Installs stress-ng on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  vm.InstallPackages('stress-ng')


def _ParseStressngResult(metadata, output):
  """Returns stress-ng data as a sample.

  Sample output eg:
    stress-ng: info:  [2566] dispatching hogs: 2 context
    stress-ng: info:  [2566] successful run completed in 5.00s
    stress-ng: info:  [2566] stressor      bogo ops real time  usr time  sys
  time   bogo ops/s   bogo ops/s
    stress-ng: info:  [2566]                          (secs)    (secs)    (secs)
  (real time) (usr+sys time)
    stress-ng: info:  [2566] context          22429      5.00      5.49
  4.48      4485.82      2249.65
  Args:
    metadata: metadata of the sample.
    output: the output of the stress-ng benchmark.
  """
  output_list = output.splitlines()
  output_matrix = [i.split() for i in output_list]
  if len(output_matrix) != 5:
    logging.error('output is missing')
    return ''
  assert output_matrix[2][-4] == 'bogo' and output_matrix[2][-3] == 'ops/s'
  assert output_matrix[3][-4] == '(real' and output_matrix[3][-3] == 'time)'
  line = output_matrix[4]
  name = line[3]
  value = float(line[-2])  # parse bogo ops/s (real time)
  return sample.Sample(
      metric=name,
      value=value,
      unit='bogus_ops_sec',  # bogus operations per second
      metadata=metadata)


def Run(benchmark_spec):
  """Runs stress-ng on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vm = benchmark_spec.vms[0]

  metadata = {
      'duration_sec': FLAGS.stress_ng_duration,
      'threads': vm.num_cpus
  }

  # Rather than running stress-ng with --class cpu,cpu-cache,memory all in one
  # RobustRemoteCommand we run each stressor indivually. The reason is that
  # RobustRemoteCommand periodically SSHs into the VM, but one of the memory
  # stressors stresses the VM so much that SSH instantly returns 255, causing
  # the benchmark to fail completely.

  cpu_suites = [
      'af-alg', 'bsearch', 'context', 'cpu', 'cpu-online', 'crypt', 'fp-error',
      'getrandom', 'heapsort', 'hsearch', 'longjmp', 'lsearch', 'matrix',
      'mergesort', 'numa', 'qsort', 'rdrand', 'str', 'stream', 'tsc', 'tsearch',
      'vecmath', 'wcs', 'zlib'
  ]
  cpu_cache_suites = [
      'bsearch', 'cache', 'heapsort', 'hsearch', 'icache', 'lockbus', 'lsearch',
      'malloc', 'matrix', 'membarrier', 'memcpy', 'mergesort', 'qsort', 'str',
      'stream', 'tsearch', 'vecmath', 'wcs', 'zlib'
  ]
  memory_suites = [
      'bsearch', 'context', 'heapsort', 'hsearch', 'lockbus', 'lsearch',
      'malloc', 'matrix', 'membarrier', 'memcpy', 'memfd', 'mergesort',
      'mincore', 'null', 'numa', 'oom-pipe', 'pipe', 'qsort',
      'stack', 'str', 'stream', 'tsearch', 'vm', 'vm-rw',
      'wcs', 'zero', 'zlib'
  ]

  stressors = sorted(set(cpu_suites + cpu_cache_suites + memory_suites))

  if FLAGS.stress_ng_custom_stressors:
    stressors = FLAGS.stress_ng_custom_stressors

  samples = []
  values_to_geomean_list = []

  for stressor in stressors:
    cmd = ('stress-ng --{stressor} {numthreads} --metrics-brief '
           '-t {duration}'.format(stressor=stressor, numthreads=vm.num_cpus,
                                  duration=FLAGS.stress_ng_duration))
    stdout, _ = vm.RemoteCommand(cmd)
    stressng_sample = _ParseStressngResult(metadata, stdout)
    if stressng_sample:
      samples.append(stressng_sample)
      values_to_geomean_list.append(stressng_sample.value)

  # Only calculate geomean if each stressors provided a value
  if FLAGS.stress_ng_calc_geomean and len(values_to_geomean_list) == len(
      stressors):
    geomean_metadata = metadata.copy()
    geomean_metadata['stressors'] = stressors
    geomean_sample = sample.Sample(
        metric='STRESS_NG_GEOMEAN',
        value=_GeoMeanOverflow(values_to_geomean_list),
        unit='bogus_ops_sec',
        metadata=geomean_metadata)
    samples.append(geomean_sample)

  return samples


def Cleanup(benchmark_spec):
  """Cleans up stress-ng from the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  vm.Uninstall('stress-ng')
