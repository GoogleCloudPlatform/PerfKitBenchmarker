# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing memtier installation and cleanup functions."""


import json
import logging
import pathlib
import re
from typing import Any, Dict, List, Text, Tuple, Union

from absl import flags
import dataclasses
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import sample

GIT_REPO = 'https://github.com/RedisLabs/memtier_benchmark'
GIT_TAG = '1.2.15'
LIBEVENT_TAR = 'libevent-2.0.21-stable.tar.gz'
LIBEVENT_URL = 'https://github.com/downloads/libevent/libevent/' + LIBEVENT_TAR
LIBEVENT_DIR = '%s/libevent-2.0.21-stable' % linux_packages.INSTALL_DIR
MEMTIER_DIR = '%s/memtier_benchmark' % linux_packages.INSTALL_DIR
APT_PACKAGES = ('autoconf automake libpcre3-dev '
                'libevent-dev pkg-config zlib1g-dev')
YUM_PACKAGES = 'zlib-devel pcre-devel libmemcached-devel'
MEMTIER_RESULTS = pathlib.PosixPath('memtier_results')

_LOAD_NUM_PIPELINES = 100  # Arbitrarily high for loading
_WRITE_ONLY = '1:0'

MemtierHistogram = List[Dict[str, Union[float, int]]]

FLAGS = flags.FLAGS

flags.DEFINE_enum(
    'memtier_protocol', 'memcache_binary',
    ['memcache_binary', 'redis', 'memcache_text'],
    'Protocol to use. Supported protocols are redis, '
    'memcache_text, and memcache_binary. '
    'Defaults to memcache_binary.')
flags.DEFINE_integer(
    'memtier_run_count', 1, 'Number of full-test iterations to perform. '
    'Defaults to 1.')
flags.DEFINE_integer(
    'memtier_run_duration', None, 'Mutually exclusive with memtier_requests.'
    'Duration for each client count in seconds. '
    'By default, test length is set '
    'by memtier_requests, the number of requests sent by each '
    'client. By specifying run_duration, key space remains '
    'the same (from 1 to memtier_requests), but test stops '
    'once run_duration is passed. '
    'Total test duration = run_duration * runs * '
    'len(memtier_clients).')
flags.DEFINE_integer(
    'memtier_requests', 10000, 'Mutually exclusive with memtier_run_duration. '
    'Number of total requests per client. Defaults to 10000.')
flag_util.DEFINE_integerlist(
    'memtier_clients', [50],
    'Comma separated list of number of clients per thread. '
    'Specify more than 1 value to vary the number of clients. '
    'Defaults to [50].')
flag_util.DEFINE_integerlist('memtier_threads', [4],
                             'Number of threads. Defaults to 4.')
flags.DEFINE_integer(
    'memtier_ratio', 9,
    'Set:Get ratio. Defaults to 9x Get versus Sets (9 Gets to '
    '1 Set in 10 total requests).')
flags.DEFINE_integer('memtier_data_size', 32,
                     'Object data size. Defaults to 32 bytes.')
flags.DEFINE_string(
    'memtier_key_pattern', 'R:R',
    'Set:Get key pattern. G for Gaussian distribution, R for '
    'uniform Random, S for Sequential. Defaults to R:R.')
flags.DEFINE_integer(
    'memtier_key_maximum', 10000000, 'Key ID maximum value. The range of keys '
    'will be from 1 (min) to this specified max key value.')
flag_util.DEFINE_integerlist(
    'memtier_pipeline', [1],
    'Number of pipelines to use for memtier. Defaults to 1, '
    'i.e. no pipelining.')


def YumInstall(vm):
  """Installs the memtier package on the VM."""
  vm.Install('build_tools')
  vm.InstallPackages(YUM_PACKAGES)
  vm.Install('wget')
  vm.RemoteCommand('wget {0} -P {1}'.format(LIBEVENT_URL,
                                            linux_packages.INSTALL_DIR))
  vm.RemoteCommand('cd {0} && tar xvzf {1}'.format(linux_packages.INSTALL_DIR,
                                                   LIBEVENT_TAR))
  vm.RemoteCommand('cd {0} && ./configure && sudo make install'.format(
      LIBEVENT_DIR))
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, MEMTIER_DIR))
  vm.RemoteCommand('cd {0} && git checkout {1}'.format(MEMTIER_DIR, GIT_TAG))
  pkg_config = 'PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:${PKG_CONFIG_PATH}'
  vm.RemoteCommand('cd {0} && autoreconf -ivf && {1} ./configure && '
                   'sudo make install'.format(MEMTIER_DIR, pkg_config))


def AptInstall(vm):
  """Installs the memtier package on the VM."""
  vm.Install('build_tools')
  vm.InstallPackages(APT_PACKAGES)
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, MEMTIER_DIR))
  vm.RemoteCommand('cd {0} && git checkout {1}'.format(MEMTIER_DIR, GIT_TAG))
  vm.RemoteCommand('cd {0} && autoreconf -ivf && ./configure && '
                   'sudo make install'.format(MEMTIER_DIR))


def _Uninstall(vm):
  """Uninstalls the memtier package on the VM."""
  vm.RemoteCommand('cd {0} && sudo make uninstall'.format(MEMTIER_DIR))


def YumUninstall(vm):
  """Uninstalls the memtier package on the VM."""
  _Uninstall(vm)


def AptUninstall(vm):
  """Uninstalls the memtier package on the VM."""
  _Uninstall(vm)


def BuildMemtierCommand(
    server: str = None,
    port: str = None,
    protocol: str = None,
    clients: int = None,
    threads: int = None,
    ratio: str = None,
    data_size: int = None,
    pipeline: int = None,
    key_minimum: int = None,
    key_maximum: int = None,
    key_pattern: str = None,
    requests: Union[str, int] = None,
    run_count: int = None,
    random_data: bool = None,
    test_time: int = None,
    outfile: pathlib.PosixPath = None,
) -> str:
  """Returns command arguments used to run memtier."""
  # Arguments passed with a parameter
  args = {
      'server': server,
      'port': port,
      'protocol': protocol,
      'clients': clients,
      'threads': threads,
      'ratio': ratio,
      'data-size': data_size,
      'pipeline': pipeline,
      'key-minimum': key_minimum,
      'key-maximum': key_maximum,
      'key-pattern': key_pattern,
      'requests': requests,
      'run-count': run_count,
      'test-time': test_time,
  }
  # Arguments passed without a parameter
  no_param_args = {
      'random-data': random_data
  }
  # Build the command
  cmd = ['memtier_benchmark']
  for arg, value in args.items():
    if value is not None:
      cmd.extend([f'--{arg}', str(value)])
  for no_param_arg, value in no_param_args.items():
    if value:
      cmd.append(f'--{no_param_arg}')
  if outfile:
    cmd.extend(['>', str(outfile)])
  return ' '.join(cmd)


def Load(client_vm, server_ip, server_port):
  """Preload the server with data."""
  cmd = BuildMemtierCommand(
      server=server_ip,
      port=server_port,
      protocol=FLAGS.memtier_protocol,
      clients=1,
      threads=1,
      ratio=_WRITE_ONLY,
      data_size=FLAGS.memtier_data_size,
      pipeline=_LOAD_NUM_PIPELINES,
      key_minimum=1,
      key_maximum=FLAGS.memtier_key_maximum,
      requests='allkeys')
  client_vm.RemoteCommand(cmd)


def RunOverAllThreadsAndPipelines(client_vm, server_ip, server_port):
  """Runs memtier over all pipeline and thread combinations."""
  samples = []
  for pipeline in FLAGS.memtier_pipeline:
    for client_thread in FLAGS.memtier_threads:
      logging.info(
          'Start benchmarking memcached using memtier:\n'
          '\tmemtier threads: %s'
          '\tmemtier pipeline, %s',
          client_thread, pipeline)
      tmp_samples = Run(
          client_vm, server_ip, server_port,
          client_thread, pipeline)
      samples.extend(tmp_samples)
  return samples


def Run(vm, server_ip, server_port, threads, pipeline):
  """Runs the memtier benchmark on the vm."""
  memtier_ratio = '1:{0}'.format(FLAGS.memtier_ratio)
  samples = []

  for client_count in FLAGS.memtier_clients:
    vm.RemoteCommand('rm -f {0}'.format(MEMTIER_RESULTS))
    # Specify one of run requests or run duration.
    requests = (
        FLAGS.memtier_requests if FLAGS.memtier_run_duration is None else None)
    cmd = BuildMemtierCommand(
        server=server_ip,
        port=server_port,
        protocol=FLAGS.memtier_protocol,
        run_count=FLAGS.memtier_run_count,
        clients=client_count,
        threads=threads,
        ratio=memtier_ratio,
        data_size=FLAGS.memtier_data_size,
        key_pattern=FLAGS.memtier_key_pattern,
        pipeline=pipeline,
        key_minimum=1,
        key_maximum=FLAGS.memtier_key_maximum,
        random_data=True,
        test_time=FLAGS.memtier_run_duration,
        requests=requests,
        outfile=MEMTIER_RESULTS)
    vm.RemoteCommand(cmd)

    output, _ = vm.RemoteCommand('cat {0}'.format(MEMTIER_RESULTS))
    metadata = GetMetadata(threads, pipeline)
    metadata['memtier_clients'] = client_count
    results = MemtierResult.Parse(output)
    run_samples = results.GetSamples(metadata)
    samples.extend(run_samples)

  return samples


def GetMetadata(threads, pipeline):
  """Metadata for memtier test."""
  meta = {'memtier_protocol': FLAGS.memtier_protocol,
          'memtier_run_count': FLAGS.memtier_run_count,
          'memtier_requests': FLAGS.memtier_requests,
          'memtier_threads': threads,
          'memtier_ratio': FLAGS.memtier_ratio,
          'memtier_key_maximum': FLAGS.memtier_key_maximum,
          'memtier_data_size': FLAGS.memtier_data_size,
          'memtier_key_pattern': FLAGS.memtier_key_pattern,
          'memtier_pipeline': pipeline,
          'memtier_version': GIT_TAG}
  if FLAGS.memtier_run_duration:
    meta['memtier_run_duration'] = FLAGS.memtier_run_duration
  return meta


@dataclasses.dataclass
class MemtierResult:
  """Class that represents memtier results."""
  ops_per_sec: float
  kb_per_sec: float
  latency_ms: float
  get_latency_histogram: MemtierHistogram
  set_latency_histogram: MemtierHistogram

  @classmethod
  def Parse(cls, memtier_results: Text) -> 'MemtierResult':
    """Parse memtier_benchmark result textfile and return results.

    Args:
      memtier_results: Text output of running Memtier benchmark.
    Returns:
      MemtierResult object.

    Example memtier_benchmark output, note Hits/sec and Misses/sec are displayed
    in error for version 1.2.8+ due to bug:
    https://github.com/RedisLabs/memtier_benchmark/issues/46

    4         Threads
    50        Connections per thread
    20        Seconds
    Type        Ops/sec     Hits/sec   Misses/sec      Latency       KB/sec
    ------------------------------------------------------------------------
    Sets        4005.50          ---          ---      4.50600       308.00
    Gets       40001.05         0.00     40001.05      4.54300      1519.00
    Totals     44006.55         0.00     40001.05      4.54000      1828.00

    Request Latency Distribution
    Type        <= msec      Percent
    ------------------------------------------------------------------------
    SET               0         9.33
    SET               1        71.07
    ...
    SET              33       100.00
    SET              36       100.00
    ---
    GET               0        10.09
    GET               1        70.88
    ..
    GET              40       100.00
    GET              41       100.00
    """
    ops_per_sec, latency_ms, kb_per_sec = _ParseTotalThroughputAndLatency(
        memtier_results)
    set_histogram, get_histogram = _ParseHistogram(memtier_results)
    return cls(ops_per_sec, kb_per_sec, latency_ms, get_histogram,
               set_histogram)

  def GetSamples(self, metadata: Dict[str, Any]) -> List[sample.Sample]:
    """Return this result as a list of samples."""
    samples = [
        sample.Sample('Ops Throughput', self.ops_per_sec, 'ops/s', metadata),
        sample.Sample('KB Throughput', self.kb_per_sec, 'KB/s', metadata),
        sample.Sample('Latency', self.latency_ms, 'ms', metadata),
    ]
    for name, histogram in [('get', self.get_latency_histogram),
                            ('set', self.set_latency_histogram)]:
      hist_meta = metadata.copy()
      hist_meta.update({'histogram': json.dumps(histogram)})
      samples.append(
          sample.Sample(f'{name} latency histogram', 0, '', hist_meta))
    return samples


def _ParseHistogram(
    memtier_results: Text) -> Tuple[MemtierHistogram, MemtierHistogram]:
  """Parses the 'Request Latency Distribution' section of memtier output."""
  set_histogram = []
  get_histogram = []
  total_requests = FLAGS.memtier_requests
  approx_total_sets = round(float(total_requests) / (FLAGS.memtier_ratio + 1))
  last_total_sets = 0
  approx_total_gets = total_requests - approx_total_sets
  last_total_gets = 0
  for raw_line in memtier_results.splitlines():
    line = raw_line.strip()
    last_total_sets = _ParseLine(
        r'^SET',
        line,
        approx_total_sets,
        last_total_sets,
        set_histogram)
    last_total_gets = _ParseLine(
        r'^GET',
        line,
        approx_total_gets,
        last_total_gets,
        get_histogram)
  return set_histogram, get_histogram


def _ParseTotalThroughputAndLatency(
    memtier_results: Text) -> Tuple[float, float, float]:
  """Parses the 'TOTALS' output line and return throughput and latency."""
  for raw_line in memtier_results.splitlines():
    line = raw_line.strip()
    if re.match(r'^Totals', line):
      _, ops_per_sec, _, _, latency_ms, kb_per_sec = line.split()
      return float(ops_per_sec), float(latency_ms), float(kb_per_sec)
  raise errors.Benchmarks.RunError('No "TOTALS" line in memtier output.')


def _ParseLine(pattern, line, approx_total, last_total, histogram):
  """Helper function to parse an output line."""
  if not re.match(pattern, line):
    return last_total

  _, msec, percent = line.split()
  counts = _ConvertPercentToAbsolute(approx_total, float(percent))
  bucket_counts = int(round(counts - last_total))
  if bucket_counts > 0:
    histogram.append({'microsec': float(msec) * 1000,
                      'count': bucket_counts})
  return counts


def _ConvertPercentToAbsolute(total_value, percent):
  """Given total value and a 100-based percentage, returns the actual value."""
  return percent / 100 * total_value
