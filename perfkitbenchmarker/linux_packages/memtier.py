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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import re
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import INSTALL_DIR

GIT_REPO = 'https://github.com/RedisLabs/memtier_benchmark'
GIT_TAG = '1.2.15'
LIBEVENT_TAR = 'libevent-2.0.21-stable.tar.gz'
LIBEVENT_URL = 'https://github.com/downloads/libevent/libevent/' + LIBEVENT_TAR
LIBEVENT_DIR = '%s/libevent-2.0.21-stable' % INSTALL_DIR
MEMTIER_DIR = '%s/memtier_benchmark' % INSTALL_DIR
APT_PACKAGES = ('autoconf automake libpcre3-dev '
                'libevent-dev pkg-config zlib1g-dev')
YUM_PACKAGES = 'zlib-devel pcre-devel libmemcached-devel'
MEMTIER_RESULTS = 'memtier_results'


FLAGS = flags.FLAGS

flags.DEFINE_enum('memtier_protocol', 'memcache_binary',
                  ['memcache_binary', 'redis', 'memcache_text'],
                  'Protocol to use. Supported protocols are redis, '
                  'memcache_text, and memcache_binary. '
                  'Defaults to memcache_binary.')
flags.DEFINE_integer('memtier_run_count', 1,
                     'Number of full-test iterations to perform. '
                     'Defaults to 1.')
flags.DEFINE_integer('memtier_requests', 10000,
                     'Number of total requests per client. Defaults to 10000.')
flags.DEFINE_list('memtier_clients', [50],
                  'Comma separated list of number of clients per thread. '
                  'Specify more than 1 value to vary the number of clients. '
                  'Defaults to [50].')
flags.DEFINE_integer('memtier_threads', 4,
                     'Number of threads. Defaults to 4.')
flags.DEFINE_integer('memtier_ratio', 9,
                     'Set:Get ratio. Defaults to 9x Get versus Sets (9 Gets to '
                     '1 Set in 10 total requests).')
flags.DEFINE_integer('memtier_data_size', 32,
                     'Object data size. Defaults to 32 bytes.')
flags.DEFINE_string('memtier_key_pattern', 'R:R',
                    'Set:Get key pattern. G for Gaussian distribution, R for '
                    'uniform Random, S for Sequential. Defaults to R:R.')


def YumInstall(vm):
  """Installs the memtier package on the VM."""
  vm.Install('build_tools')
  vm.InstallPackages(YUM_PACKAGES)
  vm.Install('wget')
  vm.RemoteCommand('wget {0} -P {1}'.format(LIBEVENT_URL, INSTALL_DIR))
  vm.RemoteCommand('cd {0} && tar xvzf {1}'.format(INSTALL_DIR,
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


def Run(vm, server_ip, server_port):
  """Runs the memtier benchmark on the vm."""
  memtier_ratio = '1:{0}'.format(FLAGS.memtier_ratio)
  samples = []

  for client_count in FLAGS.memtier_clients:
    vm.RemoteCommand('rm -f {0}'.format(MEMTIER_RESULTS))
    vm.RemoteCommand(
        'memtier_benchmark '
        '-s {server_ip} '
        '-p {server_port} '
        '-P {protocol} '
        '--run-count {run_count} '
        '--requests {requests} '
        '--clients {clients} '
        '--threads {threads} '
        '--ratio {ratio} '
        '--data-size {data_size} '
        '--key-pattern {key_pattern} '
        '--random-data > {output_file}'.format(
            server_ip=server_ip,
            server_port=server_port,
            protocol=FLAGS.memtier_protocol,
            run_count=FLAGS.memtier_run_count,
            requests=FLAGS.memtier_requests,
            clients=client_count,
            threads=FLAGS.memtier_threads,
            ratio=memtier_ratio,
            data_size=FLAGS.memtier_data_size,
            key_pattern=FLAGS.memtier_key_pattern,
            output_file=MEMTIER_RESULTS))

    results, _ = vm.RemoteCommand('cat {0}'.format(MEMTIER_RESULTS))
    metadata = GetMetadata()
    metadata['memtier_clients'] = client_count
    samples.extend(ParseResults(results, metadata))

  return samples


def GetMetadata():
  meta = {'memtier_protocol': FLAGS.memtier_protocol,
          'memtier_run_count': FLAGS.memtier_run_count,
          'memtier_requests': FLAGS.memtier_requests,
          'memtier_threads': FLAGS.memtier_threads,
          'memtier_ratio': FLAGS.memtier_ratio,
          'memtier_data_size': FLAGS.memtier_data_size,
          'memtier_key_pattern': FLAGS.memtier_key_pattern,
          'memtier_version': GIT_TAG}
  return meta


def ParseResults(memtier_results, meta):
  """Parse memtier_benchmark result textfile into samples.

  Args:
    memtier_results: Text output of running Memtier benchmark.
    meta: metadata associated with the results.
  Yields:
    List of sample.Sample objects.

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
  set_histogram = []
  get_histogram = []
  total_requests = FLAGS.memtier_requests
  approx_total_sets = round(float(total_requests) / (FLAGS.memtier_ratio + 1))
  last_total_sets = 0
  approx_total_gets = total_requests - approx_total_sets
  last_total_gets = 0
  for raw_line in memtier_results.splitlines():
    line = raw_line.strip()

    if re.match(r'^Totals', line):
      _, ops, _, _, _, kilobyte = line.split()
      yield sample.Sample('Ops Throughput', float(ops), 'ops/s', meta)
      yield sample.Sample('KB Throughput', float(kilobyte), 'KB/s', meta)

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

  for name, histogram in [('get', get_histogram), ('set', set_histogram)]:
    hist_meta = meta.copy()
    hist_meta.update({'histogram': json.dumps(histogram)})
    yield sample.Sample('{0} latency histogram'.format(name), 0, '', hist_meta)


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
