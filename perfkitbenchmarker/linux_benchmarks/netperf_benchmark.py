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

"""Runs plain netperf in a few modes.

docs:
http://www.netperf.org/svn/netperf2/tags/netperf-2.4.5/doc/netperf.html#TCP_005fRR
manpage: http://manpages.ubuntu.com/manpages/maverick/man1/netperf.1.html

Runs TCP_RR, TCP_CRR, and TCP_STREAM benchmarks from netperf across two
machines.
"""

import csv
import io
import json
import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import netperf

flags.DEFINE_integer('netperf_max_iter', None,
                     'Maximum number of iterations to run during '
                     'confidence interval estimation. If unset, '
                     'a single iteration will be run.',
                     lower_bound=3, upper_bound=30)

flags.DEFINE_integer('netperf_test_length', 60,
                     'netperf test length, in seconds',
                     lower_bound=1)
flags.DEFINE_bool('netperf_enable_histograms', True,
                  'Determines whether latency histograms are '
                  'collected/reported.')

ALL_BENCHMARKS = ['TCP_RR', 'TCP_CRR', 'TCP_STREAM', 'UDP_RR']
flags.DEFINE_list('netperf_benchmarks', ALL_BENCHMARKS,
                  'The netperf benchmark(s) to run.')
flags.RegisterValidator(
    'netperf_benchmarks',
    lambda benchmarks: benchmarks and set(benchmarks).issubset(ALL_BENCHMARKS))

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'netperf'
BENCHMARK_CONFIG = """
netperf:
  description: Run TCP_RR, TCP_CRR, UDP_RR and TCP_STREAM
  vm_groups:
    vm_1:
      vm_spec: *default_single_core
    vm_2:
      vm_spec: *default_single_core
"""

MBPS = 'Mbits/sec'
TRANSACTIONS_PER_SECOND = 'transactions_per_second'

COMMAND_PORT = 20000
DATA_PORT = 20001


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def PrepareNetperf(vm):
  """Installs netperf on a single vm."""
  vm.Install('netperf')


def Prepare(benchmark_spec):
  """Install netperf on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vms = vms[:2]
  vm_util.RunThreaded(PrepareNetperf, vms)

  if vm_util.ShouldRunOnExternalIpAddress():
    vms[1].AllowPort(COMMAND_PORT)
    vms[1].AllowPort(DATA_PORT)

  vms[1].RemoteCommand('%s -p %s' %
                       (netperf.NETSERVER_PATH, COMMAND_PORT))


def RunNetperf(vm, benchmark_name, server_ip):
  """Spawns netperf on a remove VM, parses results.

  Args:
    vm: The VM that the netperf TCP_RR benchmark will be run upon.
    benchmark_name: The netperf benchmark to run, see the documentation.
    server_ip: A machine that is running netserver.

  Returns:
    A sample.Sample object with the result.
  """
  # Flags:
  # -o specifies keys to include in CSV output.
  # -j keeps additional latency numbers
  # -v sets the verbosity level so that netperf will print out histograms
  # -I specifies the confidence % and width - here 99% confidence that the true
  #    value is within +/- 2.5% of the reported value
  # -i specifies the maximum and minimum number of iterations.
  confidence = ('-I 99,5 -i {0},3'.format(FLAGS.netperf_max_iter)
                if FLAGS.netperf_max_iter else '')
  verbosity = '-v2 ' if FLAGS.netperf_enable_histograms else ''
  netperf_cmd = ('{netperf_path} -p {command_port} -j {verbosity}'
                 '-t {benchmark_name} -H {server_ip} -l {length} {confidence} '
                 ' -- '
                 '-P {data_port} '
                 '-o THROUGHPUT,THROUGHPUT_UNITS,P50_LATENCY,P90_LATENCY,'
                 'P99_LATENCY,STDDEV_LATENCY,'
                 'MIN_LATENCY,MAX_LATENCY,'
                 'CONFIDENCE_ITERATION,THROUGHPUT_CONFID').format(
                     netperf_path=netperf.NETPERF_PATH,
                     benchmark_name=benchmark_name,
                     server_ip=server_ip, command_port=COMMAND_PORT,
                     data_port=DATA_PORT,
                     length=FLAGS.netperf_test_length,
                     confidence=confidence, verbosity=verbosity)
  stdout, _ = vm.RemoteCommand(netperf_cmd,
                               timeout=2 * FLAGS.netperf_test_length *
                               (FLAGS.netperf_max_iter or 1))

  fp = io.StringIO(stdout)
  # "-o" flag above specifies CSV output, but there is one extra header line:
  banner = next(fp)
  assert banner.startswith('MIGRATED'), stdout
  r = csv.DictReader(fp)
  row = next(r)
  logging.info('Netperf Results: %s', row)
  assert 'Throughput' in row, row

  value = float(row['Throughput'])
  unit = {'Trans/s': TRANSACTIONS_PER_SECOND,
          '10^6bits/s': MBPS}[row['Throughput Units']]
  if unit == MBPS:
    metric = '%s_Throughput' % benchmark_name
  else:
    metric = '%s_Transaction_Rate' % benchmark_name

  meta_keys = [('Confidence Iterations Run', 'confidence_iter'),
               ('Throughput Confidence Width (%)', 'confidence_width_percent')]
  metadata = {meta_key: row[np_key] for np_key, meta_key in meta_keys}
  metadata.update(netperf_test_length=FLAGS.netperf_test_length,
                  max_iter=FLAGS.netperf_max_iter or 1)

  samples = [sample.Sample(metric, value, unit, metadata)]

  # No tail latency for throughput.
  if unit == MBPS:
    return samples

  if FLAGS.netperf_enable_histograms:
    # Generate a sample containing the entire histogram of
    # latencies.
    hist = netperf.ParseHistogram(stdout)
    hist_metadata = {'histogram': json.dumps(hist)}
    hist_metadata.update(metadata)
    samples.append(sample.Sample(
        '%s_Latency_Histogram' % benchmark_name, 0, 'us', hist_metadata))

  for metric_key, metric_name in [
      ('50th Percentile Latency Microseconds', 'p50'),
      ('90th Percentile Latency Microseconds', 'p90'),
      ('99th Percentile Latency Microseconds', 'p99'),
      ('Minimum Latency Microseconds', 'min'),
      ('Maximum Latency Microseconds', 'max'),
      ('Stddev Latency Microseconds', 'stddev')]:
    samples.append(
        sample.Sample('%s_Latency_%s' % (benchmark_name, metric_name),
                      float(row[metric_key]), 'us', metadata))
  return samples


def Run(benchmark_spec):
  """Run netperf TCP_RR on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  client_vm = vms[0]
  server_vm = vms[1]
  logging.info('netperf running on %s', client_vm)
  results = []
  metadata = {'ip_type': 'external'}
  for vm_specifier, vm in ('receiving', server_vm), ('sending', client_vm):
    metadata['{0}_zone'.format(vm_specifier)] = vm.zone
    for k, v in vm.GetMachineTypeDict().iteritems():
      metadata['{0}_{1}'.format(vm_specifier, k)] = v

  for netperf_benchmark in FLAGS.netperf_benchmarks:

    if vm_util.ShouldRunOnExternalIpAddress():
      external_ip_results = RunNetperf(client_vm, netperf_benchmark,
                                       server_vm.ip_address)
      for external_ip_result in external_ip_results:
        external_ip_result.metadata.update(metadata)
      results.extend(external_ip_results)

    if vm_util.ShouldRunOnInternalIpAddress(client_vm, server_vm):
      internal_ip_results = RunNetperf(client_vm, netperf_benchmark,
                                       server_vm.internal_ip)
      for internal_ip_result in internal_ip_results:
        internal_ip_result.metadata.update(metadata)
        internal_ip_result.metadata['ip_type'] = 'internal'
      results.extend(internal_ip_results)

  return results


def Cleanup(benchmark_spec):
  """Cleanup netperf on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vms[1].RemoteCommand('sudo pkill netserver')
