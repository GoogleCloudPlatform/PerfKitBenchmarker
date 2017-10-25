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

from collections import Counter
import csv
import io
import json
import logging
import os
import re
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import flag_util
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
                  'collected/reported. Only for *RR benchmarks')
flag_util.DEFINE_integerlist('netperf_num_streams', flag_util.IntegerList([1]),
                             'Number of netperf processes to run. Netperf '
                             'will run once for each value in the list.')
flags.DEFINE_integer('netperf_thinktime', 0,
                     'Time in nanoseconds to do work for each request.')
flags.DEFINE_integer('netperf_thinktime_array_size', 0,
                     'The size of the array to traverse for thinktime.')
flags.DEFINE_integer('netperf_thinktime_run_length', 0,
                     'The number of contiguous numbers to sum at a time in the '
                     'thinktime array.')

ALL_BENCHMARKS = ['TCP_RR', 'TCP_CRR', 'TCP_STREAM', 'UDP_RR']
flags.DEFINE_list('netperf_benchmarks', ALL_BENCHMARKS,
                  'The netperf benchmark(s) to run.')
flags.register_validator(
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

# Command ports are even (id*2), data ports are odd (id*2 + 1)
PORT_START = 20000

REMOTE_SCRIPTS_DIR = 'netperf_test_scripts'
REMOTE_SCRIPT = 'netperf_test.py'

PERCENTILES = [50, 90, 99]

# By default, Container-Optimized OS (COS) host firewall allows only
# outgoing connections and incoming SSH connections. To allow incoming
# connections from VMs running netperf, we need to add iptables rules
# on the VM running netserver.
_COS_RE = re.compile(r'\b(cos|gci)-')


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

  num_streams = max(FLAGS.netperf_num_streams)

  # See comments where _COS_RE is defined.
  if vms[1].image and re.search(_COS_RE, vms[1].image):
    _SetupHostFirewall(benchmark_spec)

  # Start the netserver processes
  if vm_util.ShouldRunOnExternalIpAddress():
    # Open all of the command and data ports
    vms[1].AllowPort(PORT_START, PORT_START + num_streams * 2 - 1)
  netserver_cmd = ('for i in $(seq {port_start} 2 {port_end}); do '
                   '{netserver_path} -p $i & done').format(
                       port_start=PORT_START,
                       port_end=PORT_START + num_streams * 2 - 1,
                       netserver_path=netperf.NETSERVER_PATH)
  vms[1].RemoteCommand(netserver_cmd)

  # Copy remote test script to client
  path = data.ResourcePath(os.path.join(REMOTE_SCRIPTS_DIR, REMOTE_SCRIPT))
  logging.info('Uploading %s to %s', path, vms[0])
  vms[0].PushFile(path)
  vms[0].RemoteCommand('sudo chmod 777 %s' % REMOTE_SCRIPT)


def _SetupHostFirewall(benchmark_spec):
  """Set up host firewall to allow incoming traffic.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """

  client_vm = benchmark_spec.vms[0]
  server_vm = benchmark_spec.vms[1]

  ip_addrs = [client_vm.internal_ip]
  if vm_util.ShouldRunOnExternalIpAddress():
    ip_addrs.append(client_vm.ip_address)

  logging.info('setting up host firewall on %s running %s for client at %s',
               server_vm.name, server_vm.image, ip_addrs)
  cmd = 'sudo iptables -A INPUT -p %s -s %s -j ACCEPT'
  for protocol in 'tcp', 'udp':
    for ip_addr in ip_addrs:
      server_vm.RemoteHostCommand(cmd % (protocol, ip_addr))


def _HistogramStatsCalculator(histogram, percentiles=PERCENTILES):
  """Computes values at percentiles in a distribution as well as stddev.

  Args:
    histogram: A dict mapping values to the number of samples with that value.
    percentiles: An array of percentiles to calculate.

  Returns:
    A dict mapping stat names to their values.
  """
  stats = {}

  # Histogram data in list form sorted by key
  by_value = sorted([(value, count) for value, count in histogram.items()],
                    key=lambda x: x[0])
  total_count = sum(histogram.values())

  cur_value_index = 0  # Current index in by_value
  cur_index = 0  # Number of values we've passed so far
  for p in percentiles:
    index = int(float(total_count) * float(p) / 100.0)
    index = min(index, total_count - 1)  # Handle 100th percentile
    for value, count in by_value[cur_value_index:]:
      if cur_index + count > index:
        stats['p%s' % str(p)] = by_value[cur_value_index][0]
        break
      else:
        cur_index += count
        cur_value_index += 1

  # Compute stddev
  value_sum = float(sum([value * count for value, count in histogram.items()]))
  average = value_sum / float(total_count)
  if total_count > 1:
    total_of_squares = sum([(value - average) ** 2 * count
                            for value, count in histogram.items()])
    stats['stddev'] = (total_of_squares / (total_count - 1)) ** 0.5
  else:
    stats['stddev'] = 0
  return stats


def _ParseNetperfOutput(stdout, metadata, benchmark_name,
                        enable_latency_histograms):
  """Parses the stdout of a single netperf process.

  Args:
    stdout: the stdout of the netperf process
    metadata: metadata for any sample.Sample objects we create

  Returns:
    A tuple containing (throughput_sample, latency_samples, latency_histogram)
  """
  # Don't modify the metadata dict that was passed in
  metadata = metadata.copy()

  # Extract stats from stdout
  # Sample output:
  #
  # "MIGRATED TCP REQUEST/RESPONSE TEST from 0.0.0.0 (0.0.0.0) port 20001
  # AF_INET to 104.154.50.86 () port 20001 AF_INET : +/-2.500% @ 99% conf.
  # : first burst 0",\n
  # Throughput,Throughput Units,Throughput Confidence Width (%),
  # Confidence Iterations Run,Stddev Latency Microseconds,
  # 50th Percentile Latency Microseconds,90th Percentile Latency Microseconds,
  # 99th Percentile Latency Microseconds,Minimum Latency Microseconds,
  # Maximum Latency Microseconds\n
  # 1405.50,Trans/s,2.522,4,783.80,683,735,841,600,900\n
  try:
    fp = io.StringIO(stdout)
    # "-o" flag above specifies CSV output, but there is one extra header line:
    banner = next(fp)
    assert banner.startswith('MIGRATED'), stdout
    r = csv.DictReader(fp)
    results = next(r)
    logging.info('Netperf Results: %s', results)
    assert 'Throughput' in results
  except:
    raise Exception('Netperf ERROR: Failed to parse stdout. STDOUT: %s' %
                    stdout)

  # Update the metadata with some additional infos
  meta_keys = [('Confidence Iterations Run', 'confidence_iter'),
               ('Throughput Confidence Width (%)', 'confidence_width_percent')]
  metadata.update({meta_key: results[netperf_key]
                   for netperf_key, meta_key in meta_keys})

  # Create the throughput sample
  throughput = float(results['Throughput'])
  throughput_units = results['Throughput Units']
  if throughput_units == '10^6bits/s':
    # TCP_STREAM benchmark
    unit = MBPS
    metric = '%s_Throughput' % benchmark_name
  elif throughput_units == 'Trans/s':
    # *RR benchmarks
    unit = TRANSACTIONS_PER_SECOND
    metric = '%s_Transaction_Rate' % benchmark_name
  else:
    raise ValueError('Netperf output specifies unrecognized throughput units %s'
                     % throughput_units)
  throughput_sample = sample.Sample(metric, throughput, unit, metadata)

  latency_hist = None
  latency_samples = []
  if enable_latency_histograms:
    # Parse the latency histogram. {latency: count} where "latency" is the
    # latency in microseconds with only 2 significant figures and "count" is the
    # number of response times that fell in that latency range.
    latency_hist = netperf.ParseHistogram(stdout)
    hist_metadata = {'histogram': json.dumps(latency_hist)}
    hist_metadata.update(metadata)
    latency_samples.append(sample.Sample(
        '%s_Latency_Histogram' % benchmark_name, 0, 'us', hist_metadata))
  if unit != MBPS:
    for metric_key, metric_name in [
        ('50th Percentile Latency Microseconds', 'p50'),
        ('90th Percentile Latency Microseconds', 'p90'),
        ('99th Percentile Latency Microseconds', 'p99'),
        ('Minimum Latency Microseconds', 'min'),
        ('Maximum Latency Microseconds', 'max'),
        ('Stddev Latency Microseconds', 'stddev')]:
      if metric_key in results:
        latency_samples.append(
            sample.Sample('%s_Latency_%s' % (benchmark_name, metric_name),
                          float(results[metric_key]), 'us', metadata))

  return (throughput_sample, latency_samples, latency_hist)


def RunNetperf(vm, benchmark_name, server_ip, num_streams):
  """Spawns netperf on a remote VM, parses results.

  Args:
    vm: The VM that the netperf TCP_RR benchmark will be run upon.
    benchmark_name: The netperf benchmark to run, see the documentation.
    server_ip: A machine that is running netserver.
    num_streams: The number of netperf client threads to run.

  Returns:
    A sample.Sample object with the result.
  """
  enable_latency_histograms = FLAGS.netperf_enable_histograms or num_streams > 1
  # Throughput benchmarks don't have latency histograms
  enable_latency_histograms = enable_latency_histograms and \
      benchmark_name != 'TCP_STREAM'
  # Flags:
  # -o specifies keys to include in CSV output.
  # -j keeps additional latency numbers
  # -v sets the verbosity level so that netperf will print out histograms
  # -I specifies the confidence % and width - here 99% confidence that the true
  #    value is within +/- 2.5% of the reported value
  # -i specifies the maximum and minimum number of iterations.
  confidence = ('-I 99,5 -i {0},3'.format(FLAGS.netperf_max_iter)
                if FLAGS.netperf_max_iter else '')
  verbosity = '-v2 ' if enable_latency_histograms else ''
  netperf_cmd = ('{netperf_path} -p {{command_port}} -j {verbosity} '
                 '-t {benchmark_name} -H {server_ip} -l {length} {confidence}'
                 ' -- '
                 '-P ,{{data_port}} '
                 '-o THROUGHPUT,THROUGHPUT_UNITS,P50_LATENCY,P90_LATENCY,'
                 'P99_LATENCY,STDDEV_LATENCY,'
                 'MIN_LATENCY,MAX_LATENCY,'
                 'CONFIDENCE_ITERATION,THROUGHPUT_CONFID').format(
                     netperf_path=netperf.NETPERF_PATH,
                     benchmark_name=benchmark_name,
                     server_ip=server_ip,
                     length=FLAGS.netperf_test_length,
                     confidence=confidence, verbosity=verbosity)
  if FLAGS.netperf_thinktime != 0:
    netperf_cmd += (' -X {thinktime},{thinktime_array_size},'
                    '{thinktime_run_length} ').format(
                        thinktime=FLAGS.netperf_thinktime,
                        thinktime_array_size=FLAGS.netperf_thinktime_array_size,
                        thinktime_run_length=FLAGS.netperf_thinktime_run_length)

  # Run all of the netperf processes and collect their stdout
  # TODO: Record process start delta of netperf processes on the remote machine

  # Give the remote script the max possible test length plus 5 minutes to
  # complete
  remote_cmd_timeout = \
      FLAGS.netperf_test_length * (FLAGS.netperf_max_iter or 1) + 300
  remote_cmd = ('./%s --netperf_cmd="%s" --num_streams=%s --port_start=%s' %
                (REMOTE_SCRIPT, netperf_cmd, num_streams, PORT_START))
  remote_stdout, _ = vm.RemoteCommand(remote_cmd,
                                      timeout=remote_cmd_timeout)

  # Decode stdouts, stderrs, and return codes from remote command's stdout
  json_out = json.loads(remote_stdout)
  stdouts = json_out[0]

  # Metadata to attach to samples
  metadata = {'netperf_test_length': FLAGS.netperf_test_length,
              'max_iter': FLAGS.netperf_max_iter or 1,
              'sending_thread_count': num_streams}

  parsed_output = [_ParseNetperfOutput(stdout, metadata, benchmark_name,
                                       enable_latency_histograms)
                   for stdout in stdouts]

  if len(parsed_output) == 1:
    # Only 1 netperf thread
    throughput_sample, latency_samples, histogram = parsed_output[0]
    return [throughput_sample] + latency_samples
  else:
    # Multiple netperf threads

    samples = []

    # Unzip parsed output
    # Note that latency_samples are invalid with multiple threads because stats
    # are computed per-thread by netperf, so we don't use them here.
    throughput_samples, _, latency_histograms = [list(t)
                                                 for t in zip(*parsed_output)]
    # They should all have the same units
    throughput_unit = throughput_samples[0].unit
    # Extract the throughput values from the samples
    throughputs = [s.value for s in throughput_samples]
    # Compute some stats on the throughput values
    throughput_stats = sample.PercentileCalculator(throughputs, [50, 90, 99])
    throughput_stats['min'] = min(throughputs)
    throughput_stats['max'] = max(throughputs)
    # Calculate aggregate throughput
    throughput_stats['total'] = throughput_stats['average'] * len(throughputs)
    # Create samples for throughput stats
    for stat, value in throughput_stats.items():
      samples.append(
          sample.Sample('%s_Throughput_%s' % (benchmark_name, stat),
                        float(value),
                        throughput_unit, metadata))
    if enable_latency_histograms:
      # Combine all of the latency histogram dictionaries
      latency_histogram = Counter()
      for histogram in latency_histograms:
        latency_histogram.update(histogram)
      # Create a sample for the aggregate latency histogram
      hist_metadata = {'histogram': json.dumps(latency_histogram)}
      hist_metadata.update(metadata)
      samples.append(sample.Sample(
          '%s_Latency_Histogram' % benchmark_name, 0, 'us', hist_metadata))
      # Calculate stats on aggregate latency histogram
      latency_stats = _HistogramStatsCalculator(latency_histogram, [50, 90, 99])
      # Create samples for the latency stats
      for stat, value in latency_stats.items():
        samples.append(
            sample.Sample('%s_Latency_%s' % (benchmark_name, stat),
                          float(value),
                          'us', metadata))
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
  client_vm = vms[0]  # Client aka "sending vm"
  server_vm = vms[1]  # Server aka "receiving vm"
  logging.info('netperf running on %s', client_vm)
  results = []
  metadata = {
      'sending_zone': client_vm.zone,
      'sending_machine_type': client_vm.machine_type,
      'receiving_zone': server_vm.zone,
      'receiving_machine_type': server_vm.machine_type
  }

  for num_streams in FLAGS.netperf_num_streams:
    assert(num_streams >= 1)

    for netperf_benchmark in FLAGS.netperf_benchmarks:
      if vm_util.ShouldRunOnExternalIpAddress():
        external_ip_results = RunNetperf(client_vm, netperf_benchmark,
                                         server_vm.ip_address, num_streams)
        for external_ip_result in external_ip_results:
          external_ip_result.metadata['ip_type'] = 'external'
          external_ip_result.metadata.update(metadata)
        results.extend(external_ip_results)

      if vm_util.ShouldRunOnInternalIpAddress(client_vm, server_vm):
        internal_ip_results = RunNetperf(client_vm, netperf_benchmark,
                                         server_vm.internal_ip, num_streams)
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
  vms[1].RemoteCommand('sudo killall netserver')
  vms[0].RemoteCommand('sudo rm -rf %s' % REMOTE_SCRIPT)
