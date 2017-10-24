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

"""Generates bidirectional network load using netperf.

docs:
https://hewlettpackard.github.io/netperf/doc/netperf.html

Runs TCP_STREAM and TCP_MAERTS benchmark from netperf between several machines
to fully saturate the NIC on the primary vm.
"""

import csv
import io
import json
import logging
import os
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import netperf

flags.DEFINE_list('bidirectional_network_tests',
                  ['TCP_STREAM', 'TCP_MAERTS', 'TCP_MAERTS'],
                  'The network tests to run.')
flags.register_validator(
    'bidirectional_network_tests',
    lambda benchmarks: benchmarks and set(benchmarks).issubset(ALL_TESTS))

flags.DEFINE_integer('bidirectional_network_test_length', 60,
                     'bidirectional_network test length, in seconds',
                     lower_bound=1)
flags.DEFINE_integer('bidirectional_stream_num_streams', 8,
                     'Number of netperf processes to run.',
                     lower_bound=1)

ALL_TESTS = ['TCP_STREAM', 'TCP_MAERTS']

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'bidirectional_network'
BENCHMARK_CONFIG = """
bidirectional_network:
  description: Run multiple network tests
  flags:
    netperf_enable_histograms: false
  vm_groups:
    primary:
      vm_spec: *default_single_core
    secondary:
      vm_spec: *default_single_core
"""


MBPS = 'Mbits/sec'
SEC = 'seconds'

# Command ports are even (id*2), data ports are odd (id*2 + 1)
PORT_START = 20000

REMOTE_SCRIPTS_DIR = 'netperf_test_scripts'
REMOTE_SCRIPT = 'netperf_test.py'


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

  # each network test has its own secondary vm to ensure the bottleneck is
  # the primary vm
  config['vm_groups']['secondary']['vm_count'] = len(
      FLAGS.bidirectional_network_tests)
  return config


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
  vm_util.RunThreaded(PrepareNetperf, vms)

  num_streams = FLAGS.bidirectional_stream_num_streams

  # Start the netserver processes
  for vm in vms[1:]:
    netserver_cmd = ('for i in $(seq {port_start} 2 {port_end}); do '
                     '{netserver_path} -p $i & done').format(
                         port_start=PORT_START,
                         port_end=PORT_START + num_streams * 2 - 1,
                         netserver_path=netperf.NETSERVER_PATH)
    vm.RemoteCommand(netserver_cmd)

  # Copy remote test script to client
  path = data.ResourcePath(os.path.join(REMOTE_SCRIPTS_DIR, REMOTE_SCRIPT))
  logging.info('Uploading %s to %s', path, vms[0])
  vms[0].PushFile(path)
  vms[0].RemoteCommand('sudo chmod 777 %s' % REMOTE_SCRIPT)


def _ParseNetperfOutput(stdout, metadata, benchmark_name, iteration_id):
  """Parses the stdout of a single netperf process.

  Args:
    stdout: the stdout of the netperf process
    metadata: metadata for any sample.Sample objects we create
    benchmark_name: name of the netperf benchmark
    iteration_id: the unique id of the benchmark within the bidirectional
        network benchmark

  Returns:
    A tuple containing throughput_sample

  Raises:
    ValueError: when netperf output is not recognized.
  """
  # Don't modify the metadata dict that was passed in
  metadata = metadata.copy()

  # Extract stats from stdout
  # Sample output:
  # MIGRATED TCP STREAM TEST from 0.0.0.0 (0.0.0.0) port 0 AF_INET to \
  # 10.240.0.61 () port 20005 AF_INET : histogram
  # Throughput,Throughput Units,50th Percentile Latency Microseconds,\
  # 90th Percentile Latency Microseconds,99th Percentile Latency \
  # Microseconds,Stddev Latency Microseconds,Minimum Latency \
  # Microseconds,Maximum Latency Microseconds,Confidence Iterations \
  # Run,Throughput Confidence Width (%)
  # 408.78,10^6bits/s,3,4,5793,3235.53,0,61003,1,-1.000
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
    # TCP_STREAM, TCP_MAERTS benchmarks
    unit = MBPS
    metric = '%s_%s_Throughput' % (iteration_id, benchmark_name)
  else:
    raise ValueError('Netperf output specifies unrecognized throughput units %s'
                     % throughput_units)
  throughput_sample = sample.Sample(metric, throughput, unit, metadata)

  return throughput_sample


def RunNetperf(primary_vm, secondary_vm, benchmark_name, num_streams,
               iteration, results):
  """Spawns netperf on a remote VM, parses results.

  Args:
    primary_vm: The VM that the netperf benchmark will be run upon.
    secondary_vm: The VM that the netperf server is running on.
    benchmark_name: The netperf benchmark to run, see the documentation.
    num_streams: The number of netperf client threads to run.
    iteration: The iteration to prefix the metrics with, as well as the index
      into the array where the results will be stored.
    results: The results variable shared by all threads.  The iteration-th
      element holds a tuple of
      (Samples[], begin_starting_processes, end_starting_processes)
  """
  # Flags:
  # -o specifies keys to include in CSV output.
  # -j keeps additional latency numbers
  netperf_cmd = ('{netperf_path} -p {{command_port}} -j '
                 '-t {benchmark_name} -H {server_ip} -l {length}'
                 ' -- '
                 '-P ,{{data_port}} '
                 '-o THROUGHPUT,THROUGHPUT_UNITS,P50_LATENCY,P90_LATENCY,'
                 'P99_LATENCY,STDDEV_LATENCY,'
                 'MIN_LATENCY,MAX_LATENCY,'
                 'CONFIDENCE_ITERATION,THROUGHPUT_CONFID').format(
                     netperf_path=netperf.NETPERF_PATH,
                     benchmark_name=benchmark_name,
                     server_ip=secondary_vm.internal_ip,
                     length=FLAGS.bidirectional_network_test_length)

  # Run all of the netperf processes and collect their stdout

  # Give the remote script the test length plus 5 minutes to complete
  remote_cmd_timeout = FLAGS.bidirectional_network_test_length + 300
  remote_cmd = ('./%s --netperf_cmd="%s" --num_streams=%s --port_start=%s' %
                (REMOTE_SCRIPT, netperf_cmd, num_streams, PORT_START))
  remote_stdout, _ = primary_vm.RemoteCommand(remote_cmd,
                                              timeout=remote_cmd_timeout)

  # Decode the remote command's stdout which the stdouts, stderrs and return
  # code from each sub invocation of netperf (per stream)
  json_out = json.loads(remote_stdout)
  stdouts = json_out[0]
  # unused_stderrs = json_out[1]
  # unused_return_codes = json_out[2]
  begin_starting_processes = json_out[3]
  end_starting_processes = json_out[4]

  local_results = []

  # Metadata to attach to samples
  metadata = {
      'bidirectional_network_test_length':
          FLAGS.bidirectional_network_test_length,
      'bidirectional_stream_num_streams': num_streams,
      'ip_type': 'internal',
      'primary_machine_type': primary_vm.machine_type,
      'primary_zone': primary_vm.zone,
      'secondary_machine_type': secondary_vm.machine_type,
      'secondary_zone': secondary_vm.zone,
  }

  stream_start_delta = end_starting_processes - begin_starting_processes
  local_results.append(sample.Sample('%s_%s_start_delta' %
                                     (iteration, benchmark_name),
                                     float(stream_start_delta),
                                     SEC, metadata))

  throughput_samples = [_ParseNetperfOutput(stdout, metadata, benchmark_name,
                                            iteration)
                        for stdout in stdouts]

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
    local_results.append(
        sample.Sample('%s_%s_Throughput_%s' %
                      (iteration, benchmark_name, stat),
                      float(value), throughput_unit, metadata))
  results[iteration] = (local_results,
                        begin_starting_processes, end_starting_processes)


def Run(benchmark_spec):
  """Run the specified networking tests.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  num_tests = len(FLAGS.bidirectional_network_tests)
  num_streams = FLAGS.bidirectional_stream_num_streams
  test_results = [None] * num_tests

  args = [((vms[0], vms[i + 1], FLAGS.bidirectional_network_tests[i],
            num_streams, i, test_results), {}) for i in xrange(num_tests)]

  vm_util.RunThreaded(RunNetperf, args, num_tests)

  total_inbound = 0
  total_outbound = 0

  # Extract results from each netperf test
  results = [item for sublist in test_results for item in sublist[0]]

  # Compute aggregate metrics
  for r in results:
    if r.metric.endswith('TCP_STREAM_Throughput_total'):
      total_outbound += r.value
    elif r.metric.endswith('TCP_MAERTS_Throughput_total'):
      total_inbound += r.value

  primary_vm = vms[0]  # client in netperf terms
  metadata = {
      'primary_machine_type': primary_vm.machine_type,
      'primary_zone': primary_vm.zone,
      'ip_type': 'internal',
      'bidirectional_network_tests':
      ','.join(FLAGS.bidirectional_network_tests),
      'bidirectional_network_test_length':
      FLAGS.bidirectional_network_test_length,
      'bidirectional_stream_num_streams': num_streams,
  }

  if total_outbound > 0:
    results.append(sample.Sample('outbound_network_total', total_outbound,
                                 MBPS, metadata))
  if total_inbound > 0:
    results.append(sample.Sample('inbound_network_total', total_inbound,
                                 MBPS, metadata))

  start_times = [r[1] for r in test_results]
  end_times = [r[2] for r in test_results]
  results.append(sample.Sample('all_streams_start_delta',
                               max(end_times) - min(start_times),
                               SEC, metadata))
  return results


def Cleanup(benchmark_spec):
  """Cleanup netperf on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  for vm in vms[1:]:
    vm.RemoteCommand('sudo killall netserver')
  vms[0].RemoteCommand('sudo rm -rf %s' % REMOTE_SCRIPT)
