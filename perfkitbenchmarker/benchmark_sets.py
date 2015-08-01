# Copyright 2014 Google Inc. All rights reserved.
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

"""Benchmark set specific functions and definitions."""

from perfkitbenchmarker import benchmarks
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import flags
from perfkitbenchmarker import windows_benchmarks

FLAGS = flags.FLAGS

MESSAGE = 'message'
BENCHMARK_LIST = 'benchmark_list'
STANDARD_SET = 'standard_set'

BENCHMARK_SETS = {
    STANDARD_SET: {
        MESSAGE: ('The standard_set is a community agreed upon set of '
                  'benchmarks to measure Cloud performance.'),
        BENCHMARK_LIST: [
            'aerospike', 'cassandra_stress', 'object_storage_service',
            'cluster_boot', 'copy_throughput', 'coremark', 'fio',
            'hadoop_terasort', 'hpcc', 'iperf', 'mesh_network', 'mongodb_ycsb',
            'netperf', 'ping', 'redis', 'speccpu2006', 'block_storage_workload',
            'sysbench_oltp', 'unixbench']
    },
    'arm_set': {
        MESSAGE: 'ARM benchmark set.',
        BENCHMARK_LIST: [STANDARD_SET]
    },
    'broadcom_set': {
        MESSAGE: 'Broadcom benchmark set.',
        BENCHMARK_LIST: [STANDARD_SET]
    },
    'canonical_set': {
        MESSAGE: 'Canonical benchmark set.',
        BENCHMARK_LIST: [STANDARD_SET]
    },
    'centurylinkcloud_set': {
        MESSAGE: 'This benchmark set is supported on CenturyLink Cloud.',
        BENCHMARK_LIST: ['hpcc', 'unixbench', 'sysbench_oltp', 'mongodb_ycsb',
                         'mesh_network', 'ping', 'iperf', 'redis',
                         'cassandra_stress', 'copy_throughput']
    },
    'cisco_set': {
        MESSAGE: 'Cisco benchmark set.',
        BENCHMARK_LIST: [STANDARD_SET]
    },
    'cloudharmony_set': {
        MESSAGE: 'CloudHarmony benchmark set.',
        BENCHMARK_LIST: ['speccpu2006', 'unixbench']
    },
    'cloudspectator_set': {
        MESSAGE: 'CloudSpectator benchmark set.',
        BENCHMARK_LIST: [STANDARD_SET]
    },
    'ecocloud_epfl_set': {
        MESSAGE: 'EcoCloud/EPFL benchmark set.',
        BENCHMARK_LIST: [STANDARD_SET]
    },
    'google_set': {
        MESSAGE: ('This benchmark set is maintained by Google Cloud Platform '
                  'Performance Team.'),
        BENCHMARK_LIST: [STANDARD_SET, 'oldisim']
    },
    'intel_set': {
        MESSAGE: 'Intel benchmark set.',
        BENCHMARK_LIST: ['fio', 'iperf', 'unixbench', 'hpcc',
                         'cluster_boot', 'redis', 'cassandra_stress',
                         'object_storage_service', 'sysbench_oltp']
    },
    'mellanox_set': {
        MESSAGE: 'Mellanox benchmark set.',
        BENCHMARK_LIST: [STANDARD_SET]
    },
    'microsoft_set': {
        MESSAGE: 'Microsoft benchmark set.',
        BENCHMARK_LIST: [STANDARD_SET]
    },
    'qualcomm_technologies_set': {
        MESSAGE: 'Qualcomm Technologies, Inc. benchmark set.',
        BENCHMARK_LIST: [STANDARD_SET]
    },
    'rackspace_set': {
        MESSAGE: 'Rackspace benchmark set.',
        BENCHMARK_LIST: ['aerospike', 'cassandra_stress', 'cluster_boot',
                         'copy_throughput', 'fio', 'hpcc', 'iperf',
                         'mesh_network', 'mongodb_ycsb', 'netperf', 'ping',
                         'redis', 'block_storage_workload', 'sysbench_oltp',
                         'unixbench', 'oldisim', 'silo']
    },
    'red_hat_set': {
        MESSAGE: 'Red Hat benchmark set.',
        BENCHMARK_LIST: [STANDARD_SET]
    },
    'tradeworx_set': {
        MESSAGE: 'Tradeworx Inc. benchmark set.',
        BENCHMARK_LIST: [STANDARD_SET]
    },
    'thesys_technologies_set': {
        MESSAGE: 'Thesys Technologies LLC. benchmark set.',
        BENCHMARK_LIST: [STANDARD_SET]
    },
    'stanford_set': {
        MESSAGE: 'Stanford University benchmark set.',
        BENCHMARK_LIST: [STANDARD_SET, 'oldisim']
    },
    'mit_set': {
        MESSAGE: 'Massachusetts Institute of Technology benchmark set.',
        BENCHMARK_LIST: [STANDARD_SET, 'silo']
    }
}


def GetBenchmarksFromFlags():
  """Returns a list of benchmarks to run based on the benchmarks flag.

  If no benchmarks (or sets) are specified, this will return the standard set.
  If multiple sets or mixes of sets and benchmarks are specified, this will
  return the union of all sets and individual benchmarks.
  """
  benchmark_names = set()
  for benchmark in FLAGS.benchmarks:
    if benchmark in BENCHMARK_SETS:
      benchmark_names |= set(BENCHMARK_SETS[benchmark][BENCHMARK_LIST])
    else:
      benchmark_names.add(benchmark)

  # Expand recursive sets
  expanded = set()
  did_expansion = True
  while did_expansion:
    did_expansion = False
    for benchmark_name in benchmark_names:
      if (benchmark_name in BENCHMARK_SETS):
        did_expansion = True
        benchmark_names.remove(benchmark_name)
        if (benchmark_name not in expanded):
            expanded.add(benchmark_name)
            benchmark_names |= set(BENCHMARK_SETS[
                benchmark_name][BENCHMARK_LIST])
        break

  if FLAGS.os_type == benchmark_spec.WINDOWS:
    valid_benchmarks = windows_benchmarks.VALID_BENCHMARKS
  else:
    valid_benchmarks = benchmarks.VALID_BENCHMARKS

  # create a list of modules to return
  benchmark_module_list = []
  for benchmark_name in benchmark_names:
    if benchmark_name in valid_benchmarks:
      benchmark_module_list.append(valid_benchmarks[benchmark_name])
    else:
      raise ValueError('Benchmark "%s" not valid on os_type "%s"' %
                       (benchmark_name, FLAGS.os_type))

  return benchmark_module_list
