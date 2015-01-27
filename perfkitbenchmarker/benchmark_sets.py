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

import logging

from perfkitbenchmarker import benchmarks
from perfkitbenchmarker import flags

FLAGS = flags.FLAGS

MESSAGE = 'message'
BENCHMARK_LIST = 'benchmark_list'
STANDARD_SET = 'standard_set'

BENCHMARK_SETS = {
    STANDARD_SET: {
        MESSAGE: 'This is the standard PKB set.',
        BENCHMARK_LIST: [
            'aerospike', 'bonnie++', 'cassandra_stress',
            'cloud_storage_storage', 'cluster_boot', 'copy_throughput',
            'coremark', 'fio', 'hadoop_terasort', 'hpcc', 'iperf',
            'mesh_network', 'mongodb', 'netperf', 'ping', 'redis',
            'speccpu2006', 'synthetic_storage_workload', 'sysbench_oltp',
            'unixbench']
    }
}


def GetBenchmarksFromFlags():
  """Returns a list of benchmarks to run based on the benchmarks flag.

  If no benchmarks (or sets) are specified, this will return the standard set.
  If multiple sets or mixes of sets and benchmarks are specified, this will
  return the union of all sets and individual benchmarks. This function will
  log a set specific message for all specified sets at the info level.
  """
  benchmark_names = set()
  for benchmark in FLAGS.benchmarks:
    if benchmark in BENCHMARK_SETS:
      benchmark_names |= set(BENCHMARK_SETS[benchmark][BENCHMARK_LIST])
      logging.info(BENCHMARK_SETS[benchmark][MESSAGE])
    else:
      benchmark_names.add(benchmark)
  return [benchmark for benchmark in benchmarks.BENCHMARKS
          if benchmark.GetInfo()['name'] in benchmark_names]
