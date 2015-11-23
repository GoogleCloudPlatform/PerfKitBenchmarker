# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs Silo.

Silo is a high performance, scalable in-memory database for modern multicore
machines

Documentation & code: https://github.com/stephentu/silo
"""

import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import silo

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'silo'
BENCHMARK_CONFIG = """
silo:
  description: Runs Silo
  vm_groups:
    default:
      vm_spec: *default_single_core
"""


flags.DEFINE_string('silo_benchmark', 'tpcc',
                    'benchmark to run with silo. Options include tpcc, ycsb,'
                    ' queue, bid')

AGG_THPUT_REGEX = \
    r'(agg_throughput):\s+(\d+\.?\d*e?[+-]?\d*)\s+([a-z/]+)'
PER_CORE_THPUT_REGEX = \
    r'(avg_per_core_throughput):\s+(\d+\.?\d*e?[+-]?\d*)\s+([a-z/]+)'
LAT_REGEX = r'(avg_latency):\s+(\d+\.?\d*e?[+-]?\d*)\s+([a-z]+)'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install Silo on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('Preparing Silo on %s', vm)
  vm.Install('silo')


def ParseResults(results):
  """Result parser for Silo.

  This is what a smaple output looks like:
  --- table statistics ---
  table customer_0 size 30000 (+0 records)
  table customer_name_idx_0 size 30000 (+0 records)
  table district_0 size 10 (+0 records)
  table history_0 size 792182 (+762182 records)
  table item_0 size 100000 (+0 records)
  table new_order_0 size 122238 (+113238 records)
  table oorder_0 size 829578 (+799578 records)
  table oorder_c_id_idx_0 size 829578 (+799578 records)
  table order_line_0 size 8300509 (+8000949 records)
  table stock_0 size 100000 (+0 records)
  table stock_data_0 size 100000 (+0 records)
  table warehouse_0 size 1 (+0 records)
  --- benchmark statistics ---
  runtime: 30.0007 sec
  memory delta: 768.336 MB
  memory delta rate: 25.6106 MB/sec
  logical memory delta: 112.705 MB
  logical memory delta rate: 3.75673 MB/sec
  agg_nosync_throughput: 59150.1 ops/sec
  avg_nosync_per_core_throughput: 59150.1 ops/sec/core
  agg_throughput: 59150.1 ops/sec
  avg_per_core_throughput: 59150.1 ops/sec/core
  agg_persist_throughput: 59150.1 ops/sec
  avg_per_core_persist_throughput: 59150.1 ops/sec/core
  avg_latency: 0.0168378 ms
  avg_persist_latency: 0 ms
  agg_abort_rate: 0 aborts/sec
  avg_per_core_abort_rate: 0 aborts/sec/core
  txn breakdown: [[Delivery, 70967], [NewOrder, 799578], [OrderStatus, 70813],
  [Payment, 762182], [StockLevel, 71006]]
  --- system counters (for benchmark) ---
  --- perf counters (if enabled, for benchmark) ---
  --- allocator stats ---
  [allocator] ncpus=0
  ---------------------------------------
  """
  samples = []

  # agg throughput
  match = regex_util.ExtractAllMatches(AGG_THPUT_REGEX, results)[0]
  samples.append(sample.Sample(
      match[0], float(match[1]), match[2]))

  # per core throughput
  match = regex_util.ExtractAllMatches(PER_CORE_THPUT_REGEX, results)[0]
  samples.append(sample.Sample(
      match[0], float(match[1]), match[2]))

  # avg latency
  match = regex_util.ExtractAllMatches(LAT_REGEX, results)[0]
  samples.append(sample.Sample(
      match[0], float(match[1]), match[2]))

  return samples


def Run(benchmark_spec):
  """Run Silo on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  nthreads = vm.num_cpus
  logging.info('Silo running on %s', vm)
  command = 'cd {0} && '\
            'out-perf.masstree/benchmarks/dbtest '\
            '--bench {1} --num-threads {2} --verbose'.format(
                silo.SILO_DIR,
                FLAGS.silo_benchmark,
                nthreads)
  logging.info('Silo Results:')
  stdout, stderr = vm.RemoteCommand(command, should_log=True)
  return ParseResults(stderr)


def Cleanup(benchmark_spec):
  """Cleanup Silo on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass
