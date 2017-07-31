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

"""Benchmark set specific functions and definitions."""

import copy
import itertools

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_benchmarks
from perfkitbenchmarker import os_types
from perfkitbenchmarker import windows_benchmarks

FLAGS = flags.FLAGS

flags.DEFINE_string('flag_matrix', None,
                    'The name of the flag matrix to run.')
flags.DEFINE_string('flag_zip', None,
                    'The name of the flag zip to run.')

MESSAGE = 'message'
BENCHMARK_LIST = 'benchmark_list'
STANDARD_SET = 'standard_set'

BENCHMARK_SETS = {
    STANDARD_SET: {
        MESSAGE: ('The standard_set is a community agreed upon set of '
                  'benchmarks to measure Cloud performance.'),
        BENCHMARK_LIST: [
            'aerospike',
            'block_storage_workload',
            'cassandra_stress',
            'cluster_boot',
            'copy_throughput',
            'coremark',
            'fio',
            'hadoop_terasort',
            'hpcc',
            'iperf',
            'mesh_network',
            'mongodb_ycsb',
            'netperf',
            'object_storage_service',
            'ping',
            'redis',
            'speccpu2006',
            'sysbench_oltp',
            'unixbench',
        ]
    },
    'arm_set': {
        MESSAGE: 'ARM benchmark set.',
        BENCHMARK_LIST: [STANDARD_SET]
    },
    'alicloud_set': {
        MESSAGE: 'AliCloud benchmark set.',
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
        BENCHMARK_LIST: [
            'cassandra_stress',
            'copy_throughput',
            'hpcc',
            'iperf',
            'mesh_network',
            'mongodb_ycsb',
            'ping',
            'redis',
            'sysbench_oltp',
            'unixbench',
        ]
    },
    'cisco_set': {
        MESSAGE: 'Cisco benchmark set.',
        BENCHMARK_LIST: [STANDARD_SET]
    },
    'cloudharmony_set': {
        MESSAGE: 'CloudHarmony benchmark set.',
        BENCHMARK_LIST: [
            'speccpu2006',
            'unixbench',
        ]
    },
    'cloudspectator_set': {
        MESSAGE: 'CloudSpectator benchmark set.',
        BENCHMARK_LIST: [STANDARD_SET]
    },
    'google_set': {
        MESSAGE: ('This benchmark set is maintained by Google Cloud Platform '
                  'Performance Team.'),
        BENCHMARK_LIST: [
            'aerospike_ycsb',
            'bidirectional_network',
            'block_storage_workload',
            'cassandra_stress',
            'cassandra_ycsb',
            'cluster_boot',
            'copy_throughput',
            'fio',
            'gpu_pcie_bandwidth',
            'hadoop_terasort',
            'hpcc',
            'hpcg',
            'iperf',
            'mesh_network',
            'mnist',
            'mongodb_ycsb',
            'multichase',
            'netperf',
            'object_storage_service',
            'oldisim',
            'pgbench',
            'ping',
            'redis_ycsb',
            'stencil2d',
            'speccpu2006',
            'sysbench_oltp',
            'tensorflow',
            'tomcat_wrk',
            'unixbench',
        ]
    },
    'intel_set': {
        MESSAGE: 'Intel benchmark set.',
        BENCHMARK_LIST: [
            'fio',
            'iperf',
            'unixbench',
            'hpcc',
            'cluster_boot',
            'redis',
            'cassandra_stress',
            'object_storage_service',
            'sysbench_oltp',
        ]
    },
    'kubernetes_set': {
        MESSAGE: 'Kubernetes benchmark set.',
        BENCHMARK_LIST: [
            'block_storage_workload',
            'cassandra_ycsb',
            'cassandra_stress',
            'cluster_boot',
            'fio',
            'iperf',
            'mesh_network',
            'mongodb_ycsb',
            'netperf',
            'redis',
            'sysbench_oltp',
        ]
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
        BENCHMARK_LIST: [
            'aerospike',
            'block_storage_workload',
            'cassandra_stress',
            'cluster_boot',
            'copy_throughput',
            'fio',
            'hpcc',
            'iperf',
            'mesh_network',
            'mongodb_ycsb',
            'netperf',
            'oldisim',
            'ping',
            'redis',
            'silo',
            'sysbench_oltp',
            'unixbench',
        ]
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
    },
    'cloudsuite_set': {
        MESSAGE: 'CloudSuite benchmark set.',
        BENCHMARK_LIST: [
            'cloudsuite_data_analytics',
            'cloudsuite_data_caching',
            'cloudsuite_graph_analytics',
            'cloudsuite_in_memory_analytics',
            'cloudsuite_media_streaming',
            'cloudsuite_web_search',
            'cloudsuite_web_serving',
        ]
    }
}


class FlagMatrixNotFoundException(Exception):
  pass


class FlagZipNotFoundException(Exception):
  pass


def _GetValidBenchmarks():
  """Returns a dict mapping valid benchmark names to their modules."""
  if FLAGS.os_type == os_types.WINDOWS:
    return windows_benchmarks.VALID_BENCHMARKS
  return linux_benchmarks.VALID_BENCHMARKS


def _GetBenchmarksFromUserConfig(user_config):
  """Returns a list of benchmark module, config tuples."""
  benchmarks = user_config.get('benchmarks', [])
  valid_benchmarks = _GetValidBenchmarks()
  benchmark_config_list = []

  for entry in benchmarks:
    name, user_config = entry.popitem()
    try:
      benchmark_module = valid_benchmarks[name]
    except KeyError:
      raise ValueError('Benchmark "%s" not valid on os_type "%s"' %
                       (name, FLAGS.os_type))
    benchmark_config_list.append((benchmark_module, user_config))

  return benchmark_config_list


def _GetConfigForAxis(benchmark_config, flag_config):
  config = copy.copy(benchmark_config)
  config_local_flags = config.get('flags', {})
  config['flags'] = copy.deepcopy(configs.GetConfigFlags())
  config['flags'].update(config_local_flags)
  for setting in flag_config:
    config['flags'].update(setting)
  return config


def _AssertZipAxesHaveSameLength(axes):
  expected_length = len(axes[0])
  for axis in axes[1:]:
    if len(axis) != expected_length:
      raise ValueError('flag_zip axes must all be the same length')



def _AssertFlagMatrixAndZipDefsExist(benchmark_config,
                                     flag_matrix_name,
                                     flag_zip_name):
  """Asserts that specified flag_matrix and flag_zip exist.

  Both flag_matrix_name and flag_zip_name can be None, meaning that the user
  (or the benchmark_config) did not specify them.

  Args:
    benchmark_config: benchmark_config
    flag_matrix_name: name of the flag_matrix_def specified by the user via a
      flag, specified in the benchmark_config, or None.
    flag_zip_name: name of the flag_zip_def specified by the user via a flag,
      specified in the benchmark_config, or None.

  Raises:
    FlagMatrixNotFoundException if flag_matrix_name is not None, and is not
      found in the flag_matrix_defs section of the benchmark_config.
    FlagZipNotFoundException if flag_zip_name is not None, and is not
      found in the flag_zip_defs section of the benchmark_config.
  """
  if (flag_matrix_name and
      flag_matrix_name not in
      benchmark_config.get('flag_matrix_defs', {})):
    raise FlagMatrixNotFoundException('No flag_matrix with name {0}'
                                      .format(flag_matrix_name))
  if (flag_zip_name and
      flag_zip_name not in
      benchmark_config.get('flag_zip_defs', {})):
    raise FlagZipNotFoundException('No flag_zip with name {0}'
                                   .format(flag_zip_name))


def GetBenchmarksFromFlags():
  """Returns a list of benchmarks to run based on the benchmarks flag.

  If no benchmarks (or sets) are specified, this will return the standard set.
  If multiple sets or mixes of sets and benchmarks are specified, this will
  return the union of all sets and individual benchmarks.
  """
  user_config = configs.GetUserConfig()
  benchmark_config_list = _GetBenchmarksFromUserConfig(user_config)
  if benchmark_config_list and not FLAGS['benchmarks'].present:
    return benchmark_config_list

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

  valid_benchmarks = _GetValidBenchmarks()

  # create a list of module, config tuples to return
  benchmark_config_list = []
  for benchmark_name in benchmark_names:
    benchmark_config = user_config.get(benchmark_name, {})
    benchmark_name = benchmark_config.get('name', benchmark_name)
    benchmark_module = valid_benchmarks.get(benchmark_name)

    if benchmark_module is None:
      raise ValueError('Benchmark "%s" not valid on os_type "%s"' %
                       (benchmark_name, FLAGS.os_type))

    flag_matrix_name = (
        FLAGS.flag_matrix or benchmark_config.get('flag_matrix', None)
    )
    flag_zip_name = (
        FLAGS.flag_zip or benchmark_config.get('flag_zip', None)
    )
    _AssertFlagMatrixAndZipDefsExist(benchmark_config,
                                     flag_matrix_name,
                                     flag_zip_name)

    # We need to remove the 'flag_matrix', 'flag_matrix_defs', 'flag_zip',
    # 'flag_zip_defs', and 'flag_matrix_filters' keys from the config
    # dictionary since they aren't actually part of the config spec and will
    # cause errors if they are left in.
    benchmark_config.pop('flag_matrix', None)
    benchmark_config.pop('flag_zip', None)

    flag_matrix = benchmark_config.pop(
        'flag_matrix_defs', {}).get(flag_matrix_name, {})
    flag_matrix_filter = benchmark_config.pop(
        'flag_matrix_filters', {}).get(flag_matrix_name, {})
    flag_zip = benchmark_config.pop(
        'flag_zip_defs', {}).get(flag_zip_name, {})

    zipped_axes = []
    crossed_axes = []
    if flag_zip:
      flag_axes = []
      for flag, values in flag_zip.iteritems():
        flag_axes.append([{flag: v} for v in values])

      _AssertZipAxesHaveSameLength(flag_axes)

      for flag_config in itertools.izip(*flag_axes):
        config = _GetConfigForAxis(benchmark_config, flag_config)
        zipped_axes.append((benchmark_module, config))

      crossed_axes.append([benchmark_tuple[1]['flags'] for
                           benchmark_tuple in zipped_axes])

    for flag, values in flag_matrix.iteritems():
      crossed_axes.append([{flag: v} for v in values])

    for flag_config in itertools.product(*crossed_axes):
      config = _GetConfigForAxis(benchmark_config, flag_config)
      if (flag_matrix_filter and not eval(
          flag_matrix_filter, {}, config['flags'])):
          continue
      benchmark_config_list.append((benchmark_module, config))

  return benchmark_config_list
