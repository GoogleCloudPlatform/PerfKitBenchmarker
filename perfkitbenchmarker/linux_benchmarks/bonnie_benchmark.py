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

"""Runs plain vanilla bonnie++."""

import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample


BENCHMARK_NAME = 'bonnie++'
BENCHMARK_CONFIG = """
bonnie++:
  description: >
      Runs Bonnie++. Running this benchmark inside
      a container is currently not supported,
      since Docker tries to run it as root, which
      is not recommended.
  vm_groups:
    default:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
"""

LATENCY_REGEX = r'([0-9]*\.?[0-9]+)(\w+)'
# Bonnie++ result fields mapping, see man bon_csv2txt for details.
BONNIE_RESULTS_MAPPING = {
    'format_version': 0,
    'bonnie_version': 1,
    'name': 2,
    'concurrency': 3,
    'seed': 4,
    'file_size': 5,
    'chunk_size': 6,
    'putc': 7,
    'putc_cpu': 8,
    'put_block': 9,
    'put_block_cpu': 10,
    'rewrite': 11,
    'rewrite_cpu': 12,
    'getc': 13,
    'getc_cpu': 14,
    'get_block': 15,
    'get_block_cpu': 16,
    'seeks': 17,
    'seeks_cpu': 18,
    'num_files': 19,
    'max_size': 20,
    'min_size': 21,
    'num_dirs': 22,
    'file_chunk_size': 23,
    'seq_create': 24,
    'seq_create_cpu': 25,
    'seq_stat': 26,
    'seq_stat_cpu': 27,
    'seq_del': 28,
    'seq_del_cpu': 29,
    'ran_create': 30,
    'ran_create_cpu': 31,
    'ran_stat': 32,
    'ran_stat_cpu': 33,
    'ran_del': 34,
    'ran_del_cpu': 35,
    'putc_latency': 36,
    'put_block_latency': 37,
    'rewrite_latency': 38,
    'getc_latency': 39,
    'get_block_latency': 40,
    'seeks_latency': 41,
    'seq_create_latency': 42,
    'seq_stat_latency': 43,
    'seq_del_latency': 44,
    'ran_create_latency': 45,
    'ran_stat_latency': 46,
    'ran_del_latency': 47}


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install Bonnie++ on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('Bonnie++ prepare on %s', vm)
  vm.Install('bonnieplusplus')


def IsValueValid(value):
  """Validate the value.

  An invalid value is either an empty string or a string of multiple '+'.

  Args:
    value: string. The value in raw result.

  Returns:
    A boolean indicates if the value is valid or not.
  """
  if value == '' or '+' in value:
    return False
  return True


def IsCpuField(field):
  """Check if the field is cpu percentage.

  Args:
    field: string. The name of the field.

  Returns:
    A boolean indicates if the field contains keyword 'cpu'.
  """
  return 'cpu' in field


def IsLatencyField(field):
  """Check if the field is latency.

  Args:
    field: string. The name of the field.

  Returns:
    A boolean indicates if the field contains keyword 'latency'.
  """
  return 'latency' in field


def ParseLatencyResult(result):
  """Parse latency result into value and unit.

  Args:
    result: string. Latency value in string format, contains value and unit.
            eg. 200ms

  Returns:
    A tuple of value (float) and unit (string).
  """
  match = regex_util.ExtractAllMatches(LATENCY_REGEX, result)[0]
  return float(match[0]), match[1]


def UpdateMetadata(metadata, key, value):
  """Check if the value is valid, update metadata with the key, value pair.

  Args:
    metadata: dict. A dictionary of sample metadata.
    key: string. Key that will be added into metadata dictionary.
    value: Value that of the key.
  """
  if IsValueValid(value):
    metadata[key] = value


def CreateSamples(results, start_index, end_index, metadata,
                  field_index_mapping):
  """Create samples with data in results from start_index to end_index.

  Args:
    results: A list of string representing bonnie++ results.
    start_index: integer. The start index in results list of the samples.
    end_index: integer. The end index in results list of the samples.
    metadata: dict. A dictionary of metadata added into samples.
    field_index_mapping: dict. A dictionary maps field index to field names.

  Returns:
    A list of sample.Sample instances.
  """
  samples = []
  for field_index in range(start_index, end_index):
    field_name = field_index_mapping[field_index]
    value = results[field_index]
    if not IsValueValid(value):
      continue
    if IsCpuField(field_name):
      unit = '%s'
    elif IsLatencyField(field_name):
      value, unit = ParseLatencyResult(value)
    else:
      unit = 'K/sec'
    samples.append(sample.Sample(field_name, float(value), unit, metadata))
  return samples


def ParseCSVResults(results):
  """Parse csv format bonnie++ results.

  Sample Results:
    1.96,1.96,perfkit-7b22f510-0,1,1421800799,7423M,,,,72853,15,47358,5,,,
    156821,7,537.7,10,100,,,,,49223,58,+++++,+++,54405,53,2898,97,+++++,+++,
    59089,60,,512ms,670ms,,44660us,200ms,3747us,1759us,1643us,33518us,192us,
    839us

  Args:
    results: string. Bonnie++ results.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  field_index_mapping = {}
  for field, value in BONNIE_RESULTS_MAPPING.iteritems():
    field_index_mapping[value] = field
  results = results.split(',')
  assert len(results) == len(BONNIE_RESULTS_MAPPING)
  samples = []
  metadata = {}
  for field_index in range(BONNIE_RESULTS_MAPPING['format_version'],
                           BONNIE_RESULTS_MAPPING['chunk_size'] + 1):
    UpdateMetadata(metadata, field_index_mapping[field_index],
                   results[field_index])

  for field_index in range(BONNIE_RESULTS_MAPPING['num_files'],
                           BONNIE_RESULTS_MAPPING['file_chunk_size'] + 1):
    UpdateMetadata(metadata, field_index_mapping[field_index],
                   results[field_index])
  samples.extend(CreateSamples(results,
                               BONNIE_RESULTS_MAPPING['putc'],
                               BONNIE_RESULTS_MAPPING['num_files'],
                               metadata, field_index_mapping))
  samples.extend(CreateSamples(results,
                               BONNIE_RESULTS_MAPPING['seq_create'],
                               BONNIE_RESULTS_MAPPING['ran_del_latency'] + 1,
                               metadata, field_index_mapping))
  return samples


def Run(benchmark_spec):
  """Run Bonnie++ on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('Bonnie++ running on %s', vm)
  bonnie_command = ('/usr/sbin/bonnie++ -q -d %s -s %d -n 100 -f' %
                    (vm.GetScratchDir(),
                     2 * vm.total_memory_kb / 1024))
  logging.info('Bonnie++ Results:')
  out, _ = vm.RemoteCommand(bonnie_command, should_log=True)
  return ParseCSVResults(out.strip())


def Cleanup(benchmark_spec):
  """Cleanup Bonnie++ on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass
