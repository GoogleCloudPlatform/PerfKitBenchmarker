# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Run LMbench.

Suite of simple, portable benchmarks. Compares different systems performance.
Homepage: http://www.bitmover.com/lmbench/index.html
"""

import itertools
import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import lmbench

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'lmbench'
BENCHMARK_CONFIG = """
lmbench:
  description: Runs Lmbench Microbenchmark.
  vm_groups:
    default:
      vm_spec: *default_dual_core
      vm_count: null
"""

_LMBENCH_HARDWARE_DEFAULT = 'NO'

flags.DEFINE_string(
    'lmbench_mem_size', None,
    'The range of memory on which several benchmarks operate. If not provided, '
    'the memory size should be 8MB as default'
)
flags.DEFINE_enum(
    'lmbench_hardware', _LMBENCH_HARDWARE_DEFAULT, ['YES', 'NO'],
    'The decision to run BENCHMARK_HARDWARE tests: YES or NO. The default is NO'
)


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _PrepareLmbench(vm):
  """Builds Lmbench on a single vm."""
  logging.info('Installing Lmbench on %s', vm)
  vm.Install('lmbench')


def _ConfigureRun(vm):
  """Configure Lmbench tests."""
  logging.info('Set Lmbench run parameters')
  vm.RemoteCommand('cd {0} && mkdir bin && cd bin && '
                   'mkdir x86_64-linux-gnu'.format(lmbench.LMBENCH_DIR))
  vm.RobustRemoteCommand(
      'cd {0} && cd scripts && echo "1" >>input.txt && '
      'echo "1" >>input.txt && ./config-run <input.txt'.format(
          lmbench.LMBENCH_DIR))
  sed_cmd = (
      'sed -i -e "s/OUTPUT=\\/dev\\/tty/OUTPUT=\\/dev\\/null/" '
      '{0}/bin/x86_64-linux-gnu/CONFIG.*'.format(lmbench.LMBENCH_DIR))
  vm.RemoteCommand(sed_cmd)

  if FLAGS.lmbench_mem_size:
    sed_cmd = (
        'sed -i -e "s/MB=/MB={0}/" {1}/bin/x86_64-linux-gnu/CONFIG.*'.format(
            FLAGS.lmbench_mem_size, lmbench.LMBENCH_DIR))
    vm.RemoteCommand(sed_cmd)

  if FLAGS.lmbench_hardware == _LMBENCH_HARDWARE_DEFAULT:
    sed_cmd = (
        'sed -i -e "s/BENCHMARK_HARDWARE=YES/BENCHMARK_HARDWARE={0}/" '
        '{1}/bin/x86_64-linux-gnu/CONFIG.*'.format(
            FLAGS.lmbench_hardware, lmbench.LMBENCH_DIR))
    vm.RemoteCommand(sed_cmd)


def Prepare(benchmark_spec):
  """Install Lmbench on the target vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  _PrepareLmbench(vm)
  _ConfigureRun(vm)


def _ParseContextSwitching(lines, title, metadata, results):
  """Parse the context switching test results.

  Context switching - times in microseconds - smaller is better.

  Args:
    lines: The lines following context switching title size=* ovr=*.
    title: The context switching subset title.
    metadata: A diction of metadata.
    results: A list of samples to be published.
  Context switching test results:
    "size=0k ovr=0.93
      2 11.57
      4 14.81
      8 14.69
      16 12.86
      24 12.50
      32 12.63
      64 13.30
      96 11.45
  """
  size = regex_util.ExtractGroup('"size=([0-9]*)', title)
  ovr = regex_util.ExtractGroup('"size=.* ovr=([0-9]*\\.[0-9]*)', title)

  metadata_clone = metadata.copy()
  metadata_clone['memory_size'] = '%sk' % size
  metadata_clone['ovr'] = ovr
  for line in lines:
    metric_value = line.split()
    current_metadata = metadata_clone.copy()
    current_metadata['num_of_processes'] = int(metric_value[0])
    results.append(sample.Sample('context_switching_time',
                                 float(metric_value[1]),
                                 'microseconds', current_metadata))


def _UpdataMetadata(lmbench_output, metadata):
  metadata['MB'] = regex_util.ExtractGroup('MB: ([0-9]*)', lmbench_output)
  metadata['BENCHMARK_HARDWARE'] = regex_util.ExtractGroup(
      'BENCHMARK_HARDWARE: (YES|NO)', lmbench_output)
  metadata['BENCHMARK_OS'] = regex_util.ExtractGroup(
      'BENCHMARK_OS: (YES|NO)', lmbench_output)


def _ParseSections(lmbench_output, parse_section_func_dict, metadata, results):
  """Parse some sections from the output.

  For different output sections, we may apply different parsing strategies.
  Such that parse_section_func_dict is defined where the key is the section
  name, the value is the corresponding function. All the associated parsing
  functions take same parameters. So When we parse more
  sections, simply define new parsing functions if needed.

  Args:
    lmbench_output: A string containing the test results of lmbench.
    parse_section_func_dict: A dictionary where the key is the section name,
                             the value is the corresponding function name to
                             parse that section.
    metadata: A dictionary of metadata.
    results: A list of samples to be published.
  """

  lines_iter = iter(lmbench_output.split('\n'))
  stop_parsing = ''
  while True:
    lines_iter = itertools.dropwhile(
        lambda line: line not in parse_section_func_dict, lines_iter)
    title = next(lines_iter, None)
    if title is None:
      break
    function = parse_section_func_dict[title]
    reading_buffer = [
        item for item in itertools.takewhile(lambda line: line != stop_parsing,
                                             lines_iter)
    ]
    function(reading_buffer, title, metadata, results)


def _AddProcessorMetricSamples(lmbench_output, processor_metric_list, metadata,
                               results):
  """Parse results for "Processor, Processes - times in microseconds - smaller is better."

  Args:
    lmbench_output: A string containing the test results of lmbench.
    processor_metric_list: A tuple of metrics.
    metadata: A dictionary of metadata.
    results: A list of samples to be published.
  Processor test output:
    Simple syscall: 0.2345 microseconds
    Simple read: 0.3515 microseconds
    Simple write: 0.3082 microseconds
    Simple stat: 0.6888 microseconds
    Simple fstat: 0.3669 microseconds
    Simple open/close: 1.5541 microseconds
    Select on 10 fd's: 0.4464 microseconds
    Select on 100 fd's: 1.0422 microseconds
    Select on 250 fd's: 2.0069 microseconds
    Select on 500 fd's: 3.7366 microseconds
    Select on 10 tcp fd's: 0.5690 microseconds
    Select on 100 tcp fd's: 6.4521 microseconds
    Select on 250 tcp fd's: 16.7513 microseconds
    Select on 500 tcp fd's: 32.8527 microseconds
    Signal handler installation: 0.3226 microseconds
    Signal handler overhead: 1.1736 microseconds
    Protection fault: 0.7491 microseconds
    Pipe latency: 25.5437 microseconds
    AF_UNIX sock stream latency: 25.2813 microseconds
    Process fork+exit: 121.7399 microseconds
    Process fork+execve: 318.6445 microseconds
    Process fork+/bin/sh -c: 800.2188 microseconds
    Pagefaults on /var/tmp/XXX: 0.1639 microseconds
  """

  for metric in processor_metric_list:
    regex = '%s: (.*)' % metric
    value_unit = regex_util.ExtractGroup(regex, lmbench_output)
    [value, unit] = value_unit.split(' ')
    results.append(
        sample.Sample('%s' % metric.replace('\\', ''), float(value), unit,
                      metadata))


def _ParseOutput(lmbench_output):
  """Parse the output from lmbench.

  Args:
    lmbench_output: A string containing the test results of lmbench.
  Returns:
    A list of samples to be published (in the same format as Run() returns).
  """
  results = []
  metadata = dict()

  # Updata metadata
  _UpdataMetadata(lmbench_output, metadata)

  # Parse results for "Processor, Processes - times in microseconds - smaller is better"
  # TODO(yanfeiren): Parse more metric for processor section.
  processor_metric_list = ('syscall', 'read', 'write', 'stat', 'fstat',
                           'open/close', 'Signal handler installation',
                           'Signal handler overhead', 'Protection fault',
                           'Pipe latency', r'Process fork\+exit',
                           r'Process fork\+execve', r'Process fork\+/bin/sh -c',
                           'Pagefaults on /var/tmp/XXX')
  _AddProcessorMetricSamples(lmbench_output, processor_metric_list, metadata,
                             results)

  # Parse some sections from the output.
  parse_section_func_dict = {}
  contex_switching_titles = regex_util.ExtractAllMatches('"size=.* ovr=.*',
                                                         lmbench_output)
  for title in contex_switching_titles:
    parse_section_func_dict[title] = _ParseContextSwitching

  _ParseSections(lmbench_output, parse_section_func_dict, metadata, results)

  return results


def Run(benchmark_spec):
  """Run LMBENCH on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  vm = vms[0]

  # Use the current configuration to run the benchmark tests.
  vm.RobustRemoteCommand(
      'cd {0} && sudo make rerun'.format(lmbench.LMBENCH_DIR))

  stdout, _ = vm.RobustRemoteCommand(
      'cd {0} && cd results/x86_64-linux-gnu && cat *.*'.format(
          lmbench.LMBENCH_DIR), should_log=True)

  return _ParseOutput(stdout)


def Cleanup(unused_benchmark_spec):
  pass
