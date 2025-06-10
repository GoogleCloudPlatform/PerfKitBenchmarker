# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
"""Runs a benchmark from the multichase benchmark suite.

multichase is a pointer chaser benchmark. It measures the average latency of
pointer-chase operations.

multichase codebase: https://github.com/google/multichase
"""

import itertools
import logging
import posixpath
import re
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import units
from perfkitbenchmarker.linux_packages import multichase


_CHASES_WITHOUT_ARGS = (
    'simple',
    'incr',
    't0',
    't1',
    't2',
    'nta',
    'movdqa',
    'movntdqa',
    'parallel2',
    'parallel3',
    'parallel4',
    'parallel5',
    'parallel6',
    'parallel7',
    'parallel8',
    'parallel9',
    'parallel10',
)
_CHASES_WITH_ARGS = 'critword', 'critword2', 'work'

# Dict mapping possible chase types accepted by the multichase -c flag to a
# boolean indicating whether the chase requires an integer argument. For
# example, the 'simple' chase type does not require an argument and is specified
# as `multichase -c simple`, but the 'work' chase type requires an argument and
# is specified as `multichase -c work:N`.
_CHASES = {
    c: c in _CHASES_WITH_ARGS for c in _CHASES_WITHOUT_ARGS + _CHASES_WITH_ARGS
}

_BENCHMARK_SPECIFIC_VM_STATE_ATTR = 'multichase_vm_state'
_NOT_ENOUGH_CPUS = 'error: more threads than cpus available'

BENCHMARK_NAME = 'multichase'
BENCHMARK_CONFIG = """
multichase:
  description: >
      Run a benchmark from the multichase benchmark suite.
  vm_groups:
    default:
      vm_spec: *default_dual_core
  flags:
    enable_transparent_hugepages: True
"""

FLAGS = flags.FLAGS


class _MemorySizeParser(flag_util.UnitsParser):
  syntactic_help = (
      'An explicit memory size that must be convertible to an integer number '
      "of bytes (e.g. '7.5 MiB') or a percentage of the total memory rounded "
      "down to the next integer byte (e.g. '97.5%', which translates to "
      '1046898278 bytes if a total of 1 GiB memory is available).'
  )

  def __init__(self):
    super().__init__(convertible_to=(units.byte, units.percent))

  def parse(self, inp):
    """Parse the input.

    Args:
      inp: string or units.Quantity.

    Returns:
      A units.Quantity.

    Raises:
      ValueError: If the input cannot be parsed, or if it parses to a value that
          does not meet the requirements described in self.syntactic_help.
    """
    size = super().parse(inp)
    if size.units != units.percent:
      size_byte_count = size.to(units.byte).magnitude
      if size_byte_count != int(size_byte_count):
        raise ValueError(
            'Expression {!r} parses to memory size {!r}, which is not '
            'convertible to an integer number of bytes.'.format(inp, str(size))
        )
    return size


_MEMORY_SIZE_PARSER = _MemorySizeParser()
_UNITS_SERIALIZER = flag_util.UnitsSerializer()
_DEFAULT_MEMORY_SIZE_MIN = units.Quantity('1 KiB')
# Multichase will OOM at high thread counts if using much more than 50%.
_DEFAULT_MEMORY_SIZE_MAX = units.Quantity('50%')
_DEFAULT_STRIDE_SIZE = units.Quantity('512 bytes')


def _DefineMemorySizeFlag(name, default, help, flag_values=FLAGS, **kwargs):
  flags.DEFINE(
      _MEMORY_SIZE_PARSER,
      name,
      default,
      help,
      flag_values,
      _UNITS_SERIALIZER,
      **kwargs,
  )


_MULTICHASE = 'multichase'
_MULTILOAD = 'multiload'
MULTICHASE_BENCHMARKS = [_MULTICHASE, _MULTILOAD]
flags.DEFINE_list(
    'multichase_benchmarks',
    MULTICHASE_BENCHMARKS,
    'The multichase benchmark(s) to run.',
)


flags.DEFINE_enum(
    'multichase_chase_type',
    'simple',
    sorted(_CHASES),
    'Chase type to use when executing multichase. Passed to multichase via its '
    '-c flag.',
)
flags.DEFINE_integer(
    'multichase_chase_arg',
    1,
    'Argument to refine the chase type specified with --multichase_chase_type. '
    'Applicable for the following types: {}.'.format(
        ', '.join(_CHASES_WITH_ARGS)
    ),
)
flag_util.DEFINE_integerlist(
    'multichase_thread_count',
    flag_util.IntegerList([1]),
    'Number of threads (one per core), to use when executing multichase. '
    'Passed to multichase via its -t flag.',
    module_name=__name__,
)

# TODO(user): Unitfy multiload_thread_count with multichase_thread_count.
flags.DEFINE_integer(
    'multiload_thread_count',
    None,
    'Number of threads to use when executing multiload. Different from '
    'multichase_thread_count, and is passed to multiload via its -t flag. '
    'When unset use the number of vCPUs on the machine.',
)
flags.DEFINE_integer(
    'multiload_num_samples',
    50,
    'Multiload -n parameter',
)
flags.DEFINE_string(
    'multiload_buffer_size',
    '1G',
    'Multiload -m parameter',
)
flags.DEFINE_integer(
    'multiload_delay',
    1,
    'Multiload -d parameter',
)
flags.DEFINE_string(
    'multiload_traffic_pattern',
    'stream-triad',
    'Multiload -l parameter',
)
_DefineMemorySizeFlag(
    'multichase_memory_size_min',
    _DEFAULT_MEMORY_SIZE_MIN,
    'Memory size to use when executing multichase. Passed to multichase via '
    'its -m flag. If it differs from multichase_memory_size_max, then '
    'multichase is executed multiple times, starting with a memory size equal '
    'to the min and doubling while the memory size does not exceed the max. '
    'Can be specified as a percentage of the total memory on the machine.',
)
_DefineMemorySizeFlag(
    'multichase_memory_size_max',
    _DEFAULT_MEMORY_SIZE_MAX,
    'Memory size to use when executing multichase. Passed to multichase via '
    'its -m flag. If it differs from multichase_memory_size_min, then '
    'multichase is executed multiple times, starting with a memory size equal '
    'to the min and doubling while the memory size does not exceed the max. '
    'Can be specified as a percentage of the total memory on the machine.',
)
_DefineMemorySizeFlag(
    'multichase_stride_size_min',
    _DEFAULT_STRIDE_SIZE,
    'Stride size to use when executing multichase. Passed to multichase via '
    'its -s flag. If it differs from multichase_stride_size_max, then '
    'multichase is executed multiple times, starting with a stride size equal '
    'to the min and doubling while the stride size does not exceed the max. '
    'Can be specified as a percentage of the maximum memory (-m flag) of each '
    'multichase execution.',
)
_DefineMemorySizeFlag(
    'multichase_stride_size_max',
    _DEFAULT_STRIDE_SIZE,
    'Stride size to use when executing multichase. Passed to multichase via '
    'its -s flag. If it differs from multichase_stride_size_min, then '
    'multichase is executed multiple times, starting with a stride size equal '
    'to the min and doubling while the stride size does not exceed the max. '
    'Can be specified as a percentage of the maximum memory (-m flag) of each '
    'multichase execution.',
)
flags.DEFINE_string(
    'multichase_numactl_options',
    '--localalloc',
    'If provided, numactl is used to control memory placement and process '
    'CPU affinity. Examples: "--membind=0" or "--cpunodebind=0".',
)
flags.DEFINE_string(
    'multichase_additional_flags',
    '-T 8m',
    "Additional flags to use when executing multichase. Example: '-O 16 -y'.",
)
flags.DEFINE_bool(
    'skip_default_multiload_run',
    False,
    'If true, skip the default run of multiload with the'
    ' stream-triad-nontemporal-injection-delay traffic pattern.',
)


def _TranslateMemorySize(get_total_memory, size):
  """Translates a value parsed from a memory size flag to a byte count.

  Args:
    get_total_memory: Function that accepts no arguments and returns an integer
      specifying the total amount of memory available in bytes.
    size: units.Quantity specifying either an explicit memory size in a unit
      convertible to bytes or a percentage of the total memory.

  Returns:
    int expressing the specified memory size in bytes.
  """
  if size.units == units.percent:
    return int(get_total_memory() * size.magnitude / 100.0)
  return int(size.to(units.byte).magnitude)


def _IterMemorySizes(get_total_memory, min_size, max_size):
  """Iterates over a range of memory sizes determined by a min and max.

  Args:
    get_total_memory: Function that accepts no arguments and returns an integer
      specifying the total amount of memory available.
    min_size: units.Quantity specifying either an explicit memory size in a unit
      convertible to bytes or a percentage of the total memory.
    max_size: units.Quantity specifying either an explicit memory size in a unit
      convertible to bytes or a percentage of the total memory.

  Yields:
    int expressing memory sizes in bytes. The first yielded value is the
    specified minimum size, each subsequent yielded value is twice the previous,
    and no yielded values are greater than the specified maximum size. If
    max_size specifies a size that is less than min_size, no values are yielded.
  """
  min_bytes = _TranslateMemorySize(get_total_memory, min_size)
  max_bytes = _TranslateMemorySize(get_total_memory, max_size)
  size = min_bytes
  while size <= max_bytes:
    yield size
    size *= 2


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Performs input verification checks.

  Raises:
    ValueError: If an invalid combination of flag values is specified.
  """
  chase_type = FLAGS.multichase_chase_type
  if not _CHASES[chase_type] and FLAGS['multichase_chase_arg'].present:
    raise ValueError(
        'Cannot use --multichase_chase_arg with chase type {0!r}. Chase type '
        '{0!r} does not require an argument.'.format(chase_type)
    )


class _MultichaseSpecificState:
  """State specific to this benchmark that must be preserved between PKB stages.

  An instance of this class is attached to the VM as an attribute and is
  therefore preserved as part of the pickled BenchmarkSpec between PKB stages.

  Attributes:
    dir: Optional string. Path of a directory on the remote machine where files
      files related to this benchmark are stored.
    multichase_dir: Optional string. Path of a directory on the remote machine
      where multichase files are stored. A subdirectory within dir.
  """

  def __init__(self):
    self.dir = None
    self.multichase_dir = None


def Prepare(benchmark_spec):
  """Install multichase on the VM.

  Args:
    benchmark_spec: BenchmarkSpec.
  """
  vm = benchmark_spec.vms[0]
  vm_state = _MultichaseSpecificState()
  setattr(vm, _BENCHMARK_SPECIFIC_VM_STATE_ATTR, vm_state)
  vm.Install('multichase')
  vm.Install('numactl')
  remote_benchmark_dir = '_'.join(('pkb', FLAGS.run_uri, benchmark_spec.uid))
  vm.RemoteCommand('mkdir ' + remote_benchmark_dir)
  vm_state.dir = remote_benchmark_dir
  vm_state.multichase_dir = posixpath.join(vm_state.dir, 'multichase')
  vm.RemoteCommand(
      'cp -ar {} {}'.format(multichase.INSTALL_PATH, vm_state.multichase_dir)
  )


def Run(benchmark_spec):
  """Run multichase and multiload benchmarks on the VM.

  Args:
    benchmark_spec: BenchmarkSpec.

  Returns:
    A list of sample.Sample objects.
  """
  samples = []
  if _MULTICHASE in FLAGS.multichase_benchmarks:
    samples.extend(RunMultichase(benchmark_spec))
  if _MULTILOAD in FLAGS.multichase_benchmarks:
    samples.extend(
        RunMultiload(
            benchmark_spec,
            FLAGS.multiload_thread_count,
            FLAGS.multiload_num_samples,
            FLAGS.multiload_buffer_size,
            FLAGS.multiload_delay,
            FLAGS.multiload_traffic_pattern,
        )
    )
    if not FLAGS.skip_default_multiload_run:
      samples.extend(
          RunMultiload(
              benchmark_spec,
              None,
              50,
              '1G',
              0,
              'stream-triad-nontemporal-injection-delay',
          )
      )
  return samples


def RunMultichase(benchmark_spec):
  """Run multichase on the VM.

  Args:
    benchmark_spec: BenchmarkSpec.

  Returns:
    A list of sample.Sample objects.
  """
  samples = []
  base_metadata = {
      'additional_flags': FLAGS.multichase_additional_flags,
      'chase_type': FLAGS.multichase_chase_type,
      'multichase_version': multichase.GIT_VERSION,
  }
  vm = benchmark_spec.vms[0]
  vm_state = getattr(vm, _BENCHMARK_SPECIFIC_VM_STATE_ATTR)
  max_thread_count = float('inf')

  base_cmd = []
  if FLAGS.multichase_numactl_options:
    base_cmd.extend(('numactl', FLAGS.multichase_numactl_options))
    base_metadata['numactl_options'] = FLAGS.multichase_numactl_options

  multichase_path = posixpath.join(vm_state.multichase_dir, 'multichase')
  base_cmd.extend((multichase_path, '-a', '-v', '-H'))

  chase_type = FLAGS.multichase_chase_type
  if _CHASES[chase_type]:
    chase_type = '{}:{}'.format(chase_type, FLAGS.multichase_chase_arg)
    base_metadata['chase_arg'] = FLAGS.multichase_chase_arg
  base_cmd.extend(('-c', chase_type))

  for thread_count in FLAGS.multichase_thread_count:
    if thread_count > vm.NumCpusForBenchmark():
      continue
    memory_size_iterator = _IterMemorySizes(
        lambda: vm.total_memory_kb * 1024,
        FLAGS.multichase_memory_size_min,
        FLAGS.multichase_memory_size_max,
    )
    for memory_size in memory_size_iterator:
      stride_size_iterator = _IterMemorySizes(
          lambda: memory_size,
          FLAGS.multichase_stride_size_min,
          FLAGS.multichase_stride_size_max,
      )
      for stride_size in stride_size_iterator:
        if thread_count >= max_thread_count:
          continue
        cmd = ' '.join(
            str(s)
            for s in itertools.chain(
                base_cmd,
                (
                    '-m',
                    memory_size,
                    '-s',
                    stride_size,
                    '-t',
                    thread_count,
                    FLAGS.multichase_additional_flags,
                ),
            )
        )

        stdout, stderr, retcode = vm.RemoteCommandWithReturnCode(
            cmd, ignore_failure=True
        )
        if retcode:
          if _NOT_ENOUGH_CPUS in stderr:
            logging.warning(
                'Not enough CPUs to run %s threads. If you have more than that '
                'number of CPUs on the system, it could be due to process '
                'CPU affinity.',
                thread_count,
            )
            max_thread_count = min(max_thread_count, thread_count)
            continue
          else:
            raise errors.VirtualMachine.RemoteCommandError(
                'Multichase failed.\nSTDOUT: %s\nSTDERR: %s' % (stdout, stderr)
            )

        # Latency is printed in ns in the last line.
        latency_ns = float(stdout.split()[-1])

        # Generate one sample from one run of multichase.
        metadata = base_metadata.copy()
        metadata.update({
            'memory_size_bytes': memory_size,
            'stride_size_bytes': stride_size,
            'thread_count': thread_count,
        })
        samples.append(sample.Sample('latency', latency_ns, 'ns', metadata))

  return samples


def RunMultiload(
    benchmark_spec,
    num_threads,
    num_samples,
    buffer_size,
    delay,
    traffic_pattern,
):
  """Run multiload on the VM.

  Args:
    benchmark_spec: BenchmarkSpec.
    num_threads: Number of CPU threads to use for the benchmark. If None, use
      number of CPUs on the machine.
    num_samples: Number of multiload samples to collect.
    buffer_size: Memory buffer size.
    delay: Time delay.
    traffic_pattern: Memory traffic pattern.

  Returns:
    A list of sample.Sample objects.
  """
  samples = []
  vm = benchmark_spec.vms[0]
  vm_state = getattr(vm, _BENCHMARK_SPECIFIC_VM_STATE_ATTR)

  base_cmd = []
  if FLAGS.multichase_numactl_options:
    base_cmd.extend(('numactl', FLAGS.multichase_numactl_options))

  multiload_path = posixpath.join(vm_state.multichase_dir, 'multiload')
  if not num_threads:
    num_threads = vm.NumCpusForBenchmark()

  run_cmd = (
      f'{multiload_path} -t {num_threads} -n {num_samples} -m {buffer_size} -d'
      f' {delay} -l {traffic_pattern}'
  )
  stdout, _ = vm.RemoteCommand(run_cmd)
  lines = stdout.strip().splitlines()
  header = [h.strip() for h in re.split(r',\s*', lines[0])]
  data = [d.strip() for d in re.split(r',\s*', lines[1])]
  result_dict = {k: v for k, v in zip(header, data)}
  load_avg_mibs = int(result_dict['LdAvgMibs'])
  load_max_mibs = int(result_dict['LdMaxMibs'])
  metadata = {
      'multichase_version': multichase.GIT_VERSION,
      'numactl_options': FLAGS.multichase_numactl_options,
      'num_samples': num_samples,
      'buffer_size': buffer_size,
      'delay': delay,
      'traffic_pattern': traffic_pattern,
  }
  metadata.update(result_dict)
  samples.append(
      sample.Sample('LdAvgMibs', load_avg_mibs, 'Mibs', metadata.copy())
  )
  samples.append(
      sample.Sample('LdMaxMibs', load_max_mibs, 'Mibs', metadata.copy())
  )
  return samples


def Cleanup(benchmark_spec):
  """Remove multichase from the VM.

  Args:
    benchmark_spec: BenchmarkSpec.
  """
  vm = benchmark_spec.vms[0]
  vm_state = getattr(vm, _BENCHMARK_SPECIFIC_VM_STATE_ATTR)
  if vm_state.dir:
    # Directory has been created on the VM. Delete it.
    vm.RemoteCommand('rm -rf ' + vm_state.dir)
