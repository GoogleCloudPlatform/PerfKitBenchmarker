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
import posixpath

from perfkitbenchmarker import configs
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import units

from perfkitbenchmarker.linux_packages import multichase


_CHASES_WITHOUT_ARGS = (
    'simple', 'incr', 't0', 't1', 't2', 'nta', 'movdqa', 'movntdqa',
    'parallel2', 'parallel3', 'parallel4', 'parallel5', 'parallel6',
    'parallel7', 'parallel8', 'parallel9', 'parallel10')
_CHASES_WITH_ARGS = 'critword', 'critword2', 'work'

# Dict mapping possible chase types accepted by the multichase -c flag to a
# boolean indicating whether the chase requires an integer argument. For
# example, the 'simple' chase type does not require an argument and is specified
# as `multichase -c simple`, but the 'work' chase type requires an argument and
# is specified as `multichase -c work:N`.
_CHASES = {c: c in _CHASES_WITH_ARGS
           for c in _CHASES_WITHOUT_ARGS + _CHASES_WITH_ARGS}

_BENCHMARK_SPECIFIC_VM_STATE_ATTR = 'multichase_vm_state'

BENCHMARK_NAME = 'multichase'
BENCHMARK_CONFIG = """
multichase:
  description: >
      Run a benchmark from the multichase benchmark suite.
  vm_groups:
    default:
      vm_spec: *default_single_core
"""

FLAGS = flags.FLAGS


class _MemorySizeParser(flag_util.UnitsParser):

  syntactic_help = (
      "An explicit memory size that must be convertible to an integer number "
      "of bytes (e.g. '7.5 MiB') or a percentage of the total memory rounded "
      "down to the next integer byte (e.g. '97.5%', which translates to "
      "1046898278 bytes if a total of 1 GiB memory is available).")

  def __init__(self):
    super(_MemorySizeParser, self).__init__(convertible_to=(units.byte,
                                                            units.percent))

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
    size = super(_MemorySizeParser, self).parse(inp)
    if size.units != units.percent:
      size_byte_count = size.to(units.byte).magnitude
      if size_byte_count != int(size_byte_count):
        raise ValueError(
            'Expression {0!r} parses to memory size {1!r}, which is not '
            'convertible to an integer number of bytes.'.format(inp, str(size)))
    return size


_MEMORY_SIZE_PARSER = _MemorySizeParser()
_UNITS_SERIALIZER = flag_util.UnitsSerializer()
_DEFAULT_MEMORY_SIZE = units.Quantity('256 MiB')
_DEFAULT_STRIDE_SIZE = units.Quantity('256 bytes')


def _DefineMemorySizeFlag(name, default, help, flag_values=FLAGS, **kwargs):
  flags.DEFINE(_MEMORY_SIZE_PARSER, name, default, help, flag_values,
               _UNITS_SERIALIZER, **kwargs)


flags.DEFINE_enum(
    'multichase_chase_type', 'simple', sorted(_CHASES),
    'Chase type to use when executing multichase. Passed to multichase via its '
    '-c flag.')
flags.DEFINE_integer(
    'multichase_chase_arg', 1,
    'Argument to refine the chase type specified with --multichase_chase_type. '
    'Applicable for the following types: {0}.'.format(', '.join(
        _CHASES_WITH_ARGS)))
flag_util.DEFINE_integerlist(
    'multichase_thread_count', flag_util.IntegerList([1]),
    'Number of threads (one per core), to use when executing multichase. '
    'Passed to multichase via its -t flag.')
_DefineMemorySizeFlag(
    'multichase_memory_size_min', _DEFAULT_MEMORY_SIZE,
    'Memory size to use when executing multichase. Passed to multichase via '
    'its -m flag. If it differs from multichase_memory_size_max, then '
    'multichase is executed multiple times, starting with a memory size equal '
    'to the min and doubling while the memory size does not exceed the max. '
    'Can be specified as a percentage of the total memory on the machine.')
_DefineMemorySizeFlag(
    'multichase_memory_size_max', _DEFAULT_MEMORY_SIZE,
    'Memory size to use when executing multichase. Passed to multichase via '
    'its -m flag. If it differs from multichase_memory_size_min, then '
    'multichase is executed multiple times, starting with a memory size equal '
    'to the min and doubling while the memory size does not exceed the max. '
    'Can be specified as a percentage of the total memory on the machine.')
_DefineMemorySizeFlag(
    'multichase_stride_size_min', _DEFAULT_STRIDE_SIZE,
    'Stride size to use when executing multichase. Passed to multichase via '
    'its -s flag. If it differs from multichase_stride_size_max, then '
    'multichase is executed multiple times, starting with a stride size equal '
    'to the min and doubling while the stride size does not exceed the max. '
    'Can be specified as a percentage of the maximum memory (-m flag) of each '
    'multichase execution.')
_DefineMemorySizeFlag(
    'multichase_stride_size_max', _DEFAULT_STRIDE_SIZE,
    'Stride size to use when executing multichase. Passed to multichase via '
    'its -s flag. If it differs from multichase_stride_size_min, then '
    'multichase is executed multiple times, starting with a stride size equal '
    'to the min and doubling while the stride size does not exceed the max. '
    'Can be specified as a percentage of the maximum memory (-m flag) of each '
    'multichase execution.')
flags.DEFINE_string(
    'multichase_taskset_options', None,
    "If provided, taskset is used to limit the cores available to multichase. "
    "The value of this flag contains the options to provide to taskset. "
    "Examples: '0x00001FE5' or '-c 0,2,5-12'.")
flags.DEFINE_string(
    'multichase_additional_flags', '',
    "Additional flags to use when executing multichase. Example: '-O 16 -y'.")


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
    return int(get_total_memory() * size.magnitude / 100.)
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
        '{0!r} does not require an argument.'.format(chase_type))


class _MultichaseSpecificState(object):
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
  remote_benchmark_dir = '_'.join(('pkb', FLAGS.run_uri, benchmark_spec.uid))
  vm.RemoteCommand('mkdir ' + remote_benchmark_dir)
  vm_state.dir = remote_benchmark_dir
  vm_state.multichase_dir = posixpath.join(vm_state.dir, 'multichase')
  vm.RemoteCommand('cp -ar {0} {1}'.format(
                   multichase.INSTALL_PATH, vm_state.multichase_dir))


def Run(benchmark_spec):
  """Run multichase on the VM.

  Args:
    benchmark_spec: BenchmarkSpec.

  Returns:
    A list of sample.Sample objects.
  """
  samples = []
  base_metadata = {'additional_flags': FLAGS.multichase_additional_flags,
                   'chase_type': FLAGS.multichase_chase_type,
                   'multichase_version': multichase.GIT_VERSION}
  vm = benchmark_spec.vms[0]
  vm_state = getattr(vm, _BENCHMARK_SPECIFIC_VM_STATE_ATTR)

  base_cmd = []
  if FLAGS.multichase_taskset_options:
    base_cmd.extend(('taskset', FLAGS.multichase_taskset_options))
    base_metadata['taskset_options'] = FLAGS.multichase_taskset_options

  multichase_path = posixpath.join(vm_state.multichase_dir, 'multichase')
  base_cmd.extend((multichase_path, '-a', '-v'))

  chase_type = FLAGS.multichase_chase_type
  if _CHASES[chase_type]:
    chase_type = '{0}:{1}'.format(chase_type, FLAGS.multichase_chase_arg)
    base_metadata['chase_arg'] = FLAGS.multichase_chase_arg
  base_cmd.extend(('-c', chase_type))

  for thread_count in FLAGS.multichase_thread_count:
    if thread_count > vm.num_cpus:
      break
    memory_size_iterator = _IterMemorySizes(
        lambda: vm.total_memory_kb * 1024, FLAGS.multichase_memory_size_min,
        FLAGS.multichase_memory_size_max)
    for memory_size in memory_size_iterator:
      stride_size_iterator = _IterMemorySizes(
          lambda: memory_size, FLAGS.multichase_stride_size_min,
          FLAGS.multichase_stride_size_max)
      for stride_size in stride_size_iterator:
        cmd = ' '.join(str(s) for s in itertools.chain(base_cmd, (
            '-m', memory_size, '-s', stride_size, '-t', thread_count,
            FLAGS.multichase_additional_flags)))
        stdout, _ = vm.RemoteCommand(cmd)

        # Latency is printed in ns in the last line.
        latency_ns = float(stdout.split()[-1])

        # Generate one sample from one run of multichase.
        metadata = base_metadata.copy()
        metadata.update({'memory_size_bytes': memory_size,
                         'stride_size_bytes': stride_size,
                         'thread_count': thread_count})
        samples.append(sample.Sample('latency', latency_ns, 'ns', metadata))

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
