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
"""Records system performance counters during benchmark runs using dstat.

http://dag.wiee.rs/home-made/dstat/
"""

import functools
import logging
import os
import posixpath
import threading
import uuid

from perfkitbenchmarker import events
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

flags.DEFINE_boolean('dstat', False,
                     'Run dstat (http://dag.wiee.rs/home-made/dstat/) '
                     'on each VM to collect system performance metrics during '
                     'each benchmark run.')
flags.DEFINE_integer('dstat_interval', None,
                     'dstat sample collection frequency, in seconds. Only '
                     'applicable when --dstat is specified.')
flags.DEFINE_string('dstat_output', None,
                    'Output directory for dstat output. '
                    'Only applicable when --dstat is specified. '
                    'Default: run temporary directory.')


class _DStatCollector(object):
  """dstat collector.

  Installs and runs dstat on a collection of VMs.
  """

  def __init__(self, interval=None, output_directory=None):
    """Runs dstat on 'vms'.

    Start dstat collection via `Start`. Stop via `Stop`.

    Args:
      interval: Optional int. Interval in seconds in which to collect samples.
    """
    self.interval = interval
    self.output_directory = output_directory or vm_util.GetTempDir()
    self._lock = threading.Lock()
    self._pids = {}
    self._file_names = {}

    if not os.path.isdir(self.output_directory):
      raise IOError('dstat output directory does not exist: {0}'.format(
          self.output_directory))

  def _StartOnVm(self, vm, suffix='-dstat'):
    vm.Install('dstat')

    num_cpus = vm.num_cpus

    # List block devices so that I/O to each block device can be recorded.
    block_devices, _ = vm.RemoteCommand(
        'lsblk --nodeps --output NAME --noheadings')
    block_devices = block_devices.splitlines()
    dstat_file = posixpath.join(
        vm_util.VM_TMP_DIR, '{0}{1}.csv'.format(vm.name, suffix))
    cmd = ('dstat --epoch -C total,0-{max_cpu} '
           '-D total,{block_devices} '
           '-clrdngyi -pms --fs --ipc --tcp '
           '--udp --raw --socket --unix --vm --rpc '
           '--noheaders --output {output} {dstat_interval} > /dev/null 2>&1 & '
           'echo $!').format(
               max_cpu=num_cpus - 1,
               block_devices=','.join(block_devices),
               output=dstat_file,
               dstat_interval=self.interval or '')
    stdout, _ = vm.RemoteCommand(cmd)
    with self._lock:
      self._pids[vm.name] = stdout.strip()
      self._file_names[vm.name] = dstat_file

  def _StopOnVm(self, vm):
    """Stop dstat on 'vm', copy the results to the run temporary directory."""
    if vm.name not in self._pids:
      logging.warn('No dstat PID for %s', vm.name)
      return
    else:
      with self._lock:
        pid = self._pids.pop(vm.name)
        file_name = self._file_names.pop(vm.name)
    cmd = 'kill {0} || true'.format(pid)
    vm.RemoteCommand(cmd)
    try:
      vm.PullFile(self.output_directory, file_name)
    except Exception:
      logging.exception('Failed fetching dstat result from %s.', vm.name)

  def Start(self, sender, benchmark_spec):
    """Install and start dstat on all VMs in 'benchmark_spec'."""
    suffix = '-{0}-{1}-dstat'.format(benchmark_spec.uid,
                                     str(uuid.uuid4())[:8])
    start_on_vm = functools.partial(self._StartOnVm, suffix=suffix)
    vm_util.RunThreaded(start_on_vm, benchmark_spec.vms)

  def Stop(self, sender, benchmark_spec):
    """Stop dstat on all VMs in 'benchmark_spec', fetch results."""
    vm_util.RunThreaded(self._StopOnVm, benchmark_spec.vms)


def Register(parsed_flags):
  """Registers the dstat collector if FLAGS.dstat is set."""
  if not parsed_flags.dstat:
    return

  output_directory = (parsed_flags.dstat_output
                      if parsed_flags['dstat_output'].present
                      else vm_util.GetTempDir())

  logging.debug('Registering dstat collector with interval %s, output to %s.',
                parsed_flags.dstat_interval, output_directory)

  if not os.path.isdir(output_directory):
    os.makedirs(output_directory)
  collector = _DStatCollector(interval=parsed_flags.dstat_interval,
                              output_directory=output_directory)
  events.before_phase.connect(collector.Start, events.RUN_PHASE, weak=False)
  events.after_phase.connect(collector.Stop, events.RUN_PHASE, weak=False)
