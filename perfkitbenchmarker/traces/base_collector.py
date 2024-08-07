# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing abstract classes related to collectors."""


import abc
import functools
import logging
import os
import posixpath
import threading
import time
import uuid
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import errors
from perfkitbenchmarker import events
from perfkitbenchmarker import os_types
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

_TRACE_VM_GROUPS = flags.DEFINE_list(
    'trace_vm_groups',
    None,
    (
        'Run traces on all vms in the vm groups. By default traces runs on all'
        ' VMs, for client and server architecture, specify trace vm groups to'
        ' servers to only collect metrics on server vms.'
    ),
)

_TRACE_PRIMARY_SERVER = flags.DEFINE_bool(
    'trace_primary_server',
    False,
    (
        'Run traces only on a single vm.  Use with trace_vm_groups'
        ' to only collect metrics on a single primary server vm.'
    ),
)


def Register(parsed_flags):
  """Registers the collector if FLAGS.<collector> is set.

  See dstat.py for an example on how to register a collector.

  Args:
    parsed_flags: argument passed into each call to Register()
  """
  del parsed_flags  # unused


class BaseCollector:
  """Object representing a Base Collector.

  A Collector is a utility that is ran alongside benchmarks to record stats
  at various points when running a benchmark. A Base collector is an abstract
  class with common routines that derived collectors use.
  """

  def __init__(self, interval=None, output_directory=None):
    """Runs collector on 'vms'.

    Start collector collection via `Start`. Stop via `Stop`.

    Args:
      interval: Optional int. Interval in seconds in which to collect samples.
      output_directory: Optional directory where to save collection output.

    Raises:
      IOError: for when the output directory doesn't exist.
    """
    self.interval = interval
    self.output_directory = output_directory or vm_util.GetTempDir()
    self._lock = threading.Lock()
    self._pid_files = {}
    self._role_mapping = {}  # mapping vm role to output file
    self._start_time = 0
    self.vm_groups = {}

    if not os.path.isdir(self.output_directory):
      raise OSError(
          'collector output directory does not exist: {}'.format(
              self.output_directory
          )
      )

  @abc.abstractmethod
  def _CollectorName(self):
    pass

  @abc.abstractmethod
  def _InstallCollector(self, vm):
    pass

  @abc.abstractmethod
  def _CollectorRunCommand(self, vm, collector_file):
    pass

  def _KillCommand(self, vm, pid):
    """Command to kill off the collector."""
    if vm.BASE_OS_TYPE == os_types.WINDOWS:
      return 'taskkill /PID {} /F'.format(pid)
    else:
      return 'kill -INT {}'.format(pid)

  def _StartOnVm(self, vm, suffix=''):
    """Start collector, having it write to an output file."""
    self._InstallCollector(vm)
    suffix = '{}-{}'.format(suffix, self._CollectorName())
    if vm.BASE_OS_TYPE == os_types.WINDOWS:
      collector_file = os.path.join(
          vm.temp_dir, '{}{}.log'.format(vm.name, suffix)
      )
    else:
      collector_file = posixpath.join(
          vm_util.VM_TMP_DIR, '{}{}.log'.format(vm.name, suffix)
      )

    cmd = self._CollectorRunCommand(vm, collector_file)

    stdout, _ = vm.RemoteCommand(cmd)
    with self._lock:
      self._pid_files[vm.name] = (stdout.strip(), collector_file)

  def _StopOnVm(self, vm, vm_role):
    """Stop collector on 'vm' and copy the files back."""
    if vm.name not in self._pid_files:
      logging.warning('No collector PID for %s', vm.name)
      return
    else:
      with self._lock:
        pid, file_name = self._pid_files.pop(vm.name)
    vm.RemoteCommand(self._KillCommand(vm, pid), ignore_failure=True)

    try:
      vm.PullFile(
          os.path.join(self.output_directory, os.path.basename(file_name)),
          file_name,
      )
      self._role_mapping[vm_role] = file_name
    except errors.VirtualMachine.RemoteCommandError as ex:
      logging.exception('Failed fetching collector result from %s.', vm.name)
      raise ex

  def Start(self, sender, benchmark_spec):
    """Install and start collector on VMs specified in trace vm groups'."""
    suffix = '-{}-{}'.format(benchmark_spec.uid, str(uuid.uuid4())[:8])
    if _TRACE_VM_GROUPS.value:
      for vm_group in _TRACE_VM_GROUPS.value:
        if vm_group in benchmark_spec.vm_groups:
          self.vm_groups[vm_group] = benchmark_spec.vm_groups[vm_group]
        else:
          logging.warning('TRACE_VM_GROUPS % does not exists.', vm_group)
    else:
      self.vm_groups = benchmark_spec.vm_groups
    vms = sum(self.vm_groups.values(), [])

    if _TRACE_PRIMARY_SERVER.value:
      vms = vms[:1]

    self.StartOnVms(sender, vms, suffix)

  def StartOnVms(self, sender, vms, id_suffix):
    """Install and start collector on given subset of vms.

    Args:
      sender: sender of the request/event to start collector.
      vms: vms to run the collector on.
      id_suffix: id_suffix of the collector output file.
    """
    del sender  # unused
    func = functools.partial(self._StartOnVm, suffix=id_suffix)
    background_tasks.RunThreaded(func, vms)
    self._start_time = time.time()
    return

  def Stop(self, sender, benchmark_spec, name=''):  # pylint: disable=unused-argument
    """Stop collector on VMs."""
    self.StopOnVms(sender, name)

  def StopOnVms(self, sender, name):
    """Stop collector on given subset of vms, fetch results.

    Args:
      sender: sender of the event to stop the collector.
      name: name of event to be stopped.
    """
    events.record_event.send(
        sender,
        event=name,
        start_timestamp=self._start_time,
        end_timestamp=time.time(),
        metadata={},
    )
    args = []
    for role, vms in self.vm_groups.items():
      args.extend(
          [((vm, '%s_%s' % (role, idx)), {}) for idx, vm in enumerate(vms)]
      )
    background_tasks.RunThreaded(self._StopOnVm, args)
    return

  @abc.abstractmethod
  def Analyze(self, sender, benchmark_spec, samples):
    """Analyze collector file and record samples."""
    pass
