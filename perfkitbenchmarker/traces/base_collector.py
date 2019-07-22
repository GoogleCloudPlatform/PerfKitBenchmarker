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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import abc
import functools
import logging
import os
import posixpath
import threading
import time
import uuid

from perfkitbenchmarker import errors
from perfkitbenchmarker import events
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
import six

FLAGS = flags.FLAGS


def Register(parsed_flags):
  """Registers the collector if FLAGS.<collector> is set.

  See dstat.py for an example on how to register a collector.

  Args:
    parsed_flags: argument passed into each call to Register()
  """
  del parsed_flags  # unused


class BaseCollector(object):
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

    if not os.path.isdir(self.output_directory):
      raise IOError('collector output directory does not exist: {0}'.format(
          self.output_directory))

  @abc.abstractmethod
  def _CollectorName(self):
    pass

  @abc.abstractmethod
  def _InstallCollector(self, vm):
    pass

  @abc.abstractmethod
  def _CollectorRunCommand(self, vm, collector_file):
    pass

  def _StartOnVm(self, vm, suffix=''):
    """Start collector, having it write to an output file."""
    self._InstallCollector(vm)
    suffix = '{0}-{1}'.format(suffix, self._CollectorName())
    collector_file = posixpath.join(
        vm_util.VM_TMP_DIR, '{0}{1}.stdout'.format(vm.name, suffix))

    cmd = self._CollectorRunCommand(vm, collector_file)

    stdout, _ = vm.RemoteCommand(cmd)
    with self._lock:
      self._pid_files[vm.name] = (stdout.strip(), collector_file)

  def _StopOnVm(self, vm, vm_role):
    """Stop collector on 'vm' and copy the files back."""
    if vm.name not in self._pid_files:
      logging.warn('No collector PID for %s', vm.name)
      return
    else:
      with self._lock:
        pid, file_name = self._pid_files.pop(vm.name)
    cmd = 'kill {0} || true'.format(pid)
    vm.RemoteCommand(cmd)
    try:
      vm.PullFile(self.output_directory, file_name)
      self._role_mapping[vm_role] = file_name
    except errors.VirtualMachine.RemoteCommandError as ex:
      logging.exception('Failed fetching collector result from %s.', vm.name)
      raise ex

  def Start(self, sender, benchmark_spec):
    """Install and start collector on all VMs in 'benchmark_spec'."""
    suffix = '-{0}-{1}'.format(benchmark_spec.uid, str(uuid.uuid4())[:8])
    self.StartOnVms(sender, benchmark_spec.vms, suffix)

  def StartOnVms(self, sender, vms, id_suffix):
    """Install and start collector on given subset of vms.

    Args:
      sender: sender of the request/event to start collector.
      vms: vms to run the collector on.
      id_suffix: id_suffix of the collector output file.
    """
    del sender  # unused
    func = functools.partial(self._StartOnVm, suffix=id_suffix)
    vm_util.RunThreaded(func, vms)
    self._start_time = time.time()
    return

  def Stop(self, sender, benchmark_spec, name=''):
    """Stop collector on all VMs in 'benchmark_spec', fetch results."""
    self.StopOnVms(sender, benchmark_spec.vm_groups, name)

  def StopOnVms(self, sender, vm_groups, name):
    """Stop collector on given subset of vms, fetch results.

    Args:
      sender: sender of the event to stop the collector.
      vm_groups: vm_groups to stop the collector on.
      name: name of event to be stopped.
    """
    events.record_event.send(sender, event=name,
                             start_timestamp=self._start_time,
                             end_timestamp=time.time(),
                             metadata={})
    args = []
    for role, vms in six.iteritems(vm_groups):
      args.extend([((
          vm, '%s_%s' % (role, idx)), {}) for idx, vm in enumerate(vms)])
    vm_util.RunThreaded(self._StopOnVm, args)
    return

  @abc.abstractmethod
  def Analyze(self, sender, benchmark_spec, samples):
    """Analyze collector file and record samples."""
    pass
