# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing abstract classes related to triggers.."""


import abc
import datetime
import logging
import threading
import time

from absl import flags
from perfkitbenchmarker import events
from perfkitbenchmarker import stages

FLAGS = flags.FLAGS

_TRIGGER_VM_GROUPS = flags.DEFINE_list(
    'trigger_vm_groups', None, 'Run trigger on all vms in the vm groups.'
    'By default trigger runs on all VMs, for client and server achitecture,'
    'specify trace vm groups to servers to only collect metrics on server vms.')


class BaseTimeTrigger(metaclass=abc.ABCMeta):
  """Object representing a Base Time Trigger.

  A Time Trigger is a utility that is run before benchmarks invoke a method
  after a given time. A Base time trigger is an abstract
  class with common routines that derived trigger use.
  """

  def __init__(self, delay=0):
    """Runs a time trigger on 'vms'."""
    self.delay = delay
    self.vms = []
    self.trigger_time = None
    self.metadata = {
        self.trigger_name: True,
        self.trigger_name + '_delay': delay
    }

  def CreateAndStartTriggerThread(self, vm) -> None:
    """Create and start threads that runs the trigger.

    Threads are guarded by a semaphore thus we never keep track of the
    wait on these threads.

    Args:
      vm: A virtual machine.
    """
    def TriggerEvent():
      time.sleep(self.delay)
      logging.info('Triggering %s on %s', self.trigger_name, vm.name)
      self.TriggerMethod(vm)

    t = threading.Thread(target=TriggerEvent)
    t.daemon = True
    t.start()

  def SetUpTrigger(self, unused_sender, benchmark_spec):
    """Sets up the trigger. This method install relevant scripts on the VM."""
    logging.info('Setting up Trigger %s.', self.trigger_name)
    if _TRIGGER_VM_GROUPS.value:
      for vm_group in _TRIGGER_VM_GROUPS.value:
        if vm_group in benchmark_spec.vm_groups:
          self.vms += benchmark_spec.vm_groups[vm_group]
        else:
          logging.error('TRIGGER_VM_GROUPS % does not exist.', vm_group)
    else:
      self.vms = benchmark_spec.vms

    self.SetUp()

  def RunTrigger(self, unused_sender):
    """Run the trigger event."""
    for vm in self.vms:
      self.CreateAndStartTriggerThread(vm)
    trigger_time = (
        datetime.datetime.now() +
        datetime.timedelta(seconds=self.delay))
    self.metadata[self.trigger_name + '_time'] = str(trigger_time)

  # pylint: disable=unused-argument
  def UpdateMetadata(self, unused_sender, benchmark_spec, samples):
    """Update global metadata."""
    for s in samples:
      s.metadata.update(self.metadata)

  def Register(self):
    events.before_phase.connect(
        self.SetUpTrigger, stages.RUN, weak=False)
    events.trigger_phase.connect(
        self.RunTrigger, weak=False)
    events.benchmark_samples_created.connect(self.AppendSamples, weak=False)
    events.all_samples_created.connect(
        self.UpdateMetadata, weak=False)

  @property
  def trigger_name(self) -> str:
    return ''

  @abc.abstractmethod
  def TriggerMethod(self, vm):
    pass

  @abc.abstractmethod
  def SetUp(self):
    pass

  @abc.abstractmethod
  def AppendSamples(self, unused_sender, benchmark_spec, samples):
    pass


def Register(unused_parsed_flags):
  pass

