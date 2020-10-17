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

"""Defines observable events in PerfKitBenchmarker.

All events are passed keyword arguments, and possibly a sender. See event
definitions below.

Event handlers are run synchronously in an unspecified order; any exceptions
raised will be propagated.
"""
import logging
import os

from absl import flags
import blinker
from perfkitbenchmarker import data
from perfkitbenchmarker import sample


FLAGS = flags.FLAGS
_events = blinker.Namespace()


initialization_complete = _events.signal('system-ready', doc="""
Signal sent once after the system is initialized (command-line flags
parsed, temporary directory initialized, run_uri set).

Sender: None
Payload: parsed_flags, the parsed FLAGS object.""")

provider_imported = _events.signal('provider-imported', doc="""
Signal sent after a cloud provider's modules have been imported.

Sender: string. Cloud provider name chosen from providers.VALID_CLOUDS.""")

benchmark_start = _events.signal('benchmark-start', doc="""
Signal sent at the beginning of a benchmark before any resources are
provisioned.

Sender: None
Payload: benchmark_spec.""")

on_vm_startup = _events.signal('on-vm-startup', doc="""
Signal sent on vm startup.

Sender: None
Payload: vm (VirtualMachine object).""")


benchmark_end = _events.signal('benchmark-end', doc="""
Signal sent at the end of a benchmark after any resources have been
torn down (if run_stage includes teardown).

Sender: None
Payload: benchmark_spec.""")

RUN_PHASE = 'run'

before_phase = _events.signal('before-phase', doc="""
Signal sent immediately before a phase runs.

Sender: the phase. Currently only RUN_PHASE.
Payload: benchmark_spec.""")

after_phase = _events.signal('after-phase', doc="""
Signal sent immediately after a phase runs, regardless of whether it was
successful.

Sender: the phase. Currently only RUN_PHASE.
Payload: benchmark_spec.""")

samples_created = _events.signal('samples-created', doc="""
Called with samples list and benchmark spec.

Signal sent immediately after a sample is created.
The samples' metadata is mutable, and may be updated by the subscriber.

Sender: the phase. Currently only RUN_PHASE.
Payload: benchmark_spec (BenchmarkSpec), samples (list of sample.Sample).""")

record_event = _events.signal('record-event', doc="""
Signal sent when an event is recorded.

Signal sent after an event occurred. Record start, end timestamp and metadata
of the event for analysis.

Sender: None
Payload: event (string), start_timestamp (float), end_timestamp (float),
metadata (dict).""")


def RegisterTracingEvents():
  record_event.connect(AddEvent, weak=False)


class TracingEvent(object):
  """Represents an event object.

  Attributes:
    sender: string. Name of the sending class/object.
    event: string. Name of the event.
    start_timestamp: float. Represents the start timestamp of the event.
    end_timestamp: float. Represents the end timestamp of the event.
    metadata: dict. Additional metadata of the event.
  """

  events = []

  def __init__(self, sender, event, start_timestamp, end_timestamp, metadata):
    self.sender = sender
    self.event = event
    self.start_timestamp = start_timestamp
    self.end_timestamp = end_timestamp
    self.metadata = metadata


def AddEvent(sender, event, start_timestamp, end_timestamp, metadata):
  """Record a TracingEvent."""
  TracingEvent.events.append(
      TracingEvent(sender, event, start_timestamp, end_timestamp, metadata))


@on_vm_startup.connect
def _RunStartupScript(unused_sender, vm):
  """Run startup script if necessary."""
  if FLAGS.startup_script:
    vm.RemoteCopy(data.ResourcePath(FLAGS.startup_script))
    vm.startup_script_output = vm.RemoteCommand(
        './%s' % os.path.basename(FLAGS.startup_script))


@samples_created.connect
def _AddScriptSamples(unused_sender, benchmark_spec, samples):
  def _ScriptResultToMetadata(out):
    return {'stdout': out[0], 'stderr': out[1]}
  for vm in benchmark_spec.vms:
    if FLAGS.startup_script:
      samples.append(sample.Sample(
          'startup', 0, '', _ScriptResultToMetadata(vm.startup_script_output)))
    if FLAGS.postrun_script:
      samples.append(sample.Sample(
          'postrun', 0, '', _ScriptResultToMetadata(vm.postrun_script_output)))


@after_phase.connect
def _RunPostRunScript(sender, benchmark_spec):
  if sender != RUN_PHASE:
    logging.info(
        'Receive after_phase signal from :%s, not '
        'triggering _RunPostRunScript.', sender)
  if FLAGS.postrun_script:
    for vm in benchmark_spec.vms:
      vm.RemoteCopy(FLAGS.postrun_script)
      vm.postrun_script_output = vm.RemoteCommand(
          './%s' % os.path.basename(FLAGS.postrun_script))
