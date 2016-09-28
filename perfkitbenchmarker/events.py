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

from blinker import Namespace

_events = Namespace()


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

sample_created = _events.signal('sample-created', doc="""
Called with sample object and benchmark spec.

Signal sent immediately after a sample is created by a publisher.
The sample's metadata is mutable, and may be updated by the subscriber.

Sender: None
Payload: benchmark_spec (BenchmarkSpec), sample (dict).""")

samples_modified = _events.signal('samples-modified', doc="""
Called with list of samples and benchmark spec.

Sender: the phase
Payload: samples (list of sample.Sample)
""")

record_event = _events.signal('record-event', doc="""
Signal sent when an event is recorded.

Sender: None
Payload: event (string), start_timestamp (float), end_timestamp (float),
metadata (dict).""")


def RegisterTracingEvents():
  e = TracingEvents()
  record_event.connect(e.Add, weak=False)


class TracingEvents(object):
  """Represents an event object."""

  events = []

  def Add(self, sender, event, start_timestamp, end_timestamp, metadata):
    self.sender = sender
    self.event = event
    self.start_timestamp = start_timestamp
    self.end_timestamp = end_timestamp
    self.metadata = metadata
    self.events.append(self)
