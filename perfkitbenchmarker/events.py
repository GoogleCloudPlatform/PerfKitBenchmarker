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
