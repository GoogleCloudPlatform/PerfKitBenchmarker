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
"""Systems for tracking performance counters during a run.

Individual trackers should define a function, 'Register', which will be
called with a single argument, the parsed FLAGS instance, once the Benchmarker
is initialized.
"""


from perfkitbenchmarker import import_util


TRACE_COLLECTORS = list(import_util.LoadModulesForPath(__path__, __name__))


def RegisterAll(sender, parsed_flags):
  for module in TRACE_COLLECTORS:
    module.Register(parsed_flags)
