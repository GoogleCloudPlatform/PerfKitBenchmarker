# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
"""Contains benchmark imports and a list of benchmarks.

All modules within this package are considered benchmarks, and are loaded
dynamically. Add non-benchmark code to other packages.
"""

from perfkitbenchmarker import import_util


def _LoadBenchmarks():
  return list(import_util.LoadModulesForPath(__path__, __name__))

BENCHMARKS = _LoadBenchmarks()

VALID_BENCHMARKS = {module.BENCHMARK_NAME: module
                    for module in BENCHMARKS}
