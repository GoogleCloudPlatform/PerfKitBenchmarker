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

"""Module for working with the current thread context."""

import threading


class _ThreadData(threading.local):
  def __init__(self):
    self.benchmark_spec = None


_thread_local = _ThreadData()


def SetThreadBenchmarkSpec(benchmark_spec):
  """Sets the current thread's BenchmarkSpec object."""
  _thread_local.benchmark_spec = benchmark_spec


def GetThreadBenchmarkSpec():
  """Gets the current thread's BenchmarkSpec object.

  If SetThreadBenchmarkSpec() has not been called in either the current thread
  or in an ancestor, then this method will return None by default.
  """
  return _thread_local.benchmark_spec
