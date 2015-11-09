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

import gflags as flags


class FlagsModuleProxy(object):
  """Class which acts as a proxy for the flags module.

  When the FLAGS attribute is accessed, BENCHMARK_FLAGS will be returned
  rather than the global FlagValues object. BENCHMARK_FLAGS is an instance
  of FlagValuesProxy, which enables benchmarks to run with different and
  even conflicting flags. Accessing the GLOBAL_FLAGS attribute will return
  the global FlagValues object. Otherwise, this will behave just like the
  flags module.
  """

  def __getattr__(self, name):
    if name == 'FLAGS':
      return BENCHMARK_FLAGS
    elif name == 'GLOBAL_FLAGS':
      return flags.FLAGS
    return flags.__dict__[name]


class FlagValuesProxy(object):
  """Class which provides the same interface as FlagValues.

  By acting as a proxy for the FlagValues object (i.e. flags.FLAGS),
  this enables benchmark specific flags. This proxy attempts to
  use the current thread's BenchmarkSpec's FlagValues object, but
  falls back to using flags.FLAGS if the thread has no BenchmarkSpec
  object.
  """

  @property
  def _thread_flag_values(self):
    """Returns the correct FlagValues object for the current thread.

    This first tries to get the BenchmarkSpec object corresponding to the
    current thread. If there is one, it returns that spec's FlagValues
    object. If there isn't one, it will return the global FlagValues
    object.
    """
    benchmark_spec = GetThreadBenchmarkSpec()
    if benchmark_spec:
      return benchmark_spec.FLAGS
    else:
      return flags.FLAGS


def _AddProxyMethod(f_name):
  """Adds a method to FlagValuesProxy that forwards to _thread_flag_values."""
  def f(self, *args, **kwargs):
    return getattr(self._thread_flag_values, f_name)(*args, **kwargs)
  f.__name__ = f_name
  f.__doc__ = 'Proxied ' + f_name
  setattr(FlagValuesProxy, f_name, f)


# TODO: introduce a more generic proxy.
for _f_name in ['FlagDict', 'Reset', 'SetDefault', 'RegisteredFlags',
                'FlagValuesDict', '__contains__', '__iter__', '__call__',
                '__setattr__', '__getattr__', '__setitem__', '__getitem__',
                '__str__']:
  _AddProxyMethod(_f_name)
del _f_name
del _AddProxyMethod


BENCHMARK_FLAGS = FlagValuesProxy()


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
