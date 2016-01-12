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

"""Module containing class for provider data

This contains the BaseProviderInfo class which is
used for IsBenchmarkSupported

"""

_PROVIDER_INFO_REGISTRY = {}


def GetProviderInfoClass(cloud):
  """Returns the provider info class corresponding to the cloud"""
  return _PROVIDER_INFO_REGISTRY.get(cloud, BaseProviderInfo)


class AutoRegisterProviderInfoMeta(type):
  """Metaclass which allows ProviderInfos to automatically be registered"""

  def __init__(cls, name, bases, dct):
    super(AutoRegisterProviderInfoMeta, cls).__init__(name, bases, dct)
    if cls.CLOUD is not None:
      _PROVIDER_INFO_REGISTRY[cls.CLOUD] = cls


class BaseProviderInfo():
  """Class that holds provider-related data ."""
  __metaclass__ = AutoRegisterProviderInfoMeta

  CLOUD = None

  UNSUPPORTED_BENCHMARKS = []

  @classmethod
  def IsBenchmarkSupported(cls, benchmark):
    if benchmark in cls.UNSUPPORTED_BENCHMARKS:
      return False
    else:
      return True
