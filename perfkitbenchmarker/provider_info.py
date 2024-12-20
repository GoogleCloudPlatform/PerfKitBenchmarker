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

"""Module containing class for provider data.

This contains the BaseProviderInfo class which is
used for IsBenchmarkSupported
"""

from absl import flags


GCP = 'GCP'
AZURE = 'Azure'
AWS = 'AWS'
IBMCLOUD = 'IBMCloud'
ALICLOUD = 'AliCloud'
KUBERNETES = 'Kubernetes'
DIGITALOCEAN = 'DigitalOcean'
OPENSTACK = 'OpenStack'
CLOUDSTACK = 'CloudStack'
RACKSPACE = 'Rackspace'
MESOS = 'Mesos'
PROFITBRICKS = 'ProfitBricks'
# Though Docker is not a cloud provider, it's inclusion is useful
# for performing on premise to cloud benchmarks
DOCKER = 'Docker'
# Likewise, UnitTest is not a cloud provider, but is useful for testing.
UNIT_TEST = 'UnitTest'

PROVIDER_DIRECTORY_NAMES = (
    GCP,
    AZURE,
    AWS,
    IBMCLOUD,
    DIGITALOCEAN,
    OPENSTACK,
    RACKSPACE,
    CLOUDSTACK,
    ALICLOUD,
    MESOS,
    PROFITBRICKS,
    DOCKER,
)
# TODO(user): Remove Kubernetes from VALID_CLOUDS.
VALID_CLOUDS = tuple(list(PROVIDER_DIRECTORY_NAMES) + [KUBERNETES, UNIT_TEST])

_PROVIDER_INFO_REGISTRY = {}

# With b/302543184, VM Platform replaces Cloud as that attribute is overused &
# overloaded.
# TODO(user): Add STATIC, DOCKER_ON_VM, DOCKER_INSTANCE, & BARE_METAL.
DEFAULT_VM_PLATFORM = 'DEFAULT_VM'
KUBERNETES_PLATFORM = KUBERNETES
ALL_PLATFORMS = [
    DEFAULT_VM_PLATFORM,
    KUBERNETES_PLATFORM,
]
VM_PLATFORM = flags.DEFINE_enum(
    'vm_platform', DEFAULT_VM_PLATFORM, ALL_PLATFORMS, 'VM platform to use.'
)


def GetProviderInfoClass(cloud):
  """Returns the provider info class corresponding to the cloud."""
  return _PROVIDER_INFO_REGISTRY.get(cloud, BaseProviderInfo)


class AutoRegisterProviderInfoMeta(type):
  """Metaclass which allows ProviderInfos to automatically be registered."""

  def __init__(cls, name, bases, dct):
    super().__init__(name, bases, dct)
    if hasattr(cls, 'CLOUD') and cls.CLOUD is not None:
      _PROVIDER_INFO_REGISTRY[cls.CLOUD] = cls


class BaseProviderInfo(metaclass=AutoRegisterProviderInfoMeta):
  """Class that holds provider-related data."""

  CLOUD = None

  UNSUPPORTED_BENCHMARKS = []

  @classmethod
  def IsBenchmarkSupported(cls, benchmark):
    if benchmark in cls.UNSUPPORTED_BENCHMARKS:
      return False
    else:
      return True
