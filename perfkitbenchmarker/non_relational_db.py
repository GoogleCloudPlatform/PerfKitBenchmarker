# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing base class for non-relational databases."""

import abc
from typing import Any
from absl import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker.configs import freeze_restore_spec
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec

# List of nonrelational database types
DYNAMODB = 'dynamodb'
BIGTABLE = 'bigtable'
_VALID_TYPES = [
    DYNAMODB,
    BIGTABLE,
]


FLAGS = flags.FLAGS
_SERVICE_TYPE = flags.DEFINE_enum(
    'non_relational_db_service_type',
    None,
    _VALID_TYPES,
    'The type of the non-relational database service, e.g. firestore',
)


class BaseNonRelationalDbSpec(freeze_restore_spec.FreezeRestoreSpec):
  """Configurable options of a nonrelational database service."""

  # Needed for registering the spec class and its subclasses. See BaseSpec.
  SPEC_TYPE = 'BaseNonRelationalDbSpec'
  SPEC_ATTRS = ['SERVICE_TYPE']

  def __init__(
      self,
      component_full_name: str,
      flag_values: flags.FlagValues | None = None,
      **kwargs
  ):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)
    self.service_type: str

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments
      to construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'service_type': (
            option_decoders.EnumDecoder,
            {
                'default': None,
                'valid_values': _VALID_TYPES,
            },
        ),
    })
    return result

  @classmethod
  def _ApplyFlags(
      cls, config_values: dict[str, Any], flag_values: flags.FlagValues
  ) -> None:
    """See base class."""
    super()._ApplyFlags(config_values, flag_values)
    option_name_from_flag = {
        'non_relational_db_service_type': 'service_type',
    }
    for flag_name, option_name in option_name_from_flag.items():
      if flag_values[flag_name].present:
        config_values[option_name] = flag_values[flag_name].value


class BaseNonRelationalDb(resource.BaseResource):
  """Object representing a nonrelational database."""

  REQUIRED_ATTRS = ['SERVICE_TYPE']
  RESOURCE_TYPE = 'BaseNonRelationalDb'
  SERVICE_TYPE = 'Base'

  @classmethod
  def FromSpec(cls, db_spec: BaseNonRelationalDbSpec) -> 'BaseNonRelationalDb':
    """Constructs an instance from the class given a spec.

    Overridden by children.
    Args:
      db_spec: A Base spec with details of the db.

    Returns:
      newly created instance.
    """
    del db_spec
    return cls()

  def SetVms(self, vm_groups):
    self._client_vms = vm_groups[
        'clients' if 'clients' in vm_groups else 'default'
    ]


class BaseManagedMongoDb(BaseNonRelationalDb):
  """Base class for managed MongoDB instances.

  Attributes:
    tls_enabled: Whether transport-layer security is enabled for the instance.
    endpoint: The endpoint of the instance.
    port: The port of the instance.
  """

  tls_enabled: bool
  endpoint: str
  port: int

  @abc.abstractmethod
  def GetConnectionString(self) -> str:
    """Returns the connection string used to connect to the instance."""
    raise NotImplementedError()

  def SetupClientTls(self) -> None:
    """Sets up client TLS."""
    pass

  def GetJvmTrustStoreArgs(self) -> str:
    """Returns JVM args needed for using TLS."""
    return ''


def GetNonRelationalDbSpecClass(
    service_type: str,
) -> type[BaseNonRelationalDbSpec]:
  """Gets the non-relational db spec class corresponding to 'service_type'."""
  return spec.GetSpecClass(BaseNonRelationalDbSpec, SERVICE_TYPE=service_type)


def GetNonRelationalDbClass(service_type: str) -> type[BaseNonRelationalDb]:
  """Gets the non-relational database class corresponding to 'service_type'."""
  return resource.GetResourceClass(
      BaseNonRelationalDb, SERVICE_TYPE=service_type
  )
