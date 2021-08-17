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

from typing import Dict, Optional

from absl import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker.configs import freeze_restore_spec
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec

# List of nonrelational database types
DYNAMODB = 'dynamodb'


FLAGS = flags.FLAGS


class BaseNonRelationalDbSpec(freeze_restore_spec.FreezeRestoreSpec):
  """Configurable options of a nonrelational database service."""

  # Needed for registering the spec class and its subclasses. See BaseSpec.
  SPEC_TYPE = 'BaseNonRelationalDbSpec'
  SPEC_ATTRS = ['SERVICE_TYPE']

  def __init__(self,
               component_full_name: str,
               flag_values: Optional[Dict[str, flags.FlagValues]] = None,
               **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

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
                'default':
                    None,
                'valid_values': [
                    DYNAMODB,
                ],
            }),
    })
    return result


class BaseNonRelationalDb(resource.BaseResource):
  """Object representing a nonrelational database."""
  REQUIRED_ATTRS = ['SERVICE_TYPE']
  RESOURCE_TYPE = 'BaseNonRelationalDb'
  SERVICE_TYPE = 'Base'


def GetNonRelationalDbSpecClass(
    service_type: str) -> Optional[spec.BaseSpecMetaClass]:
  """Gets the non-relational db spec class corresponding to 'service_type'."""
  return spec.GetSpecClass(BaseNonRelationalDbSpec, SERVICE_TYPE=service_type)


def GetNonRelationalDbClass(
    service_type: str) -> Optional[resource.AutoRegisterResourceMeta]:
  """Gets the non-relational database class corresponding to 'service_type'."""
  return resource.GetResourceClass(BaseNonRelationalDb,
                                   SERVICE_TYPE=service_type)
