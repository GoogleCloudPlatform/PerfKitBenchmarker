# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""A simple example spec.

Spec for a simple example resource. Note that a spec is only needed to
initialize from a BENCHMARK_CONFIG. To initialize the example_resource object
from another file (e.g. as a dependent resource of another, or even within a
benchmark), you can just instantiate the object.

When adding a new spec, also add substantial references from
benchmark_config_spec.py.
"""

from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec


class BaseExampleResourceSpec(spec.BaseSpec):
  """A simple example resource spec.

  Attributes:
    log_text: The text the resource should log.
  """

  SPEC_TYPE = 'BaseExampleResourceSpec'
  SPEC_ATTRS = ['EXAMPLE_TYPE']

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    self.log_text: str
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
        'example_type': (
            option_decoders.StringDecoder,
            {
                'default': 'ImplementedExampleResource',
                'none_ok': True,
            },
        ),
        'log_text': (
            option_decoders.StringDecoder,
            {
                'none_ok': True,
                'default': 'Default Set By Decoder',
            },
        ),
    })
    return result


class ImplementedExampleResourceSpec(BaseExampleResourceSpec):
  """An implementation of a simple example resource spec."""

  EXAMPLE_TYPE = 'ImplementedExampleResource'


def GetExampleResourceSpecClass(
    example_type: str,
) -> spec.BaseSpecMetaClass | None:
  """Gets the example spec class corresponding to 'example_type'."""
  return spec.GetSpecClass(BaseExampleResourceSpec, EXAMPLE_TYPE=example_type)
