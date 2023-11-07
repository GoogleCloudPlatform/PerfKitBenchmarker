"""Base classes for cryptographic keys."""

import dataclasses
from typing import Optional

from absl import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec

FLAGS = flags.FLAGS


@dataclasses.dataclass
class BaseKeySpec(spec.BaseSpec):
  """Configurable options of a cryptographic key."""
  # Needed for registering the spec class and its subclasses. See BaseSpec.
  SPEC_TYPE = 'BaseKeySpec'
  CLOUD = None

  def __init__(self,
               component_full_name: str,
               flag_values: Optional[flags.FlagValues] = None,
               **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)
    self.cloud: str

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option."""
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'cloud': (option_decoders.EnumDecoder, {
            # Uncomment when there are other cloud implementations
            # 'valid_values': provider_info.VALID_CLOUDS
            'valid_values': ['GCP']
        }),
    })
    return result


class BaseKey(resource.BaseResource):
  """Object representing a cryptographic key."""
  RESOURCE_TYPE = 'BaseKey'
  CLOUD = None


def GetKeySpecClass(cloud: str) -> Optional[spec.BaseSpecMetaClass]:
  """Gets the key spec class corresponding to 'cloud'."""
  return spec.GetSpecClass(BaseKeySpec, CLOUD=cloud)


def GetKeyClass(cloud: str) -> Optional[resource.AutoRegisterResourceMeta]:
  """Gets the key class corresponding to 'cloud'."""
  return resource.GetResourceClass(BaseKey, CLOUD=cloud)
