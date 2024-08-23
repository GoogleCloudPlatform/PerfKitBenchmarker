# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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

"""A managed AI model resource.

A managed AI model uses a platform like Vertex AI or Sagemaker to manage the
resource, but the customer pays for the underlying resources it runs on, rather
than simply for calls like with an API. This also gives the customer more
control & ownership.
"""
import time
from typing import Any

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample

FLAGS = flags.FLAGS


class BaseManagedAiModel(resource.BaseResource):
  """A managed AI model."""

  RESOURCE_TYPE = 'BaseManagedAiModel'
  REQUIRED_ATTRS = ['CLOUD']

  region: str

  def __init__(self, **kwargs):
    super().__init__(**kwargs)
    if not FLAGS.zone:
      raise errors.Setup.InvalidConfigurationError(
          'Zone flag is required for Managed AI models but was not set. Note AI'
          ' only cares about region but PKB cares about zone.'
      )
    self.region: str = self.GetRegionFromZone(FLAGS.zone[0])
    self.response_timings: list[float] = []
    self.metadata.update({
        'region': self.region,
    })

  def GetRegionFromZone(self, zone: str) -> str:
    """Returns the region a zone is in."""
    raise NotImplementedError(
        'GetRegionFromZone is not implemented for this model type.'
    )

  def ListExistingEndpoints(self, region: str | None = None) -> list[str]:
    """Returns a list of existing endpoints in the same region.

    Args:
      region: If not provided, uses self.region.

    Returns:
      List of endpoint names. Endpoints can actually serve traffic.
    """
    raise NotImplementedError(
        'ListExistingModels is not implemented for this model type.'
    )

  def SendPrompt(
      self, prompt: str, max_tokens: int, temperature: float, **kwargs: Any
  ) -> list[str]:
    """Sends a prompt to the model, times it, and returns the response."""
    start_time = time.time()
    response = self._SendPrompt(prompt, max_tokens, temperature, **kwargs)
    end_time = time.time()
    self.response_timings.append(end_time - start_time)
    return response

  def _SendPrompt(
      self, prompt: str, max_tokens: int, temperature: float, **kwargs: Any
  ) -> list[str]:
    """Sends a prompt to the model and returns the response."""
    raise NotImplementedError(
        'SendPrompt is not implemented for this model type.'
    )

  def GetSamples(self) -> list[sample.Sample]:
    """Gets samples relating to the provisioning of the resource."""
    samples = super().GetSamples()
    metadata = self.GetResourceMetadata()
    for idx, timing in enumerate(self.response_timings):
      samples.append(
          sample.Sample(
              f'response_time_{idx}',
              timing,
              'seconds',
              metadata,
          )
      )
    return samples


def GetManagedAiModelClass(
    cloud: str,
) -> resource.AutoRegisterResourceMeta | None:
  """Gets the managed AI model class for the given cloud."""
  return resource.GetResourceClass(BaseManagedAiModel, CLOUD=cloud)
