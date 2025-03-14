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
import logging
import time
from typing import Any

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.resources import managed_ai_model_spec

FLAGS = flags.FLAGS


class BaseManagedAiModel(resource.BaseResource):
  """A managed AI model.

  Attributes:
    model_name: The official name of the model, e.g. Llama2.
    region: The region, derived from the zone.
    vm: A way to run commands on the machine.
    child_models: A list of child models that were created with
      InitializeNewModel.
    max_scaling: The max number of nodes to scale to.
  """

  RESOURCE_TYPE = 'BaseManagedAiModel'
  REQUIRED_ATTRS = ['CLOUD']

  region: str
  child_models: list['BaseManagedAiModel'] = []
  vm: virtual_machine.BaseVirtualMachine
  model_name: str
  max_scaling: int

  def __init__(
      self,
      model_spec: managed_ai_model_spec.BaseManagedAiModelSpec,
      vm: virtual_machine.BaseVirtualMachine,
      **kwargs,
  ):
    super().__init__(**kwargs)
    if not FLAGS.zone:
      raise errors.Setup.InvalidConfigurationError(
          'Zone flag is required for Managed AI models but was not set. Note AI'
          ' only cares about region but PKB cares about zone.'
      )
    self.region: str = self.GetRegionFromZone(FLAGS.zone[0])
    self.response_timings: list[float] = []
    self.child_models = []
    self.model_name = model_spec.model_name
    self.max_scaling = model_spec.max_scale
    self.metadata.update({
        'max_scaling': self.max_scaling,
        'region': self.region,
        # Add these to general ResourceMetadata rather than just Create/Delete.
        'resource_type': self.RESOURCE_TYPE,
        'resource_class': self.__class__.__name__,
    })
    self.vm: virtual_machine.BaseVirtualMachine = vm

  def InitializeNewModel(self) -> 'BaseManagedAiModel':
    """Returns a new instance of the same class."""
    child_model = self._InitializeNewModel()
    self.child_models.append(child_model)
    return child_model

  def _InitializeNewModel(self) -> 'BaseManagedAiModel':
    """Returns a new instance of the same class."""
    raise NotImplementedError(
        'InitializeNewModel is not implemented for this model type.'
    )

  def _DeleteDependencies(self):
    for child_model in self.child_models:
      child_model.Delete()

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

  def _IsReady(self):
    """Return true if the underlying resource is ready.

    Supplying this method is optional.  Use it when a resource can exist
    without being ready.  If the subclass does not implement
    it then it just returns true.

    Returns:
      True if the resource was ready in time, False if the wait timed out.
    """
    try:
      self.SendPrompt('What is 2 + 2?', 100, 1.0)
    except (
        errors.Resource.GetError,
        errors.VmUtil.IssueCommandError,
        errors.VirtualMachine.RemoteCommandError,
    ) as ex:
      logging.exception('Tried sending prompt but got error %s. Retrying.', ex)
      return False
    return True

  def SendPrompt(
      self, prompt: str, max_tokens: int, temperature: float, **kwargs: Any
  ) -> list[str]:
    """Sends a prompt to the model, times it, and returns the response.

    Args:
      prompt: The prompt to send.
      max_tokens: The max tokens to return.
      temperature: The temperature to use.
      **kwargs: Additional arguments to pass to _SendPrompt.

    Throws:
      errors.Resource.GetError if the prompt fails.

    Returns:
      The response from the model.
    """
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

  def GetPromptCommand(self, prompt: str, max_tokens: int, temperature: float):
    """Returns the command needed to send a prompt to the model."""
    raise NotImplementedError(
        'GetPromptCommand is not implemented for this model type.'
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
