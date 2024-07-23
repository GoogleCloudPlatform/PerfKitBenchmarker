"""Implementation of a model & endpoint in Vertex AI.

Uses gcloud python libraries to manage those resources.
"""

import logging
from absl import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker.resources import managed_ai_model
from perfkitbenchmarker.resources import managed_ai_model_spec

FLAGS = flags.FLAGS


class VertexAiModelInRegistry(managed_ai_model.BaseManagedAiModel):
  """Represents a Vertex AI model in the model registry."""

  CLOUD = 'GCP'

  def __init__(
      self, model_spec: managed_ai_model_spec.BaseManagedAiModelSpec, **kwargs
  ):
    super().__init__(**kwargs)
    self.model_name = model_spec.model_name
    self.name = 'pkb' + FLAGS.run_uri
    self.metadata.update({
        'name': self.name,
        'model_name': self.model_name,
    })
    self.endpoint = VertexAiEndpoint(name=self.name)

  def _Create(self) -> None:
    """Creates the underlying resource."""
    # TODO(user): Add actual code here.
    logging.info('Creating the resource: %s.', self.model_name)

  def _CreateDependencies(self):
    self.endpoint.Create()

  def _Delete(self) -> None:
    """Deletes the underlying resource."""
    # TODO(user): Add actual code here.
    logging.info('Deleting the resource: %s.', self.model_name)

  def _DeleteDependencies(self):
    self.endpoint.Delete()


class VertexAiEndpoint(resource.BaseResource):
  """Represents a Vertex AI endpoint."""

  def __init__(
      self, name: str, **kwargs
  ):
    super().__init__(**kwargs)
    self.name = name

  def _Create(self) -> None:
    """Creates the underlying resource."""
    # TODO(user): Add actual code here.
    logging.info('Creating the endpoint: %s.', self.name)

  def _Delete(self) -> None:
    """Deletes the underlying resource."""
    # TODO(user): Add actual code here.
    logging.info('Deleting the endpoint: %s.', self.name)
