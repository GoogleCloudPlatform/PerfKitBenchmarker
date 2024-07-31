"""Implementation of a model & endpoint in Vertex AI.

Uses gcloud python libraries to manage those resources.
"""

import logging
import os
from absl import flags
# pylint: disable=g-import-not-at-top, g-statement-before-imports
# External needs from google.cloud.
# pytype: disable=module-attr
try:
  from google.cloud.aiplatform import aiplatform
except ImportError:
  from google.cloud import aiplatform
from perfkitbenchmarker import resource
from perfkitbenchmarker.resources import managed_ai_model
from perfkitbenchmarker.resources import managed_ai_model_spec

FLAGS = flags.FLAGS


# TODO(user): Use flag defined project.
BUCKET_URI = 'gs://test-howellz-tmp-20240717162644-2ec5'
SERVICE_ACCOUNT = '319419405142-compute@developer.gserviceaccount.com'
STAGING_BUCKET = os.path.join(BUCKET_URI, 'temporal')
# TODO(user): Package model specific details like args & docker image
# into a spec class.
MODEL_BUCKET = os.path.join(BUCKET_URI, 'llama2')
VLLM_ARGS = [
    '--host=0.0.0.0',
    '--port=7080',
    '--tensor-parallel-size=1',
    '--swap-space=16',
    '--gpu-memory-utilization=0.95',
    '--max-model-len=2048',
    '--max-num-batched-tokens=4096',
]
# The pre-built serving docker images.
VLLM_DOCKER_URI = 'us-docker.pkg.dev/vertex-ai/vertex-vision-model-garden-dockers/pytorch-vllm-serve:20240222_0916_RC00'


class VertexAiModelInRegistry(managed_ai_model.BaseManagedAiModel):
  """Represents a Vertex AI model in the model registry."""

  CLOUD = 'GCP'

  def __init__(
      self, model_spec: managed_ai_model_spec.BaseManagedAiModelSpec, **kwargs
  ):
    super().__init__(**kwargs)
    self.model_name = model_spec.model_name
    self.model_id = os.path.join(MODEL_BUCKET, self.model_name)
    self.name = 'pkb' + FLAGS.run_uri
    self.metadata.update({
        'name': self.name,
        'model_name': self.model_name,
    })
    self.endpoint = VertexAiEndpoint(name=self.name)
    self.region = 'us-east4'
    self.project = 'test-howellz'
    self.gcloud_model = None

  def _Create(self) -> None:
    """Creates the underlying resource."""
    # TODO(user): Unhardcode some of these values.
    logging.info('Creating the resource: %s for ai model.', self.model_name)
    env_vars = {
        'MODEL_ID': self.model_id,
        'DEPLOY_SOURCE': 'notebook',
    }
    self.gcloud_model = aiplatform.Model.upload(
        display_name=self.name,
        serving_container_image_uri=VLLM_DOCKER_URI,
        serving_container_command=[
            'python',
            '-m',
            'vllm.entrypoints.api_server',
        ],
        serving_container_args=VLLM_ARGS,
        serving_container_ports=[7080],
        serving_container_predict_route='/generate',
        serving_container_health_route='/ping',
        serving_container_environment_variables=env_vars,
        artifact_uri=self.model_id,
    )
    self.gcloud_model.deploy(
        endpoint=self.endpoint.ai_endpoint,
        machine_type='g2-standard-8',
        accelerator_type='NVIDIA_L4',
        accelerator_count=1,
        deploy_request_timeout=1800,
        service_account=SERVICE_ACCOUNT,
    )

  def _CreateDependencies(self):
    aiplatform.init(
        project=self.project,
        location=self.region,
        staging_bucket=STAGING_BUCKET,
    )
    self.endpoint.Create()

  def Delete(self, freeze: bool = False) -> None:
    """Deletes the underlying resource & its dependencies."""
    # Normally _DeleteDependencies is called by parent after _Delete, but we
    # need to call it before.
    self._DeleteDependencies()
    super().Delete(freeze)

  def _Delete(self) -> None:
    """Deletes the underlying resource."""
    logging.info('Deleting the resource: %s.', self.model_name)
    assert self.gcloud_model
    self.gcloud_model.delete()

  def _DeleteDependencies(self):
    self.endpoint.Delete()


class VertexAiEndpoint(resource.BaseResource):
  """Represents a Vertex AI endpoint."""

  def __init__(self, name: str, **kwargs):
    super().__init__(**kwargs)
    self.name = name
    self.ai_endpoint = None

  def _Create(self) -> None:
    """Creates the underlying resource."""
    logging.info('Creating the endpoint: %s.', self.name)
    self.ai_endpoint = aiplatform.Endpoint.create(
        display_name=f'{self.name}-endpoint'
    )

  def _Delete(self) -> None:
    """Deletes the underlying resource."""
    logging.info('Deleting the endpoint: %s.', self.name)
    assert self.ai_endpoint
    self.ai_endpoint.delete(force=True)
