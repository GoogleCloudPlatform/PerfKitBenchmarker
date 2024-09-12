"""Implementation of a model & endpoint in Vertex AI.

Uses gcloud python libraries to manage those resources.

One time setup of service account:
- We assume the existence of a
"{PROJECT_NUMBER}-compute@developer.gserviceaccount.com" service account with
the required permissions.
- Follow instructions from
https://cloud.google.com/vertex-ai/docs/general/custom-service-account
to create it & give permissions if one doesn't exist.
"""

import logging
import os
import time
from typing import Any
from absl import flags
# pylint: disable=g-import-not-at-top, g-statement-before-imports
# External needs from google.cloud.
# pytype: disable=module-attr
try:
  from google.cloud.aiplatform import aiplatform
except ImportError:
  from google.cloud import aiplatform
from google.api_core import exceptions as google_exceptions
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.resources import managed_ai_model
from perfkitbenchmarker.resources import managed_ai_model_spec

FLAGS = flags.FLAGS


# TODO(user): Create new bucket & unique service account.
BUCKET_URI = 'gs://test-howellz-tmp-20240717162644-2ec5'
SERVICE_ACCOUNT_BASE = '{}-compute@developer.gserviceaccount.com'
STAGING_BUCKET = os.path.join(BUCKET_URI, 'temporal')
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


class VertexAiModelInRegistry(managed_ai_model.BaseManagedAiModel):
  """Represents a Vertex AI model in the model registry.

  Attributes:
    model_name: The official name of the model in Model Garden, e.g. Llama2.
    model_bucket_path: Where the model bucket is located.
    name: The name of the created model in private model registry.
    region: The region, derived from the zone.
    project: The project.
    endpoint: The PKB resource endpoint the model is deployed to.
    gcloud_model: Representation of the model in gcloud python library.
    service_account: Name of the service account used by the model.
    model_deploy_time: Time it took to deploy the model.
    model_upload_time: Time it took to upload the model.
  """

  CLOUD = 'GCP'

  endpoint: 'VertexAiEndpoint'
  model_spec: 'VertexAiModelSpec'
  model_name: str
  model_bucket_path: str
  name: str
  region: str
  project: str
  gcloud_model: aiplatform.Model | None
  service_account: str
  model_deploy_time: float | None
  model_upload_time: float | None

  def __init__(
      self,
      model_spec: managed_ai_model_spec.BaseManagedAiModelSpec,
      name: str | None = None,
      **kwargs,
  ):
    super().__init__(**kwargs)
    if not isinstance(model_spec, VertexAiModelSpec):
      raise errors.Config.InvalidValue(
          f'Invalid model spec class: "{model_spec.__class__.__name__}". '
          'Must be a VertexAiModelSpec. It had config values of '
          f'{model_spec.model_name} & {model_spec.cloud}'
      )
    self.model_spec = model_spec
    self.model_name = model_spec.model_name
    self.model_bucket_path = os.path.join(
        BUCKET_URI, self.model_spec.model_bucket_suffix
    )
    if name:
      self.name = name
    else:
      self.name = 'pkb' + FLAGS.run_uri
    self.endpoint = VertexAiEndpoint(name=self.name)
    self.project = FLAGS.project
    if not self.project:
      raise errors.Setup.InvalidConfigurationError(
          'Project is required for Vertex AI but was not set.'
      )
    self.gcloud_model = None
    self.metadata.update({
        'name': self.name,
        'model_name': self.model_name,
        'machine_type': self.model_spec.machine_type,
        'accelerator_type': self.model_spec.accelerator_type,
    })
    project_number = util.GetProjectNumber(self.project)
    self.service_account = SERVICE_ACCOUNT_BASE.format(project_number)
    self.model_upload_time = None
    self.model_deploy_time = None

  def _InitializeNewModel(self) -> 'VertexAiModelInRegistry':
    """Returns a new instance of the same class."""
    return self.__class__(model_spec=self.model_spec, name=self.name + '2')

  def GetRegionFromZone(self, zone: str) -> str:
    return util.GetRegionFromZone(zone)

  def ListExistingEndpoints(self, region: str | None = None) -> list[str]:
    """Returns a list of existing model endpoint ids in the same region."""
    if region is None:
      region = self.region
    # Expected output example:
    # ENDPOINT_ID          DISPLAY_NAME
    # 12345                some_endpoint_name
    out, _, _ = vm_util.IssueCommand([
        'gcloud',
        'ai',
        'endpoints',
        'list',
        f'--region={region}',
        f'--project={self.project}',
    ])
    lines = out.splitlines()
    ids = [line.split()[0] for line in lines]
    ids.pop(0)  # Remove the first line which just has titles
    return ids

  def GetSamples(self) -> list[sample.Sample]:
    """Gets samples relating to the provisioning of the resource."""
    samples = super().GetSamples()
    metadata = self.GetResourceMetadata()
    if self.model_upload_time:
      samples.append(
          sample.Sample(
              'Model Upload Time',
              self.model_upload_time,
              'seconds',
              metadata,
          )
      )
    if self.model_deploy_time:
      samples.append(
          sample.Sample(
              'Model Deploy Time',
              self.model_deploy_time,
              'seconds',
              metadata,
          )
      )
    return samples

  def _SendPrompt(
      self, prompt: str, max_tokens: int, temperature: float, **kwargs: Any
  ) -> list[str]:
    """Sends a prompt to the model and returns the response."""
    assert self.endpoint.ai_endpoint
    instances = self.model_spec.ConvertToInstances(
        prompt, max_tokens, temperature, **kwargs
    )
    response = self.endpoint.ai_endpoint.predict(instances=instances)
    str_responses = [str(response) for response in response.predictions]
    return str_responses

  def _Create(self) -> None:
    """Creates the underlying resource."""
    logging.info('Creating the resource: %s for ai model.', self.model_name)
    env_vars = self.model_spec.GetEnvironmentVariables(
        model_bucket_path=self.model_bucket_path
    )
    start_model_upload = time.time()
    self.gcloud_model = aiplatform.Model.upload(
        display_name=self.name,
        serving_container_image_uri=self.model_spec.container_image_uri,
        serving_container_command=self.model_spec.serving_container_command,
        serving_container_args=self.model_spec.serving_container_args,
        serving_container_ports=self.model_spec.serving_container_ports,
        serving_container_predict_route=self.model_spec.serving_container_predict_route,
        serving_container_health_route=self.model_spec.serving_container_health_route,
        serving_container_environment_variables=env_vars,
        artifact_uri=self.model_bucket_path,
    )
    end_model_upload = time.time()
    self.model_upload_time = end_model_upload - start_model_upload
    try:
      start_model_deploy = time.time()
      self.gcloud_model.deploy(
          endpoint=self.endpoint.ai_endpoint,
          machine_type=self.model_spec.machine_type,
          accelerator_type=self.model_spec.accelerator_type,
          accelerator_count=1,
          deploy_request_timeout=1800,
      )
      end_model_deploy = time.time()
      self.model_deploy_time = end_model_deploy - start_model_deploy
    except google_exceptions.ServiceUnavailable as ex:
      logging.info('Tried to deploy model but got unavailable error %s', ex)
      raise errors.Benchmarks.QuotaFailure(ex)

  def _CreateDependencies(self):
    aiplatform.init(
        project=self.project,
        location=self.region,
        staging_bucket=STAGING_BUCKET,
        service_account=self.service_account,
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
    super()._DeleteDependencies()
    self.endpoint.Delete()

  def __getstate__(self):
    """Override pickling as the AI platform objects are not picklable."""
    to_pickle_dict = {
        'name': self.name,
        'model_name': self.model_name,
        'model_bucket_path': self.model_bucket_path,
        'region': self.region,
        'project': self.project,
        'service_account': self.service_account,
        'model_upload_time': self.model_upload_time,
        'model_deploy_time': self.model_deploy_time,
        'model_spec': self.model_spec,
    }
    return to_pickle_dict

  def __setstate__(self, pickled_dict):
    """Override pickling as the AI platform objects are not picklable."""
    self.name = pickled_dict['name']
    self.model_name = pickled_dict['model_name']
    self.model_bucket_path = pickled_dict['model_bucket_path']
    self.region = pickled_dict['region']
    self.project = pickled_dict['project']
    self.service_account = pickled_dict['service_account']
    self.model_upload_time = pickled_dict['model_upload_time']
    self.model_deploy_time = pickled_dict['model_deploy_time']
    self.model_spec = pickled_dict['model_spec']


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
    self.ai_endpoint = None  # Object is not picklable - none it out


class VertexAiModelSpec(managed_ai_model_spec.BaseManagedAiModelSpec):
  """Spec for a Vertex AI model.

  Attributes:
    env_vars: Environment variables set on the node.
    serving_container_command: Command run on container to start the model.
    serving_container_args: The arguments passed to container create.
    serving_container_ports: The ports to expose for the model.
    serving_container_predict_route: The route to use for prediction requests.
    serving_container_health_route: The route to use for health checks.
    machine_type: The machine type for model's cluster.
    accelerator_type: The type of the GPU/TPU.
  """

  CLOUD = 'GCP'

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)
    # The pre-built serving docker images.
    self.container_image_uri: str
    self.model_bucket_suffix: str
    self.serving_container_command: list[str]
    self.serving_container_args: list[str]
    self.serving_container_ports: list[int]
    self.serving_container_predict_route: str
    self.serving_container_health_route: str
    self.machine_type: str
    self.accelerator_type: str

  def GetEnvironmentVariables(self, **kwargs) -> dict[str, str]:
    """Returns container's environment variables, with whatever args needed."""
    del kwargs
    return {}

  def ConvertToInstances(
      self, prompt: str, max_tokens: int, temperature: float, **kwargs: Any
  ) -> list[dict[str, Any]]:
    """Converts input to the form expected by the model."""
    return []


class VertexAiLlama27bSpec(VertexAiModelSpec):
  """Spec for running the Llama2 7b model."""

  MODEL_NAME = 'llama2_7b'

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)
    # The pre-built serving docker images.
    self.container_image_uri = 'us-docker.pkg.dev/vertex-ai/vertex-vision-model-garden-dockers/pytorch-vllm-serve:20240222_0916_RC00'
    self.serving_container_command = [
        'python',
        '-m',
        'vllm.entrypoints.api_server',
    ]
    self.model_bucket_suffix = os.path.join('llama2', 'llama2-7b-hf')
    self.serving_container_args = VLLM_ARGS
    self.serving_container_ports = [7080]
    self.serving_container_predict_route = '/generate'
    self.serving_container_health_route = '/ping'
    self.machine_type = 'g2-standard-8'
    self.accelerator_type = 'NVIDIA_L4'

  def GetEnvironmentVariables(self, **kwargs) -> dict[str, str]:
    """Returns container's environment variables needed by Llama2."""
    return {
        'MODEL_ID': kwargs['model_bucket_path'],
        'DEPLOY_SOURCE': 'notebook',
    }

  def ConvertToInstances(
      self, prompt: str, max_tokens: int, temperature: float, **kwargs: Any
  ) -> list[dict[str, Any]]:
    """Converts input to the form expected by the model."""
    instances = {
        'prompt': prompt,
        'max_tokens': max_tokens,
        'temperature': temperature,
    }
    for params in ['top_p', 'top_k', 'raw_response']:
      if params in kwargs:
        instances[params] = kwargs[params]
    return [instances]
