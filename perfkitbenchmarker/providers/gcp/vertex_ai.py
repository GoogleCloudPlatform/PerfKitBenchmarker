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

import json
import logging
import os
import re
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
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.providers.gcp import flags as gcp_flags
from perfkitbenchmarker.providers.gcp import gcs
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.resources import managed_ai_model
from perfkitbenchmarker.resources import managed_ai_model_spec

FLAGS = flags.FLAGS


SERVICE_ACCOUNT_BASE = '{}-compute@developer.gserviceaccount.com'
VLLM_ARGS = [
    '--host=0.0.0.0',
    '--port=7080',
    '--swap-space=16',
    '--gpu-memory-utilization=0.9',
    '--max-model-len=2048',
    '--max-num-batched-tokens=4096',
]


class VertexAiModelInRegistry(managed_ai_model.BaseManagedAiModel):
  """Represents a Vertex AI model in the model registry.

  Attributes:
    model_name: The official name of the model in Model Garden, e.g. Llama2.
    name: The name of the created model in private model registry.
    model_resource_name: The full resource name of the created model, e.g.
      projects/123/locations/us-east1/models/1234.
    region: The region, derived from the zone.
    project: The project.
    endpoint: The PKB resource endpoint the model is deployed to.
    gcloud_model: Representation of the model in gcloud python library.
    service_account: Name of the service account used by the model.
    model_deploy_time: Time it took to deploy the model.
    model_upload_time: Time it took to upload the model.
    vm: A way to run commands on the machine.
    json_write_times: List of times it took to write the json request to disk.
    json_cache: Cache from request JSON -> JSON request file.
    gcs_bucket_copy_time: Time it took to copy the model to the GCS bucket.
    gcs_client: The GCS client used to copy the model to the GCS bucket. Only
      instantiated if ai_create_bucket flag is True.
    bucket_uri: The GCS bucket where the model is stored.
    model_bucket_path: Where the model bucket is located.
    staging_bucket: The staging bucket used by the model.
  """

  CLOUD = 'GCP'

  endpoint: 'VertexAiEndpoint'
  model_spec: 'VertexAiModelSpec'
  model_name: str
  name: str
  region: str
  project: str
  gcloud_model: aiplatform.Model | None
  service_account: str
  model_deploy_time: float | None
  model_upload_time: float | None
  json_write_times: list[float]
  json_cache: dict[str, str]
  gcs_bucket_copy_time: float | None
  gcs_client: gcs.GoogleCloudStorageService | None
  bucket_uri: str
  model_bucket_path: str
  staging_bucket: str

  def __init__(
      self,
      vm: virtual_machine.BaseVirtualMachine,
      model_spec: managed_ai_model_spec.BaseManagedAiModelSpec,
      name: str | None = None,
      bucket_uri: str | None = None,
      **kwargs,
  ):
    super().__init__(model_spec, vm, **kwargs)
    if not isinstance(model_spec, VertexAiModelSpec):
      raise errors.Config.InvalidValue(
          f'Invalid model spec class: "{model_spec.__class__.__name__}". '
          'Must be a VertexAiModelSpec. It had config values of '
          f'{model_spec.model_name} & {model_spec.cloud}'
      )
    self.model_spec = model_spec
    self.model_name = model_spec.model_name
    self.model_resource_name = None
    if name:
      self.name = name
    else:
      self.name = 'pkb' + FLAGS.run_uri
    self.project = FLAGS.project
    self.endpoint = VertexAiEndpoint(
        name=self.name, region=self.region, project=self.project, vm=self.vm
    )
    if not self.project:
      raise errors.Setup.InvalidConfigurationError(
          'Project is required for Vertex AI but was not set.'
      )
    self.gcloud_model = None
    self.metadata.update({
        'name': self.name,
        'model_name': self.model_name,
        'model_size': self.model_spec.model_size,
        'machine_type': self.model_spec.machine_type,
        'accelerator_type': self.model_spec.accelerator_type,
        'accelerator_count': self.model_spec.accelerator_count,
    })
    project_number = util.GetProjectNumber(self.project)
    self.service_account = SERVICE_ACCOUNT_BASE.format(project_number)
    self.model_upload_time = None
    self.model_deploy_time = None
    self.json_write_times = []
    self.json_cache = {}
    self.gcs_client = None
    if bucket_uri is not None:
      self.bucket_uri = bucket_uri
    elif gcp_flags.AI_BUCKET_URI.value is not None:
      self.bucket_uri = gcp_flags.AI_BUCKET_URI.value
    else:
      self.gcs_client = gcs.GoogleCloudStorageService()
      self.gcs_client.PrepareService(self.region)
      self.bucket_uri = f'{self.project}-{self.region}-tmp-{self.name}'
    self.model_bucket_path = 'gs://' + os.path.join(
        self.bucket_uri, self.model_spec.model_bucket_suffix
    )
    self.staging_bucket = 'gs://' + os.path.join(self.bucket_uri, 'temporal')
    self.gcs_bucket_copy_time = None

  def _InitializeNewModel(self) -> 'VertexAiModelInRegistry':
    """Returns a new instance of the same class."""
    return self.__class__(
        vm=self.vm,
        model_spec=self.model_spec,
        name=self.name + '2',
        # Reuse the same bucket for the next model.
        bucket_uri=self.bucket_uri,
    )

  def GetRegionFromZone(self, zone: str) -> str:
    return util.GetRegionFromZone(zone)

  def ListExistingEndpoints(self, region: str | None = None) -> list[str]:
    """Returns a list of existing model endpoint ids in the same region."""
    if region is None:
      region = self.region
    # Expected output example:
    # ENDPOINT_ID          DISPLAY_NAME
    # 12345                some_endpoint_name
    out, _, _ = self.vm.RunCommand(
        f'gcloud ai endpoints list --region={region} --project={self.project}'
    )
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
    if self.json_write_times:
      samples.append(
          sample.Sample(
              'Max JSON Write Time',
              max(self.json_write_times),
              'seconds',
              metadata,
          )
      )
    if self.gcs_bucket_copy_time:
      samples.append(
          sample.Sample(
              'GCS Bucket Copy Time',
              self.gcs_bucket_copy_time,
              'seconds',
              metadata,
          )
      )
    return samples

  def _SendPrompt(
      self, prompt: str, max_tokens: int, temperature: float, **kwargs: Any
  ) -> list[str]:
    """Sends a prompt to the model and returns the response."""
    instances = self.model_spec.ConvertToInstances(
        prompt, max_tokens, temperature, **kwargs
    )
    if gcp_flags.AI_USE_SDK.value:
      assert self.endpoint.ai_endpoint
      response = self.endpoint.ai_endpoint.predict(instances=instances)
      str_responses = [str(response) for response in response.predictions]
      return str_responses
    out, _, _ = self.vm.RunCommand(
        self.GetPromptCommand(prompt, max_tokens, temperature, **kwargs),
    )
    responses = out.strip('[]').split(',')
    return responses

  def GetPromptCommand(
      self, prompt: str, max_tokens: int, temperature: float, **kwargs: Any
  ) -> str:
    """Returns the command to send a prompt to the model."""
    instances = self.model_spec.ConvertToInstances(
        prompt, max_tokens, temperature, **kwargs
    )
    instances_dict = {'instances': instances, 'parameters': {}}
    start_write_time = time.time()
    json_dump = json.dumps(instances_dict)
    if json_dump in self.json_cache:
      name = self.json_cache[json_dump]
    else:
      name = self.vm.WriteTemporaryFile(json_dump)
      self.json_cache[json_dump] = name
    end_write_time = time.time()
    write_time = end_write_time - start_write_time
    self.json_write_times.append(write_time)
    return (
        'gcloud ai endpoints predict'
        f' {self.endpoint.endpoint_name} --json-request={name}'
    )

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
        labels=util.GetDefaultTags(),
    )
    self.model_resource_name = self.gcloud_model.resource_name
    end_model_upload = time.time()
    self.model_upload_time = end_model_upload - start_model_upload
    logging.info(
        'Model resource uploaded with name: %s in %s seconds',
        self.model_resource_name,
        self.model_upload_time,
    )
    try:
      start_model_deploy = time.time()
      self.gcloud_model.deploy(
          endpoint=self.endpoint.ai_endpoint,
          machine_type=self.model_spec.machine_type,
          accelerator_type=self.model_spec.accelerator_type,
          accelerator_count=self.model_spec.accelerator_count,
          deploy_request_timeout=1800,
          max_replica_count=self.max_scaling,
      )
      end_model_deploy = time.time()
      self.model_deploy_time = end_model_deploy - start_model_deploy
    except google_exceptions.ServiceUnavailable as ex:
      logging.info('Tried to deploy model but got unavailable error %s', ex)
      raise errors.Benchmarks.QuotaFailure(ex)

  def _CreateDependencies(self):
    if self.gcs_client:
      gcs_bucket_copy_start_time = time.time()
      self.gcs_client.MakeBucket(
          self.bucket_uri
      )  # pytype: disable=attribute-error
      self.gcs_client.Copy(
          self.model_spec.model_garden_bucket,
          self.model_bucket_path,
          recursive=True,
          timeout=60 * 40,
      )  # pytype: disable=attribute-error
      self.gcs_bucket_copy_time = time.time() - gcs_bucket_copy_start_time

    aiplatform.init(
        project=self.project,
        location=self.region,
        staging_bucket=self.staging_bucket,
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
    if self.gcs_client:
      self.gcs_client.DeleteBucket(
          self.bucket_uri
      )  # pytype: disable=attribute-error

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
  """Represents a Vertex AI endpoint.

  Attributes:
    name: The name of the endpoint.
    project: The project.
    region: The region, derived from the zone.
    endpoint_name: The full resource name of the created endpoint, e.g.
      projects/123/locations/us-east1/endpoints/1234.
    ai_endpoint: The AIPlatform object representing the endpoint.
  """

  def __init__(
      self,
      name: str,
      project: str,
      region: str,
      vm: virtual_machine.BaseVirtualMachine,
      **kwargs,
  ):
    super().__init__(**kwargs)
    self.name = name
    self.ai_endpoint = None
    self.project = project
    self.region = region
    self.vm = vm
    self.endpoint_name = None

  def _Create(self) -> None:
    """Creates the underlying resource."""
    logging.info('Creating the endpoint: %s.', self.name)
    if gcp_flags.AI_USE_SDK.value:
      self.ai_endpoint = aiplatform.Endpoint.create(
          display_name=f'{self.name}-endpoint'
      )
      return

    _, err, _ = self.vm.RunCommand(
        f'gcloud ai endpoints create --display-name={self.name}-endpoint'
        f' --project={self.project} --region={self.region}'
        f' --labels={util.MakeFormattedDefaultTags()}',
        ignore_failure=True,
    )
    self.endpoint_name = _FindRegexInOutput(
        err, r'Created Vertex AI endpoint: (.+)\.'
    )
    if not self.endpoint_name:
      raise errors.VmUtil.IssueCommandError(
          f'Could not find endpoint name in output {err}.'
      )
    logging.info('Successfully created endpoint %s', self.endpoint_name)
    self.ai_endpoint = aiplatform.Endpoint(self.endpoint_name)

  def _Delete(self) -> None:
    """Deletes the underlying resource."""
    logging.info('Deleting the endpoint: %s.', self.name)
    if gcp_flags.AI_USE_SDK.value:
      assert self.ai_endpoint
      self.ai_endpoint.delete(force=True)
      self.ai_endpoint = None  # Object is not picklable - none it out
      return
    out, _, _ = self.vm.RunCommand(
        f'gcloud ai endpoints describe {self.endpoint_name}',
    )
    model_id = _FindRegexInOutput(out, r'  id: \'(.+)\'')
    if model_id:
      self.vm.RunCommand(
          'gcloud ai endpoints undeploy-model'
          f' {self.endpoint_name} --deployed-model-id={model_id} --quiet',
      )
    else:
      if 'deployedModels:' not in out:
        logging.info(
            'No deployed models found; perhaps they failed to deploy or were'
            ' already deleted?'
        )
      else:
        raise errors.VmUtil.IssueCommandError(
            'Found deployed models but Could not find model id in'
            f' output.\n{out}'
        )
    self.vm.RunCommand(
        f'gcloud ai endpoints delete {self.endpoint_name} --quiet'
    )
    # None it out here as well, until all commands are supported over gcloud.
    self.ai_endpoint = None


def _FindRegexInOutput(output: str, regex: str) -> str | None:
  """Returns the first match of the regex in the output."""
  matches = re.search(regex, output)
  if not matches:
    return None
  return matches.group(1)


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
    model_bucket_suffix: Suffix with the particular version of the model (eg 7b)
    model_garden_bucket: The bucket in Model Garden to copy from.
  """

  CLOUD = 'GCP'

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)
    # The pre-built serving docker images.
    self.container_image_uri: str
    self.model_bucket_suffix: str
    self.model_garden_bucket: str
    self.serving_container_command: list[str]
    self.serving_container_args: list[str]
    self.serving_container_ports: list[int]
    self.serving_container_predict_route: str
    self.serving_container_health_route: str
    self.machine_type: str
    self.accelerator_count: int
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


class VertexAiLlama2Spec(VertexAiModelSpec):
  """Spec for running the Llama2 7b model."""

  MODEL_NAME: str = 'llama2'
  MODEL_SIZE: list[str] = ['7b', '70b']

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)
    # The pre-built serving docker images.
    self.container_image_uri = 'us-docker.pkg.dev/vertex-ai/vertex-vision-model-garden-dockers/pytorch-vllm-serve:20240222_0916_RC00'
    self.serving_container_command = [
        'python',
        '-m',
        'vllm.entrypoints.api_server',
    ]
    size_suffix = os.path.join('llama2', f'llama2-{self.model_size}-hf')
    self.model_garden_bucket = os.path.join(
        'gs://vertex-model-garden-public-us-central1', size_suffix
    )
    self.model_bucket_suffix = size_suffix
    self.serving_container_ports = [7080]
    self.serving_container_predict_route = '/generate'
    self.serving_container_health_route = '/ping'
    # Machine type from deployment notebook:
    # https://pantheon.corp.google.com/vertex-ai/colab/notebooks?e=13802955
    if self.model_size == '7b':
      self.machine_type = 'g2-standard-8'
      self.accelerator_count = 1
    else:
      self.machine_type = 'g2-standard-96'
      self.accelerator_count = 8
    self.accelerator_type = 'NVIDIA_L4'
    self.serving_container_args = VLLM_ARGS
    self.serving_container_args.append(
        f'--tensor-parallel-size={self.accelerator_count}'
    )

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
