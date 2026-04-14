"""GCP-specific AI agent service resource."""

import json
import logging
import os
import tarfile
from typing import cast, override

from absl import flags
from perfkitbenchmarker import context
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import gcs
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.resources import ai_agent_service

FLAGS = flags.FLAGS


class JobNotDoneError(Exception):
  pass


class GcpAiAgentService(ai_agent_service.BaseAiAgentService):
  """GCP base class for AI agent services."""

  CLOUD = 'GCP'

  def __init__(self, client_vm, ai_agent_spec):
    super().__init__(client_vm, ai_agent_spec)
    self.project = FLAGS.project
    self.region = util.GetRegionFromZone(FLAGS.zone[0])
    self._storage_service = gcs.GoogleCloudStorageService()
    # TODO(odiego): Add support for user managed bucket
    self.manage_bucket = True
    self._bucket = 'pkb-' + FLAGS.run_uri

  @property
  def storage_service(self):
    return self._storage_service

  @override
  def _CreateDependencies(self):
    """Creates the GCS bucket dependency and discovers service account."""
    if self.region:
      self.storage_service.PrepareService(location=self.region)
    self.storage_service.MakeBucket(self._bucket)

  @override
  def _DeleteDependencies(self):
    """Deletes the GCS bucket dependency."""
    if self.manage_bucket and self.storage_service:
      self.storage_service.DeleteBucket(self._bucket)

  @property
  def base_dir(self) -> str:
    return 'gs://' + self._bucket

  def _CreateWorkloadTarball(
      self, workload_name: str, workload_data_path: str
  ) -> tuple[str, str]:
    """Creates tarball of the workload. Returns the filename and local path."""
    tar_filename = f'{workload_name}.tar.gz'
    tar_local_path = vm_util.PrependTempDir(tar_filename)
    full_local_workload_data_path = data.ResourcePath(workload_data_path)

    with tarfile.open(tar_local_path, 'w:gz') as tar:
      tar.add(
          full_local_workload_data_path,
          arcname=workload_name,
      )
    return tar_filename, tar_local_path


class JobNotCompleteError(Exception):
  """Exception raised when a Custom Job is still running and needs more time."""


class GcpClientVmAiAgentService(GcpAiAgentService):
  """AI agent service running on a GCP client VM."""

  DEPLOYMENT_TYPE = 'client_vm'

  @override
  def _Create(self):
    self.client_vm.Install('pip')

  @override
  def _Delete(self):
    pass

  @override
  def Execute(
      self,
      workload_name: str,
      model: str,
      output_dir: str,
      model_location: str | None = None,
  ) -> None:
    """Runs the workload on the client VM."""
    location = model_location or self.region
    command = (
        'export GOOGLE_GENAI_USE_VERTEXAI=TRUE &&'
        f' export GOOGLE_CLOUD_PROJECT={self.project} &&'
        f' export GOOGLE_CLOUD_LOCATION={location} &&'
        f' cd workload/{workload_name} &&'
        f' python3 {workload_name}.py'
        f' --model {model}'
        f' --output_dir {output_dir}'
    )
    self.client_vm.RobustRemoteCommand(command)

  @override
  def PrepareWorkload(self, workload_name: str, workload_data_path: str):
    """Packages, transfers, and sets up the agent code for this service."""
    self._CheckVmCanCallVertexAI(
        cast(gce_virtual_machine.GceVirtualMachine, self.client_vm)
    )
    self.client_vm.RemoteCommand('mkdir -p workload')

    tar_filename, tar_local_path = self._CreateWorkloadTarball(
        workload_name, workload_data_path
    )

    self.client_vm.PushDataFile(tar_local_path, f'workload/{tar_filename}')
    self.client_vm.RemoteCommand(f'cd workload && tar -xzf {tar_filename}')

    pyproject_toml_path = f'workload/{workload_name}/pyproject.toml'
    try:
      self.client_vm.RemoteCommand(f'ls {pyproject_toml_path}')
      self.client_vm.RemoteCommand(
          f'cd workload/{workload_name} && pip3 install .'
      )
    except errors.VirtualMachine.RemoteCommandError:
      logging.info('No pyproject.toml found in %s', pyproject_toml_path)

  def _CheckVmCanCallVertexAI(
      self, client_vm: gce_virtual_machine.GceVirtualMachine
  ):
    """Check if the VM has permissions to call Vertex AI otherwise fail early."""
    try:
      logging.info('Checking Vertex AI permissions on VM...')
      # Fetch models list as a way to verify the Service Account has
      # aiplatform.user
      client_vm.RemoteCommand(
          f'export GOOGLE_CLOUD_PROJECT={client_vm.project} && '
          'gcloud ai models list --limit=1 '
          f'--region={util.GetRegionFromZone(client_vm.zone)} --quiet'
      )
    except errors.VirtualMachine.RemoteCommandError as e:
      raise errors.Benchmarks.PrepareException(
          'VM lacks permissions to call Vertex AI. Ensure Vertex AI API is'
          ' enabled, gcloud_scopes includes "cloud-platform" and the Service'
          ' Account has "roles/aiplatform.user".'
      ) from e


class VertexAiCustomJobAiAgentService(GcpAiAgentService):
  """Object representing a Vertex AI Custom Job AI agent service."""

  DEPLOYMENT_TYPE = 'custom_job'

  # Vertex AI Custom Job API States
  STATE_SUCCEEDED = 'JOB_STATE_SUCCEEDED'
  STATE_FAILED = 'JOB_STATE_FAILED'
  STATE_CANCELLED = 'JOB_STATE_CANCELLED'
  STATE_EXPIRED = 'JOB_STATE_EXPIRED'

  # Helper tuple for terminal failure states
  TERMINAL_FAIL_STATES = (STATE_FAILED, STATE_CANCELLED, STATE_EXPIRED)

  def __init__(self, client_vm, ai_agent_spec):
    super().__init__(client_vm, ai_agent_spec)
    self.job_count = 0
    self._replica_count = ai_agent_spec.replica_count
    self._machine_type = ai_agent_spec.machine_type
    self._image_uri: str | None = None

  @override
  def CheckPrerequisites(self):
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if not benchmark_spec.container_registry:
      raise errors.Config.MissingOption(
          f'{type(self)} requires container_registry in benchmark config spec.'
      )

  @override
  def _Create(self):
    pass

  @override
  def _Delete(self):
    pass

  @override
  def PrepareWorkload(self, workload_name: str, workload_data_path: str):
    """Packages, transfers, and sets up the agent code for this service."""
    benchmark_spec = context.GetThreadBenchmarkSpec()
    self._image_uri = benchmark_spec.container_registry.GetOrBuild(
        os.path.basename((workload_data_path)), workload_data_path
    )

  @override
  def Execute(
      self,
      workload_name: str,
      model: str,
      output_dir: str,
      model_location: str | None = None,
  ):
    """Triggers the Custom Job and blocks until completion."""
    job_name = f'pkb-{FLAGS.run_uri}-{self.job_count}'
    self.job_count += 1
    location = model_location or self.region

    config = {
        'workerPoolSpecs': [{
            'machineSpec': {
                'machineType': self._machine_type,
            },
            'replicaCount': self._replica_count,
            'containerSpec': {
                'imageUri': self._image_uri,
                'args': [
                    f'--model={model}',
                    f'--output_dir={output_dir}',
                ],
                'env': [
                    {'name': 'GOOGLE_GENAI_USE_VERTEXAI', 'value': 'TRUE'},
                    {'name': 'GOOGLE_CLOUD_PROJECT', 'value': self.project},
                    {'name': 'GOOGLE_CLOUD_LOCATION', 'value': location},
                ],
            },
        }]
    }

    config_filename = f'{job_name}_config.json'
    config_local_path = vm_util.PrependTempDir(config_filename)
    config_json_str = json.dumps(config, indent=2)
    logging.info(
        'About to create AI Custom Job with this config:\n%s', config_json_str
    )
    with open(config_local_path, 'w') as f:
      f.write(config_json_str)

    cmd = util.GcloudCommand(self, 'ai', 'custom-jobs', 'create')
    cmd.flags['region'] = self.region
    cmd.flags['display-name'] = job_name
    cmd.flags['config'] = config_local_path
    stdout, _, _ = cmd.Issue()

    try:
      response = json.loads(stdout)
      job_id = response['name'].split('/')[-1]
      logging.info('Triggered Vertex AI Custom Job with ID: %s', job_id)
    except (KeyError, ValueError) as e:
      raise errors.Benchmarks.RunError(
          f'Failed to parse job execution output: {stdout}'
      ) from e

    self._WaitForJobCompletion(job_id)

  def _GetJobStatus(self, job_id: str) -> str:
    """Helper to fetch the exact current state of the job."""
    cmd = util.GcloudCommand(self, 'ai', 'custom-jobs', 'describe', job_id)
    cmd.flags['region'] = self.region
    cmd.flags['format'] = 'value(state)'
    stdout, _, _ = cmd.Issue()
    return stdout.strip()

  @vm_util.Retry(
      poll_interval=15,
      timeout=3600,
      retryable_exceptions=(JobNotCompleteError,),
      fuzz=0.1,
  )
  def _WaitForJobCompletion(self, job_id: str):
    """Waits for the Vertex AI Custom Job to complete."""
    status = self._GetJobStatus(job_id)
    logging.info('Job %s current status: %s', job_id, status)

    if status == self.STATE_SUCCEEDED:
      return True
    elif status in self.TERMINAL_FAIL_STATES:
      raise errors.Benchmarks.RunError(
          f'Job {job_id} failed with terminal status: {status}'
      )
    else:
      raise JobNotCompleteError(
          f'Job {job_id} is not finished. Status: {status}'
      )
