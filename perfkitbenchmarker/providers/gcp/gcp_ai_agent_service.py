"""GCP-specific AI agent service resource."""

import json
import logging
import os
import re
import tarfile
from typing import Any, cast, override

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
  def _EnsureObjectStorage(self):
    """Ensures intermediate bucket for Agent communication exists."""
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
  def _StageAgentCode(self):
    self.client_vm.Install('pip')
    self._CheckVmCanCallVertexAI(
        cast(gce_virtual_machine.GceVirtualMachine, self.client_vm)
    )
    self.client_vm.RemoteCommand('mkdir -p workload')

    workload_name = f'{self.spec.workload}_{self.spec.framework}'
    workload_data_path = (
        f'agentic_framework/{self.spec.workload}/{self.spec.framework}'
    )

    tar_filename, tar_local_path = self._CreateWorkloadTarball(
        workload_name, workload_data_path
    )

    self.client_vm.PushDataFile(tar_local_path, f'workload/{tar_filename}')
    self.client_vm.RemoteCommand(f'cd workload && tar -xzf {tar_filename}')

    pyproject_toml_path = f'workload/{workload_name}/pyproject.toml'
    self.client_vm.RemoteCommand(f'ls {pyproject_toml_path}')
    self.client_vm.RemoteCommand(
        f'cd workload/{workload_name} && pip3 install .'
    )

    # Push generic run local script to VM
    run_remote_script_local_path = data.ResourcePath(
        'agentic_framework/run_local_agent.py'
    )
    self.client_vm.PushDataFile(
        run_remote_script_local_path,
        f'workload/{workload_name}/run_local_agent.py',
    )

  @override
  def _Create(self):
    pass

  @override
  def _Delete(self):
    pass

  @override
  def Execute(
      self,
      output_dir: str,
      prompt: str | None = None,
      agent_config: dict[str, Any] | None = None,
  ) -> None:
    """Runs the workload on the client VM."""
    location = self.spec.model_location or self.region
    workload_name = f'{self.spec.workload}_{self.spec.framework}'

    prompt_file_path = vm_util.PrependTempDir('prompt.txt')
    with open(prompt_file_path, 'w') as f:
      f.write(prompt or '')
    self.client_vm.PushDataFile(
        prompt_file_path, f'workload/{workload_name}/prompt.txt'
    )

    self.UploadRunConfigToClientVm(
        f'workload/{workload_name}/run_config.yaml',
        output_dir,
        'prompt.txt',
        agent_config,
    )

    command = (
        'export GOOGLE_GENAI_USE_VERTEXAI=TRUE &&'
        f' export GOOGLE_CLOUD_PROJECT={self.project} &&'
        f' export GOOGLE_CLOUD_LOCATION={location} &&'
        f' cd workload/{workload_name} &&'
        ' python3 run_local_agent.py'
        ' --config_file run_config.yaml'
    )
    self.client_vm.RobustRemoteCommand(command)

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

  def _GetDeploymentConfig(self) -> dict[str, Any]:
    """Gets config dict for deployment/creation. No-op for this class."""
    return {}


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
          f'{type(self).__name__} requires container_registry in benchmark'
          ' config spec.'
      )

  @override
  def _StageAgentCode(self):
    workload_data_path = (
        f'agentic_framework/{self.spec.workload}/{self.spec.framework}'
    )
    benchmark_spec = context.GetThreadBenchmarkSpec()
    self._image_uri = benchmark_spec.container_registry.GetOrBuild(
        os.path.basename(workload_data_path), workload_data_path
    )

  @override
  def _Create(self):
    pass

  @override
  def _Delete(self):
    pass

  @override
  def Execute(
      self,
      output_dir: str,
      prompt: str | None = None,
      agent_config: dict[str, Any] | None = None,
  ):
    """Triggers the Custom Job and blocks until completion."""
    job_name = f'pkb-{FLAGS.run_uri}-{self.job_count}'
    self.job_count += 1
    location = self.spec.model_location or self.region

    prompt_file_path = vm_util.PrependTempDir('prompt.txt')
    with open(prompt_file_path, 'w') as f:
      f.write(prompt or '')
    prompt_client_path = f'workload/{job_name}_prompt.txt'
    self.client_vm.PushDataFile(prompt_file_path, prompt_client_path)
    prompt_gcs_path = f'{self.base_dir}/{job_name}_prompt.txt'
    self.client_vm.RemoteCommand(
        f'gcloud storage cp {prompt_client_path} {prompt_gcs_path}'
    )

    prompt_fuse_path = f'/gcs/{self._bucket}/{job_name}_prompt.txt'
    run_config_client_path = f'workload/{job_name}_run_config.yaml'
    self.UploadRunConfigToClientVm(
        run_config_client_path, output_dir, prompt_fuse_path, agent_config
    )
    run_config_gcs_path = f'{self.base_dir}/{job_name}_run_config.yaml'
    self.client_vm.RemoteCommand(
        f'gcloud storage cp {run_config_client_path} {run_config_gcs_path}'
    )
    run_config_fuse_path = f'/gcs/{self._bucket}/{job_name}_run_config.yaml'

    config = {
        'workerPoolSpecs': [{
            'machineSpec': {
                'machineType': self._machine_type,
            },
            'replicaCount': self._replica_count,
            'containerSpec': {
                'imageUri': self._image_uri,
                'args': [
                    f'--config_file={run_config_fuse_path}',
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

  def _GetDeploymentConfig(self) -> dict[str, Any]:
    """Gets config dict for deployment/creation. No-op for this class."""
    return {}


class VertexAiAgentEngineAiAgentService(GcpAiAgentService):
  """Object representing a Vertex AI Agent Engine AI agent service."""

  DEPLOYMENT_TYPE = 'agent_engine'

  def __init__(self, client_vm, ai_agent_spec):
    super().__init__(client_vm, ai_agent_spec)
    self._remote_agent_name = None
    self._staging_bucket = self.base_dir
    self.spec = ai_agent_spec

  @override
  def _StageAgentCode(self):
    self.client_vm.Install('pip')
    workload = self.spec.workload
    framework = self.spec.framework
    workload_framework = f'{workload}_{framework}'
    workload_dir_local_path = f'agentic_framework/{workload}/{framework}'

    logging.info(
        'Preparing workload %s for deployment from client VM', workload
    )

    # 1. Setup directories on VM
    self.client_vm.RemoteCommand('mkdir -p workload')

    # 2. Create tarball of agent code and push to VM
    tar_filename, tar_local_path = self._CreateWorkloadTarball(
        workload_framework, workload_dir_local_path
    )
    self.client_vm.PushDataFile(tar_local_path, f'workload/{tar_filename}')
    self.client_vm.RemoteCommand(f'cd workload && tar -xzf {tar_filename}')

    # Install dependencies and build wheel on client VM
    self.client_vm.RemoteCommand(
        f'cd workload/{workload_framework} && pip3 install . && pip3 install'
        ' build && python3 -m build'
    )

    # 3. Push generic deployment script to VM
    deploy_script_local_path = data.ResourcePath(
        'agentic_framework/deploy_agent_engine.py'
    )
    self.client_vm.PushDataFile(
        deploy_script_local_path,
        f'workload/{workload_framework}/deploy_agent_engine.py',
    )

    # 4. Push generic remote run script to VM
    run_remote_script_local_path = data.ResourcePath(
        'agentic_framework/run_agent_engine.py'
    )
    self.client_vm.PushDataFile(
        run_remote_script_local_path,
        f'workload/{workload_framework}/run_agent_engine.py',
    )

    self._GrantPermissionToReasoningEngine()

  def _GetDeploymentConfig(self) -> dict[str, Any]:
    """Gets config dict for deployment/creation."""
    config = {
        'workload': self.spec.workload,
        'framework': self.spec.framework,
        'staging_bucket': self._staging_bucket,
        'agent_config': self.agent_config,
    }
    return config

  def _Create(self):
    """Initializes Vertex AI and deploys agent."""
    workload = self.spec.workload
    framework = self.spec.framework
    workload_framework = f'{workload}_{framework}'

    self.UploadDeployConfigToClientVm(
        f'workload/{workload_framework}/deploy_config.yaml'
    )

    # 4. Trigger deployment script on VM
    # TODO(odiego): Honor model_location. There are models with only global
    # endpoints.
    location = self.region
    command_parts = [
        f'export GOOGLE_CLOUD_PROJECT={self.project}',
        f'export GOOGLE_CLOUD_LOCATION={location}',
    ]

    command_parts.append(f'cd workload/{workload_framework}')
    deploy_cmd = 'python3 deploy_agent_engine.py --config deploy_config.yaml'
    command_parts.append(deploy_cmd)
    command = ' && '.join(command_parts)
    stdout, _ = self.client_vm.RemoteCommand(command)

    # 5. Parse output to get remote agent name
    for line in stdout.split('\n'):
      if line.startswith('Resource name: '):
        _, _, agent_name = line.partition('Resource name: ')
        self._remote_agent_name = agent_name.strip()
        break

    if not self._remote_agent_name:
      raise errors.Benchmarks.PrepareException(
          'Failed to get remote agent name from deploy script output.'
      )

    logging.info(
        'Successfully triggered Agent Engine deployment from VM. Remote Agent'
        ' Name: %s',
        self._remote_agent_name,
    )

  def _Delete(self):
    """Deletes the remote agent."""
    if not self._remote_agent_name:
      raise errors.Resource.CleanupError(
          'Cannot delete Agent Engine: remote agent name is missing.'
      )
    logging.info(
        'Deleting remote agent %s via client VM using SDK...',
        self._remote_agent_name,
    )
    delete_script = (
        'import vertexai; '
        f"vertexai.init(project='{self.project}', location='{self.region}'); "
        'client = vertexai.Client(); '
        'op = client.agent_engines.delete('
        f"name='{self._remote_agent_name}', force=True); "
        "op.result() if hasattr(op, 'result') else None"
    )
    delete_cmd = (
        f'export GOOGLE_CLOUD_PROJECT={self.project} && '
        f'export GOOGLE_CLOUD_LOCATION={self.region} && '
        f'python3 -c "{delete_script}"'
    )
    self.client_vm.RemoteCommand(delete_cmd)
    logging.info(
        'Successfully deleted remote agent %s.', self._remote_agent_name
    )

  def _GrantPermissionToReasoningEngine(self) -> None:
    """Grants permission to Reasoning Engine service account on the bucket."""

    # Get project number using describe
    cmd = util.GcloudCommand(None, 'projects', 'describe', self.project)
    cmd.flags['format'] = 'value(projectNumber)'
    stdout, _, _ = cmd.Issue(raise_on_failure=True)
    project_number = stdout.strip()

    # TODO(odiego): Allow customizing service account to pass each agent one
    # with minimum privileges.
    service_account = (
        f'service-{project_number}@gcp-sa-aiplatform-re.iam.gserviceaccount.com'
    )

    logging.info(
        'Granting storage.objectAdmin to %s on %s',
        service_account,
        self._staging_bucket,
    )

    try:
      cmd = util.GcloudCommand(
          None,
          'storage',
          'buckets',
          'add-iam-policy-binding',
          self._staging_bucket,
      )
      cmd.flags['member'] = f'serviceAccount:{service_account}'
      cmd.flags['role'] = 'roles/storage.objectAdmin'
      cmd.Issue(raise_on_failure=True)
    except errors.VmUtil.IssueCommandError as e:
      default_service_account_missing = re.search(
          r'Service account \S+ does not exist\.', str(e)
      )
      if default_service_account_missing:
        # The fact new service accounts take some seconds to propagate
        # needlessly complicates the logic unless you sleep ~1 minute on every
        # run. Since this is a once-per-project thing, I better just surface
        # this message to let the user what they have to do.
        error_msg = (
            'Default Vertex AI service account is not created for this project.'
            ' Run "gcloud beta services identity create'
            f' --service=aiplatform.googleapis.com --project={self.project}",'
            ' wait a minute, then retry.'
        )
        raise errors.Resource.CreationError(error_msg) from e
      raise

  @override
  def _GetRunConfig(
      self,
      output_dir: str,
      prompt_file: str,
      agent_config: dict[str, Any] | None = None,
  ) -> dict[str, Any]:
    """Gets config dict for running the agent."""
    config = super()._GetRunConfig(output_dir, prompt_file, agent_config)
    config['agent_engine_id'] = self._remote_agent_name
    return config

  @override
  def Execute(
      self,
      output_dir: str,
      prompt: str | None = None,
      agent_config: dict[str, Any] | None = None,
  ) -> None:
    """Runs the agent on Vertex AI Agent Engine."""
    if not self._remote_agent_name:
      raise errors.Benchmarks.RunError(
          'Agent not deployed. Call Create() first.'
      )

    workload = self.spec.workload
    framework = self.spec.framework
    workload_framework = f'{workload}_{framework}'

    logging.info('Running agent on Vertex AI Agent Engine via client VM...')

    prompt_file_path = vm_util.PrependTempDir('prompt.txt')
    with open(prompt_file_path, 'w') as f:
      f.write(prompt or '')
    self.client_vm.PushDataFile(
        prompt_file_path, f'workload/{workload_framework}/prompt.txt'
    )

    self.UploadRunConfigToClientVm(
        f'workload/{workload_framework}/run_config.yaml',
        output_dir,
        'prompt.txt',
        agent_config,
    )

    location = self.region
    command = (
        f'export GOOGLE_CLOUD_PROJECT={self.project} && export'
        f' GOOGLE_CLOUD_LOCATION={location} && cd workload/{workload_framework}'
        ' && python3 run_agent_engine.py --config_file run_config.yaml'
    )
    stdout, _ = self.client_vm.RemoteCommand(command)

    logging.info(
        'Agent execution finished. Raw output:\n%s',
        stdout,
    )
