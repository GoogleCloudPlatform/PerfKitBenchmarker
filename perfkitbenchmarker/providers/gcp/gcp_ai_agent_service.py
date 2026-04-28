"""GCP-specific AI agent service resource."""

import logging
import os

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
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

  def _CreateDependencies(self):
    """Creates the GCS bucket dependency and discovers service account."""
    if self.region:
      self.storage_service.PrepareService(location=self.region)
    self.storage_service.MakeBucket(self._bucket)

  def _DeleteDependencies(self):
    """Deletes the GCS bucket dependency."""
    if self.manage_bucket and self.storage_service:
      self.storage_service.DeleteBucket(self._bucket)

  @property
  def base_dir(self) -> str:
    return 'gs://' + self._bucket


class GcpClientVmAiAgentService(GcpAiAgentService):
  """AI agent service running on a GCP client VM."""

  DEPLOYMENT_TYPE = 'client_vm'

  def _Create(self):
    pass

  def _Delete(self):
    pass

  def Execute(
      self,
      workload_script: str,
      model: str,
      output_dir: str,
      model_location: str | None = None,
  ) -> None:
    """Runs the workload on the client VM."""
    local_workload_script = f'workload/{os.path.basename(workload_script)}'
    self.client_vm.RunCommand(
        f'gsutil cp {workload_script} {local_workload_script}'
    )
    location = model_location or self.region
    command = (
        'export GOOGLE_GENAI_USE_VERTEXAI=TRUE &&'
        f' export GOOGLE_CLOUD_PROJECT={self.project} &&'
        f' export GOOGLE_CLOUD_LOCATION={location} &&'
        f' python3 {local_workload_script}'
        f' --model {model}'
        f' --output_dir {output_dir}'
    )
    self.client_vm.RobustRemoteCommand(command)

  def PrepareClientVm(
      self, client_vm: virtual_machine.BaseVirtualMachine, packages: list[str]
  ):
    assert isinstance(client_vm, gce_virtual_machine.GceVirtualMachine)
    self.client_vm = client_vm
    self._CheckVmCanCallVertexAI(client_vm)
    client_vm.Install('google_cloud_storage')
    for package in packages:
      client_vm.Install(package)

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
