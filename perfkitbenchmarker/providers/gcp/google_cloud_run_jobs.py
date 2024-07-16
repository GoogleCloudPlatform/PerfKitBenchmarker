"""Module containing implementation of the GoogleCloudRunJob resource.

This module contains classes responsible for preparing the deployment artifacts
and managing lifecycle of Google Cloud Run Jobs. More details at
https://cloud.google.com/run/docs/quickstarts/jobs/create-execute
"""

import threading
from absl import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS
CLOUD_RUN_PRODUCT = 'GoogleCloudRunJobs'


class GoogleCloudRunJob(resource.BaseResource):
  """Class representing a Google Cloud Run Job."""

  RESOURCE_TYPE: str = 'CloudRunJob'
  REQUIRED_ATTRS: list[str] = ['SERVICE']
  SERVICE: str
  SERVICE = CLOUD_RUN_PRODUCT
  _default_region = 'us-central1'
  _job_counter: int = 0
  _job_counter_lock = threading.Lock()

  def __init__(self, region, image):
    """Initializes the GoogleCloudRunJob object."""
    super().__init__()
    self.name: str = self._GenerateName()
    self.image = image
    self.region = self._default_region or region
    self.user_managed = False
    # Cloud Run jobs only use the second generation execution environment.
    # For details, see
    # https://cloud.google.com/run/docs/configuring/execution-environments

  def _GenerateName(self) -> str:
    """Generates a unique name for the job.

    Locking the counter variable allows for each created job name to be
    unique within the python process.

    Returns:
      The newly generated name.
    """
    with self._job_counter_lock:
      self.job_number: int = self._job_counter
      name: str = f'pkb-{FLAGS.run_uri}'
      name += f'-{self.job_number}'
      GoogleCloudRunJob._job_counter += 1
      return name

  def _Create(self):
    """Creates the underlying resource."""
    # https://cloud.google.com/sdk/gcloud/reference/run/jobs/create
    cmd = [
        'gcloud',
        'run',
        'jobs',
        'create',
        self.name,
        '--image=%s' % self.image,
        '--region=%s' % self.region,
        '--memory=%s' % '1Gi',
        '--project=p3rf-serverless-gcf2',
    ]

    vm_util.IssueCommand(cmd)
    # future work: use vm.RemoteCommand instead by setting up a vm via spec.

  def _Delete(self):
    """Deletes the underlying resource."""
    # https://cloud.google.com/sdk/gcloud/reference/run/jobs/delete
    cmd = [
        'gcloud',
        'run',
        'jobs',
        'delete',
        self.name,
        '--region=%s' % self.region,
        '--project=p3rf-serverless-gcf2',
        '--quiet',
    ]

    vm_util.IssueCommand(cmd)
    # future work: use vm.RemoteCommand instead by setting up a vm via spec.
