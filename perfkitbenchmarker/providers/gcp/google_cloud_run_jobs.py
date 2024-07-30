"""Module containing implementation of the GoogleCloudRunJob resource.

This module contains classes responsible for preparing the deployment artifacts
and managing lifecycle of Google Cloud Run Jobs. More details at
https://cloud.google.com/run/docs/quickstarts/jobs/create-execute
"""

import logging
import threading
from absl import flags
from perfkitbenchmarker import errors
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

  def _Exists(self):
    cmd = [
        'gcloud',
        'run',
        'jobs',
        'describe',
        self.name,
        '--region=%s' % self.region,
        '--project=p3rf-serverless-gcf2',
    ]
    try:
      out = vm_util.IssueCommand(cmd)
      logging.info(out)
    except (KeyError, ValueError, errors.VmUtil.IssueCommandError):
      logging.error(
          'Cloud Run Job: %s, in region: %s could not be found.',
          self.name,
          self.region,
      )
      return False
    return True

  def Execute(self):
    """Executes the job."""
    # https://cloud.google.com/sdk/gcloud/reference/run/jobs/execute

    cmd = [
        'gcloud',
        'run',
        'jobs',
        'execute',
        self.name,
        '--wait',  # wait for the execution to complete.
        '--region=%s' % self.region,
        '--project=p3rf-serverless-gcf2',
    ]
    vm_util.IssueCommand(cmd)
