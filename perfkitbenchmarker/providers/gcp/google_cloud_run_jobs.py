"""Module containing implementation of GoogleCloudRun service and builder.

This module contains classes responsible for preparing the deployment artifacts
and managing lifecycle of Google Cloud Run jobs. More details at
https://cloud.google.com/run/docs/quickstarts/jobs/create-execute
"""

import logging
import time

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.resources import base_job
from perfkitbenchmarker.resources import jobs_setter


CLOUD_RUN_PRODUCT = 'GoogleCloudRunJob'
_JOB_SPEC = jobs_setter.BaseJobSpec
# For more info, see:
# https://cloud.google.com/blog/products/serverless/cloud-run-gets-always-on-cpu-allocation
_TWO_HOURS = 60 * 60 * 2  # Two hours in seconds


class GoogleCloudRunJobsSpec(_JOB_SPEC):
  """Spec storing various data needed to create cloud run jobs."""

  SERVICE = CLOUD_RUN_PRODUCT
  CLOUD = 'GCP'

FLAGS = flags.FLAGS


class GoogleCloudRunJob(base_job.BaseJob):
  """Class representing a Google cloud Run Job / instance."""

  SERVICE = CLOUD_RUN_PRODUCT
  _default_region = 'us-central1'

  def __init__(self, job_spec: _JOB_SPEC, container_registry):
    super().__init__(job_spec, container_registry)
    self.region = self.region or self._default_region
    self.user_managed = False
    self.project = FLAGS.project or util.GetDefaultProject()
    self.metadata.update({
        'Product_ID': self.SERVICE,
        'region': self.region,
        'Run_Environment': 'gen2',
    })
    self.project_number = util.GetProjectNumber(self.project)

  def _Create(self) -> None:
    """Creates the underlying resource."""
    # https://cloud.google.com/sdk/gcloud/reference/run/jobs/create
    cmd = ['gcloud']
    # TODO(user): Remove alpha once the feature is GA.
    if self.job_gpu_type or self.job_gpu_count:
      cmd.append('alpha')

    cmd.extend([
        'run',
        'jobs',
        'create',
        self.name,
        '--image=%s' % self.container_image,
        '--region=%s' % self.region,
        '--memory=%s' % self.backend,
        '--project=%s' % self.project,
        '--tasks=%s' % self.task_count,
    ])

    if self.job_gpu_type:
      cmd.append('--gpu-type=%s' % self.job_gpu_type)
    if self.job_gpu_count:
      cmd.append('--gpu=%s' % self.job_gpu_count)

    self.metadata.update({
        'container_image': self.container_image,
    })
    logging.info(
        'Created Cloud Run Job. View internal stats at:\n%s',
        self.GetMonitoringLink(),
    )
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
        '--project=%s' % self.project,
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
        '--project=%s' % self.project,
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
    self.last_execution_start_time = time.time()
    cmd = [
        'gcloud',
        'run',
        'jobs',
        'execute',
        self.name,
        '--wait',  # wait for the execution to complete.
        '--region=%s' % self.region,
        '--project=%s' % self.project,
    ]
    vm_util.IssueCommand(cmd)
    self.last_execution_end_time = time.time()

  def GetMonitoringLink(self):
    """Returns link to internal Cloud Run Jobsmonitoring page."""

    utc_end = int(time.time()) + _TWO_HOURS - 5 * 60

    return (
        'https://monitoring.corp.google.com/dashboard/run/jobs%2Finstances?'
        f'scope=cloud_project_number%3D{self.project_number}&'
        f'duration={_TWO_HOURS}&filters=module%3D{self.name}&utc_end={utc_end}'
    )
