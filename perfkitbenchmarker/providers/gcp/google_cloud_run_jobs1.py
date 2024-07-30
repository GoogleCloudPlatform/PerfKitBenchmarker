"""Module containing implementation of GoogleCloudRun service and builder.

This module contains classes responsible for preparing the deployment artifacts
and managing lifecycle of Google Cloud Run jobs. More details at
https://cloud.google.com/run/docs/quickstarts/jobs/create-execute
"""

import logging
import time
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources import base_job
from perfkitbenchmarker.resources import jobs_setter


CLOUD_RUN_PRODUCT = 'GoogleCloudRunJob'
_JOB_SPEC = jobs_setter.BaseJobSpec
# For more info, see:
# https://cloud.google.com/blog/products/serverless/cloud-run-gets-always-on-cpu-allocation
_TWO_HOURS = 60 * 60 * 2  # Two hours in seconds
# The p3rf-serverless-gcf2 project number.
_P3RF_PROJECT_NUMBER = '89953586185'


class GoogleCloudRunJobsSpec(_JOB_SPEC):
  """Spec storing various data needed to create cloud run jobs."""

  SERVICE = CLOUD_RUN_PRODUCT
  CLOUD = 'GCP'


class GoogleCloudRunJob(base_job.BaseJob):
  """Class representing a Google cloud Run Job / instance."""

  SERVICE = CLOUD_RUN_PRODUCT
  _default_region = 'us-central1'

  def __init__(self, job_spec: _JOB_SPEC):
    super().__init__(job_spec)
    self.region = self.region or self._default_region
    self.user_managed = False
    self.metadata.update({
        'Product_ID': self.SERVICE,
        'region': self.region,
        'Run_Environment': 'gen2',
    })

  def _Create(self) -> None:
    """Creates the underlying resource."""
    # https://cloud.google.com/sdk/gcloud/reference/run/jobs/create
    cmd = [
        'gcloud',
        'run',
        'jobs',
        'create',
        self.name,
        # TODO(user): build the image at runtime instead of hardcoding.
        # Now using prebuilt image as logs didn't show up when built @ runtime.
        # The image below is from an echo workload.
        (
            '--image='
            'us-central1-docker.pkg.dev/p3rf-serverless-gcf2/cloud-run-source-deploy/cjob@sha256:a3ea8ff5143620165fccc702b78d7796b0bb4fcb4bfced57cb4fb0b53eadb99e'
        ),
        '--region=%s' % self.region,
        '--memory=%s' % self.backend,
        '--project=p3rf-serverless-gcf2',
    ]
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
    self.last_execution_start_time = time.time()
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
    self.last_execution_end_time = time.time()

  def GetMonitoringLink(self):
    """Returns link to internal Cloud Run Jobsmonitoring page."""

    utc_end = int(time.time()) + _TWO_HOURS - 5 * 60

    return (
        'https://monitoring.corp.google.com/dashboard/run/jobs%2Finstances?'
        f'scope=cloud_project_number%3D{_P3RF_PROJECT_NUMBER}&'
        f'duration={_TWO_HOURS}&filters=module%3D{self.name}&utc_end={utc_end}'
    )
