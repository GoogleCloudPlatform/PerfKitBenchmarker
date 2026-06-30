# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Module containing implementation of Google Cloud Batch jobs.

More details at https://cloud.google.com/batch/docs
"""

import json
import os
import time
from typing import Any, Dict, Optional

from absl import flags
import dateutil.parser
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.resources import base_job
from perfkitbenchmarker.resources import jobs_setter

CLOUD_BATCH_PRODUCT = 'GoogleCloudBatchJob'
_JOB_SPEC = jobs_setter.BaseJobSpec


class GoogleCloudBatchJobsSpec(_JOB_SPEC):
  """Specification for Google Cloud Batch jobs."""

  SERVICE = CLOUD_BATCH_PRODUCT
  CLOUD = 'GCP'


FLAGS = flags.FLAGS


class GoogleCloudBatchJob(base_job.BaseJob):
  """Class representing a Google Cloud Batch Job."""

  SERVICE = CLOUD_BATCH_PRODUCT
  _default_region = 'us-central1'
  DEFAULT_MACHINE_TYPE = 'c4-standard-2'

  def __init__(self, job_spec: _JOB_SPEC, container_registry):
    super().__init__(job_spec, container_registry)
    self.region = self.region or self._default_region
    self.user_managed = False
    self.project = FLAGS.project or util.GetDefaultProject()
    self.machine_type = FLAGS.machine_type or self.DEFAULT_MACHINE_TYPE
    self.metadata.update({
        'Product_ID': self.SERVICE,
        'region': self.region,
        'machine_type': self.machine_type,
    })
    self.project_number = util.GetProjectNumber(self.project)
    self.config_path: Optional[str] = None
    self.start_timestamp: Optional[float] = None

  def _Create(self) -> None:
    # Omit computeResource when machineType is specified to allow
    # the task to use the full VM resources.
    config = {
        'taskGroups': [{
            'taskSpec': {
                'runnables': [
                    {'container': {'imageUri': self.container_image}}
                ],
            },
            'taskCount': self.task_count,
        }],
        'allocationPolicy': {
            'instances': [{
                'policy': {
                    'machineType': self.machine_type,
                    'provisioningModel': 'STANDARD',
                }
            }]
        },
        'logsPolicy': {'destination': 'CLOUD_LOGGING'},
    }

    self.config_path = os.path.join(
        vm_util.GetTempDir(), f'{self.name}_config.json'
    )
    with open(self.config_path, 'w') as f:
      json.dump(config, f, indent=2)

  def _Delete(self) -> None:
    if not self._Exists():
      return

    if self.config_path and os.path.exists(self.config_path):
      os.remove(self.config_path)

    cmd = [
        'gcloud',
        'batch',
        'jobs',
        'delete',
        self.name,
        f'--location={self.region}',
        f'--project={self.project}',
        '--quiet',
    ]
    vm_util.IssueCommand(cmd)

  def _Exists(self) -> bool:
    # If the job has not been submitted yet, it only exists as a local config
    if not self.submit_timestamp:
      return bool(self.config_path and os.path.exists(self.config_path))

    cmd = [
        'gcloud',
        'batch',
        'jobs',
        'describe',
        self.name,
        f'--location={self.region}',
        f'--project={self.project}',
        '--format=json',
    ]
    try:
      stdout, _, _ = vm_util.IssueCommand(cmd)
      job_data = json.loads(stdout)
      state = job_data.get('status', {}).get('state')

      # If the job is being deleted, treat it as non-existent. Google Cloud
      # automatically deletes a Cloud Batch job and its history 60 days after
      # it finishes (succeeds, fails, or is cancelled), and all underlying GCE
      # VMs are immediately cleaned up. Therefore, we do not need to block the
      # PKB teardown phase waiting for the metadata-only deletion status to
      # fully complete.
      # Reference: https://cloud.google.com/batch/docs/delete-job
      if state == 'DELETION_IN_PROGRESS':
        return False

      return True
    except errors.VmUtil.IssueCommandError:
      return False

  def Execute(self) -> None:
    self.submit_timestamp = time.time()

    submit_cmd = [
        'gcloud',
        'batch',
        'jobs',
        'submit',
        self.name,
        f'--location={self.region}',
        f'--project={self.project}',
        f'--config={self.config_path}',
        '--format=json',
    ]
    vm_util.IssueCommand(submit_cmd)

    self.last_execution_start_time = time.time()

    describe_cmd = [
        'gcloud',
        'batch',
        'jobs',
        'describe',
        self.name,
        f'--location={self.region}',
        f'--project={self.project}',
        '--format=json',
    ]

    @vm_util.Retry(
        poll_interval=10,
        timeout=300,
        retryable_exceptions=(errors.Resource.RetryableCreationError,),
    )
    def _WaitForCompletion():
      stdout, _, _ = vm_util.IssueCommand(describe_cmd)
      job_data = json.loads(stdout)
      state = job_data.get('status', {}).get('state')

      if state in ('QUEUED', 'SCHEDULED', 'RUNNING'):
        raise errors.Resource.RetryableCreationError(
            f'Cloud Batch job not completed yet (state: {state}).'
        )
      if state != 'SUCCEEDED':
        raise errors.Resource.CreationError(
            f'Cloud Batch job failed with state: {state}.'
        )
      return job_data

    job_data = _WaitForCompletion()

    self.last_execution_end_time = time.time()

    events = job_data.get('status', {}).get('statusEvents', [])
    running_event = next(
        (
            e
            for e in events
            if e.get('type') == 'STATUS_CHANGED'
            and 'to RUNNING' in e.get('description', '')
        ),
        None,
    )
    if running_event:
      self.start_timestamp = dateutil.parser.parse(
          running_event.get('eventTime')
      ).timestamp()

  def GetResourceMetadata(self) -> Dict[str, Any]:
    metadata = super().GetResourceMetadata()
    metadata.update({
        'job_compute_type': 'GCE',
        'config_path': self.config_path,
    })
    return metadata

  def GetMonitoringLink(self) -> str:
    return (
        'https://console.cloud.google.com/batch/jobs/details/'
        f'{self.region}/{self.name}?project={self.project}'
    )
