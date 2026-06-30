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
"""Module containing implementation of Google Cloud Run jobs.

More details at
https://cloud.google.com/run/docs/quickstarts/jobs/create-execute
"""

import json
import logging
import time
from typing import Any, Dict

from absl import flags
import dateutil.parser
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.resources import base_job
from perfkitbenchmarker.resources import jobs_setter

CLOUD_RUN_PRODUCT = 'GoogleCloudRunJob'
_JOB_SPEC = jobs_setter.BaseJobSpec
_TWO_HOURS = 60 * 60 * 2  # Two hours in seconds


class GoogleCloudRunJobsSpec(_JOB_SPEC):
  """Specification for Google Cloud Run jobs."""

  SERVICE = CLOUD_RUN_PRODUCT
  CLOUD = 'GCP'


FLAGS = flags.FLAGS


class GoogleCloudRunJob(base_job.BaseJob):
  """Class representing a Google Cloud Run Job."""

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
    self.start_timestamp = None

  def _Create(self) -> None:
    if not self.container_image:
      raise AttributeError('container_image must be set')
    cmd = ['gcloud']
    # TODO(user): Remove alpha once the feature is GA.
    if self.job_gpu_type or self.job_gpu_count:
      cmd.append('beta')

    cmd.extend([
        'run',
        'jobs',
        'create',
        self.name,
        f'--image={self.container_image}',
        f'--region={self.region}',
        f'--memory={self.backend}',
        f'--project={self.project}',
        f'--tasks={self.task_count}',
    ])

    if self.job_gpu_type:
      cmd.append(f'--gpu-type={self.job_gpu_type}')
    if self.job_gpu_count:
      cmd.append(f'--gpu={self.job_gpu_count}')
    if self.job_gpu_type or self.job_gpu_count:
      cmd.append('--no-gpu-zonal-redundancy')

    self.metadata.update({
        'container_image': self.container_image,
    })
    logging.info(
        'Created Cloud Run Job. View internal stats at:\n%s',
        self.GetMonitoringLink(),
    )
    vm_util.IssueCommand(cmd)

  def _Delete(self) -> None:
    if not self._Exists():
      return

    cmd = [
        'gcloud',
        'run',
        'jobs',
        'delete',
        self.name,
        f'--region={self.region}',
        f'--project={self.project}',
        '--quiet',
    ]
    vm_util.IssueCommand(cmd)

  def _Exists(self) -> bool:
    cmd = [
        'gcloud',
        'run',
        'jobs',
        'describe',
        self.name,
        f'--region={self.region}',
        f'--project={self.project}',
    ]
    try:
      vm_util.IssueCommand(cmd)
      return True
    except errors.VmUtil.IssueCommandError:
      return False

  def Execute(self) -> None:
    self.submit_timestamp = time.time()
    self.last_execution_start_time = time.time()
    cmd = [
        'gcloud',
        'run',
        'jobs',
        'execute',
        self.name,
        '--wait',
        f'--region={self.region}',
        f'--project={self.project}',
    ]
    vm_util.IssueCommand(cmd)
    self.last_execution_end_time = time.time()

    list_cmd = [
        'gcloud',
        'run',
        'jobs',
        'executions',
        'list',
        f'--job={self.name}',
        f'--region={self.region}',
        f'--project={self.project}',
        '--limit=1',
        '--format=json',
    ]
    stdout, _, _ = vm_util.IssueCommand(list_cmd)
    data = json.loads(stdout)[0]

    # The Cloud Run API returns two "start" times:
    # - status.startTime: When GCP began orchestration/provisioning.
    # - status.conditions['Started'].lastTransitionTime: When the container
    #   actually booted.
    # We use the latter to measure true container start latency.
    started_cond = next(
        c for c in data['status']['conditions'] if c['type'] == 'Started'
    )
    self.start_timestamp = dateutil.parser.parse(
        started_cond['lastTransitionTime']
    ).timestamp()

  def GetResourceMetadata(self) -> Dict[str, Any]:
    metadata = super().GetResourceMetadata()
    metadata.update({
        'job_compute_type': 'GCP_SERVERLESS',
    })
    return metadata

  def GetMonitoringLink(self) -> str:
    utc_end = int(time.time()) + _TWO_HOURS - 5 * 60
    return (
        'https://monitoring.corp.google.com/dashboard/run/jobs%2Finstances?'
        f'scope=cloud_project_number%3D{self.project_number}&'
        f'duration={_TWO_HOURS}&filters=module%3D{self.name}&utc_end={utc_end}'
    )
