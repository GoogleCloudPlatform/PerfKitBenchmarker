# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for GCP's Dataflow service.
Use this module for running Dataflow jobs from pre-built Dataflow templates
such as https://cloud.google.com/dataflow/docs/guides/templates/provided-templates

No Clusters can be created or destroyed, since it is a managed solution
See details at: https://cloud.google.com/dataflow/
"""

import os
import re
import time
import json
import logging
import datetime

from absl import flags
from perfkitbenchmarker import beam_benchmark_helper
from perfkitbenchmarker import errors
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gcp_dpb_dataflow
from perfkitbenchmarker.providers.gcp import util

# Refer to flags with prefix 'dpb_df_template' defined in gcp provider flags.py
FLAGS = flags.FLAGS

class GcpDpbDataflowTemplate(gcp_dpb_dataflow.GcpDpbDataflow):
  """Object representing GCP Dataflow service for running job templates."""

  CLOUD = providers.GCP
  SERVICE_TYPE = 'dataflow_template'

  def __init__(self, dpb_service_spec):
    super(GcpDpbDataflowTemplate, self).__init__(dpb_service_spec)
    self.project = util.GetDefaultProject()
    self.input_sub_empty = False
    self.job_drained = False

  @staticmethod
  def CheckPrerequisites(benchmark_config):
    del benchmark_config  # Unused
    if not FLAGS.dpb_df_template_gcs_location:
      raise errors.Config.InvalidValue('Template GCS location missing.')

  def Create(self):
    """See base class."""
    pass

  def Delete(self):
    """See base class."""
    pass

  def SubmitJob(
      self,
      template_gcs_location=None,
      job_poll_interval=None,
      job_arguments=None,
      job_input_sub = None):
    
    worker_machine_type = self.spec.worker_group.vm_spec.machine_type
    num_workers = self.spec.worker_count
    max_workers = self.spec.worker_count

    now = datetime.datetime.now()
    job_name = template_gcs_location.split('/')[-1] \
            + '_' + now.strftime("%Y%m%d_%H%M%S")
    region = util.GetRegionFromZone(FLAGS.dpb_service_zone)

    cmd = util.GcloudCommand(self, 'dataflow', 'jobs', 'run', job_name)
    cmd.flags = {
        'project': self.project,
        'gcs-location': template_gcs_location,
        'staging-location': FLAGS.dpb_dataflow_temp_location,
        'region': region,
        'worker-region': region,
        'worker-machine-type': worker_machine_type,
        'num-workers': num_workers,
        'max-workers': max_workers,
        'parameters': ','.join(job_arguments),
        'format': 'json',
    }

    stdout, _, _ = cmd.Issue()

    # Parse output to retrieve submitted job ID
    try:
      result = json.loads(stdout)
      self.job_id = result['id']
    except Exception as err:
      logging.error('Failed to parse Dataflow job ID: {}'.format(err))
      raise

    logging.info('Dataflow job ID: %s', self.job_id)
    # TODO: return JobResult() with pre-computed time stats
    return self._WaitForJob(
        job_input_sub, FLAGS.dpb_dataflow_timeout, job_poll_interval)

  def _GetCompletedJob(self, job_id):
    """See base class."""
    job_input_sub = job_id

    # Job completed if input subscription is empty *and* job is drained;
    # otherwise keep waiting
    if not self.input_sub_empty:
      backlog_size = self.GetSubscriptionBacklogSize(job_input_sub)
      logging.info('Polling: Backlog size of subscription {} is {}'
          .format(job_input_sub, backlog_size))
      if backlog_size == 0:
        self.input_sub_empty = True
        # Start draining job once input subscription is empty
        cmd = util.GcloudCommand(
            self, 'dataflow', 'jobs', 'drain', self.job_id)
        cmd.flags = {
            'project': self.project,
            'region': util.GetRegionFromZone(FLAGS.dpb_service_zone),
            'format': 'json',
        }
        logging.info('Polling: Draining job {} ...'.format(self.job_id))
        stdout, _, _ = cmd.Issue()
      else:
        return None

    if not self.job_drained:
      # Confirm job is drained
      cmd = util.GcloudCommand(self, 'dataflow', 'jobs', 'show', self.job_id)
      cmd.flags = {
          'project': self.project,
          'region': util.GetRegionFromZone(FLAGS.dpb_service_zone),
          'format': 'json',
      }
      stdout, _, _ = cmd.Issue()
      job_state = json.loads(stdout)['state']
      logging.info('Polling: Job state is {} '.format(job_state))
      if job_state == "Drained":
          self.job_drained = True
      else:
          return None

    # TODO: calculate run_time, pending_time as args for JobResult()
    return True