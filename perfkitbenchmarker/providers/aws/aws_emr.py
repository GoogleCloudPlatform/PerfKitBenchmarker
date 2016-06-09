# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for AWS's spark service.

Spark clusters can be created and deleted.
"""

import json
import logging

from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import spark_service
from perfkitbenchmarker import vm_util
import time
import util


FLAGS = flags.FLAGS

DEFAULT_MACHINE_TYPE = 'm1.large'
RELEASE_LABEL = 'emr-4.5.0'
READY_CHECK_SLEEP = 30
READY_CHECK_TRIES = 60
READY_STATE = 'WAITING'

JOB_WAIT_SLEEP = 30

DELETED_STATES = ['TERMINATING', 'TERMINATED_WITH_ERRORS', 'TERMINATED']


class AwsEMR(spark_service.BaseSparkService):
  """Object representing a AWS EMR cluster.

  Attributes:
    cluster_id: Cluster identifier, set in superclass.
    num_workers: Number of works, set in superclass.
    machine_type: Machine type to use.
    project: Enclosing project for the cluster.
    cmd_prefix: emr prefix, including region
  """

  CLOUD = providers.AWS
  SERVICE_NAME = 'emr'

  def __init__(self, spark_service_spec):
    super(AwsEMR, self).__init__(spark_service_spec)
    # TODO(hildrum) use availability zone when appropriate
    if self.machine_type is None:
      self.machine_type = DEFAULT_MACHINE_TYPE
    self.cmd_prefix = util.AWS_PREFIX + ['emr']
    if FLAGS.zones:
      region = util.GetRegionFromZone(FLAGS.zones[0])
      self.cmd_prefix += ['--region', region]

  def _Create(self):
    """Creates the cluster."""
    name = 'pkb_' + FLAGS.run_uri
    # we need to store the cluster id.
    cmd = self.cmd_prefix + ['create-cluster', '--name', name,
                             '--release-label', RELEASE_LABEL,
                             '--use-default-roles',
                             '--instance-count', str(self.num_workers),
                             '--instance-type', self.machine_type,
                             '--application', 'Name=SPARK']
    stdout, _, _ = vm_util.IssueCommand(cmd)
    result = json.loads(stdout)
    self.cluster_id = result['ClusterId']
    logging.info('Cluster created with id %s', self.cluster_id)

  def _Delete(self):
    """Deletes the cluster."""
    cmd = self.cmd_prefix + ['terminate-clusters', '--cluster-ids',
                             self.cluster_id]
    vm_util.IssueCommand(cmd)

  def _Exists(self):
    """Check to see whether the cluster exists."""
    cmd = self.cmd_prefix + ['describe-cluster',
                             '--cluster-id', self.cluster_id]
    stdout, _, rc = vm_util.IssueCommand(cmd)
    if rc != 0:
      return False
    result = json.loads(stdout)
    if result['Cluster']['Status']['State'] in DELETED_STATES:
      return False
    else:
      return True

  def _WaitUntilReady(self):
    """Check to see if the cluster is ready."""
    cmd = self.cmd_prefix + ['describe-cluster', '--cluster-id',
                             self.cluster_id]
    tries = 0
    while tries < READY_CHECK_TRIES:
      tries += 1
      stdout, _, rc = vm_util.IssueCommand(cmd)
      result = json.loads(stdout)
      if result['Cluster']['Status']['State'] == READY_STATE:
        return True
      time.sleep(READY_CHECK_SLEEP)
    return False

  def SubmitJob(self, jarfile, classname, job_poll_interval=JOB_WAIT_SLEEP):
    arg_list = ['--class', classname, jarfile]
    arg_string = '[' + ','.join(arg_list) + ']'
    step_list = ['Type=Spark', 'Args=' + arg_string]
    step_string = ','.join(step_list)
    cmd = self.cmd_prefix + ['add-steps', '--cluster-id',
                             self.cluster_id, '--steps', step_string]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    result = json.loads(stdout)
    step_id = result['StepIds'][0]
    # Now, we wait for the step to be completed.
    while True:
      cmd = self.cmd_prefix + ['describe-step', '--cluster-id',
                               self.cluster_id, '--step-id', step_id]

      stdout, _, _ = vm_util.IssueCommand(cmd)
      result = json.loads(stdout)
      state = result['Step']['Status']['State']
      if state == "COMPLETED":
        return True
      elif state == "FAILED":
        return False
      else:
        logging.info('Waiting %s seconds for job to finish', job_poll_interval)
        time.sleep(job_poll_interval)

  def SetClusterProperty(self):
    pass
