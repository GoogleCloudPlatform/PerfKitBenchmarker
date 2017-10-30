# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Contains classes/functions related to AWS container clusters."""

import os
import uuid

from perfkitbenchmarker import container_service
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import s3
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS


class AwsKopsCluster(container_service.KubernetesCluster):

  CLOUD = providers.AWS

  def __init__(self, spec):
    super(AwsKopsCluster, self).__init__(spec)
    self.name = self.name + '.k8s.local'
    self.config_bucket = 'kops-%s-%s' % (FLAGS.run_uri, str(uuid.uuid4()))
    self.region = util.GetRegionFromZone(self.zone)
    self.s3_service = s3.S3Service()
    self.s3_service.PrepareService(self.region)

  def _CreateDependencies(self):
    """Create the bucket to store cluster config."""
    self.s3_service.MakeBucket(self.config_bucket)

  def _DeleteDependencies(self):
    """Delete the bucket that stores cluster config."""
    self.s3_service.DeleteBucket(self.config_bucket)

  def _Create(self):
    """Creates the cluster."""
    create_cmd = [
        FLAGS.kops, 'create', 'cluster',
        '--name=%s' % self.name,
        '--state=s3://%s' % self.config_bucket,
        '--zones=%s' % self.zone,
        '--node-count=%s' % self.num_nodes,
        '--node-size=%s' % self.machine_type,
        '--yes'
    ]
    env = os.environ.copy()
    env['KUBECONFIG'] = FLAGS.kubeconfig
    vm_util.IssueCommand(create_cmd, env=env)

  def _Delete(self):
    """Deletes the cluster."""
    delete_cmd = [
        FLAGS.kops, 'delete', 'cluster',
        '--name=%s' % self.name,
        '--state=s3://%s' % self.config_bucket,
        '--yes'
    ]
    vm_util.IssueCommand(delete_cmd)

  def _IsReady(self):
    """Returns True if the cluster is ready, else False."""
    validate_cmd = [
        FLAGS.kops, 'validate', 'cluster',
        '--name=%s' % self.name,
        '--state=s3://%s' % self.config_bucket
    ]
    env = os.environ.copy()
    env['KUBECONFIG'] = FLAGS.kubeconfig
    _, _, retcode = vm_util.IssueCommand(validate_cmd, env=env,
                                         suppress_warning=True)
    return not retcode
