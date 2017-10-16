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
"""Module containing class for GCP's cloud TPU.

cloud TPU can be created and deleted.
"""

import json
import logging

from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import cloud_tpu
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS
CLOUD_TPU_TIMEOUT = 1200


class GcpCloudTpu(cloud_tpu.BaseCloudTpu):
  """class representing a GCP cloud TPU.

  Attributes:
    name: Name of the cloud TPU to create.
    project: the GCP project.
    version: the TPU version.
    zone: the GCP zone.
    tpu_ip: the TPU IP.
  """

  CLOUD = providers.GCP
  SERVICE_NAME = 'cloud_tpu'
  TPU_IP = '10.240.{}.2'
  DEFAULT_CLOUD_TPU_VERSION = 'nightly'

  def __init__(self, cloud_tpu_spec):
    super(GcpCloudTpu, self).__init__(cloud_tpu_spec)
    self.spec = cloud_tpu_spec
    self.project = FLAGS.project or util.GetDefaultProject()

  def _Create(self):
    """Create Cloud TPU."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'tpus', 'create',
                             self.spec.tpu_name)
    cmd.flags['range'] = self.spec.tpu_cidr_range
    if self.spec.tpu_accelerator_type:
      cmd.flags['accelerator-type'] = self.spec.tpu_accelerator_type
    if self.spec.tpu_description:
      cmd.flags['description'] = self.spec.tpu_description
    if self.spec.tpu_network:
      cmd.flags['network'] = self.spec.tpu_network
    if self.spec.tpu_tf_version:
      cmd.flags['version'] = self.spec.tpu_tf_version
    if self.spec.tpu_zone:
      cmd.flags['zone'] = self.spec.tpu_zone
    cmd.flags['project'] = self.project
    _, _, retcode = cmd.Issue()
    if retcode != 0:
      logging.error('Create GCP cloud TPU failed.')

  def _Delete(self):
    """Deletes the cloud TPU."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'tpus', 'delete',
                             self.spec.tpu_name)
    if self.spec.tpu_zone:
      cmd.flags['zone'] = self.spec.tpu_zone
    cmd.flags['project'] = self.project
    _, _, retcode = cmd.Issue(timeout=CLOUD_TPU_TIMEOUT)
    if retcode != 0:
      logging.error('Delete GCP cloud TPU failed.')
    else:
      logging.info('Deleted GCP cloud TPU.')

  def _Exists(self):
    """Returns true if the cloud TPU exists."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'tpus', 'describe',
                             self.spec.tpu_name)
    if self.spec.tpu_zone:
      cmd.flags['zone'] = self.spec.tpu_zone
    cmd.flags['project'] = self.project
    _, _, retcode = cmd.Issue()
    if retcode != 0:
      logging.info('Could not found GCP cloud TPU %s.',
                   self.spec.tpu_name)
      return False
    return True

  def GetCloudTpuIp(self):
    """Gets the cloud TPU IP."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'tpus', 'describe',
                             self.spec.tpu_name)
    if self.spec.tpu_zone:
      cmd.flags['zone'] = self.spec.tpu_zone
    cmd.flags['project'] = self.project
    stdout, _, retcode = cmd.Issue()
    if retcode != 0:
      logging.error('Could not found GCP cloud TPU %s.',
                    self.spec.tpu_name)
    result = json.loads(stdout)
    return result['ipAddress']

  def GetResourceMetadata(self):
    """Returns the metadata associated with the resource.

    All keys will be prefaced with cloud_tpu before
    being published (done in publisher.py).

    Returns:
      metadata: dict of GCP cloud TPU metadata.
    """
    metadata = super(GcpCloudTpu, self).GetResourceMetadata()
    metadata.update({
        'project': self.project,
        'cloud': self.CLOUD
    })
    return metadata
