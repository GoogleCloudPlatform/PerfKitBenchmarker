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
from perfkitbenchmarker import resource
from perfkitbenchmarker.providers.gcp import util

TPU_TIMEOUT = 1200

FLAGS = flags.FLAGS
flags.DEFINE_string('tpu_version', 'nightly',
                    'The version for the Cloud TPU.')


class GcpCloudTpu(resource.BaseResource):
  """class representing a GCP cloud TPU.

  Attributes:
    name: Name of the cloud TPU to create.
    project: the GCP project.
    version: the TPU version.
    zone: the GCP zone.
    tpu_ip: the TPU IP.
  """

  def __init__(self, name, project):
    super(GcpCloudTpu, self).__init__()
    self.name = name
    self.project = project
    self.version = FLAGS.tpu_version
    self.zone = FLAGS.zones[0]
    self.tpu_ip = '10.240.{}.2'

  def ListIpAndCidr(self):
    """List Cloud TPUs."""

  def _Create(self):
    """Create Cloud TPU."""
    def List():
      cmd = util.GcloudCommand(self, 'alpha', 'compute', 'tpus', 'list')
      cmd.flags['project'] = self.project
      cmd.flags['zone'] = self.zone
      stdout, _, retcode = cmd.Issue()
      if retcode != 0:
        logging.error('List GCP cloud TPU failed.')
        return
      result = json.loads(stdout)
      return [tpu['ipAddress'] for tpu in result]

    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'tpus', 'create',
                             self.name)
    tpu_ips_cidr = List()
    for i in range(2 ** 8):
      self.tpu_ip = self.tpu_ip.format(i)
      if self.tpu_ip not in tpu_ips_cidr:
        cmd.flags['range'] = '{}0/29'.format(self.tpu_ip[:-1])
        break

    cmd.flags['project'] = self.project
    cmd.flags['version'] = self.version
    cmd.flags['zone'] = self.zone
    _, _, retcode = cmd.Issue()
    if retcode != 0:
      logging.error('Create GCP cloud TPU failed.')
      return

  def _Delete(self):
    """Deletes the cloud TPU."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'tpus', 'delete',
                             self.name)
    cmd.flags['project'] = self.project
    cmd.flags['zone'] = self.zone
    _, _, retcode = cmd.Issue(timeout=TPU_TIMEOUT)
    if retcode != 0:
      logging.error('Delete GCP cloud TPU failed.')
    else:
      logging.info('Deleted GCP cloud TPU.')

  def _Exists(self):
    """Returns true if the cloud TPU exists."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'tpus', 'describe',
                             self.name)
    cmd.flags['project'] = self.project
    cmd.flags['zone'] = self.zone
    _, _, retcode = cmd.Issue()
    if retcode != 0:
      logging.info('Could not found GCP cloud TPU %s.', self.name)
      return False
    return True
