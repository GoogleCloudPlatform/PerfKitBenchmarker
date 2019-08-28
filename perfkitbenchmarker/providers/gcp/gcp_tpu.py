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

from perfkitbenchmarker import cloud_tpu
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS
TPU_TIMEOUT = 1200


class GcpTpu(cloud_tpu.BaseTpu):
  """class representing a GCP cloud TPU.

  Attributes:
    name: Name of the cloud TPU to create.
    project: the GCP project.
    version: the TPU version.
    zone: the GCP zone.
    tpu_ip: the TPU IP.
  """

  CLOUD = providers.GCP
  SERVICE_NAME = 'tpu'
  TPU_IP = '10.240.{}.2'
  DEFAULT_TPU_VERSION = '1.6'

  def __init__(self, tpu_spec):
    super(GcpTpu, self).__init__(tpu_spec)
    self.spec = tpu_spec
    self.project = FLAGS.project or util.GetDefaultProject()

  def _Create(self):
    """Create Cloud TPU."""
    cmd = util.GcloudCommand(self, 'compute', 'tpus', 'create',
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
    if self.spec.tpu_preemptible:
      cmd.flags['preemptible'] = self.spec.tpu_preemptible
    cmd.flags['project'] = self.project
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Create GCP cloud TPU failed.')

  def _Delete(self):
    """Deletes the cloud TPU."""
    cmd = util.GcloudCommand(self, 'compute', 'tpus', 'delete',
                             self.spec.tpu_name)
    if self.spec.tpu_zone:
      cmd.flags['zone'] = self.spec.tpu_zone
    cmd.flags['project'] = self.project
    _, _, retcode = cmd.Issue(timeout=TPU_TIMEOUT, raise_on_failure=False)
    if retcode != 0:
      logging.error('Delete GCP cloud TPU failed.')
    else:
      logging.info('Deleted GCP cloud TPU.')

  def _GetTpuDescription(self):
    """Gets the cloud TPU description."""
    cmd = util.GcloudCommand(self, 'compute', 'tpus', 'describe',
                             self.spec.tpu_name)
    if self.spec.tpu_zone:
      cmd.flags['zone'] = self.spec.tpu_zone
    cmd.flags['project'] = self.project
    stdout, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not found GCP cloud TPU %s.',
                   self.spec.tpu_name)
    return stdout and json.loads(stdout), retcode

  def _Exists(self):
    """Returns true if the cloud TPU exists."""
    _, retcode = self._GetTpuDescription()
    return retcode == 0

  def GetName(self):
    """Gets the name of the cloud TPU."""
    return self.spec.tpu_name

  def GetMasterGrpcAddress(self):
    """Gets the grpc address of the 0th NetworkEndpoint."""
    master_network_endpoint = self._GetTpuDescription()[0]['networkEndpoints'][
        0]

    return 'grpc://{ip_address}:{port}'.format(
        ip_address=master_network_endpoint['ipAddress'],
        port=master_network_endpoint['port'])

  def GetNumShards(self):
    """Gets the number of TPU shards."""
    num_tpus = len(self._GetTpuDescription()[0]['networkEndpoints'])
    return num_tpus * FLAGS.tpu_cores_per_donut

  def GetZone(self):
    """Gets the TPU zone."""
    return self.spec.tpu_zone

  def GetAcceleratorType(self):
    """Gets the TPU accelerator type."""
    return self.spec.tpu_accelerator_type

  def GetResourceMetadata(self):
    """Returns the metadata associated with the resource.

    All keys will be prefaced with tpu before
    being published (done in publisher.py).

    Returns:
      metadata: dict of GCP cloud TPU metadata.
    """
    metadata = super(GcpTpu, self).GetResourceMetadata()
    metadata.update({
        'project': self.project,
        'cloud': self.CLOUD
    })
    return metadata
