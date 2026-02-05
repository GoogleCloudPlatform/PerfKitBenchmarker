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
"""Module containing class for GCP's cloud TPU.

cloud TPU can be created and deleted.
"""

import json
import logging
import time
from absl import flags
from perfkitbenchmarker import cloud_tpu
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS
TPU_TIMEOUT = 1200
_INSUFFICIENT_CAPACITY = 'There is no more capacity in the zone'


class GcpTpu(cloud_tpu.BaseTpu):
  """class representing a GCP cloud TPU.

  Attributes:
    name: Name of the cloud TPU to create.
    project: the GCP project.
    version: the TPU version.
    zone: the GCP zone.
    tpu_ip: the TPU IP.
  """

  CLOUD = provider_info.GCP
  SERVICE_NAME = 'tpu'
  TPU_IP = '10.240.{}.2'
  DEFAULT_TPU_VERSION = '1.6'

  def __init__(self, tpu_spec):
    super().__init__(tpu_spec)
    self.project = FLAGS.project or util.GetDefaultProject()

  def _Create(self):
    """Create Cloud TPU."""
    is_v6e = self.spec.tpu_type and self.spec.tpu_type.startswith('v6e')
    components = ['compute', 'tpus', 'tpu-vm', 'create', self.spec.tpu_name]
    if is_v6e:
      components = ['alpha'] + components
    cmd = util.GcloudCommand(self, *components)
    if self.spec.tpu_type:
      cmd.flags['type'] = self.spec.tpu_type
    if self.spec.tpu_topology:
      cmd.flags['topology'] = self.spec.tpu_topology
    if self.spec.tpu_tf_version:
      cmd.flags['version'] = self.spec.tpu_tf_version
    if self.spec.tpu_zone:
      cmd.flags['zone'] = self.spec.tpu_zone
    cmd.flags['project'] = self.project
    self.create_start_time = time.time()
    _, stderr, retcode = cmd.Issue(raise_on_failure=False)
    self.create_time = time.time() - self.create_start_time

    if _INSUFFICIENT_CAPACITY in stderr:
      logging.error(util.STOCKOUT_MESSAGE)
      raise errors.Benchmarks.InsufficientCapacityCloudFailure(
          util.STOCKOUT_MESSAGE
      )

    if retcode != 0:
      logging.error('Create GCP cloud TPU failed.')

  def _Delete(self):
    """Deletes the cloud TPU."""
    components = ['compute', 'tpus', 'tpu-vm', 'delete', self.spec.tpu_name]
    if self.spec.tpu_type and self.spec.tpu_type.startswith('v6e'):
      components = ['alpha'] + components
    cmd = util.GcloudCommand(self, *components)
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
    components = ['compute', 'tpus', 'tpu-vm', 'describe', self.spec.tpu_name]
    if self.spec.tpu_type and self.spec.tpu_type.startswith('v6e'):
      components = ['alpha'] + components
    cmd = util.GcloudCommand(self, *components)
    if self.spec.tpu_zone:
      cmd.flags['zone'] = self.spec.tpu_zone
    cmd.flags['project'] = self.project
    cmd.flags['format'] = 'json'
    stdout, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not found GCP cloud TPU %s.', self.spec.tpu_name)
    return stdout and json.loads(stdout), retcode

  def _Exists(self):
    """Returns true if the cloud TPU exists."""
    _, retcode = self._GetTpuDescription()
    return retcode == 0

  def SshCommand(self, command):
    cmd = [FLAGS.gcloud_path]
    if self.spec.tpu_type and self.spec.tpu_type.startswith('v6e'):
      cmd.append('alpha')
    cmd.extend(['compute', 'tpus', 'tpu-vm', 'ssh', self.spec.tpu_name])
    cmd.extend(['--zone', self.spec.tpu_zone])
    cmd.extend(['--project', self.project])
    cmd.extend(['--', command])
    return vm_util.IssueCommand(cmd, timeout=60)

  @vm_util.Retry(poll_interval=1, timeout=600, log_errors=False)
  def WaitForSshBecameReady(self):
    self.SshCommand('hostname')

  def GetZone(self):
    """Gets the TPU zone."""
    return self.spec.tpu_zone

  def GetAcceleratorType(self):
    """Gets the TPU accelerator type."""
    return self.spec.tpu_type

  def GetResourceMetadata(self):
    """Returns the metadata associated with the resource.

    All keys will be prefaced with tpu before
    being published (done in publisher.py).

    Returns:
      metadata: dict of GCP cloud TPU metadata.
    """
    metadata = super().GetResourceMetadata()
    metadata.update({'project': self.project, 'cloud': self.CLOUD})
    return metadata
