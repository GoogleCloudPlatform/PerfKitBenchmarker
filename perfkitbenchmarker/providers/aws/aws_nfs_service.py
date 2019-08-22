# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""AWS NFS implementation.

See https://aws.amazon.com/efs/

This launches an EFS instance and creates a mount point.  Individual AwsDisks
will then mount the share.

The AfsNfsService object is a resource.BaseResource that has two resources
underneath it:
1. A resource to connect to the filer.
2. A resource to connect to the mount point on the filer.

Lifecycle:
1. EFS service created and blocks until it is available, as it is needed to
   make the mount point.
2. Issues a non-blocking call to create the mount point.  Does not block as the
   NfsDisk will block on it being available.
3. The NfsDisk then mounts the mount point and uses the disk like normal.
4. On teardown the mount point is first deleted.  Blocks on that returning.
5. The EFS service is then deleted.  Does not block as can take some time.
"""

import json
import logging

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import nfs_service
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util

from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS


class AwsNfsService(nfs_service.BaseNfsService):
  """An AWS NFS resource.

  Creates the AWS EFS file system and mount point for use with NFS clients.

  See https://aws.amazon.com/efs/
  """

  CLOUD = providers.AWS
  NFS_TIERS = ('generalPurpose', 'maxIO')
  DEFAULT_NFS_VERSION = '4.1'
  DEFAULT_TIER = 'generalPurpose'

  def __init__(self, disk_spec, zone):
    super(AwsNfsService, self).__init__(disk_spec, zone)
    self.region = util.GetRegionFromZone(self.zone)
    self.aws_commands = AwsEfsCommands(self.region)
    self.disk_spec.disk_size = 0
    self.filer_id = None
    self.mount_id = None
    self.throughput_mode = FLAGS.efs_throughput_mode
    self.provisioned_throughput = FLAGS.efs_provisioned_throughput

  @property
  def network(self):
    network_spec = aws_network.AwsNetworkSpec(self.zone)
    return aws_network.AwsNetwork.GetNetworkFromNetworkSpec(network_spec)

  @property
  def subnet_id(self):
    if hasattr(self.network, 'subnet'):
      return self.network.subnet.id
    else:
      raise errors.Config.InvalidValue('No subnet in network %s' % self.network)

  @property
  def security_group(self):
    if hasattr(self.network, 'vpc'):
      return self.network.vpc.default_security_group_id
    # not required when making the mount target
    return None

  def _Create(self):
    logging.info('Creating NFS resource, subnet: %s, security group: %s',
                 self.subnet_id, self.security_group)
    self._CreateFiler()
    logging.info('Waiting for filer to start up')
    self.aws_commands.WaitUntilFilerAvailable(self.filer_id)
    # create the mount point but do not wait for it, superclass will call the
    # _IsReady() method.
    self._CreateMount()

  def _Delete(self):
    # deletes on the file-system and mount-target are immediate
    self._DeleteMount()
    if not FLAGS.aws_delete_file_system:
      return
    self._DeleteFiler()

  def GetRemoteAddress(self):
    if self.filer_id is None:
      raise errors.Resource.RetryableGetError('Filer not created')
    return '{name}.efs.{region}.amazonaws.com'.format(
        name=self.filer_id, region=self.region)

  def _IsReady(self):
    return self.aws_commands.IsMountAvailable(self.mount_id)

  def _CreateFiler(self):
    """Creates the AWS EFS service."""
    if self.filer_id:
      logging.warn('_CreateFiler() already called for %s', self.filer_id)
      return
    if FLAGS.aws_efs_token:
      filer = self.aws_commands.GetFiler(FLAGS.aws_efs_token)
      if filer:
        self.nfs_tier = filer['PerformanceMode']
        self.filer_id = filer['FileSystemId']
        self.disk_spec.disk_size = int(
            round(filer['SizeInBytes']['Value'] / 10.0 ** 9))
        return
    token = FLAGS.aws_efs_token or 'nfs-token-%s' % FLAGS.run_uri
    self.filer_id = self.aws_commands.CreateFiler(
        token, self.nfs_tier, self.throughput_mode, self.provisioned_throughput)
    self.aws_commands.AddTagsToFiler(self.filer_id)
    logging.info('Created filer %s with address %s', self.filer_id,
                 self.GetRemoteAddress())

  def _CreateMount(self):
    """Creates an NFS mount point on an EFS service."""
    if self.mount_id:
      logging.warn('_CreateMount() already called for %s', self.mount_id)
      return
    if not self.filer_id:
      raise errors.Resource.CreationError('Did not create a filer first')
    logging.info('Creating NFS mount point')
    self.mount_id = self.aws_commands.CreateMount(
        self.filer_id, self.subnet_id, self.security_group)
    logging.info('Mount target %s starting up', self.mount_id)

  def _DeleteMount(self):
    """Deletes the EFS mount point.
    """
    if not self.mount_id:
      return
    logging.info('Deleting NFS mount mount %s', self.mount_id)
    self.aws_commands.DeleteMount(self.mount_id)
    self.mount_id = None

  def _DeleteFiler(self):
    """Deletes the EFS service.

    Raises:
      RetryableDeletionError: If the mount point exists.
    """
    if not self.filer_id:
      return
    if self.mount_id:
      # this isn't retryable as the mount point wasn't deleted
      raise errors.Resource.RetryableDeletionError(
          'Did not delete mount point first')
    logging.info('Deleting NFS filer %s', self.filer_id)
    self.aws_commands.DeleteFiler(self.filer_id)
    self.filer_id = None


class AwsEfsCommands(object):
  """Commands for interacting with AWS EFS.

  Args:
    region: AWS region for the NFS service.
  """

  def __init__(self, region):
    self.efs_prefix = util.AWS_PREFIX + ['--region', region, 'efs']

  def GetFiler(self, token):
    """Returns the filer using the creation token or None."""
    args = ['describe-file-systems', '--creation-token', token]
    response = self._IssueAwsCommand(args)
    file_systems = response['FileSystems']
    if not file_systems:
      return None
    assert len(file_systems) < 2, 'Too many file systems.'
    return file_systems[0]

  def CreateFiler(self, token, nfs_tier, throughput_mode,
                  provisioned_throughput):
    args = ['create-file-system', '--creation-token', token]
    if nfs_tier is not None:
      args += ['--performance-mode', nfs_tier]
    args += ['--throughput-mode', throughput_mode]
    if throughput_mode == 'provisioned':
      args += ['--provisioned-throughput-in-mibps', provisioned_throughput]
    return self._IssueAwsCommand(args)['FileSystemId']

  def AddTagsToFiler(self, filer_id):
    tags = util.MakeFormattedDefaultTags()
    args = ['create-tags', '--file-system-id', filer_id, '--tags'] + tags
    self._IssueAwsCommand(args, False)

  @vm_util.Retry()
  def WaitUntilFilerAvailable(self, filer_id):
    if not self._IsAvailable('describe-file-systems', '--file-system-id',
                             'FileSystems', filer_id):
      raise errors.Resource.RetryableCreationError(
          '{} not ready'.format(filer_id))

  @vm_util.Retry()
  def DeleteFiler(self, file_system_id):
    args = self.efs_prefix + [
        'delete-file-system', '--file-system-id', file_system_id]
    _, stderr, retcode = vm_util.IssueCommand(args, raise_on_failure=False)
    if retcode and 'FileSystemInUse' in stderr:
      raise Exception('Mount Point hasn\'t finished deleting.')

  def CreateMount(self, file_system_id, subnet_id, security_group=None):
    args = [
        'create-mount-target', '--file-system-id', file_system_id,
        '--subnet-id', subnet_id
    ]
    if security_group:
      args += ['--security-groups', security_group]
    return self._IssueAwsCommand(args)['MountTargetId']

  def IsMountAvailable(self, mount_target_id):
    if mount_target_id is None:
      # caller called _IsReady() before the mount point was created
      return False
    return self._IsAvailable('describe-mount-targets', '--mount-target-id',
                             'MountTargets', mount_target_id)

  def DeleteMount(self, mount_target_id):
    self._IssueAwsCommand(
        ['delete-mount-target', '--mount-target-id', mount_target_id], False)

  def _IsAvailable(self, describe_cmd, id_attr, response_attribute, id_value):
    describe = self._IssueAwsCommand([describe_cmd, id_attr, id_value])
    status = describe[response_attribute][0].get('LifeCycleState')
    return status == 'available'

  def _IssueAwsCommand(self, args, return_json=True):
    args = self.efs_prefix + [str(arg) for arg in args]
    stdout, _, retcode = vm_util.IssueCommand(args, raise_on_failure=False)
    if retcode:
      return None
    return json.loads(stdout) if return_json else stdout
