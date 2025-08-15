"""Resource for AWS Lustre service."""

import json
import logging

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import lustre_service
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import util


FLAGS = flags.FLAGS


class AwsLustreDiskSpec(disk.BaseLustreDiskSpec):
  CLOUD = provider_info.AWS


class AwsLustreDisk(disk.LustreDisk):

  def _GetNetworkDiskMountOptionsDict(self):
    return {'relatime': None, 'flock': None}


class AwsLustreService(lustre_service.BaseLustreService):
  """Resource for AWS Lustre service."""

  CLOUD = provider_info.AWS
  DEFAULT_TIER = 1000
  DEFAULT_CAPACITY = 19200
  LUSTRE_TIERS = [1000, 500, 250, 125]

  def __init__(self, disk_spec, zone):
    super().__init__(disk_spec, None)
    self.name = 'lustre-%s' % FLAGS.run_uri
    self.id = None
    self.region = util.GetRegionFromZone(zone)
    self.zone = zone
    self.metadata['compression'] = FLAGS.aws_lustre_compression
    self.metadata['location'] = self.region

  @property
  def network(self):
    return aws_network.AwsNetwork.GetNetworkFromNetworkSpec(self)

  def GetMountPoint(self):
    fs = self._Describe()['FileSystems'][0]
    return f'{fs["DNSName"]}@tcp:/{fs["LustreConfiguration"]["MountName"]}'

  def CreateLustreDisk(self):
    lustre_disk = AwsLustreDisk(self.disk_spec)
    lustre_disk.metadata.update(self.metadata)
    return lustre_disk

  def _Create(self):
    logging.info('Creating Lustre: %s', self.name)
    lustre_config = (
        'DeploymentType=PERSISTENT_2,'
        f'PerUnitStorageThroughput={self.lustre_tier},'
        f'EfaEnabled={str(FLAGS.aws_efa).lower()},'
        'MetadataConfiguration={Mode=AUTOMATIC}'
    )
    if FLAGS.aws_lustre_compression:
      lustre_config += ',DataCompressionType=LZ4'
    # https://cloud.google.com/managed-lustre/docs/create-instance
    self.id = self._Command(
        'create-file-system',
        [
            '--file-system-type=LUSTRE',
            f'--storage-capacity={self.capacity}',
            '--storage-type=SSD',
            f'--subnet-ids={self.network.subnet.id}',
            f'--security-group-ids={self.network.regional_network.vpc.default_security_group_id}',
            '--lustre-configuration',
            lustre_config,
            '--tags'
        ] + util.MakeFormattedDefaultTags()
    )['FileSystem']['FileSystemId']

  def _Delete(self):
    logging.info('Deleting Lustre: %s', self.name)
    try:
      self._Command(
          'delete-file-system',
          [
              f'--file-system-id={self.id}',
          ],
      )
    except errors.Error as ex:
      if self._Exists():
        raise errors.Resource.RetryableDeletionError(ex)
      else:
        logging.info('Lustre server %s already deleted', self.name)

  def _Exists(self):
    try:
      self._Describe()
      return True
    except errors.Error:
      return False

  def _IsReady(self):
    try:
      return (
          self._Describe()['FileSystems'][0].get('Lifecycle', None)
          == 'AVAILABLE'
      )
    except errors.Error:
      return False

  def _Describe(self):
    return self._Command(
        'describe-file-systems',
        [
            f'--file-system-id={self.id}',
        ],
    )

  def _Command(self, verb, args):
    cmd = util.AWS_PREFIX + ['fsx', verb, f'--region={self.region}'] + args
    stdout, stderr, retcode = vm_util.IssueCommand(
        cmd, raise_on_failure=False, timeout=1800
    )
    if retcode:
      raise errors.Error('Error running command %s : %s' % (verb, stderr))
    return json.loads(stdout)
