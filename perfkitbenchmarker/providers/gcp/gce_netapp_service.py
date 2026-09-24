"""Resource for GCP NetApp Volumes service."""

import json
import logging

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import nfs_service
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS

STANDARD = 'STANDARD'
PREMIUM = 'PREMIUM'
EXTREME = 'EXTREME'
FLEX = 'FLEX'


class GceNetAppError(errors.Error):
  """Raised when a GCNV gcloud command fails."""


class GceNetAppDiskSpec(disk.BaseNFSDiskSpec):
  CLOUD = provider_info.GCP
  DISK_TYPE = disk.NETAPP_VOLUMES


class GceNetAppService(nfs_service.BaseNfsService):
  """Resource for GCP NetApp Volumes service."""

  CLOUD = provider_info.GCP
  SERVICE_TYPE = disk.NETAPP_VOLUMES
  NFS_TIERS = (STANDARD, PREMIUM, EXTREME, FLEX)
  DEFAULT_TIER = PREMIUM
  user_managed = False

  def __init__(self, disk_spec, zone):
    super().__init__(disk_spec, zone)
    self.pool_name = f'pkb-pool-{FLAGS.run_uri}'
    self.volume_name = f'pkb-vol-{FLAGS.run_uri}'
    self.server_directory = f'/vol-{FLAGS.run_uri}'

  @property
  def network(self):
    spec = gce_network.GceNetworkSpec(project=FLAGS.project, zone=self.zone)
    network = gce_network.GceNetwork.GetNetworkFromNetworkSpec(spec)
    return network.network_resource.name

  @property
  def service_level(self) -> str:
    """Returns lower-case service level / tier."""
    return (self.nfs_tier or self.DEFAULT_TIER).lower()

  @property
  def pool_capacity_gib(self) -> int:
    """Calculates pool capacity (minimum 2048 GiB, or 1024 GiB for Flex).

    Storage pool limits:
    https://docs.cloud.google.com/netapp/volumes/docs/quotas#storage_pool_limits
    """
    min_capacity = 1024 if self.service_level == 'flex' else 2048
    return max(self.disk_spec.disk_size, min_capacity)

  def GetRemoteAddress(self):
    """Gets mount IP address for GCNV Volume."""
    details = self._DescribeVolume()

    mount_opts = details.get('mountOptions', []) or details.get(
        'mountInstructions', []
    )
    if mount_opts:
      for opt in mount_opts:
        if 'ipAddress' in opt and opt['ipAddress']:
          return opt['ipAddress']
        if 'exportPath' in opt and opt['exportPath']:
          return opt['exportPath'].split(':')[0]
        if 'exportFull' in opt and opt['exportFull']:
          return opt['exportFull'].split(':')[0]
    raise errors.Error(f'Could not find mount IP for volume {self.volume_name}')

  def GetResourceMetadata(self):
    result = super().GetResourceMetadata()
    result['netapp_service_level'] = self.nfs_tier or self.DEFAULT_TIER
    result['netapp_pool_capacity_gib'] = self.pool_capacity_gib
    return result

  def _EnsureServiceNetworkingPeering(self):
    cmd = util.GcloudCommand(self, 'services', 'vpc-peerings', 'connect')
    if 'zone' in cmd.flags:
      del cmd.flags['zone']
    cmd.flags['service'] = 'netapp.servicenetworking.goog'
    cmd.flags['ranges'] = 'google-service-range'
    cmd.flags['network'] = self.network
    cmd.Issue(raise_on_failure=False)

  def _CreateDependencies(self):
    super()._CreateDependencies()
    self._EnsureServiceNetworkingPeering()

    logging.info('Creating GCNV Storage Pool %s', self.pool_name)
    tags = util.MakeFormattedDefaultTags()
    # Only creating zonal storage pool for now. If we want to add support for
    # regional storage pool, we can do so later by not passing the --zone flag.
    pool_cmd = [
        'storage-pools',
        'create',
        self.pool_name,
        '--zone',
        self.zone,
        '--capacity',
        f'{self.pool_capacity_gib}GiB',
        '--service-level',
        self.service_level,
        '--network',
        f'name={self.network}',
        '--labels',
        tags,
        '--async',
    ]
    self._NetAppCommand(*pool_cmd)
    self._WaitUntilPoolReady()

  def _Create(self):
    tags = util.MakeFormattedDefaultTags()
    logging.info(
        'Creating GCNV Volume %s inside pool %s',
        self.volume_name,
        self.pool_name,
    )
    nfs_version = self.disk_spec.nfs_version
    protocol = (
        'NFSV4' if nfs_version and str(nfs_version).startswith('4') else 'NFSV3'
    )

    export_policy = (
        'allowed-clients=0.0.0.0/0,has-root-access=true,access-type=READ_WRITE,nfsv4=true'
        if protocol == 'NFSV4'
        else 'allowed-clients=0.0.0.0/0,has-root-access=true,access-type=READ_WRITE,nfsv3=true'
    )
    vol_cmd = [
        'volumes',
        'create',
        self.volume_name,
        '--storage-pool',
        self.pool_name,
        '--capacity',
        f'{self.disk_spec.disk_size}GiB',
        '--protocols',
        protocol,
        '--share-name',
        self.server_directory.strip('/'),
        '--export-policy',
        export_policy,
        '--labels',
        tags,
        '--async',
    ]
    self._NetAppCommand(*vol_cmd)

  def _Delete(self):
    logging.info('Deleting GCNV Volume %s', self.volume_name)
    try:
      self._NetAppCommand('volumes', 'delete', self.volume_name)
    except GceNetAppError as ex:
      logging.warning('Error deleting volume %s: %s', self.volume_name, ex)

  def _DeleteDependencies(self):
    logging.info('Deleting GCNV Storage Pool %s', self.pool_name)
    try:
      self._NetAppCommand('storage-pools', 'delete', self.pool_name, '--async')
    except GceNetAppError as ex:
      logging.warning('Error deleting storage pool %s: %s', self.pool_name, ex)
    super()._DeleteDependencies()

  def _Exists(self):
    """Returns True if the volume resource exists."""
    return bool(self._DescribeVolume())

  def _IsReady(self):
    """Returns True if the volume resource is in READY state."""
    return self._DescribeVolume().get('state') == 'READY'

  @vm_util.Retry(
      timeout=1200,
      retryable_exceptions=(errors.Resource.RetryableCreationError,),
  )
  def _WaitUntilPoolReady(self):
    state = self._DescribePool().get('state', None)
    if state == 'READY':
      return
    raise errors.Resource.RetryableCreationError(
        f'Pool {self.pool_name} state is {state}'
    )

  def _DescribeVolume(self):
    """Describes GCNV Volume state details."""
    try:
      return self._NetAppCommand('volumes', 'describe', self.volume_name)
    except GceNetAppError as ex:
      logging.debug('Volume describe failed: %s', ex)
      return {}

  def _DescribePool(self):
    """Describes GCNV Storage Pool state details."""
    try:
      return self._NetAppCommand('storage-pools', 'describe', self.pool_name)
    except GceNetAppError as ex:
      logging.debug('Storage pool describe failed: %s', ex)
      return {}

  def _NetAppCommand(self, *args):
    cmd = util.GcloudCommand(self, 'netapp', *args)
    if 'zone' in cmd.flags:
      del cmd.flags['zone']
    cmd.flags['location'] = util.GetRegionFromZone(self.zone)
    cmd.flags['format'] = 'json'
    stdout, stderr, retcode = cmd.Issue(raise_on_failure=False, timeout=1800)
    if retcode:
      raise GceNetAppError(f'Error running gcloud netapp command: {stderr}')
    return json.loads(stdout) if stdout else {}
