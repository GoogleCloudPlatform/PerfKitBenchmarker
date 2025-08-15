"""Resource for GCE Lustre service."""

import json
import logging

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import lustre_service
from perfkitbenchmarker import provider_info
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS


class GceLustreDiskSpec(disk.BaseLustreDiskSpec):
  CLOUD = provider_info.GCP


class GceLustreService(lustre_service.BaseLustreService):
  """Resource for GCE Lustre service."""

  CLOUD = provider_info.GCP
  DEFAULT_TIER = 1000
  DEFAULT_CAPACITY = 18000
  LUSTRE_TIERS = [1000, 500, 250, 125]

  def __init__(self, disk_spec, zone):
    super().__init__(disk_spec, None)
    self.name = 'lustre-%s' % FLAGS.run_uri
    self.location = zone
    self.metadata['location'] = self.zone

  @property
  def network(self):
    return FLAGS.gcp_lustre_vpc

  def GetMountPoint(self):
    return self._Describe()['mountPoint']

  def _Create(self):
    logging.info('Creating Lustre: %s', self.name)
    # https://cloud.google.com/managed-lustre/docs/create-instance
    self._Command(
        'create',
        {
            'per-unit-storage-throughput': self.lustre_tier,
            'capacity-gib': str(self.capacity),
            'filesystem': 'lustre',
            'location': self.location,
            'network': (
                f'projects/{FLAGS.project}/global/networks/{self.network}'
            ),
            'labels': util.MakeFormattedDefaultTags()
        },
    )

  def _Delete(self):
    logging.info('Deleting Lustre: %s', self.name)
    try:
      self._Command('delete', {'location': self.location})
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
      return self._Describe().get('state', None) == 'ACTIVE'
    except errors.Error:
      return False

  def _Describe(self):
    return self._Command('describe', {'location': self.location})

  def _Command(self, verb, args):
    cmd = util.GcloudCommand(self, 'lustre', 'instances', verb, self.name)
    cmd.flags.update(args)
    stdout, stderr, retcode = cmd.Issue(raise_on_failure=False, timeout=1800)
    if retcode:
      raise errors.Error('Error running command %s : %s' % (verb, stderr))
    return json.loads(stdout)
