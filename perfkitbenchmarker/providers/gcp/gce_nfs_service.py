# Lint as: python2, python3
"""Resource for GCE NFS service."""

import json
import logging
import random

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import nfs_service
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gce_network

flags.DEFINE_string('nfs_gce_ip_range', None,
                    'GCE reserved IP range.  Example: 10.197.5.0/29')

FLAGS = flags.FLAGS


def _GenRandomIpRange():
  """Returns a random /29 ip range in 172.16.0.0/12."""
  ip_range = '172.{0}.{1}.{2}/29'.format(random.randint(16, 31),
                                         random.randint(0, 255),
                                         random.randint(0, 255) & 0xf8)
  return ip_range


class GceNfsService(nfs_service.BaseNfsService):
  """Resource for GCE NFS service."""

  CLOUD = providers.GCP
  NFS_TIERS = ('STANDARD', 'PREMIUM')
  DEFAULT_NFS_VERSION = '3.0'
  DEFAULT_TIER = 'STANDARD'
  user_managed = False

  def __init__(self, disk_spec, zone):
    super(GceNfsService, self).__init__(disk_spec, zone)
    self.name = 'nfs-%s' % FLAGS.run_uri
    self.server_directory = '/vol0'

  @property
  def network(self):
    spec = gce_network.GceNetworkSpec(project=FLAGS.project)
    network = gce_network.GceNetwork.GetNetworkFromNetworkSpec(spec)
    return network.network_resource.name

  def GetRemoteAddress(self):
    return self._Describe()['networks'][0]['ipAddresses'][0]

  def _Create(self):
    logging.info('Creating NFS server %s', self.name)
    volume_arg = 'name={0},capacity={1}'.format(
        self.server_directory.strip('/'), self.disk_spec.disk_size)
    network_arg = 'name={0},reserved-ip-range={1}'.format(
        self.network, FLAGS.nfs_gce_ip_range or _GenRandomIpRange())
    args = ['--file-share', volume_arg, '--network', network_arg]
    if self.nfs_tier:
      args += ['--tier', self.nfs_tier]
    try:
      self._NfsCommand('create', *args)
    except errors.Error as ex:
      # if this NFS service already exists reuse it
      if self._Exists():
        logging.info('Reusing existing NFS server %s', self.name)
      else:
        raise errors.Resource.RetryableCreationError(
            'Error creating NFS service %s' % self.name, ex)

  def _Delete(self):
    logging.info('Deleting NFS server %s', self.name)
    try:
      self._NfsCommand('delete', '--async')
    except errors.Error as ex:
      if self._Exists():
        raise errors.Resource.RetryableDeletionError(ex)
      else:
        logging.info('NFS server %s already deleted', self.name)

  def _Exists(self):
    try:
      self._Describe()
      return True
    except errors.Error:
      return False

  def _IsReady(self):
    try:
      return self._Describe().get('state', None) == 'READY'
    except errors.Error:
      return False

  def _Describe(self):
    return self._NfsCommand('describe')

  def _NfsCommand(self, verb, *args):
    cmd = [FLAGS.gcloud_path, 'alpha', '--quiet', '--format', 'json']
    if FLAGS.project:
      cmd += ['--project', FLAGS.project]
    cmd += ['filestore', 'instances', verb, self.name]
    cmd += [str(arg) for arg in args]
    cmd += ['--location', self.zone]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode:
      raise errors.Error('Error running command %s : %s' % (verb, stderr))
    return json.loads(stdout)
