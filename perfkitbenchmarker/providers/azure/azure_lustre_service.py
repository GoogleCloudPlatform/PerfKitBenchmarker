"""Resource for Azure Lustre service."""

import json
import logging

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import disk
from perfkitbenchmarker import lustre_service
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import util


FLAGS = flags.FLAGS


class AzureLustreDiskSpec(disk.BaseLustreDiskSpec):
  CLOUD = provider_info.AZURE


class AzureLustreDisk(disk.LustreDisk):

  def _GetNetworkDiskMountOptionsDict(self):
    return {'noatime': None, 'flock': None}


class AzureLustreService(lustre_service.BaseLustreService):
  """Resource for Azure Lustre service."""

  CLOUD = provider_info.AZURE
  DEFAULT_TIER = 125
  LUSTRE_TIERS = [500, 250, 125]
  DEFAULT_CAPACITY = 16384

  def __init__(self, disk_spec, zone):
    super().__init__(disk_spec, None)
    self.name = 'lustre-%s' % FLAGS.run_uri
    self.region = util.GetRegionFromZone(zone)
    self.zone = zone
    self.resource_group = azure_network.GetResourceGroup()
    self.ip = None
    self.metadata['location'] = self.region

  @property
  def network(self):
    return azure_network.AzureNetwork.GetNetworkFromNetworkSpec(self).subnet.id

  def GetMountPoint(self):
    return f'{self.ip}@tcp:/lustrefs'

  def CreateLustreDisk(self):
    lustre_disk = AzureLustreDisk(self.disk_spec)
    lustre_disk.metadata.update(self.metadata)
    return lustre_disk

  def _Create(self):
    logging.info('Creating Lustre: %s', self.name)
    stdout, _, _ = vm_util.IssueCommand(
        [util.AZURE_PATH, 'amlfs', 'create', '--name', self.name]
        + self.resource_group.args
        + [
            '--sku',
            f'AMLFS-Durable-Premium-{self.lustre_tier}',
            '--storage-capacity',
            str(int(self.capacity / 1024)),
            '--zones',
            '[1]',
            '--maintenance-window',
            data.ResourcePath('az_lustre_maintenance.json'),
            '--filesystem-subnet',
            self.network,
        ],
        timeout=1800,
    )
    self.ip = json.loads(stdout)['clientInfo']['mgsAddress']

  def _Delete(self):
    return

  def _IsReady(self):
    return True
