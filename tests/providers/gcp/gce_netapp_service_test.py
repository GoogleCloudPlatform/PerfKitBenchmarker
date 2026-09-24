import json
import unittest
from unittest import mock

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gce_netapp_service
from perfkitbenchmarker.providers.gcp import gce_network
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_RUN_URI = 'fb810a9b'
_PROJECT = 'test-project'
_ZONE = 'us-central1-a'
_REGION = 'us-central1'
_NET_NAME = 'gce-network'

_POOL_NAME = f'pkb-pool-{_RUN_URI}'
_VOL_NAME = f'pkb-vol-{_RUN_URI}'


def _DescribeVolumeResult():
  return {
      'name': f'projects/{_PROJECT}/locations/{_REGION}/volumes/{_VOL_NAME}',
      'state': 'READY',
      'mountOptions': [{
          'exportFull': f'10.198.0.2:/{_RUN_URI}',
          'ipAddress': '10.198.0.2',
      }],
      'mountInstructions': [{
          'exportPath': f'10.198.0.2:/{_RUN_URI}',
          'ipAddress': '10.198.0.2',
      }],
  }


def _DescribePoolResult():
  return {
      'name': (
          f'projects/{_PROJECT}/locations/{_REGION}/storagePools/{_POOL_NAME}'
      ),
      'state': 'READY',
  }


class GceNetAppServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.issue_cmd = self._CreatePatched(vm_util, 'IssueCommand')
    self._SetNetwork()
    FLAGS['gce_network_name'].parse(_NET_NAME)
    FLAGS['project'].parse(_PROJECT)
    FLAGS['run_uri'].parse(_RUN_URI)
    FLAGS['gcloud_path'].parse('gcloud')

  def _SetNetwork(self):
    network_spec = self._CreatePatched(gce_network, 'GceNetwork')
    mock_network = mock.Mock()
    mock_network.network_resource.name = _NET_NAME
    network_spec.GetNetworkFromNetworkSpec.return_value = mock_network

  def _CreatePatched(self, module, method_name):
    patcher = mock.patch.object(module, method_name)
    mock_method = patcher.start()
    self.addCleanup(patcher.stop)
    return mock_method

  def _NetAppService(self, disk_size=1024, spec_kwargs=None, **kwargs):
    for key, value in kwargs.items():
      FLAGS[key].parse(value)
    spec = gce_netapp_service.GceNetAppDiskSpec(
        'test_component',
        FLAGS,
        disk_size=disk_size,
        disk_type=disk.NETAPP_VOLUMES,
        **(spec_kwargs or {}),
    )
    return gce_netapp_service.GceNetAppService(spec, _ZONE)

  def testGetRemoteAddress(self):
    service = self._NetAppService()
    self.issue_cmd.return_value = (json.dumps(_DescribeVolumeResult()), '', 0)
    address = service.GetRemoteAddress()
    self.assertEqual(address, '10.198.0.2')

  def testGetResourceMetadata(self):
    service = self._NetAppService(
        disk_size=500, gcp_netapp_service_level='PREMIUM'
    )
    metadata = service.GetResourceMetadata()
    self.assertEqual(metadata['netapp_service_level'], 'PREMIUM')
    self.assertEqual(metadata['netapp_pool_capacity_gib'], 2048)

  def testDefaultServiceLevel(self):
    service = self._NetAppService()
    self.assertEqual(service.disk_spec.netapp_service_level, 'PREMIUM')

  def testInvalidServiceLevel(self):
    with self.assertRaises(flags.IllegalFlagValueError):
      FLAGS['gcp_netapp_service_level'].parse('NonExistentTier')

  def testServiceLevelFromDiskSpec(self):
    service = self._NetAppService(
        spec_kwargs={'netapp_service_level': 'EXTREME'}
    )
    self.assertEqual(service.disk_spec.netapp_service_level, 'EXTREME')

  def testFlagOverridesDiskSpec(self):
    service = self._NetAppService(
        spec_kwargs={'netapp_service_level': 'EXTREME'},
        gcp_netapp_service_level='STANDARD',
    )
    self.assertEqual(service.disk_spec.netapp_service_level, 'STANDARD')

  def testInvalidServiceLevelInDiskSpec(self):
    with self.assertRaises(errors.Config.InvalidValue):
      self._NetAppService(
          spec_kwargs={'netapp_service_level': 'NonExistentTier'}
      )

  def testCreate(self):

    service = self._NetAppService(
        disk_size=500, gcp_netapp_service_level='PREMIUM'
    )
    self.issue_cmd.side_effect = [
        ('', '', 0),  # vpc-peerings connect
        ('', '', 0),  # storage pool create
        (json.dumps(_DescribePoolResult()), '', 0),  # pool describe ready
        ('', '', 0),  # volume create
        (json.dumps(_DescribeVolumeResult()), '', 0),  # (_WaitUntilRunning)
        (json.dumps(_DescribeVolumeResult()), '', 0),  # (_WaitUntilReady)
    ]
    service.Create()

    self.assertEqual(self.issue_cmd.call_count, 6)
    # Verify pool creation uses min 2048 GiB for PREMIUM
    pool_create_args = self.issue_cmd.call_args_list[1][0][0]
    self.assertIn('--capacity', pool_create_args)
    self.assertIn('2048GiB', pool_create_args)
    self.assertIn('--service-level', pool_create_args)
    self.assertIn('PREMIUM', pool_create_args)
    self.assertIn('--zone', pool_create_args)
    self.assertIn(_ZONE, pool_create_args)

    vol_create_args = self.issue_cmd.call_args_list[3][0][0]
    self.assertIn('--export-policy', vol_create_args)

  def testDelete(self):
    service = self._NetAppService()
    service.created = True
    self.issue_cmd.side_effect = [
        ('', '', 0),  # volume delete
        # volume describe (called by _Exists check after delete)
        ('', 'Volume not found', 1),
        ('', '', 0),  # pool delete
    ]
    service.Delete()

    self.assertEqual(self.issue_cmd.call_count, 3)

  def testExists(self):
    service = self._NetAppService()
    self.issue_cmd.return_value = (json.dumps(_DescribeVolumeResult()), '', 0)
    self.assertTrue(service._Exists())

    self.issue_cmd.return_value = ('', 'Volume not found', 1)
    self.assertFalse(service._Exists())


if __name__ == '__main__':
  unittest.main()
