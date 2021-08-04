"""Tests the GCE NFS service."""

import json
import unittest
from absl import flags
import mock
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import gce_nfs_service
from tests import pkb_common_test_case
import six

FLAGS = flags.FLAGS

_RUN_URI = 'fb810a9b'
_PROJECT = 'bionic-baton-343'
_ZONE = 'us-west1-a'
_NET_NAME = 'gce-network'

_NFS_NAME = 'nfs-%s' % _RUN_URI

_CREATE_RES = []

_ERROR = 'error'


def _CreateCmd(tier='STANDARD'):
  return [
      'create',
      _NFS_NAME,
      '--file-share',
      'name=vol0,capacity=1024',
      '--network',
      'name=%s' % _NET_NAME,
      '--labels',
      '',
      '--tier',
      tier,
  ]


def _DescribeResult(tier='STANDARD'):
  return {
      'createTime': '2018-05-04T21:38:49.862374Z',
      'name': 'projects/foo/locations/asia-east1-a/instances/nfs-xxxxxxxx',
      'networks': [{
          'ipAddresses': ['10.198.13.2'],
          'network': 'default2',
          'reservedIpRange': '10.198.13.0/29'
      }],
      'state': 'READY',
      'tier': tier,
      'volumes': [{
          'capacityGb': '1024',
          'name': 'vol0'
      }]
  }


def _FullGcloud(args, location):
  prefix = [
      'gcloud', 'alpha', '--quiet', '--format', 'json', '--project', _PROJECT,
      'filestore', 'instances'
  ]
  postfix = ['--location', location]
  return prefix + list(args) + postfix


class GceNfsServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(GceNfsServiceTest, self).setUp()
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

  def _NfsService(self, disk_size=1024, **kwargs):
    for key, value in six.iteritems(kwargs):
      FLAGS[key].parse(value)
    spec = disk.BaseDiskSpec('test_component', disk_size=disk_size)
    return gce_nfs_service.GceNfsService(spec, _ZONE)

  def _SetResponses(self, *responses):
    responses_as_tuples = []
    for response in responses:
      if response == _ERROR:
        responses_as_tuples.append(('', response, 1))
      else:
        responses_as_tuples.append((json.dumps(response), '', 0))
    self.issue_cmd.side_effect = responses_as_tuples

  def assertCommandCalled(self, *args, location=_ZONE):
    self.issue_cmd.assert_called_with(
        _FullGcloud(args, location), raise_on_failure=False, timeout=1800)

  def assertMultipleCommands(self, *cmds, location=_ZONE):
    expected_calls = []
    for cmd in cmds:
      expected_calls.append(
          mock.call(
              _FullGcloud(cmd, location=location),
              raise_on_failure=False,
              timeout=1800))
    self.assertEqual(expected_calls, self.issue_cmd.call_args_list)

  def testCreate(self):
    nfs = self._NfsService()
    self._SetResponses(_CREATE_RES)
    nfs._Create()
    self.assertCommandCalled(*_CreateCmd())

  def testCreateWithErrors(self):
    self._SetResponses(_ERROR, _ERROR)
    with self.assertRaises(errors.Resource.RetryableCreationError):
      nfs = self._NfsService()
      nfs._Create()
    describe_cmd = ['describe', 'nfs-fb810a9b']
    self.assertMultipleCommands(_CreateCmd(), describe_cmd)

  def testCreate2TBDisk(self):
    self._SetResponses(_CREATE_RES)
    nfs = self._NfsService(disk_size=2048)
    nfs._Create()
    cmd = self.issue_cmd.call_args_list[0][0][0]
    self.assertRegexpMatches(' '.join(cmd), 'capacity=2048')

  def testGetRemoteAddress(self):
    self._SetResponses(_DescribeResult())
    nfs = self._NfsService(disk_size=2048)
    self.assertEqual('10.198.13.2', nfs.GetRemoteAddress())

  def testDelete(self):
    self._SetResponses({})
    nfs = self._NfsService()
    nfs._Delete()
    self.assertCommandCalled('delete', _NFS_NAME, '--async')

  def testDeleteWithErrors(self):
    self._SetResponses(_ERROR, _DescribeResult())
    with self.assertRaises(errors.Resource.RetryableDeletionError):
      nfs = self._NfsService()
      nfs._Delete()
    delete_cmd = ['delete', _NFS_NAME, '--async']
    describe_cmd = ['describe', _NFS_NAME]
    self.assertMultipleCommands(delete_cmd, describe_cmd)

  def testIsReady(self):
    self._SetResponses(_DescribeResult())
    nfs = self._NfsService()
    self.assertTrue(nfs._IsReady())

  def testIsNotReady(self):
    self._SetResponses({})  # missing "state"
    nfs = self._NfsService()
    self.assertFalse(nfs._IsReady())


if __name__ == '__main__':
  unittest.main()
