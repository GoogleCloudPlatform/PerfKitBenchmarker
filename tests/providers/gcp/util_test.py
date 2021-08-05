# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.gcp.util."""


import inspect
import unittest

from absl.testing import parameterized
import mock
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import util
from tests import pkb_common_test_case
import six


_GCLOUD_PATH = 'path/gcloud'


def _MockIssueCommand(test_response):
  return mock.patch.object(
      vm_util,
      'IssueCommand',
      autospec=True,
      return_value=[test_response, None, None])


class GceResource(resource.BaseResource):

  def __init__(self, **kwargs):
    for k, v in six.iteritems(kwargs):
      setattr(self, k, v)

  def _Create(self):
    raise NotImplementedError()

  def _Delete(self):
    raise NotImplementedError()


class GcloudCommandTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(GcloudCommandTestCase, self).setUp()
    p = mock.patch(util.__name__ + '.FLAGS')
    self.mock_flags = p.start()
    self.addCleanup(p.stop)
    self.mock_flags.gcloud_path = _GCLOUD_PATH

  def testCommonFlagsWithoutOptionalFlags(self):
    gce_resource = GceResource(project=None)
    cmd = util.GcloudCommand(gce_resource, 'compute', 'images', 'list')
    self.assertEqual(cmd.GetCommand(), [
        'path/gcloud', 'compute', 'images', 'list', '--format', 'json',
        '--quiet'
    ])

  def testCommonFlagsWithOptionalFlags(self):
    gce_resource = GceResource(project='test-project', zone='test-zone')
    cmd = util.GcloudCommand(gce_resource, 'compute', 'images', 'list')
    self.assertEqual(cmd.GetCommand(), [
        'path/gcloud', 'compute', 'images', 'list', '--format', 'json',
        '--project', 'test-project', '--quiet', '--zone', 'test-zone'
    ])

  def testListValue(self):
    gce_resource = GceResource(project=None)
    cmd = util.GcloudCommand(gce_resource, 'compute', 'instances', 'create')
    cmd.flags['local-ssd'] = ['interface=nvme', 'interface=SCSI']
    self.assertEqual(cmd.GetCommand(), [
        'path/gcloud',
        'compute',
        'instances',
        'create',
        '--format',
        'json',
        '--local-ssd',
        'interface=nvme',
        '--local-ssd',
        'interface=SCSI',
        '--quiet',
    ])

  def testIssue(self):
    gce_resource = GceResource(project=None)
    cmd = util.GcloudCommand(gce_resource, 'compute', 'images', 'list')
    mock_issue_return_value = ('issue-return-value', 'stderr', 0)
    p = mock.patch(util.__name__ + '.vm_util.IssueCommand',
                   return_value=mock_issue_return_value)
    with p as mock_issue:
      return_value = cmd.Issue()
      mock_issue.assert_called_with(['path/gcloud', 'compute', 'images', 'list',
                                     '--format', 'json', '--quiet'])
    self.assertEqual(return_value, mock_issue_return_value)

  def testIssueWarningSuppressed(self):
    gce_resource = GceResource(project=None)
    cmd = util.GcloudCommand(gce_resource, 'compute', 'images', 'list')
    mock_issue_return_value = ('issue-return-value', 'stderr', 0)
    p = mock.patch(util.__name__ + '.vm_util.IssueCommand',
                   return_value=mock_issue_return_value)
    with p as mock_issue:
      return_value = cmd.Issue(suppress_warning=True)
      mock_issue.assert_called_with(
          ['path/gcloud', 'compute', 'images', 'list', '--format', 'json',
           '--quiet'],
          suppress_warning=True)
    self.assertEqual(return_value, mock_issue_return_value)

  def testIssueRetryable(self):
    gce_resource = GceResource(project=None)
    cmd = util.GcloudCommand(gce_resource, 'compute', 'images', 'list')
    mock_issue_return_value = ('issue-return-value', 'stderr', 0)
    p = mock.patch(util.__name__ + '.vm_util.IssueRetryableCommand',
                   return_value=mock_issue_return_value)
    with p as mock_issue:
      return_value = cmd.IssueRetryable()
      mock_issue.assert_called_with(['path/gcloud', 'compute', 'images', 'list',
                                     '--format', 'json', '--quiet'])
    self.assertEqual(return_value, mock_issue_return_value)

  def testGetRegionFromZone(self):
    zone = 'us-central1-xyz'
    self.assertEqual(util.GetRegionFromZone(zone), 'us-central1')

  def testGetAllZones(self):
    test_output = inspect.cleandoc("""
        us-east1-a
        us-west1-b
        """)
    self.enter_context(_MockIssueCommand(test_output))

    found_zones = util.GetAllZones()

    expected_zones = {'us-east1-a', 'us-west1-b'}
    self.assertEqual(found_zones, expected_zones)

  def testGetZonesInRegion(self):
    test_output = inspect.cleandoc("""
        us-east1-a
        us-east1-b
        """)
    self.enter_context(_MockIssueCommand(test_output))

    found_zones = util.GetZonesInRegion('test_region')

    expected_zones = {'us-east1-a', 'us-east1-b'}
    self.assertEqual(found_zones, expected_zones)

  def testGetAllRegions(self):
    test_output = inspect.cleandoc("""
        us-east1
        us-east2
        """)
    self.enter_context(_MockIssueCommand(test_output))

    found_regions = util.GetAllRegions()

    expected_regions = {'us-east1', 'us-east2'}
    self.assertEqual(found_regions, expected_regions)

  def testGetGeoFromRegion(self):
    test_region = 'us-central1'

    found_geo = util.GetGeoFromRegion(test_region)

    expected_geo = 'us'
    self.assertEqual(found_geo, expected_geo)

  def testGetRegionsInGeo(self):
    test_output = inspect.cleandoc("""
        us-west1
        us-west2
        asia-southeast1
        """)
    self.enter_context(_MockIssueCommand(test_output))

    found_regions = util.GetRegionsInGeo('us')

    expected_regions = {'us-west1', 'us-west2'}
    self.assertEqual(found_regions, expected_regions)

  @parameterized.named_parameters(
      ('rate_limit_exceeded',
       'ERROR: (gcloud.compute.instances.create) Could not fetch resource:\n'
       '  - Rate Limit Exceeded', True),
      ('legacy_add_labels', 'ERROR: (gcloud.compute.disks.add-labels) '
       "PERMISSION_DENIED: Quota exceeded for quota group 'ReadGroup' and "
       "limit 'Read requests per 100 seconds' of service "
       "'compute.googleapis.com' for consumer 'project_number:012345678901'.",
       True),
      ('no match', 'not a rate limit error message', False),
      ('add_labels',
       "ERROR: (gcloud.compute.disks.add-labels) PERMISSION_DENIED: "
       "Quota exceeded for quota group 'default' and limit "
       "'Queries per 100 seconds' of service 'compute.googleapis.com' for "
       "consumer 'project_number:300314462293'.", True),
      )
  def testGcloudCommand(self, error_text: str, is_rate_limit_message: bool):
    self.assertEqual(
        util.GcloudCommand._IsIssueRateLimitMessage(error_text),
        is_rate_limit_message)


if __name__ == '__main__':
  unittest.main()
