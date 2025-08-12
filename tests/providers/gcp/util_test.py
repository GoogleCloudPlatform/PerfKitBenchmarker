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


import collections
import importlib
import inspect
import unittest

from absl.testing import parameterized
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import util
from tests import pkb_common_test_case


_GCLOUD_PATH = 'path/gcloud'


def _MockIssueCommand(test_response):
  return mock.patch.object(
      vm_util,
      'IssueCommand',
      autospec=True,
      return_value=[test_response, None, None],
  )


class GceResource(resource.BaseResource):

  def __init__(self, **kwargs):
    for k, v in kwargs.items():
      setattr(self, k, v)

  def _Create(self):
    raise NotImplementedError()

  def _Delete(self):
    raise NotImplementedError()


class GcloudCommandTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    p = mock.patch(util.__name__ + '.FLAGS')
    self.mock_flags = p.start()
    self.addCleanup(p.stop)
    self.mock_flags.gcloud_path = _GCLOUD_PATH
    self.mock_flags.project = None

  def testCommonFlags(self):
    cmd = util.GcloudCommand(None, 'compute', 'images', 'list')
    self.assertEqual(
        cmd.GetCommand(),
        [
            'path/gcloud',
            'compute',
            'images',
            'list',
            '--format',
            'json',
            '--quiet',
        ],
    )

  def testCommonFlagsWithGlobalProject(self):
    self.mock_flags.project = 'default-project'
    cmd = util.GcloudCommand(None, 'compute', 'images', 'list')
    self.assertEqual(
        cmd.GetCommand(),
        [
            'path/gcloud',
            'compute',
            'images',
            'list',
            '--format',
            'json',
            '--project',
            'default-project',
            '--quiet',
        ],
    )

  def testCommonFlagsWithResource(self):
    gce_resource = GceResource(project=None)
    cmd = util.GcloudCommand(
        gce_resource, 'compute', 'images', 'describe', 'fake-image'
    )
    self.assertEqual(
        cmd.GetCommand(),
        [
            'path/gcloud',
            'compute',
            'images',
            'describe',
            'fake-image',
            '--format',
            'json',
            '--quiet',
        ],
    )

  # This case is pathological. If --project is set every GCP resource we create
  # should have project set.  It should honestly probably raise an error.
  def testCommonFlagsWithResourceAndProject(self):
    self.mock_flags.project = 'default-project'
    gce_resource = GceResource(project=None)
    cmd = util.GcloudCommand(
        gce_resource, 'compute', 'images', 'describe', 'fake-image'
    )
    self.assertEqual(
        cmd.GetCommand(),
        [
            'path/gcloud',
            'compute',
            'images',
            'describe',
            'fake-image',
            '--format',
            'json',
            '--project',
            'default-project',
            '--quiet',
        ],
    )

  def testCommonFlagsWithResourceWithProject(self):
    gce_resource = GceResource(project='test-project')
    cmd = util.GcloudCommand(
        gce_resource, 'compute', 'images', 'describe', 'fake-image'
    )
    self.assertEqual(
        cmd.GetCommand(),
        [
            'path/gcloud',
            'compute',
            'images',
            'describe',
            'fake-image',
            '--format',
            'json',
            '--project',
            'test-project',
            '--quiet',
        ],
    )

  def testCommonFlagsWithResourceWithProjectAndZone(self):
    gce_resource = GceResource(project='test-project', zone='test-zone')
    cmd = util.GcloudCommand(
        gce_resource, 'compute', 'images', 'describe', 'fake-image'
    )
    self.assertEqual(
        cmd.GetCommand(),
        [
            'path/gcloud',
            'compute',
            'images',
            'describe',
            'fake-image',
            '--format',
            'json',
            '--project',
            'test-project',
            '--quiet',
            '--zone',
            'test-zone',
        ],
    )

  def testListValue(self):
    gce_resource = GceResource(project=None)
    cmd = util.GcloudCommand(gce_resource, 'compute', 'instances', 'create')
    cmd.flags['local-ssd'] = ['interface=nvme', 'interface=SCSI']
    self.assertEqual(
        cmd.GetCommand(),
        [
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
        ],
    )

  def testIssue(self):
    gce_resource = GceResource(project=None)
    cmd = util.GcloudCommand(gce_resource, 'compute', 'images', 'list')
    mock_issue_return_value = ('issue-return-value', 'stderr', 0)
    p = mock.patch(
        util.__name__ + '.vm_util.IssueCommand',
        return_value=mock_issue_return_value,
    )
    with p as mock_issue:
      return_value = cmd.Issue()
      mock_issue.assert_called_with(
          [
              'path/gcloud',
              'compute',
              'images',
              'list',
              '--format',
              'json',
              '--quiet',
          ],
          stack_level=mock.ANY,
      )
    self.assertEqual(return_value, mock_issue_return_value)

  def testIssueWarningSuppressed(self):
    gce_resource = GceResource(project=None)
    cmd = util.GcloudCommand(gce_resource, 'compute', 'images', 'list')
    mock_issue_return_value = ('issue-return-value', 'stderr', 0)
    p = mock.patch(
        util.__name__ + '.vm_util.IssueCommand',
        return_value=mock_issue_return_value,
    )
    with p as mock_issue:
      return_value = cmd.Issue()
      mock_issue.assert_called_with(
          [
              'path/gcloud',
              'compute',
              'images',
              'list',
              '--format',
              'json',
              '--quiet',
          ],
          stack_level=mock.ANY,
      )
    self.assertEqual(return_value, mock_issue_return_value)

  def testIssueRetryable(self):
    gce_resource = GceResource(project=None)
    cmd = util.GcloudCommand(gce_resource, 'compute', 'images', 'list')
    mock_issue_return_value = ('issue-return-value', 'stderr', 0)
    p = mock.patch(
        util.__name__ + '.vm_util.IssueRetryableCommand',
        return_value=mock_issue_return_value,
    )
    with p as mock_issue:
      return_value = cmd.IssueRetryable()
      mock_issue.assert_called_with(
          [
              'path/gcloud',
              'compute',
              'images',
              'list',
              '--format',
              'json',
              '--quiet',
          ],
          stack_level=mock.ANY,
      )
    self.assertEqual(return_value, mock_issue_return_value)

  def testGetRegionFromZone(self):
    zone = 'us-central1-xyz'
    self.assertEqual(util.GetRegionFromZone(zone), 'us-central1')

  @mock.patch.object(vm_util, 'IssueCommand', autospec=True)
  def testGetAllZones(self, mock_issue_command):
    self.mock_flags.project = 'default-project'
    test_output = inspect.cleandoc("""
        us-east1-a
        us-west1-b
        """)
    mock_issue_command.return_value = (test_output, None, 0)

    found_zones = util.GetAllZones()

    expected_zones = {'us-east1-a', 'us-west1-b'}
    self.assertEqual(found_zones, expected_zones)
    cmd = mock_issue_command.call_args[0][0]
    self.assertContainsExactSubsequence(cmd, ['--project', 'default-project'])

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

  @mock.patch.object(vm_util, 'IssueCommand', autospec=True)
  def testGetZonesFromMachineType(self, mock_issue_command):
    machine_type_zone_output = inspect.cleandoc("""
        us-west1-a
        us-west2-a
        """)
    usable_zone_output = inspect.cleandoc("""
        us-west1-a
        us-west1-b
        """)
    mock_issue_command.side_effect = [
        (machine_type_zone_output, None, 0),
        (usable_zone_output, None, 0),
    ]

    found_zones = util.GetZonesFromMachineType('test-machine-type')

    expected_zones = {'us-west1-a'}
    self.assertEqual(found_zones, expected_zones)

  @mock.patch.object(vm_util, 'IssueCommand', autospec=True)
  def testGetZonesFromMachineTypeNoneAvailable(self, mock_issue_command):
    machine_type_zone_output = ''
    usable_zone_output = inspect.cleandoc("""
        us-west1-a
        us-west1-b
        """)
    mock_issue_command.side_effect = [
        (machine_type_zone_output, None, 0),
        (usable_zone_output, None, 0),
    ]

    found_zones = util.GetZonesFromMachineType('test-machine-type')

    expected_zones = {'us-west1-a', 'us-west1-b'}
    self.assertEqual(found_zones, expected_zones)

  def testProjectNumber(self):
    test_output = inspect.cleandoc("""
[
  {
    "createTime": "2021-07-19T17:07:24.467Z",
    "lifecycleState": "ACTIVE",
    "name": "project-id-name",
    "parent": {
      "id": "123",
      "type": "organization"
    },
    "projectId": "project-id-name",
    "projectNumber": "12345"
  }
]
        """)
    self.enter_context(_MockIssueCommand(test_output))

    project_number = util.GetProjectNumber('project-id-name')

    self.assertEqual(project_number, '12345')

  @parameterized.named_parameters(
      (
          'rate_limit_exceeded',
          (
              'ERROR: (gcloud.compute.instances.create) Could not fetch'
              ' resource:\n  - Rate Limit Exceeded'
          ),
          True,
      ),
      (
          'legacy_add_labels',
          (
              'ERROR: (gcloud.compute.disks.add-labels) PERMISSION_DENIED:'
              " Quota exceeded for quota group 'ReadGroup' and limit 'Read"
              " requests per 100 seconds' of service 'compute.googleapis.com'"
              " for consumer 'project_number:012345678901'."
          ),
          True,
      ),
      ('no match', 'not a rate limit error message', False),
      (
          'add_labels',
          (
              'ERROR: (gcloud.compute.disks.add-labels) PERMISSION_DENIED:'
              " Quota exceeded for quota group 'default' and limit 'Queries per"
              " 100 seconds' of service 'compute.googleapis.com' for consumer"
              " 'project_number:300314462293'."
          ),
          True,
      ),
  )
  def testGcloudCommand(self, error_text: str, is_rate_limit_message: bool):
    self.assertEqual(
        util.GcloudCommand._IsIssueRateLimitMessage(error_text),
        is_rate_limit_message,
    )


class GetDefaultProjectTest(parameterized.TestCase):
  """Separate class in order to bypass PkbCommonTestCase's setUp."""

  def setUp(self):
    super().setUp()
    # Resets the cache so each test case simulates the first invocation of
    # GetDefaultProject.
    importlib.reload(util)

  @parameterized.named_parameters(
      {
          'testcase_name': 'MaxRetries',
          'side_effects': [KeyError('key error')] * 4,
          'expected_exception': vm_util.RetriesExceededRetryError,
          'expected_result': None,
          'expected_call_count': 4,
      },
      {
          'testcase_name': 'RetryableErrorsThenSuccess',
          'side_effects': [
              KeyError('key error'),
              errors.VmUtil.IssueCommandError('issue command error'),
              ('{"core": {"project": "test-project"}}', '', 0),
          ],
          'expected_exception': None,
          'expected_result': 'test-project',
          'expected_call_count': 3,
      },
      {
          'testcase_name': 'NonRetryableError',
          'side_effects': [ValueError('value error')],
          'expected_exception': ValueError,
          'expected_result': None,
          'expected_call_count': 1,
      },
      {
          'testcase_name': 'SuccessOnFirstTry',
          'side_effects': [('{"core": {"project": "test-project"}}', '', 0)],
          'expected_exception': None,
          'expected_result': 'test-project',
          'expected_call_count': 1,
      },
  )
  @mock.patch.object(vm_util, 'IssueCommand')
  def testGetDefaultProject(
      self,
      mock_issue_command,
      side_effects,
      expected_exception,
      expected_result,
      expected_call_count,
  ):
    mock_issue_command.side_effect = side_effects

    if expected_exception:
      with self.assertRaises(expected_exception):
        util.GetDefaultProject()
    else:
      self.assertEqual(util.GetDefaultProject(), expected_result)

    self.assertEqual(mock_issue_command.call_count, expected_call_count)


class GcpUtilTest(pkb_common_test_case.PkbCommonTestCase):

  def testSplitTags(self):
    tags = 'a=b,c=d'
    self.assertEqual(
        collections.OrderedDict([('a', 'b'), ('c', 'd')]), util.SplitTags(tags)
    )

  def testFormatAndSplitAreInverses(self):
    tags = {'a': 'b', 'c': 'd'}
    self.assertEqual(tags, util.SplitTags(util.FormatTags(tags)))


if __name__ == '__main__':
  unittest.main()
