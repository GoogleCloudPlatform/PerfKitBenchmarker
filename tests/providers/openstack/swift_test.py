"""Tests for perfkitbenchmarker.providers.openstack.swift."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import unittest
import mock
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.openstack import swift


class SwiftTest(unittest.TestCase):

  def setUp(self):
    super(SwiftTest, self).setUp()
    p = mock.patch(swift.__name__ + '.FLAGS')
    self.mock_flags = p.start()
    self.addCleanup(p.stop)
    self.mock_flags.openstack_swift_insecure = False

  @mock.patch.dict(os.environ, {'OS_AUTH_URL': 'OS_AUTH_URL',
                                'OS_TENANT_NAME': 'OS_TENANT_NAME',
                                'OS_USERNAME': 'OS_USERNAME',
                                'OS_PASSWORD': 'OS_PASSWORD'})
  def testMakeBucket(self):
    swift_storage_service = swift.SwiftStorageService()
    swift_storage_service.PrepareService('location')

    with mock.patch(vm_util.__name__ + '.IssueCommand',
                    return_value=('stdout', 'stderr', 0)) as mock_util:
      swift_storage_service.MakeBucket('new_bucket')
      mock_util.assert_called_with(['swift',
                                    '--os-auth-url', 'OS_AUTH_URL',
                                    '--os-tenant-name', 'OS_TENANT_NAME',
                                    '--os-username', 'OS_USERNAME',
                                    '--os-password', 'OS_PASSWORD',
                                    'post',
                                    'new_bucket'],
                                   raise_on_failure=False)


if __name__ == '__main__':
  unittest.main()
