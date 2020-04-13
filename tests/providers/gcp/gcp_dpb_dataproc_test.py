# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.gcp.gcp_dpb_dataproc."""

import unittest
from absl import flags
import mock

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gcp_dpb_dataproc
from perfkitbenchmarker.providers.gcp import gcs
from tests import pkb_common_test_case

TEST_RUN_URI = 'fakeru'
GCP_ZONE_US_CENTRAL1_A = 'us-central1-a'
BUCKET_NAME = 'foo'

FLAGS = flags.FLAGS


class LocalGcpDpbDataproc(gcp_dpb_dataproc.GcpDpbDataproc):

  def __init__(self):
    self.dpb_service_zone = FLAGS.dpb_service_zone
    self.region = self.dpb_service_zone.rsplit('-', 1)[0]
    self.storage_service = gcs.GoogleCloudStorageService()
    self.storage_service.PrepareService(location=self.region)


class GcpDpbDataprocTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(GcpDpbDataprocTestCase, self).setUp()
    FLAGS.run_uri = TEST_RUN_URI
    FLAGS.dpb_service_zone = GCP_ZONE_US_CENTRAL1_A
    FLAGS.zones = [GCP_ZONE_US_CENTRAL1_A]

  def testCreateBucket(self):
    local_dataproc = LocalGcpDpbDataproc()
    with mock.patch(
        vm_util.__name__ + '.IssueCommand',
        return_value=('out_', 'err_', 0)) as mock_issue:
      local_dataproc.CreateBucket(BUCKET_NAME)
      self.assertEqual(mock_issue.call_count, 1)
      call_arg_list, _ = mock_issue.call_args
      self.assertListEqual([
          'gsutil', 'mb', '-l',
          GCP_ZONE_US_CENTRAL1_A.rsplit('-', 1)[0], '-c', 'regional',
          'gs://{}'.format(BUCKET_NAME)
      ], call_arg_list[0])

  def testDeleteBucket(self):
    local_dataproc = LocalGcpDpbDataproc()
    with mock.patch(
        vm_util.__name__ + '.IssueCommand',
        return_value=('out_', 'err_', 0)) as mock_issue:
      local_dataproc.DeleteBucket(BUCKET_NAME)
      self.assertEqual(mock_issue.call_count, 2)
      call_arg_list, _ = mock_issue.call_args
      self.assertListEqual(['gsutil', 'rb', 'gs://{}'.format(BUCKET_NAME)],
                           call_arg_list[0])


if __name__ == '__main__':
  unittest.main()
