# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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

import os
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import spanner_omni_cluster as gcp_spanner_omni_cluster
from perfkitbenchmarker.providers.gcp import util as gcp_util
from perfkitbenchmarker.resources import spanner_omni_cluster
from perfkitbenchmarker.resources import spanner_omni_cluster_spec
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


def _Spec(**kwargs):
  return gcp_spanner_omni_cluster.GcpSpannerOmniClusterSpec(
      'test_component',
      flag_values=FLAGS,
      cloud='GCP',
      database_name='benchbase',
      root_servers_per_zone=1,
      **kwargs,
  )


class GcpSpannerOmniClusterTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.mark_as_parsed()
    self.temp_dir = self.create_tempdir().full_path
    self.enter_context(
        mock.patch.object(
            vm_util, 'GetTempDir', autospec=True, return_value=self.temp_dir
        )
    )
    self.mock_gcloud_cmd = mock.create_autospec(
        gcp_util.GcloudCommand, instance=True, spec_set=False
    )
    self.mock_gcloud_cmd.flags = {}
    self.mock_gcloud_cmd.Issue.return_value = ('', '', 0)
    self.mock_gcloud = self.enter_context(
        mock.patch.object(
            gcp_util,
            'GcloudCommand',
            autospec=True,
            return_value=self.mock_gcloud_cmd,
        )
    )
    self.mock_cluster = mock.MagicMock(zone='us-central1-a', num_nodes=1)
    self.mock_cluster.GetResourceMetadata.return_value = {}
    self.mock_client_vm = mock.create_autospec(
        virtual_machine.BaseVirtualMachine, instance=True
    )

  def _NewCluster(self, **spec_kwargs):
    cluster = gcp_spanner_omni_cluster.GcpSpannerOmniCluster(
        _Spec(**spec_kwargs), self.mock_cluster
    )
    cluster.SetVms({'clients': [self.mock_client_vm]})
    return cluster

  def testRegisteredForGcp(self):
    self.assertIs(
        spanner_omni_cluster.GetSpannerOmniClusterClass('GCP'),
        gcp_spanner_omni_cluster.GcpSpannerOmniCluster,
    )
    self.assertIs(
        spanner_omni_cluster_spec.GetSpannerOmniClusterSpecClass('GCP'),
        gcp_spanner_omni_cluster.GcpSpannerOmniClusterSpec,
    )

  @flagsaver.flagsaver(
      spanner_omni_license_secret_name='license',
      spanner_omni_license_secret_project='project',
  )
  def testGcpFlagsOverrideSpec(self):
    for name in (
        'spanner_omni_license_secret_name',
        'spanner_omni_license_secret_project',
    ):
      FLAGS[name].present = 1

    omni_spec = _Spec(license_secret_name='other')

    self.assertEqual(omni_spec.license_secret_name, 'license')
    self.assertEqual(omni_spec.license_secret_project, 'project')

  def testGcpSpecDefaults(self):
    omni_spec = _Spec()
    self.assertIsNone(omni_spec.license_secret_name)
    self.assertIsNone(omni_spec.license_secret_project)

  def testHelmUsesGkePlatformAndHyperdisk(self):
    cluster = self._NewCluster()
    cluster.omni_version = '2026.r5'

    helm_args = cluster._BuildHelmInstallCommand('/tmp/license.bin')

    self.assertIn('global.platform=gke', helm_args)
    self.assertIn('dataStorageClass=hyperdisk-balanced-rwo', helm_args)
    self.assertIn('logsStorageClass=hyperdisk-balanced-rwo', helm_args)
    self.assertEqual(
        cluster.GetResourceMetadata()['spanner_omni_storage_class'],
        'hyperdisk-balanced-rwo',
    )

  def testFetchLicenseKeyFileUsesSecretManager(self):
    cluster = self._NewCluster(
        license_secret_name='license', license_secret_project='project'
    )

    path = cluster._FetchLicenseKeyFile()

    self.assertEqual(
        path, os.path.join(self.temp_dir, 'spanner_omni_license.bin')
    )
    self.mock_gcloud.assert_called_once_with(
        None, 'secrets', 'versions', 'access', 'latest'
    )
    self.assertEqual(
        self.mock_gcloud_cmd.flags,
        {'secret': 'license', 'project': 'project', 'out-file': path},
    )

  def testFetchLicenseKeyFileRaisesWhenSecretUnset(self):
    with self.assertRaisesRegex(
        errors.Config.InvalidValue,
        'spanner_omni_license_secret_name and'
        ' spanner_omni_license_secret_project must be set',
    ):
      self._NewCluster()._FetchLicenseKeyFile()

  def testFetchLicenseKeyFileRaisesOnGcloudFailure(self):
    self.mock_gcloud_cmd.Issue.return_value = ('', 'PERMISSION_DENIED', 1)
    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'Failed to fetch Spanner Omni license'
    ):
      self._NewCluster(
          license_secret_name='license', license_secret_project='project'
      )._FetchLicenseKeyFile()

  def testCheckSpecPrerequisitesPassesWithLicenseSecret(self):
    gcp_spanner_omni_cluster.GcpSpannerOmniCluster.CheckSpecPrerequisites(
        _Spec(license_secret_name='license', license_secret_project='project')
    )
    self.mock_gcloud.assert_not_called()

  def testCheckSpecPrerequisitesRequiresLicenseSecret(self):
    with self.assertRaisesRegex(
        errors.Config.InvalidValue,
        'spanner_omni_license_secret_name and'
        ' spanner_omni_license_secret_project must be set',
    ):
      gcp_spanner_omni_cluster.GcpSpannerOmniCluster.CheckSpecPrerequisites(
          _Spec(license_secret_name='license')
      )
    self.mock_gcloud.assert_not_called()


if __name__ == '__main__':
  unittest.main()
