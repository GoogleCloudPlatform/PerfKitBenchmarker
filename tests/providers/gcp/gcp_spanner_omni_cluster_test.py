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

import json
import os
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gcp_spanner_omni_cluster
from perfkitbenchmarker.providers.gcp import google_kubernetes_engine
from perfkitbenchmarker.providers.gcp import util as gcp_util
from perfkitbenchmarker.resources import spanner_omni_cluster
from perfkitbenchmarker.resources import spanner_omni_cluster_spec
from perfkitbenchmarker.resources.container_service import container
from tests import pkb_common_test_case

FLAGS = flags.FLAGS
_LICENSE_URI = 'projects/project/secrets/license/versions/latest'


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
    self.mock_cluster = mock.create_autospec(
        google_kubernetes_engine.BaseGkeCluster, instance=True
    )
    self.mock_cluster.zones = ['us-central1-a']
    self.mock_cluster.zone = 'us-central1-a'
    self.mock_cluster.num_nodes = 1
    self.mock_cluster.default_nodepool = mock.create_autospec(
        container.BaseNodePoolConfig, instance=True
    )
    self.mock_cluster.default_nodepool.machine_type = 'c4-standard-8'
    self.mock_cluster.GetResourceMetadata.return_value = {}
    self.mock_client_vm = mock.create_autospec(
        virtual_machine.BaseVirtualMachine, instance=True
    )
    self.mock_client_vm.zone = 'us-central1-a'
    self.mock_client_vm.machine_type = 'n2-standard-32'

  def _NewCluster(self, **spec_kwargs):
    spec_kwargs.setdefault('replicas_per_zone', self.mock_cluster.num_nodes)
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

  @flagsaver.flagsaver(spanner_omni_gcs_backup_bucket='bucket')
  def testGcpFlagsOverrideSpec(self):
    FLAGS['spanner_omni_gcs_backup_bucket'].present = 1

    omni_spec = _Spec(gcs_backup_bucket='other')

    self.assertEqual(omni_spec.gcs_backup_bucket, 'bucket')

  def testGcpSpecRejectsBackupWithoutBucket(self):
    with self.assertRaisesRegex(
        errors.Config.InvalidValue, 'gcs_backup_bucket must be set'
    ):
      _Spec(backup_descriptor_id='desc')

  def testGcpSpecNormalizesGcsBackupBucketUri(self):
    self.assertEqual(
        _Spec(gcs_backup_bucket='  gs://bucket/  ').gcs_backup_bucket, 'bucket'
    )
    self.assertIsNone(_Spec(gcs_backup_bucket='gs://').gcs_backup_bucket)

  def testGcpSpecRejectsBucketWithObjectPath(self):
    with self.assertRaisesRegex(
        errors.Config.InvalidValue, 'without an object path prefix'
    ):
      _Spec(gcs_backup_bucket='gs://bucket/prefix')

  def testNormalizedGcsBackupBucketReachesAccessCheckAndCli(self):
    self.assertIsNone(_Spec().gcs_backup_bucket)
    self.assertEqual(self._NewCluster()._ExternalStorageCliArgs(), [])
    omni_spec = _Spec(
        gcs_backup_bucket='gs://bucket/',
        backup_descriptor_id='CIOAgICAIBDIivTowuCXAxjP9ZvqwuCXAw',
        license_uri=_LICENSE_URI,
    )

    gcp_spanner_omni_cluster.GcpSpannerOmniCluster.CheckSpecPrerequisites(
        omni_spec
    )

    self.mock_gcloud.assert_called_once_with(
        None, 'storage', 'ls', 'gs://bucket'
    )
    self.assertEqual(self.mock_gcloud_cmd.flags, {'format': 'gsutil'})
    self.mock_gcloud_cmd.Issue.assert_called_once_with(raise_on_failure=False)
    cluster = gcp_spanner_omni_cluster.GcpSpannerOmniCluster(
        omni_spec, self.mock_cluster
    )
    self.assertEqual(
        cluster._ExternalStorageCliArgs(), ['--gcs-bucket-name=bucket']
    )

  def testMetadataIncludesBackupBucket(self):
    cluster = self._NewCluster(gcs_backup_bucket='bucket')
    self.assertEqual(
        cluster.GetResourceMetadata()['omni_gcs_backup_bucket'],
        'bucket',
    )

  def testHelmUsesGkePlatformAndHyperdisk(self):
    cluster = self._NewCluster()
    cluster.omni_version = '2026.r5'

    chart = cluster._BuildHelmChart('/tmp/license.bin')

    self.assertEqual(chart.set_values['global.platform'], 'gke')
    self.assertEqual(
        chart.set_values['dataStorageClass'], 'hyperdisk-balanced-rwo'
    )
    self.assertEqual(
        chart.set_values['logsStorageClass'], 'hyperdisk-balanced-rwo'
    )
    self.assertEqual(
        cluster.GetResourceMetadata()['omni_storage_class'],
        'hyperdisk-balanced-rwo',
    )

  def testGetRegionFromZoneDelegatesToGcpUtil(self):
    cluster = self._NewCluster()
    self.assertEqual(cluster._GetRegionFromZone('us-central1-a'), 'us-central1')

  def testBuildLocationsJsonAssignsUniqueShortNames(self):
    cluster = self._NewCluster()
    locations = json.loads(
        cluster._BuildLocationsJson(
            ['us-central1-c', 'us-east1-b', 'us-west1-b']
        )
    )
    short_names = []
    for region in locations:
      short_names.extend(zone['shortName'] for zone in region['zones'])
    self.assertEqual(short_names, ['c', 'b', 'a'])

  def testBuildZoneShortNamesHandlesCollisionsAndNonLetterSuffixes(self):
    cluster = self._NewCluster()
    self.assertEqual(
        cluster._BuildZoneShortNames(
            ['us-central1-a', 'us-east1-b', 'us-west1-a', 'us-central1-2']
        ),
        {
            'us-central1-a': 'a',
            'us-east1-b': 'b',
            'us-west1-a': 'c',
            'us-central1-2': 'd',
        },
    )

  def testFetchLicenseKeyFileUsesSecretManagerVersion(self):
    cluster = self._NewCluster(license_uri=_LICENSE_URI)

    path = cluster._FetchLicenseKeyFile()

    self.assertTrue(
        path.startswith(
            os.path.join(self.temp_dir, 'spanner_omni_license.bin.')
        ),
        path,
    )
    self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)
    self.mock_gcloud.assert_called_once_with(
        None, 'secrets', 'versions', 'access', _LICENSE_URI
    )
    self.assertEqual(self.mock_gcloud_cmd.flags, {'out-file': path})

  def testFetchLicenseKeyFileCopiesLocalFile(self):
    license_file = self.create_tempfile(content='license-key')

    path = self._NewCluster(
        license_uri=license_file.full_path
    )._FetchLicenseKeyFile()

    with open(path) as f:
      self.assertEqual(f.read(), 'license-key')
    self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)
    self.mock_gcloud.assert_not_called()

  def testFetchLicenseKeyFileRejectsUnsupportedUri(self):
    with self.assertRaisesRegex(
        errors.Config.InvalidValue, 'not a supported license location'
    ):
      self._NewCluster(license_uri='gs://bucket/license')._FetchLicenseKeyFile()

  def testFetchLicenseKeyFileRaisesOnGcloudFailure(self):
    self.mock_gcloud_cmd.Issue.return_value = ('', 'PERMISSION_DENIED', 1)
    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'Failed to fetch Spanner Omni license'
    ):
      self._NewCluster(license_uri=_LICENSE_URI)._FetchLicenseKeyFile()
    self.assertEqual(os.listdir(self.temp_dir), [])

  def testCheckSpecPrerequisitesFailsWhenBucketCannotBeListed(self):
    for stderr in (
        'HTTPError 403',
        'HTTPError 404: NotFound',
        'network unreachable for gs://pkb-403-404-backups',
    ):
      with self.subTest(stderr=stderr):
        self.mock_gcloud_cmd.Issue.return_value = ('', stderr, 1)
        with self.assertRaisesRegex(
            errors.Setup.InvalidConfigurationError,
            'Cannot list Spanner Omni GCS backup bucket gs://my-backup-bucket',
        ):
          gcp_spanner_omni_cluster.GcpSpannerOmniCluster.CheckSpecPrerequisites(
              _Spec(
                  gcs_backup_bucket='my-backup-bucket',
                  license_uri=_LICENSE_URI,
              )
          )

  def testCheckSpecPrerequisitesSkipsBucketCheckWithoutBucket(self):
    gcp_spanner_omni_cluster.GcpSpannerOmniCluster.CheckSpecPrerequisites(
        _Spec(license_uri=_LICENSE_URI)
    )
    self.mock_gcloud.assert_not_called()

  def testCheckSpecPrerequisitesAcceptsSecretManagerVersion(self):
    gcp_spanner_omni_cluster.GcpSpannerOmniCluster.CheckSpecPrerequisites(
        _Spec(license_uri=_LICENSE_URI)
    )

  def testCheckSpecPrerequisitesAcceptsLocalFile(self):
    license_file = self.create_tempfile(content='license-key')
    gcp_spanner_omni_cluster.GcpSpannerOmniCluster.CheckSpecPrerequisites(
        _Spec(license_uri=license_file.full_path)
    )

  def testCheckSpecPrerequisitesRequiresLicenseUri(self):
    with self.assertRaisesRegex(
        errors.Config.InvalidValue, 'spanner_omni_license_uri must be set'
    ):
      gcp_spanner_omni_cluster.GcpSpannerOmniCluster.CheckSpecPrerequisites(
          _Spec()
      )

  def testCheckSpecPrerequisitesRejectsUnsupportedLicenseUri(self):
    for license_uri in (
        'projects/p/secrets/s',
        'gs://bucket/license',
        '/does/not/exist',
    ):
      with self.subTest(license_uri=license_uri):
        with self.assertRaisesRegex(
            errors.Config.InvalidValue, 'must be a Secret Manager'
        ):
          gcp_spanner_omni_cluster.GcpSpannerOmniCluster.CheckSpecPrerequisites(
              _Spec(license_uri=license_uri)
          )

  def testCheckPrerequisitesWithGkeCluster(self):
    gke_cluster = mock.create_autospec(
        google_kubernetes_engine.BaseGkeCluster, instance=True
    )
    gke_cluster.zones = ['us-central1-a', 'us-central1-b', 'us-central1-c']
    gke_cluster.zone = 'us-central1-a,us-central1-b,us-central1-c'
    gke_cluster.num_nodes = 2
    cluster = gcp_spanner_omni_cluster.GcpSpannerOmniCluster(
        _Spec(license_uri=_LICENSE_URI),
        gke_cluster,
    )
    cluster.SetVms({'clients': [self.mock_client_vm]})

    cluster.CheckPrerequisites()

    self.assertEqual(
        cluster.zones, ['us-central1-a', 'us-central1-b', 'us-central1-c']
    )
    self.assertEqual(cluster.replicas_per_zone, 2)

  def testCheckPrerequisitesRejectsMissingLicenseUri(self):
    cluster = self._NewCluster()
    with self.assertRaisesRegex(
        errors.Config.InvalidValue, 'spanner_omni_license_uri must be set'
    ):
      cluster.CheckPrerequisites()


if __name__ == '__main__':
  unittest.main()
