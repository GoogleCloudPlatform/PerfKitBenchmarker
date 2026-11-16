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

import unittest
from unittest import mock

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.providers.gcp import gcp_spanner_omni_cluster
from perfkitbenchmarker.providers.gcp import google_kubernetes_engine
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

  def testGcpMetadataUsesHyperdisk(self):
    cluster = self._NewCluster()
    self.assertEqual(
        cluster.GetResourceMetadata()['omni_storage_class'],
        'hyperdisk-balanced-rwo',
    )

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
