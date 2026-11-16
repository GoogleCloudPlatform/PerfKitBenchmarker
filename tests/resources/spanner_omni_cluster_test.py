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
from absl.testing import flagsaver
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.gcp import google_kubernetes_engine
from perfkitbenchmarker.resources import spanner_omni_cluster
from perfkitbenchmarker.resources import spanner_omni_cluster_spec
from perfkitbenchmarker.resources.container_service import container
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_ZONES = 'us-central1-a,us-central1-b,us-central1-c'


class FakeSpannerOmniClusterSpec(
    spanner_omni_cluster_spec.SpannerOmniClusterSpec
):
  """Provider spec stub."""

  CLOUD = 'FakeOmniCloud'


class FakeSpannerOmniCluster(spanner_omni_cluster.BaseSpannerOmniCluster):
  """Provider stub used to test the cloud-agnostic base class."""

  CLOUD = 'FakeOmniCloud'
  STORAGE_CLASS = 'fake-storage-class'


def _Spec(**kwargs):
  kwargs.setdefault('cloud', FakeSpannerOmniCluster.CLOUD)
  kwargs.setdefault('database_name', 'benchbase')
  kwargs.setdefault('license_uri', '/fake/spanner_omni_license')
  return FakeSpannerOmniClusterSpec(
      'test_component', flag_values=FLAGS, **kwargs
  )


class SpannerOmniClusterTestBase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.mark_as_parsed()
    self.mock_cluster = mock.create_autospec(
        google_kubernetes_engine.BaseGkeCluster, instance=True
    )
    self.mock_cluster.zones = _ZONES.split(',')
    self.mock_cluster.zone = _ZONES
    self.mock_cluster.num_nodes = 1
    self.mock_cluster.default_nodepool = mock.create_autospec(
        container.BaseNodePoolConfig, instance=True
    )
    self.mock_cluster.default_nodepool.machine_type = 'c4-standard-8'
    self.mock_cluster.GetResourceMetadata.return_value = {'cluster': 'k8s'}
    self.mock_client_vm = mock.create_autospec(
        virtual_machine.BaseVirtualMachine, instance=True, spec_set=False
    )
    self.mock_client_vm.zone = 'us-central1-a'
    self.mock_client_vm.machine_type = 'n2-standard-32'

  def _Construct(self, omni_spec, cluster=None):
    omni_cluster = FakeSpannerOmniCluster(
        omni_spec, self.mock_cluster if cluster is None else cluster
    )
    omni_cluster.SetVms({'clients': [self.mock_client_vm]})
    return omni_cluster

  def _NewCluster(self, **spec_kwargs):
    return self._Construct(
        _Spec(root_servers_per_zone=1, **spec_kwargs), self.mock_cluster
    )


class SpecTest(SpannerOmniClusterTestBase):

  def testDefaults(self):
    omni_spec = _Spec()
    self.assertEqual(omni_spec.cloud, FakeSpannerOmniCluster.CLOUD)
    self.assertEqual(omni_spec.client_vm_group, 'clients')
    self.assertIsNone(omni_spec.replicas_per_zone)
    self.assertIsNone(omni_spec.zones)
    self.assertIsNone(omni_spec.root_servers_per_zone)
    self.assertEqual(omni_spec.cpu_request, '7')
    self.assertEqual(omni_spec.memory_request, '23Gi')
    self.assertEqual(omni_spec.data_disk_size, '1200Gi')
    self.assertTrue(omni_spec.run_analyze)

  @flagsaver.flagsaver(
      spanner_omni_zones=['us-central1-a', 'us-central1-b'],
      spanner_omni_replicas_per_zone=4,
      spanner_omni_data_disk_size='600Gi',
      spanner_omni_memory_request='50Gi',
  )
  def testFlagsOverrideConfig(self):
    FLAGS['spanner_omni_zones'].present = 1
    FLAGS['spanner_omni_replicas_per_zone'].present = 1
    FLAGS['spanner_omni_data_disk_size'].present = 1
    FLAGS['spanner_omni_memory_request'].present = 1

    omni_spec = _Spec(
        zones=['us-central1-c'],
        replicas_per_zone=2,
        data_disk_size='100Gi',
        memory_request='8Gi',
    )

    self.assertEqual(omni_spec.zones, ['us-central1-a', 'us-central1-b'])
    self.assertEqual(omni_spec.replicas_per_zone, 4)
    self.assertEqual(omni_spec.data_disk_size, '600Gi')
    self.assertEqual(omni_spec.memory_request, '50Gi')

  def testLicenseUriDefinedOnBaseSpec(self):
    omni_spec = spanner_omni_cluster_spec.SpannerOmniClusterSpec(
        'test_component', flag_values=FLAGS, cloud='Unknown'
    )
    self.assertIsNone(omni_spec.license_uri)

  @flagsaver.flagsaver(spanner_omni_license_uri='/path/from/flag')
  def testLicenseUriFlagOverridesConfig(self):
    FLAGS['spanner_omni_license_uri'].present = 1

    omni_spec = _Spec(license_uri='/path/from/config')

    self.assertEqual(omni_spec.license_uri, '/path/from/flag')

  def testCheckSpecPrerequisitesRequiresLicenseUri(self):
    with self.assertRaisesRegex(
        errors.Config.InvalidValue, 'spanner_omni_license_uri must be set'
    ):
      FakeSpannerOmniCluster.CheckSpecPrerequisites(_Spec(license_uri=None))

  def testGetSpannerOmniClusterSpecClass(self):
    self.assertIs(
        spanner_omni_cluster_spec.GetSpannerOmniClusterSpecClass(
            FakeSpannerOmniCluster.CLOUD
        ),
        FakeSpannerOmniClusterSpec,
    )
    self.assertIs(
        spanner_omni_cluster_spec.GetSpannerOmniClusterSpecClass('Unknown'),
        spanner_omni_cluster_spec.SpannerOmniClusterSpec,
    )

  def testDecodedFromBenchmarkConfig(self):
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        'omni_benchmark',
        flag_values=FLAGS,
        vm_groups={},
        spanner_omni_cluster={
            'cloud': 'GCP',
            'database_name': 'sbtest',
        },
    )
    omni_spec = config_spec.spanner_omni_cluster  # pyrefly: ignore[missing-attribute]
    # Decoded with the provider spec registered for the cloud.
    self.assertEqual(type(omni_spec).__name__, 'GcpSpannerOmniClusterSpec')
    self.assertEqual(omni_spec.cloud, 'GCP')
    self.assertEqual(omni_spec.database_name, 'sbtest')
    self.assertIsNone(omni_spec.license_uri)

  @flagsaver.flagsaver(cloud='GCP')
  def testDecodedFromBenchmarkConfigRespectsCloudFlagOverride(self):
    FLAGS['cloud'].present = 1
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        'omni_benchmark',
        flag_values=FLAGS,
        vm_groups={},
        spanner_omni_cluster={
            'cloud': 'AWS',
            'database_name': 'sbtest',
        },
    )
    omni_spec = config_spec.spanner_omni_cluster  # pyrefly: ignore[missing-attribute]
    self.assertEqual(type(omni_spec).__name__, 'GcpSpannerOmniClusterSpec')
    self.assertEqual(omni_spec.cloud, 'GCP')

  @flagsaver.flagsaver
  def testDecodeRaisesWhenNoCloudResolved(self):
    FLAGS.cloud = None
    with self.assertRaisesRegex(
        errors.Config.MissingOption, 'No cloud specified'
    ):
      benchmark_config_spec.BenchmarkConfigSpec(
          'omni_benchmark',
          flag_values=FLAGS,
          vm_groups={},
          spanner_omni_cluster={'database_name': 'sbtest'},
      )

  def testGetSpannerOmniClusterClass(self):
    self.assertIs(
        spanner_omni_cluster.GetSpannerOmniClusterClass(
            FakeSpannerOmniCluster.CLOUD
        ),
        FakeSpannerOmniCluster,
    )


class ResourceSkeletonTest(SpannerOmniClusterTestBase):

  def testCheckPrerequisitesRunsSpecPrerequisites(self):
    cluster = self._NewCluster()
    mock_check = self.enter_context(
        mock.patch.object(
            FakeSpannerOmniCluster, 'CheckSpecPrerequisites', autospec=True
        )
    )

    cluster.CheckPrerequisites()

    mock_check.assert_called_once_with(cluster.spec)

  def testSetVmsPicksConfiguredClientVmGroup(self):
    loader_vm = mock.create_autospec(
        virtual_machine.BaseVirtualMachine, instance=True
    )
    cluster = FakeSpannerOmniCluster(
        _Spec(client_vm_group='loaders'), self.mock_cluster
    )
    cluster.SetVms({'clients': [self.mock_client_vm], 'loaders': [loader_vm]})

    self.assertIs(cluster.client_vm, loader_vm)

  def testSetVmsRaisesWhenClientVmGroupMissingOrEmpty(self):
    cluster = FakeSpannerOmniCluster(_Spec(), self.mock_cluster)
    for vm_groups in ({}, {'clients': []}):
      with self.assertRaisesRegex(
          errors.Config.InvalidValue, "requires the 'clients' vm_group"
      ):
        cluster.SetVms(vm_groups)

  def testClientVmWithoutSetVmsRaises(self):
    cluster = FakeSpannerOmniCluster(_Spec(), self.mock_cluster)

    with self.assertRaisesRegex(
        errors.Resource.CreationError, r'no client VM; call SetVms'
    ):
      _ = cluster.client_vm

  def testZonesReadFromGkeClusterZonesList(self):
    gke_cluster = mock.create_autospec(
        google_kubernetes_engine.BaseGkeCluster, instance=True
    )
    gke_cluster.zones = ['us-east4-a', 'us-east4-b']
    gke_cluster.zone = 'us-east4-a,us-east4-b'
    gke_cluster.num_nodes = 1
    cluster = self._Construct(_Spec(replicas_per_zone=1), gke_cluster)
    self.assertEqual(cluster.zones, ['us-east4-a', 'us-east4-b'])

  def testSpecZonesOverrideContainerClusterZones(self):
    gke_cluster = mock.create_autospec(
        google_kubernetes_engine.BaseGkeCluster, instance=True
    )
    gke_cluster.zones = ['us-central1-a', 'us-central1-b', 'us-central1-c']
    gke_cluster.zone = 'us-central1-a,us-central1-b,us-central1-c'
    gke_cluster.num_nodes = 1
    omni_spec = _Spec(
        zones=['us-central1-a', 'us-central1-b'],
    )
    omni_cluster = self._Construct(omni_spec, gke_cluster)
    omni_cluster.CheckPrerequisites()
    self.assertEqual(omni_cluster.zones, ['us-central1-a', 'us-central1-b'])

  def testCheckPrerequisitesRejectsZonesOutsideCluster(self):
    gke_cluster = mock.create_autospec(
        google_kubernetes_engine.BaseGkeCluster, instance=True
    )
    gke_cluster.zones = ['us-central1-a', 'us-central1-b']
    gke_cluster.zone = 'us-central1-a,us-central1-b'
    gke_cluster.num_nodes = 1
    omni_spec = _Spec(
        zones=['us-central1-a', 'us-central1-f'],
    )
    omni_cluster = self._Construct(omni_spec, gke_cluster)
    with self.assertRaisesRegex(
        errors.Config.InvalidValue,
        r"spanner_omni_zones \['us-central1-f'\] must be a subset",
    ):
      omni_cluster.CheckPrerequisites()

  def testCheckPrerequisitesRejectsRegionalGkeClusterWithoutSpecZones(self):
    gke_cluster = mock.create_autospec(
        google_kubernetes_engine.BaseGkeCluster, instance=True
    )
    gke_cluster.zones = []
    gke_cluster.zone = 'us-central1'
    gke_cluster.region = 'us-central1'
    gke_cluster.num_nodes = 1
    omni_cluster = self._Construct(_Spec(), gke_cluster)
    self.assertEqual(omni_cluster.zones, [])
    with self.assertRaisesRegex(
        errors.Config.InvalidValue, 'no zones configured'
    ):
      omni_cluster.CheckPrerequisites()

  def testCheckPrerequisitesRejectsNodeCountMismatch(self):
    self.mock_cluster.num_nodes = 1
    omni_cluster = self._Construct(_Spec(replicas_per_zone=3))
    with self.assertRaisesRegex(
        errors.Config.InvalidValue,
        r'spanner_omni_replicas_per_zone \(3\) must match container_cluster'
        r' node count per zone \(1\)',
    ):
      omni_cluster.CheckPrerequisites()

  def testCheckPrerequisitesRejectsMoreRootServersThanReplicas(self):
    self.mock_cluster.num_nodes = 2
    omni_cluster = self._Construct(_Spec(root_servers_per_zone=3))
    with self.assertRaisesRegex(
        errors.Config.InvalidValue,
        r'root_servers_per_zone \(3\) must not exceed servers per zone \(2\)',
    ):
      omni_cluster.CheckPrerequisites()

  def testReplicasPerZoneDefaultsToContainerClusterNumNodes(self):
    self.mock_cluster.num_nodes = 2
    cluster = self._Construct(_Spec(), self.mock_cluster)
    self.assertEqual(cluster.replicas_per_zone, 2)
    self.assertEqual(cluster.root_servers_per_zone, 2)

  def testRootServersDefaultToServersPerZoneWhenFewerThanThree(self):
    cluster = self._Construct(_Spec(), self.mock_cluster)
    self.assertEqual(cluster.root_servers_per_zone, 1)

  def testMetadataWithoutClientVmOmitsClientFields(self):
    metadata = FakeSpannerOmniCluster(
        _Spec(replicas_per_zone=1), self.mock_cluster
    ).GetResourceMetadata()

    self.assertNotIn('omni_client_zone', metadata)
    self.assertNotIn('omni_client_machine_type', metadata)

  def testGetResourceMetadata(self):
    cluster = self._NewCluster(version='2026.r5')

    metadata = cluster.GetResourceMetadata()

    self.assertNotIn('cluster', metadata)
    self.assertEqual(metadata['omni_zones'], _ZONES)
    self.assertEqual(metadata['omni_replicas_per_zone'], 1)
    self.assertEqual(metadata['omni_root_servers_per_zone'], 1)
    for key in metadata:
      self.assertTrue(key.startswith('omni_') or key == 'db_engine', key)
    self.assertEqual(metadata['omni_storage_class'], 'fake-storage-class')
    self.assertEqual(metadata['omni_cluster_machine_type'], 'c4-standard-8')
    self.assertEqual(metadata['omni_client_machine_type'], 'n2-standard-32')
    self.assertEqual(metadata['omni_client_zone'], 'us-central1-a')
    self.assertEqual(metadata['omni_version'], '2026.r5')


if __name__ == '__main__':
  unittest.main()
