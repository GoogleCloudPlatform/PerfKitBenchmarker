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

import base64
import io
import json
import os
import string
import time
import unittest
from unittest import mock
import urllib.request

from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.resources import spanner_omni_cluster
from perfkitbenchmarker.resources import spanner_omni_cluster_spec
from perfkitbenchmarker.resources.container_service import helm
from perfkitbenchmarker.resources.container_service import kubectl
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_ZONES = 'us-central1-a,us-central1-b,us-central1-c'
_LICENSE_FILENAME = 'fake_license.bin'


class FakeSpannerOmniClusterSpec(
    spanner_omni_cluster_spec.SpannerOmniClusterSpec
):
  """Provider spec stub."""

  CLOUD = 'FakeOmniCloud'


class FakeSpannerOmniCluster(spanner_omni_cluster.BaseSpannerOmniCluster):
  """Provider stub used to test the cloud-agnostic base class."""

  CLOUD = 'FakeOmniCloud'
  PLATFORM = 'fake-platform'
  STORAGE_CLASS = 'fake-storage-class'

  def _FetchLicenseKeyFile(self) -> str:
    return os.path.join(vm_util.GetTempDir(), _LICENSE_FILENAME)


def _Pod(name, ip, ready=True, **metadata):
  return {
      'metadata': {'name': name, **metadata},
      'status': {
          'podIP': ip,
          'conditions': [
              {'type': 'Ready', 'status': 'True' if ready else 'False'}
          ],
      },
  }


def _PodsJson(*pods):
  return json.dumps({'items': list(pods)})


def _Spec(**kwargs):
  kwargs.setdefault('cloud', FakeSpannerOmniCluster.CLOUD)
  kwargs.setdefault('database_name', 'benchbase')
  return FakeSpannerOmniClusterSpec(
      'test_component', flag_values=FLAGS, **kwargs
  )


class SpannerOmniClusterTestBase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.mark_as_parsed()
    self.temp_dir = self.create_tempdir().full_path
    self.enter_context(
        mock.patch.object(
            vm_util, 'GetTempDir', autospec=True, return_value=self.temp_dir
        )
    )
    self.enter_context(mock.patch.object(time, 'sleep', autospec=True))
    self.enter_context(
        flagsaver.flagsaver(kubectl='kubectl', kubeconfig='/tmp/kubeconfig')
    )
    self.mock_cluster = mock.MagicMock(
        zone=_ZONES,
        num_nodes=1,
        default_nodepool=mock.MagicMock(machine_type='c4-standard-8'),
    )
    self.mock_cluster.GetResourceMetadata.return_value = {'cluster': 'k8s'}
    self.mock_client_vm = mock.create_autospec(
        virtual_machine.BaseVirtualMachine, instance=True, spec_set=False
    )
    self.mock_client_vm.zone = 'us-central1-a'
    self.mock_client_vm.machine_type = 'n2-standard-32'
    self.mock_client_vm.created = True
    self.mock_client_vm.deleted = False
    self.mock_client_vm.RemoteCommand.return_value = ('', '')

  def _Construct(self, omni_spec, cluster=None):
    omni_cluster = FakeSpannerOmniCluster(
        omni_spec, self.mock_cluster if cluster is None else cluster
    )
    omni_cluster.SetVms({'clients': [self.mock_client_vm]})
    return omni_cluster

  def _NewCluster(self, prepared=True, **spec_kwargs):
    cluster = self._Construct(
        _Spec(root_servers_per_zone=1, **spec_kwargs), self.mock_cluster
    )
    if prepared:
      cluster.pod_endpoints = [('spanner-a-rt-0', '10.68.0.5')]
      cluster._admin_password = 'Test-Password-123'
    return cluster

  def _MockKubectl(self, **kwargs):
    return self.enter_context(
        mock.patch.object(
            kubectl, 'RunRetryableKubectlCommand', autospec=True, **kwargs
        )
    )

  def _MockLogin(self):
    return self.enter_context(
        mock.patch.object(
            vm_util,
            'IssueCommand',
            autospec=True,
            return_value=('Logged in as admin', '', 0),
        )
    )


class SpecTest(SpannerOmniClusterTestBase):

  def testDefaults(self):
    omni_spec = _Spec()
    self.assertEqual(omni_spec.cloud, FakeSpannerOmniCluster.CLOUD)
    self.assertEqual(omni_spec.client_vm_group, 'clients')
    self.assertTrue(omni_spec.create_database)
    self.assertEqual(omni_spec.replicas_per_zone, 3)
    self.assertIsNone(omni_spec.zones)
    self.assertIsNone(omni_spec.root_servers_per_zone)
    self.assertEqual(omni_spec.cpu_request, '7')
    self.assertEqual(omni_spec.memory_request, '23Gi')
    self.assertEqual(omni_spec.data_disk_size, '1200Gi')
    self.assertTrue(omni_spec.run_analyze)

  @flagsaver.flagsaver(
      spanner_omni_replicas_per_zone=4,
      spanner_omni_data_disk_size='600Gi',
      spanner_omni_memory_request='50Gi',
  )
  def testFlagsOverrideConfig(self):
    FLAGS['spanner_omni_replicas_per_zone'].present = 1
    FLAGS['spanner_omni_data_disk_size'].present = 1
    FLAGS['spanner_omni_memory_request'].present = 1

    omni_spec = _Spec(
        replicas_per_zone=2, data_disk_size='100Gi', memory_request='8Gi'
    )

    self.assertEqual(omni_spec.replicas_per_zone, 4)
    self.assertEqual(omni_spec.data_disk_size, '600Gi')
    self.assertEqual(omni_spec.memory_request, '50Gi')

  def testBaseSpecHasNoProviderOptions(self):
    omni_spec = spanner_omni_cluster_spec.SpannerOmniClusterSpec(
        'test_component', flag_values=FLAGS, cloud='Unknown'
    )
    for option in (
        'license_secret_name',
        'license_secret_project',
        'gcs_backup_bucket',
        'gcp_service_account',
    ):
      self.assertFalse(hasattr(omni_spec, option), option)

  def testRejectsMoreRootServersThanReplicas(self):
    with self.assertRaisesRegex(
        errors.Config.InvalidValue,
        r'root_servers_per_zone \(3\) must not exceed',
    ):
      _Spec(replicas_per_zone=2, root_servers_per_zone=3)

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
            'create_database': False,
        },
    )
    omni_spec = config_spec.spanner_omni_cluster  # pyrefly: ignore[missing-attribute]
    # Decoded with the provider spec registered for the cloud.
    self.assertEqual(type(omni_spec).__name__, 'GcpSpannerOmniClusterSpec')
    self.assertEqual(omni_spec.cloud, 'GCP')
    self.assertEqual(omni_spec.database_name, 'sbtest')
    self.assertFalse(omni_spec.create_database)
    self.assertIsNone(omni_spec.license_secret_name)

  def testGetSpannerOmniClusterClass(self):
    self.assertIs(
        spanner_omni_cluster.GetSpannerOmniClusterClass(
            FakeSpannerOmniCluster.CLOUD
        ),
        FakeSpannerOmniCluster,
    )


class HelperFunctionsTest(SpannerOmniClusterTestBase):

  def testBuildLocationsJsonAssignsUniqueShortNames(self):
    locations = json.loads(
        spanner_omni_cluster.BuildLocationsJson(
            ['us-central1-c', 'us-east1-b', 'us-west1-b']
        )
    )
    short_names = []
    for region in locations:
      short_names.extend(zone['shortName'] for zone in region['zones'])
    self.assertEqual(short_names, ['c', 'b', 'a'])

  def testBuildZoneShortNamesHandlesNonLetterSuffixes(self):
    self.assertEqual(
        spanner_omni_cluster.BuildZoneShortNames(
            ['us-east-1a', 'us-east-1b', 'zone-2', 'us-central1-a']
        ),
        {
            'us-east-1a': 'a',
            'us-east-1b': 'b',
            'zone-2': 'c',
            'us-central1-a': 'd',
        },
    )

  def testResolveLatestOmniReleaseVersionPicksHighestStableRelease(self):
    gcs_payload = {
        'prefixes': [
            '2026.r1-beta/',
            '2026.r2.1-beta/',
            '2026.r4/',
            '2026.r4-lts/',
            '2026.r4-lts.1/',
            '2026.r5-beta/',
            '2026.r5/',
            '2026.r6-alpha/',
            '2026.r6-beta/',
            'unversioned-folder/',
        ]
    }
    self.enter_context(
        mock.patch.object(
            urllib.request,
            'urlopen',
            autospec=True,
            return_value=io.StringIO(json.dumps(gcs_payload)),
        )
    )
    self.assertEqual(
        spanner_omni_cluster.ResolveLatestOmniReleaseVersion(), '2026.r5'
    )

  def testResolveLatestOmniReleaseVersionRespectsExplicitVersion(self):
    self.assertEqual(
        spanner_omni_cluster.ResolveLatestOmniReleaseVersion('2026.r4-lts.1'),
        '2026.r4-lts.1',
    )

  def testResolveLatestOmniReleaseVersionRaisesWhenEmpty(self):
    self.enter_context(
        mock.patch.object(
            urllib.request,
            'urlopen',
            autospec=True,
            return_value=io.StringIO(json.dumps({'prefixes': ['misc/']})),
        )
    )
    with self.assertRaisesRegex(
        errors.Resource.CreationError,
        'No valid Spanner Omni release versions found',
    ):
      spanner_omni_cluster.ResolveLatestOmniReleaseVersion()

  def _GeneratePasswordFromChoices(self, choices):
    mock_choice = self.enter_context(
        mock.patch.object(
            spanner_omni_cluster.secrets,
            'choice',
            autospec=True,
            side_effect=list(choices),
        )
    )
    return spanner_omni_cluster._GenerateAdminPassword(), mock_choice

  def testGenerateAdminPasswordReturnsCompliantCandidate(self):
    candidate = 'Ab1-' + 'x' * 20

    password, mock_choice = self._GeneratePasswordFromChoices(candidate)

    self.assertEqual(password, candidate)
    self.assertEqual(mock_choice.call_count, 24)
    alphabet = mock_choice.call_args[0][0]
    self.assertCountEqual(
        set(alphabet),
        set(string.ascii_letters + string.digits + '-_'),
    )

  def testGenerateAdminPasswordRejectsCandidatesMissingAClass(self):
    compliant = 'zY9_' + 'q' * 20
    candidates = [
        'ab1-' + 'x' * 20,  # No uppercase letter.
        'AB1-' + 'X' * 20,  # No lowercase letter.
        'Abc-' + 'x' * 20,  # No digit.
        'Ab12' + 'x' * 20,  # No special character.
        compliant,
    ]

    password, mock_choice = self._GeneratePasswordFromChoices(
        ''.join(candidates)
    )

    self.assertEqual(password, compliant)
    self.assertEqual(mock_choice.call_count, 24 * len(candidates))


class ConfigOverridesTest(SpannerOmniClusterTestBase):

  def _Config(self):
    return {
        'container_cluster': {
            'cloud': 'GCP',
            'vm_count': 3,
            'vm_spec': {
                'GCP': {
                    'machine_type': 'c4-standard-8',
                    'zone': 'us-central1-a,us-central1-b,us-central1-f',
                }
            },
        },
        'vm_groups': {
            'clients': {
                'vm_spec': {
                    'GCP': {
                        'machine_type': 'n2-standard-32',
                        'zone': 'us-central1-a',
                    }
                }
            }
        },
    }

  @flagsaver.flagsaver(
      spanner_omni_zones=['us-east1-b', 'us-east1-c', 'us-east1-d'],
      spanner_omni_replicas_per_zone=4,
  )
  def testApplyConfigOverridesSetsZonesAndReplicas(self):
    FLAGS['spanner_omni_zones'].present = 1
    FLAGS['spanner_omni_replicas_per_zone'].present = 1

    config = spanner_omni_cluster.ApplyConfigOverrides(self._Config())

    self.assertEqual(
        config['spanner_omni_cluster'],
        {
            'replicas_per_zone': 4,
            'zones': ['us-east1-b', 'us-east1-c', 'us-east1-d'],
            'cluster_zone': 'us-east1-b,us-east1-c,us-east1-d',
            'cluster_machine_type': 'c4-standard-8',
        },
    )
    self.assertEqual(config['container_cluster']['vm_count'], 4)
    cluster_spec = config['container_cluster']['vm_spec']['GCP']
    self.assertEqual(cluster_spec['zone'], 'us-east1-b,us-east1-c,us-east1-d')
    self.assertEqual(cluster_spec['machine_type'], 'c4-standard-8')
    self.assertEqual(
        config['vm_groups']['clients']['vm_spec']['GCP']['zone'], 'us-east1-b'
    )

  def testApplyConfigOverridesPreservesConfigWhenFlagsNotPresent(self):
    config = self._Config()
    config['container_cluster']['vm_spec']['GCP'][
        'machine_type'
    ] = 'c4-standard-16'

    config = spanner_omni_cluster.ApplyConfigOverrides(config)

    self.assertEqual(config['container_cluster']['vm_count'], 3)
    self.assertEqual(
        config['container_cluster']['vm_spec']['GCP']['machine_type'],
        'c4-standard-16',
    )

  @flagsaver.flagsaver(
      spanner_omni_zones=['us-east1-b', 'us-east1-c', 'us-east1-d']
  )
  def testApplyConfigOverridesUsesCustomClientVmGroup(self):
    FLAGS['spanner_omni_zones'].present = 1
    config = self._Config()
    config['vm_groups'] = {'loaders': config['vm_groups']['clients']}
    config['spanner_omni_cluster'] = {'client_vm_group': 'loaders'}

    config = spanner_omni_cluster.ApplyConfigOverrides(config)

    self.assertEqual(
        config['vm_groups']['loaders']['vm_spec']['GCP']['zone'], 'us-east1-b'
    )
    self.assertEqual(
        config['spanner_omni_cluster']['client_vm_group'], 'loaders'
    )

  @flagsaver.flagsaver(spanner_omni_zones=['us-east1-b'])
  def testApplyConfigOverridesToleratesMissingClientVmGroup(self):
    FLAGS['spanner_omni_zones'].present = 1
    config = self._Config()
    config['spanner_omni_cluster'] = {'client_vm_group': 'missing'}

    config = spanner_omni_cluster.ApplyConfigOverrides(config)

    self.assertEqual(
        config['vm_groups']['clients']['vm_spec']['GCP']['zone'],
        'us-central1-a',
    )

  @flagsaver.flagsaver(zone=['us-central1-b'], machine_type='n2-standard-64')
  def testApplyPrerequisiteOverridesRestoresClusterSpecFromConfig(self):
    FLAGS['zone'].present = 1
    FLAGS['machine_type'].present = 1
    cluster_vm_spec = mock.MagicMock(
        zone='us-central1-b', machine_type='n2-standard-64'
    )
    client_vm_spec = mock.MagicMock(
        zone='us-central1-b', machine_type='n2-standard-64'
    )
    benchmark_config = mock.MagicMock(
        spanner_omni_cluster=_Spec(
            zones=['us-central1-a', 'us-central1-b', 'us-central1-f'],
            replicas_per_zone=4,
            cluster_zone='us-central1-a,us-central1-b,us-central1-f',
            cluster_machine_type='c4-standard-16',
        )
    )
    benchmark_config.container_cluster.vm_spec = cluster_vm_spec
    benchmark_config.vm_groups = {
        'clients': mock.MagicMock(vm_spec=client_vm_spec)
    }

    spanner_omni_cluster.ApplyPrerequisiteOverrides(benchmark_config)

    self.assertEqual(
        cluster_vm_spec.zone, 'us-central1-a,us-central1-b,us-central1-f'
    )
    self.assertEqual(cluster_vm_spec.machine_type, 'c4-standard-16')
    self.assertEqual(benchmark_config.container_cluster.vm_count, 4)
    self.assertEqual(client_vm_spec.zone, 'us-central1-b')
    self.assertEqual(client_vm_spec.machine_type, 'n2-standard-64')

  @flagsaver.flagsaver(zone=['us-east1-b'])
  def testApplyPrerequisiteOverridesUsesCustomClientVmGroup(self):
    FLAGS['zone'].present = 1
    client_vm_spec = mock.MagicMock(zone='us-east1-b')
    benchmark_config = mock.MagicMock(
        container_cluster=None,
        spanner_omni_cluster=_Spec(
            client_vm_group='loaders',
            zones=['us-central1-a', 'us-central1-b', 'us-central1-f'],
        ),
    )
    benchmark_config.vm_groups = {
        'loaders': mock.MagicMock(vm_spec=client_vm_spec)
    }

    spanner_omni_cluster.ApplyPrerequisiteOverrides(benchmark_config)

    self.assertEqual(client_vm_spec.zone, 'us-central1-a')

  @flagsaver.flagsaver(zone=['us-east1-b'])
  def testApplyPrerequisiteOverridesSnapsOutOfClusterClientZone(self):
    FLAGS['zone'].present = 1
    client_vm_spec = mock.MagicMock(zone='us-east1-b')
    benchmark_config = mock.MagicMock(
        container_cluster=None,
        spanner_omni_cluster=_Spec(
            zones=['us-central1-a', 'us-central1-b', 'us-central1-f']
        ),
    )
    benchmark_config.vm_groups = {
        'clients': mock.MagicMock(vm_spec=client_vm_spec)
    }

    spanner_omni_cluster.ApplyPrerequisiteOverrides(benchmark_config)

    self.assertEqual(client_vm_spec.zone, 'us-central1-a')
    self.assertEqual(FLAGS.zone, ['us-central1-a'])

  def _EastConfig(self):
    config = self._Config()
    config['container_cluster']['vm_spec']['GCP'][
        'zone'
    ] = 'us-east4-a,us-east4-b,us-east4-c'
    config['vm_groups']['clients']['vm_spec']['GCP']['zone'] = 'us-east4-a'
    return config

  @flagsaver.flagsaver(zone=['us-east4-a'], machine_type='n2-standard-32')
  def testNonDefaultRegionWithoutZonesFlagStaysInRegion(self):
    config = spanner_omni_cluster.ApplyConfigOverrides(self._EastConfig())
    omni_spec = _Spec(**config['spanner_omni_cluster'])
    self.assertEqual(
        omni_spec.zones, ['us-east4-a', 'us-east4-b', 'us-east4-c']
    )
    FLAGS['zone'].present = 1
    FLAGS['machine_type'].present = 1
    # Generic --zone/--machine_type clobbered the decoded cluster spec.
    cluster_vm_spec = mock.MagicMock(
        zone='us-east4-a', machine_type='n2-standard-32'
    )
    client_vm_spec = mock.MagicMock(zone='us-east4-a')
    benchmark_config = mock.MagicMock(spanner_omni_cluster=omni_spec)
    benchmark_config.container_cluster.vm_spec = cluster_vm_spec
    benchmark_config.vm_groups = {
        'clients': mock.MagicMock(vm_spec=client_vm_spec)
    }

    spanner_omni_cluster.ApplyPrerequisiteOverrides(benchmark_config)

    self.assertEqual(cluster_vm_spec.zone, 'us-east4-a,us-east4-b,us-east4-c')
    self.assertEqual(cluster_vm_spec.machine_type, 'c4-standard-8')
    self.assertEqual(client_vm_spec.zone, 'us-east4-a')
    self.assertEqual(FLAGS.zone, ['us-east4-a'])
    # Helm locations follow the same zones when the cluster reports none.
    omni_cluster = self._Construct(
        omni_spec, mock.MagicMock(zone=None, zones=None, default_nodepool=None)
    )
    omni_cluster.omni_version = '2026.r5'
    helm_args = omni_cluster._BuildHelmInstallCommand('license.bin')
    locations = next(a for a in helm_args if a.startswith('locations='))
    self.assertIn('"name":"us-east4"', locations)
    self.assertNotIn('us-central1', locations)

  def testApplyConfigOverridesUsesYamlZones(self):
    config = self._Config()
    config['spanner_omni_cluster'] = {'zones': ['us-west1-a', 'us-west1-b']}

    config = spanner_omni_cluster.ApplyConfigOverrides(config)

    self.assertEqual(
        config['container_cluster']['vm_spec']['GCP']['zone'],
        'us-west1-a,us-west1-b',
    )
    self.assertEqual(
        config['vm_groups']['clients']['vm_spec']['GCP']['zone'], 'us-west1-a'
    )

  @flagsaver.flagsaver(spanner_omni_zones=['us-east1-b'])
  def testApplyConfigOverridesZonesFlagWinsOverYaml(self):
    FLAGS['spanner_omni_zones'].present = 1
    config = self._Config()
    config['spanner_omni_cluster'] = {'zones': ['us-west1-a', 'us-west1-b']}

    config = spanner_omni_cluster.ApplyConfigOverrides(config)

    self.assertEqual(config['spanner_omni_cluster']['zones'], ['us-east1-b'])
    self.assertEqual(
        config['container_cluster']['vm_spec']['GCP']['zone'], 'us-east1-b'
    )

  @flagsaver.flagsaver(spanner_omni_zones=[])
  def testApplyConfigOverridesRejectsEmptyZonesFlag(self):
    FLAGS['spanner_omni_zones'].present = 1
    with self.assertRaisesRegex(
        errors.Config.InvalidValue, 'at least one zone'
    ):
      spanner_omni_cluster.ApplyConfigOverrides(self._Config())

  def testApplyConfigOverridesUsesYamlReplicasForVmCount(self):
    config = self._Config()
    config['spanner_omni_cluster'] = {'replicas_per_zone': 5}

    config = spanner_omni_cluster.ApplyConfigOverrides(config)

    self.assertEqual(config['container_cluster']['vm_count'], 5)

  @flagsaver.flagsaver(spanner_omni_replicas_per_zone=4)
  def testApplyConfigOverridesReplicasFlagWinsOverYaml(self):
    FLAGS['spanner_omni_replicas_per_zone'].present = 1
    config = self._Config()
    config['spanner_omni_cluster'] = {'replicas_per_zone': 5}

    config = spanner_omni_cluster.ApplyConfigOverrides(config)

    self.assertEqual(config['container_cluster']['vm_count'], 4)
    self.assertEqual(config['spanner_omni_cluster']['replicas_per_zone'], 4)

  def testApplyConfigOverridesRecordsVmCountAsReplicas(self):
    config = spanner_omni_cluster.ApplyConfigOverrides(self._Config())

    self.assertEqual(config['spanner_omni_cluster']['replicas_per_zone'], 3)
    self.assertEqual(
        config['spanner_omni_cluster']['zones'],
        ['us-central1-a', 'us-central1-b', 'us-central1-f'],
    )

  @flagsaver.flagsaver(spanner_omni_zones=['us-east1-b'])
  def testApplyConfigOverridesToleratesMissingVmSpecs(self):
    FLAGS['spanner_omni_zones'].present = 1
    config = {
        'container_cluster': {'cloud': 'GCP', 'vm_count': 2},
        'vm_groups': {'clients': {'vm_spec': {'AWS': {}}}},
    }

    config = spanner_omni_cluster.ApplyConfigOverrides(config)

    self.assertEqual(
        config['spanner_omni_cluster'],
        {'replicas_per_zone': 2, 'zones': ['us-east1-b']},
    )

  def testCheckPrerequisitesRunsSpecPrerequisites(self):
    cluster = self._NewCluster()
    mock_check = self.enter_context(
        mock.patch.object(
            FakeSpannerOmniCluster, 'CheckSpecPrerequisites', autospec=True
        )
    )

    cluster.CheckPrerequisites()

    mock_check.assert_called_once_with(cluster.spec)


class LifecycleTest(SpannerOmniClusterTestBase):

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch.object(
            spanner_omni_cluster,
            'ResolveLatestOmniReleaseVersion',
            autospec=True,
            return_value='2026.r5',
        )
    )
    self.mock_run_kubectl = self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            autospec=True,
            return_value=('namespace/spanner-ns created', '', 0),
        )
    )
    self.mock_helm = self.enter_context(
        mock.patch.object(helm, 'RunHelmCommand', autospec=True)
    )

    def _WriteLocalSecret(local_path, unused_remote_path):
      with open(local_path, 'w') as f:
        f.write('{}')

    self.mock_client_vm.PullFile.side_effect = _WriteLocalSecret

  def _KubectlResponder(self, pods_json, extra=None):
    """Returns a side_effect routing kubectl commands to canned responses."""
    applied_files = {}
    extra = list(extra or [])

    def _Respond(cmd, **unused_kwargs):
      if cmd[0] == 'apply':
        with open(cmd[-1]) as f:
          applied_files[os.path.basename(cmd[-1])] = json.load(f)
        return ('applied', '', 0)
      if cmd[:2] == ['get', 'pods']:
        return (pods_json, '', 0)
      if cmd[:2] == ['get', 'job']:
        return (
            json.dumps({
                'status': {
                    'conditions': [{'type': 'Complete', 'status': 'True'}]
                }
            }),
            '',
            0,
        )
      return extra.pop(0)

    return _Respond, applied_files

  def testCreateDeploysHelmChartAndCreatesDatabase(self):
    pods_json = _PodsJson(
        _Pod('spanner-b-rt-0', '10.68.1.6'),
        _Pod('spanner-a-rt-0', '10.68.0.5'),
        _Pod('spanner-c-rt-0', '10.68.2.7'),
    )
    responder, applied_files = self._KubectlResponder(
        pods_json, extra=[('Database created', '', 0)]
    )
    mock_kubectl = self._MockKubectl(side_effect=responder)
    mock_login = self._MockLogin()
    cluster = self._NewCluster(prepared=False)

    cluster.Create()

    self.assertTrue(cluster.created)
    self.assertEqual(cluster.omni_version, '2026.r5')
    self.assertEqual(cluster.first_pod_name, 'spanner-a-rt-0')
    helm_args = self.mock_helm.call_args[0][0]
    for expected in (
        'oci://us-docker.pkg.dev/spanner-omni/charts/spanner-omni',
        'global.platform=fake-platform',
        'global.insecureMode=false',
        'image.tag=2026.r5',
        'deployment.replicasPerZone=1',
        'deployment.rootServersPerZone=1',
        'deployment.enablePasswordAuthentication=true',
        'deployment.adminPasswordSecret=spanner-admin-password',
        'dataStorageClass=fake-storage-class',
        'logsStorageClass=fake-storage-class',
        'resources.cpu=7',
        'resources.memory=23Gi',
        'probes.liveness.failureThreshold=60',
        f'licenseKey={os.path.join(self.temp_dir, _LICENSE_FILENAME)}',
    ):
      self.assertIn(expected, helm_args)
    # The admin password is only in the applied Secret, never in a command.
    password = cluster._admin_password
    secret = applied_files['spanner_omni_admin_secret.json']
    self.assertEqual(
        base64.b64decode(secret['data']['password']).decode(), password
    )
    all_commands = (
        [str(c) for c in mock_kubectl.call_args_list]
        + [str(c) for c in self.mock_helm.call_args_list]
        + [str(c) for c in mock_login.call_args_list]
        + [str(c) for c in self.mock_client_vm.RemoteCommand.call_args_list]
    )
    for command in all_commands:
      self.assertNotIn(password, command)
    db_create_cmd = mock_kubectl.call_args_list[-1][0][0]
    self.assertIn('databases', db_create_cmd)
    self.assertIn('--database-dialect=POSTGRESQL', db_create_cmd)
    self.assertEqual(os.listdir(self.temp_dir), [])

  def _RootPods(self, zones, per_zone):
    pods = []
    for n, zone in enumerate(zones):
      pods.extend(
          _Pod(f'spanner-{zone}-rt-{i}', f'10.0.{n}.{i}')
          for i in range(per_zone)
      )
    return pods

  @flagsaver.flagsaver(spanner_omni_replicas_per_zone=4)
  def testCreateUsesDefaultRootServersPerZone(self):
    FLAGS['spanner_omni_replicas_per_zone'].present = 1
    self.mock_cluster.num_nodes = None
    responder, _ = self._KubectlResponder(
        _PodsJson(*self._RootPods(zones='abc', per_zone=4)),
        extra=[('Database created', '', 0)],
    )
    self._MockKubectl(side_effect=responder)
    self._MockLogin()
    cluster = self._Construct(_Spec(), self.mock_cluster)

    cluster.Create()

    helm_args = self.mock_helm.call_args[0][0]
    self.assertIn('deployment.replicasPerZone=4', helm_args)
    self.assertIn('deployment.rootServersPerZone=3', helm_args)

  def testSetVmsPicksConfiguredClientVmGroup(self):
    loader_vm = mock.create_autospec(
        virtual_machine.BaseVirtualMachine, instance=True
    )
    cluster = FakeSpannerOmniCluster(
        _Spec(client_vm_group='loaders'), self.mock_cluster
    )
    self.assertIsNone(cluster.client_vm)

    cluster.SetVms({'clients': [self.mock_client_vm], 'loaders': [loader_vm]})

    self.assertIs(cluster.client_vm, loader_vm)
    self.assertIs(cluster.GetClientVm(), loader_vm)

  def testSetVmsRaisesWhenClientVmGroupMissingOrEmpty(self):
    cluster = FakeSpannerOmniCluster(_Spec(), self.mock_cluster)
    for vm_groups in ({}, {'clients': []}):
      with self.assertRaisesRegex(
          errors.Config.InvalidValue, "requires the 'clients' vm_group"
      ):
        cluster.SetVms(vm_groups)

  def testCreateWithoutSetVmsRaises(self):
    cluster = FakeSpannerOmniCluster(_Spec(), self.mock_cluster)

    with self.assertRaisesRegex(
        errors.Resource.CreationError, r'no client VM; call SetVms'
    ):
      cluster.Create()

    self.mock_helm.assert_not_called()

  def testMetadataWithoutClientVmOmitsClientFields(self):
    metadata = FakeSpannerOmniCluster(
        _Spec(), self.mock_cluster
    ).GetResourceMetadata()

    self.assertNotIn('spanner_omni_client_zone', metadata)
    self.assertNotIn('spanner_omni_client_machine_type', metadata)

  def testRootServersDefaultToServersPerZoneWhenFewerThanThree(self):
    cluster = self._Construct(_Spec(replicas_per_zone=1), self.mock_cluster)
    cluster._ValidateTopology()
    self.assertEqual(cluster.root_servers_per_zone, 1)

  def testCreateRejectsMoreRootServersThanServersPerZone(self):
    cluster = self._Construct(_Spec(root_servers_per_zone=3), self.mock_cluster)
    with self.assertRaisesRegex(
        errors.Config.InvalidValue,
        r'spanner_omni_root_servers_per_zone \(3\) must not exceed servers per'
        r' zone \(1\)',
    ):
      cluster.Create()
    self.mock_helm.assert_not_called()

  def testCreateCleansUpWhenHelmInstallFails(self):
    responder, _ = self._KubectlResponder('{}')
    self._MockKubectl(side_effect=responder)
    self.mock_helm.side_effect = [
        errors.VmUtil.IssueCommandError('helm failed'),
        ('', '', 0),
    ]
    cluster = self._NewCluster(prepared=False)

    with self.assertRaises(errors.VmUtil.IssueCommandError):
      cluster.Create()

    self.mock_helm.assert_called_with(
        ['uninstall', 'spanner-omni', '--namespace', 'spanner-ns'],
        raise_on_failure=False,
    )
    self.mock_run_kubectl.assert_any_call(
        ['delete', 'pvc', '--all', '-n', 'spanner-ns'],
        raise_on_failure=False,
        raise_on_timeout=False,
    )

  def testIsReadyFalseWhenPodsMissing(self):
    self._MockKubectl(
        return_value=(_PodsJson(_Pod('spanner-a-rt-0', '10.0.0.1')), '', 0)
    )
    self.assertFalse(self._NewCluster(prepared=False)._IsReady())

  def testIsReadyFalseWhenPodNotReady(self):
    self._MockKubectl(
        return_value=(
            _PodsJson(
                _Pod('spanner-a-rt-0', '10.0.0.1'),
                _Pod('spanner-b-rt-0', '10.0.1.1', ready=False),
                _Pod('spanner-c-rt-0', '10.0.2.1'),
            ),
            '',
            0,
        )
    )
    self.assertFalse(self._NewCluster(prepared=False)._IsReady())

  def testDeleteWaitsForBoundPvs(self):
    self.mock_run_kubectl.side_effect = lambda cmd, **_: (
        ('pvc-1 pvc-2', '', 0) if cmd[:2] == ['get', 'pvc'] else ('', '', 0)
    )

    self._NewCluster()._Delete()

    self.mock_helm.assert_called_once_with(
        ['uninstall', 'spanner-omni', '--namespace', 'spanner-ns'],
        raise_on_failure=False,
    )
    self.mock_run_kubectl.assert_has_calls([
        mock.call(
            ['delete', 'pods', '--all', '-n', 'spanner-ns'],
            raise_on_failure=False,
            raise_on_timeout=False,
        ),
        mock.call(
            ['delete', 'pvc', '--all', '-n', 'spanner-ns'],
            raise_on_failure=False,
            raise_on_timeout=False,
        ),
        mock.call(
            [
                'wait',
                '--for=delete',
                'pv/pvc-1',
                'pv/pvc-2',
                '--timeout=300s',
            ],
            raise_on_failure=False,
            raise_on_timeout=False,
        ),
        mock.call(
            [
                'delete',
                'namespace',
                'spanner-ns',
                '--ignore-not-found=true',
                '--wait=false',
            ],
            raise_on_failure=False,
            raise_on_timeout=False,
        ),
    ])

  def testDeleteSkipsPvWaitWithoutClaims(self):
    self.mock_run_kubectl.return_value = ('', '', 0)

    self._NewCluster()._Delete()

    commands = [c.args[0] for c in self.mock_run_kubectl.call_args_list]
    self.assertFalse(any(cmd[0] == 'wait' for cmd in commands))
    self.assertIn(['delete', 'pvc', '--all', '-n', 'spanner-ns'], commands)

  def testEnsureNamespaceExistsRaisesOnUnexpectedError(self):
    self.mock_run_kubectl.return_value = ('', 'Error (Forbidden)', 1)
    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'Failed to create namespace'
    ):
      self._NewCluster()._EnsureNamespaceExists()

  def testConfigureTlsCertificatesRemovesTempSecretFile(self):
    self._MockKubectl(return_value=('secret/tls-certs created', '', 0))
    cluster = self._NewCluster()
    cluster.omni_version = '2026.r5'

    cluster._ConfigureTlsCertificatesAndSecrets()

    self.assertEqual(os.listdir(self.temp_dir), [])
    cert_cmd = self.mock_client_vm.RemoteCommand.call_args_list[0][0][0]
    self.assertIn('certificates create-ca', cert_cmd)
    self.assertIn('spanner-omni-cli-2026.r5-linux-x86_64.tar.gz', cert_cmd)
    self.assertNotIn('Test-Password-123', cert_cmd)
    self.assertIn('/tmp/omni_tls/tls_secrets.json', cert_cmd)
    self.mock_client_vm.PullFile.assert_called_once_with(
        os.path.join(self.temp_dir, 'omni_tls_secrets.json'),
        '/tmp/omni_tls/tls_secrets.json',
    )

  def testConfigureTlsCertificatesRestrictsLocalSecretFile(self):
    local_path = os.path.join(self.temp_dir, 'omni_tls_secrets.json')
    modes = []

    def _Apply(unused_cmd, **unused_kwargs):
      modes.append(os.stat(local_path).st_mode & 0o777)
      return ('secret/tls-certs created', '', 0)

    def _WriteWorldReadable(path, unused_remote_path):
      with open(path, 'w') as f:
        f.write('{}')
      os.chmod(path, 0o644)

    self.mock_client_vm.PullFile.side_effect = _WriteWorldReadable
    mock_kubectl = self._MockKubectl(side_effect=_Apply)
    cluster = self._NewCluster()
    cluster.omni_version = '2026.r5'

    cluster._ConfigureTlsCertificatesAndSecrets()

    mock_kubectl.assert_called_once_with(
        ['apply', '-n', 'spanner-ns', '-f', local_path]
    )
    self.assertEqual(modes, [0o600])
    self.assertFalse(os.path.exists(local_path))

  def testGetResourceMetadata(self):
    cluster = self._NewCluster()
    cluster.omni_version = '2026.r5'

    metadata = cluster.GetResourceMetadata()

    self.assertNotIn('cluster', metadata)
    self.assertEqual(metadata['spanner_omni_zones'], _ZONES)
    self.assertEqual(metadata['spanner_omni_replicas_per_zone'], 1)
    self.assertEqual(metadata['spanner_omni_root_servers_per_zone'], 1)
    for key in metadata:
      self.assertTrue(
          key.startswith('spanner_omni_') or key == 'db_engine', key
      )
    self.assertEqual(
        metadata['spanner_omni_storage_class'], 'fake-storage-class'
    )
    self.assertEqual(
        metadata['spanner_omni_cluster_machine_type'], 'c4-standard-8'
    )
    self.assertEqual(
        metadata['spanner_omni_client_machine_type'], 'n2-standard-32'
    )
    self.assertEqual(metadata['spanner_omni_client_zone'], 'us-central1-a')
    self.assertEqual(metadata['spanner_omni_version'], '2026.r5')
    self.assertNotIn('spanner_omni_pgadapter_num_channels', metadata)


class PodEndpointsTest(SpannerOmniClusterTestBase):

  def testPrioritizesRootServerInClientZone(self):
    self.mock_cluster.zone = 'us-central1-a,us-central1-c'
    self.mock_client_vm.zone = 'us-central1-c'
    self._MockKubectl(
        return_value=(
            _PodsJson(
                _Pod(
                    'spanner-a-rt-0-terminating',
                    '10.68.0.99',
                    deletionTimestamp='2026-10-31T00:00:00Z',
                ),
                _Pod('spanner-a-nrt-0', '10.68.0.5'),
                _Pod('spanner-a-rt-0', '10.68.0.6'),
                _Pod('spanner-c-nrt-0', '10.68.2.7'),
                _Pod('spanner-c-rt-0', '10.68.2.8'),
            ),
            '',
            0,
        )
    )

    self.assertEqual(
        self._NewCluster(prepared=False).WaitForPodsReady(),
        [
            ('spanner-c-rt-0', '10.68.2.8'),
            ('spanner-c-nrt-0', '10.68.2.7'),
            ('spanner-a-rt-0', '10.68.0.6'),
            ('spanner-a-nrt-0', '10.68.0.5'),
        ],
    )

  def testPodWithNullStatusIsNotReady(self):
    self.mock_cluster.zone = 'us-central1-a'
    pod = _Pod('spanner-a-rt-0', '10.68.0.5')
    pod['status'] = None
    self._MockKubectl(return_value=(_PodsJson(pod), '', 0))

    with self.assertRaisesRegex(
        errors.Resource.RetryableCreationError, 'not ready yet'
    ):
      self._NewCluster(prepared=False)._GetReadyPodEndpoints()

  def testZonesPreferContainerClusterZonesList(self):
    self.mock_cluster.zones = ['us-east4-a', 'us-east4-b']
    self.mock_cluster.zone = None
    self.assertEqual(self._NewCluster().zones, ['us-east4-a', 'us-east4-b'])

  def testValidateTopologyRequiresZones(self):
    cluster = self._Construct(
        _Spec(root_servers_per_zone=1),
        mock.MagicMock(
            zone=None, zones=None, default_nodepool=None, num_nodes=1
        ),
    )
    with self.assertRaisesRegex(
        errors.Config.InvalidValue, 'Could not determine the Spanner Omni zones'
    ):
      cluster._ValidateTopology()

  def testUsesDeduplicatedZoneShortName(self):
    self.mock_cluster.zone = 'us-central1-c,us-east1-b,us-west1-b'
    self.mock_client_vm.zone = 'us-west1-b'
    self._MockKubectl(
        return_value=(
            _PodsJson(
                _Pod('spanner-b-rt-0', '10.68.1.5'),
                _Pod('spanner-a-rt-0', '10.68.2.6'),
                _Pod('spanner-c-rt-0', '10.68.0.6'),
            ),
            '',
            0,
        )
    )

    self.assertEqual(
        self._NewCluster(prepared=False).WaitForPodsReady()[0][0],
        'spanner-a-rt-0',
    )


class CliTest(SpannerOmniClusterTestBase):

  def testLoginStreamsPasswordOverStdin(self):
    cluster = self._NewCluster()
    seen_passwords = []

    def _IssueCommand(cmd, **unused_kwargs):
      password_path = cmd[-1].rsplit('< ', 1)[1].strip("'")
      with open(password_path) as f:
        seen_passwords.append(f.read())
      self.assertEqual(os.stat(password_path).st_mode & 0o777, 0o600)
      return ('Logged in', '', 0)

    mock_issue = self.enter_context(
        mock.patch.object(
            vm_util, 'IssueCommand', autospec=True, side_effect=_IssueCommand
        )
    )

    cluster.LoginSpannerCli()

    self.assertEqual(seen_passwords, ['Test-Password-123'])
    cmd = mock_issue.call_args[0][0]
    self.assertEqual(cmd[:2], ['bash', '-c'])
    self.assertIn(
        'kubectl --kubeconfig /tmp/kubeconfig exec -i -n spanner-ns'
        ' spanner-a-rt-0',
        cmd[2],
    )
    self.assertIn('auth login admin', cmd[2])
    self.assertNotIn('Test-Password-123', cmd[2])
    self.assertEqual(os.listdir(self.temp_dir), [])

  @flagsaver.flagsaver(kubeconfig=None)
  def testLoginOmitsKubeconfigWhenUnset(self):
    mock_issue = self._MockLogin()

    self._NewCluster().LoginSpannerCli()

    cmd = mock_issue.call_args[0][0]
    self.assertNotIn('--kubeconfig', cmd[2])
    self.assertIn('kubectl exec -i -n spanner-ns spanner-a-rt-0', cmd[2])

  def testLoginSwallowsFailuresWhenRequested(self):
    self.enter_context(
        mock.patch.object(
            vm_util,
            'IssueCommand',
            autospec=True,
            side_effect=errors.VmUtil.IssueCommandError('boom'),
        )
    )
    self._NewCluster().LoginSpannerCli(raise_on_failure=False)

  def testGetAdminPasswordReadsSecretWhenNotCached(self):
    mock_kubectl = self._MockKubectl(
        return_value=(base64.b64encode(b'From-Secret-1').decode(), '', 0)
    )
    cluster = self._NewCluster()
    cluster._admin_password = None

    self.assertEqual(cluster._GetAdminPassword(), 'From-Secret-1')
    self.assertTrue(mock_kubectl.call_args.kwargs['suppress_logging'])

  def testPickleStateDropsAdminPassword(self):
    cluster = self._NewCluster()
    self.assertIsNone(cluster.__getstate__()['_admin_password'])
    self.assertEqual(cluster._admin_password, 'Test-Password-123')

  def testAuthenticatedCliRelogsInAndRetriesOnAuthError(self):
    mock_kubectl = self._MockKubectl(
        side_effect=[
            ('', 'Error: not authenticated', 1),
            ('state: READY', '', 0),
        ]
    )
    mock_login = self._MockLogin()

    self.assertTrue(self._NewCluster().DatabaseExists('benchbase'))

    self.assertEqual(mock_kubectl.call_count, 2)
    mock_login.assert_called_once()

  def testDatabaseExistsReturnsFalseOnTimeout(self):
    self._MockKubectl(
        side_effect=vm_util.TimeoutExceededRetryError('deadline exceeded')
    )
    self._MockLogin()

    self.assertFalse(self._NewCluster().DatabaseExists('benchbase'))

  def testCreateDatabaseIgnoresAlreadyExists(self):
    self._MockKubectl(
        return_value=('', 'ERROR: ALREADY_EXISTS: resource exists', 1)
    )
    self._NewCluster().CreateDatabase('benchbase')

  def testDeleteDatabaseIgnoresNotFound(self):
    mock_kubectl = self._MockKubectl(
        return_value=('', 'ERROR: NOT_FOUND: Database not found: sbtest', 1)
    )

    self._NewCluster().DeleteDatabase('sbtest')

    cli_args = mock_kubectl.call_args[0][0]
    self.assertIn('delete', cli_args)
    self.assertIn('sbtest', cli_args)
    self.assertIn('--quiet', cli_args)

  def testCreateDatabaseRelogsInAndRetriesOnAuthError(self):
    mock_kubectl = self._MockKubectl(
        side_effect=[
            ('', 'Error: not authenticated', 1),
            ('Database created', '', 0),
        ]
    )
    mock_login = self._MockLogin()

    self._NewCluster().CreateDatabase('benchbase')

    self.assertEqual(mock_kubectl.call_count, 2)
    self.assertIn('create', mock_kubectl.call_args[0][0])
    mock_login.assert_called_once()

  def testDeleteDatabaseRelogsInAndRetriesOnAuthError(self):
    mock_kubectl = self._MockKubectl(
        side_effect=[
            ('', 'ERROR: UNAUTHENTICATED: session expired', 1),
            ('', 'ERROR: NOT_FOUND: Database not found: sbtest', 1),
        ]
    )
    mock_login = self._MockLogin()

    self._NewCluster().DeleteDatabase('sbtest')

    self.assertEqual(mock_kubectl.call_count, 2)
    mock_login.assert_called_once()

  def testCreateDatabaseRaisesOnUnexpectedError(self):
    self._MockKubectl(return_value=('', 'ERROR: PERMISSION_DENIED', 1))
    self._MockLogin()
    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'Failed to create Spanner Omni database'
    ):
      self._NewCluster().CreateDatabase('benchbase')

  def testDeleteDatabaseRaisesOnUnexpectedError(self):
    self._MockKubectl(return_value=('', 'ERROR: PERMISSION_DENIED', 1))
    with self.assertRaisesRegex(
        errors.VmUtil.IssueCommandError,
        'Failed to delete Spanner Omni database',
    ):
      self._NewCluster().DeleteDatabase('sbtest')

  def testRunDDLQueryWaitsForOperation(self):
    mock_kubectl = self._MockKubectl(
        side_effect=[
            (
                (
                    'Schema update in progress. Operation name:'
                    ' instances/default/databases/benchbase/operations/'
                    '_auto_op_1'
                ),
                '',
                0,
            ),
            ('done: true', '', 0),
        ]
    )
    self._MockLogin()

    self._NewCluster().RunDDLQuery('ANALYZE;')

    self.assertIn('--ddl=ANALYZE;', mock_kubectl.call_args_list[0][0][0])
    self.assertIn('_auto_op_1', mock_kubectl.call_args_list[1][0][0])

  def testRunDDLQueryParsesOperationIdAfterSpinnerFrames(self):
    mock_kubectl = self._MockKubectl(
        side_effect=[
            (
                (
                    '\r\x1b[K⠋ Operation name: instances/default/databases/'
                    'benchbase/operations/stale_op\r\x1b[KSchema update in'
                    ' progress. Operation name: instances/default/databases/'
                    'benchbase/operations/spinner_op'
                ),
                '',
                0,
            ),
            ('done: true', '', 0),
        ]
    )
    self._MockLogin()

    self._NewCluster().RunDDLQuery('ANALYZE;')

    self.assertIn('spinner_op', mock_kubectl.call_args_list[1][0][0])

  def testRunDDLQueryRelogsInAndUsesShortSubmitTimeout(self):
    mock_kubectl = self._MockKubectl(
        side_effect=[
            ('', 'Error: not authenticated', 1),
            (
                (
                    'Schema update in progress. Operation name:'
                    ' instances/default/databases/benchbase/operations/op_2'
                ),
                '',
                0,
            ),
            ('done: true', '', 0),
        ]
    )
    mock_login = self._MockLogin()

    self._NewCluster().RunDDLQuery('ANALYZE;')

    mock_login.assert_called_once()
    self.assertEqual(mock_kubectl.call_args_list[0].kwargs['timeout'], 300)
    self.assertEqual(mock_kubectl.call_args_list[1].kwargs['timeout'], 300)
    self.assertIn('op_2', mock_kubectl.call_args_list[2][0][0])

  def testRunDDLQueryRaisesWhenSubmitFails(self):
    self._MockKubectl(return_value=('', 'ERROR: INVALID_ARGUMENT', 1))
    self._MockLogin()
    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'Failed to submit DDL'
    ):
      self._NewCluster().RunDDLQuery('ANALYZE;')

  def testRunDDLQueryRaisesWhenOperationIdMissing(self):
    self._MockKubectl(return_value=('Unexpected output', '', 0))
    self._MockLogin()
    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'Failed to parse DDL operation ID'
    ):
      self._NewCluster().RunDDLQuery('ANALYZE;')

  def testRunDDLQuerySkipsAnalyzeWhenDisabled(self):
    mock_kubectl = self._MockKubectl()
    mock_login = self._MockLogin()

    self._NewCluster(run_analyze=False).RunDDLQuery('ANALYZE;')

    mock_kubectl.assert_not_called()
    mock_login.assert_not_called()

  def testWaitForDatabaseOperationStripsCliSpinnerPrefix(self):
    self._MockKubectl(
        return_value=(
            (
                '\r\x1b[KWaiting for Server to respond\r\x1b[Kdone: true\n'
                'name: instances/default/databases/benchbase/operations/op\n'
            ),
            '',
            0,
        )
    )
    self._NewCluster().WaitForDatabaseOperation('op', 'benchbase')

  def testWaitForDatabaseOperationFallsBackToLinesForNonYamlOutput(self):
    self._MockKubectl(return_value=('Progress: [100%\ndone: true\n', '', 0))
    self._NewCluster().WaitForDatabaseOperation('op', 'benchbase')

  def testWaitForDatabaseOperationDetectsErrorLineInNonYamlOutput(self):
    self._MockKubectl(return_value=('Progress: [100%\nerror: code 13\n', '', 0))
    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'Spanner Omni operation op failed'
    ):
      self._NewCluster().WaitForDatabaseOperation('op', 'benchbase')

  def testWaitForDatabaseOperationRaisesOnError(self):
    self._MockKubectl(
        return_value=(
            'done: true\nerror:\n  code: 13\n  message: Internal error',
            '',
            0,
        )
    )
    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'Spanner Omni operation op failed'
    ):
      self._NewCluster().WaitForDatabaseOperation('op', 'benchbase')


if __name__ == '__main__':
  unittest.main()
