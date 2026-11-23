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
import pathlib
import pickle
import time
from unittest import mock
import urllib.request

from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.configs import option_decoders

# Register GCP flags (such as --spanner_omni_gcs_backup_bucket) at import time
# so PkbCommonTestCase's flagsaver snapshot preserves them across tests.
from perfkitbenchmarker.providers.gcp import gcp_spanner_omni_cluster
from perfkitbenchmarker.providers.gcp import google_kubernetes_engine
from perfkitbenchmarker.resources import spanner_omni_cluster
from perfkitbenchmarker.resources import spanner_omni_cluster_spec
from perfkitbenchmarker.resources.container_service import container
from perfkitbenchmarker.resources.container_service import helm
from perfkitbenchmarker.resources.container_service import kubectl
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_ZONES = 'us-central1-a,us-central1-b,us-central1-c'
_LICENSE_FILENAME = 'fake_license.bin'


class FakeSpannerOmniClusterSpec(
    spanner_omni_cluster_spec.SpannerOmniClusterSpec
):
  """Provider spec stub with a provider-specific backup bucket option."""

  CLOUD = 'FakeOmniCloud'

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    result = super()._GetOptionDecoderConstructions()
    result['fake_bucket'] = (
        option_decoders.StringDecoder,
        {'default': None, 'none_ok': True},
    )
    return result


class FakeSpannerOmniCluster(spanner_omni_cluster.BaseSpannerOmniCluster):
  """Provider stub used to test the cloud-agnostic base class."""

  CLOUD = 'FakeOmniCloud'
  HELM_PLATFORM = 'fake-platform'
  STORAGE_CLASS = 'fake-storage-class'

  def _GetRegionFromZone(self, zone: str) -> str:
    return zone.rsplit('-', 1)[0] if '-' in zone else zone

  def _BuildZoneShortNames(self, zones):
    used: set[str] = set()
    result: dict[str, str] = {}
    for zone in zones:
      suffix = zone.rsplit('-', 1)[-1]
      short = (
          suffix
          if len(suffix) == 1 and suffix.islower() and suffix not in used
          else next(c for c in 'abcdefghijklmnopqrstuvwxyz' if c not in used)
      )
      used.add(short)
      result[zone] = short
    return result

  def _FetchLicenseKeyFile(self) -> str:
    path = os.path.join(vm_util.GetTempDir(), _LICENSE_FILENAME)
    pathlib.Path(path).write_bytes(b'license')
    return path

  def _ExternalStorageCliArgs(self) -> list[str]:
    bucket = getattr(self.spec, 'fake_bucket', None)
    return [f'--fake-bucket={bucket}'] if bucket else []


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
  kwargs.setdefault('license_uri', '/fake/spanner_omni_license')
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
    self.assertIsNone(omni_spec.replicas_per_zone)
    self.assertIsNone(omni_spec.zones)
    self.assertIsNone(omni_spec.root_servers_per_zone)
    self.assertEqual(omni_spec.cpu_request, '7')
    self.assertEqual(omni_spec.memory_request, '23Gi')
    self.assertEqual(omni_spec.data_disk_size, '1200Gi')
    self.assertTrue(omni_spec.run_analyze)
    self.assertIsNone(omni_spec.backup_descriptor_id)

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
    self.assertFalse(hasattr(omni_spec, 'gcs_backup_bucket'))

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
    self.assertIsInstance(
        omni_spec, gcp_spanner_omni_cluster.GcpSpannerOmniClusterSpec
    )
    self.assertEqual(omni_spec.cloud, 'GCP')
    self.assertEqual(omni_spec.database_name, 'sbtest')
    self.assertIsNone(omni_spec.license_uri)
    self.assertIsNone(omni_spec.gcs_backup_bucket)

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
    self.assertIsInstance(
        omni_spec, gcp_spanner_omni_cluster.GcpSpannerOmniClusterSpec
    )
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


class HelperFunctionsTest(SpannerOmniClusterTestBase):

  def testBuildLocationsJsonUsesProviderRegionAndShortNameOverrides(self):
    cluster = self._NewCluster()
    with (
        mock.patch.object(
            cluster,
            '_GetRegionFromZone',
            side_effect=lambda z: z[:-1],
        ),
        mock.patch.object(
            cluster,
            '_BuildZoneShortNames',
            return_value={'us-east-1a': 'a', 'us-east-1b': 'b'},
        ),
    ):
      locations = json.loads(
          cluster._BuildLocationsJson(['us-east-1a', 'us-east-1b'])
      )
    self.assertEqual(
        locations,
        [{
            'name': 'us-east-1',
            'zones': [
                {'name': 'us-east-1a', 'shortName': 'a'},
                {'name': 'us-east-1b', 'shortName': 'b'},
            ],
        }],
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

  def testCreateAdminPasswordSecretMeetsPolicy(self):
    cluster = self._NewCluster()
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunRetryableKubectlCommand',
            autospec=True,
            return_value=('', '', 0),
        )
    )

    cluster._CreateAdminPasswordSecret()
    password = cluster._admin_password

    self.assertIsNotNone(password)
    assert password is not None
    self.assertBetween(len(password), 8, 32)
    self.assertTrue(any(c.isupper() for c in password))
    self.assertTrue(any(c.islower() for c in password))
    self.assertTrue(any(c.isdigit() for c in password))
    self.assertTrue(any(not c.isalnum() for c in password))


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

  def testCheckPrerequisitesRunsSpecPrerequisites(self):
    cluster = self._NewCluster()
    mock_check = self.enter_context(
        mock.patch.object(
            FakeSpannerOmniCluster, 'CheckSpecPrerequisites', autospec=True
        )
    )

    cluster.CheckPrerequisites()

    mock_check.assert_called_once_with(cluster.spec)

  def testHelmLocationsFollowSpecZonesWhenClusterZonesUnset(self):
    omni_spec = _Spec(zones=['us-east4-a', 'us-east4-b', 'us-east4-c'])
    omni_cluster = self._Construct(omni_spec, self.mock_cluster)
    omni_cluster.omni_version = '2026.r5'
    chart = omni_cluster._BuildHelmChart('license.bin')
    locations = chart.set_json_values['locations']
    self.assertIn('"name":"us-east4"', locations)
    self.assertNotIn('us-central1', locations)

  def _KubectlResponder(self, pods_json, extra=None):
    """Returns a side_effect routing kubectl commands to canned responses."""
    applied_files = {}
    extra = list(extra or [])

    def _Respond(cmd, **unused_kwargs):
      if cmd[0] == 'apply':
        self.assertEqual(
            cmd[:3], ['apply', '--server-side', '--force-conflicts']
        )
        basename = os.path.basename(cmd[-1]).rsplit('.', 1)[0]
        with open(cmd[-1]) as f:
          applied_files[basename] = json.load(f)
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
    self.assertEqual(cluster.GetResourceMetadata()['omni_version'], '2026.r5')
    self.assertEqual(cluster.first_pod_name, 'spanner-a-rt-0')
    helm_args = self.mock_helm.call_args[0][0]
    self.assertNotIn('--create-namespace', helm_args)
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
        + [str(c) for c in self.mock_run_kubectl.call_args_list]
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
    self.mock_cluster.num_nodes = 4
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
    cluster.SetVms({'clients': [self.mock_client_vm], 'loaders': [loader_vm]})

    self.assertIs(cluster.client_vm, loader_vm)

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

  def testMetadataWithoutClientVmOmitsClientFields(self):
    metadata = FakeSpannerOmniCluster(
        _Spec(replicas_per_zone=1), self.mock_cluster
    ).GetResourceMetadata()

    self.assertNotIn('omni_client_zone', metadata)
    self.assertNotIn('omni_client_machine_type', metadata)

  def testRootServersDefaultToServersPerZoneWhenFewerThanThree(self):
    cluster = self._Construct(_Spec(), self.mock_cluster)
    cluster._ValidateTopology()
    self.assertEqual(cluster.root_servers_per_zone, 1)

  def testCreateRejectsNodeCountMismatch(self):
    cluster = self._Construct(
        _Spec(replicas_per_zone=3, root_servers_per_zone=3), self.mock_cluster
    )
    with self.assertRaisesRegex(
        errors.Config.InvalidValue,
        r'spanner_omni_replicas_per_zone \(3\) must match container_cluster'
        r' node count per zone \(1\)',
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
        raise_on_timeout=False,
    )
    self.mock_run_kubectl.assert_any_call(
        ['delete', 'pvc', '--all', '-n', 'spanner-ns'],
        raise_on_failure=False,
        raise_on_timeout=False,
    )
    self.assertEqual(os.listdir(self.temp_dir), [])

  def testCreateRestoresFromBackup(self):
    responder, _ = self._KubectlResponder(
        _PodsJson(
            _Pod('spanner-a-rt-0', '10.68.0.5'),
            _Pod('spanner-b-rt-0', '10.68.1.5'),
            _Pod('spanner-c-rt-0', '10.68.2.5'),
        ),
        extra=[
            ('Created external storage', '', 0),
            ('Backup import in progress.', '', 0),
            ('state: READY', '', 0),
            ('Database restore in progress.', '', 0),
            ('state: READY', '', 0),
        ],
    )
    mock_kubectl = self._MockKubectl(side_effect=responder)
    self._MockLogin()
    cluster = self._NewCluster(
        prepared=False,
        fake_bucket='my-backup-bucket',
        backup_descriptor_id='desc-456',
    )
    self.assertFalse(cluster.spanner_restored)

    cluster.Create()

    self.assertTrue(cluster.spanner_restored)
    metadata = cluster.GetResourceMetadata()
    self.assertTrue(metadata['omni_restored_from_backup'])
    self.assertGreaterEqual(metadata['omni_restore_duration_sec'], 0.0)
    cli_commands = [
        ' '.join(c[0][0])
        for c in mock_kubectl.call_args_list
        if 'exec' in c[0][0]
    ]
    self.assertLen(cli_commands, 5)
    self.assertIn(
        'external-storages create backup-storage'
        ' --fake-bucket=my-backup-bucket',
        cli_commands[0],
    )
    self.assertIn('backups import', cli_commands[1])
    self.assertIn('desc-456', cli_commands[1])
    self.assertIn('backups describe restored-backup', cli_commands[2])
    self.assertIn('databases restore', cli_commands[3])
    self.assertIn('databases describe benchbase', cli_commands[4])
    self.assertFalse(any('databases create' in c for c in cli_commands))
    login_calls = [
        c for c in self.mock_run_kubectl.call_args_list if 'exec' in c.args[0]
    ]
    self.assertLen(login_calls, 2)

  def testCreateWithExternalStorageWithoutDescriptorCreatesDatabase(self):
    responder, _ = self._KubectlResponder(
        _PodsJson(
            _Pod('spanner-a-rt-0', '10.68.0.5'),
            _Pod('spanner-b-rt-0', '10.68.1.5'),
            _Pod('spanner-c-rt-0', '10.68.2.5'),
        ),
        extra=[
            ('Created external storage', '', 0),
            ('Database created', '', 0),
        ],
    )
    mock_kubectl = self._MockKubectl(side_effect=responder)
    self._MockLogin()
    cluster = self._NewCluster(
        prepared=False,
        fake_bucket='my-backup-bucket',
    )

    cluster.Create()

    self.assertFalse(cluster.spanner_restored)
    cli_commands = [' '.join(c[0][0]) for c in mock_kubectl.call_args_list]
    self.assertTrue(
        any(
            'external-storages create backup-storage'
            ' --fake-bucket=my-backup-bucket'
            in c
            for c in cli_commands
        )
    )
    self.assertTrue(any('databases create' in c for c in cli_commands))
    self.assertFalse(any('backups import' in c for c in cli_commands))

  def testCheckPrerequisitesRejectsBackupDescriptorWithoutExternalStorage(self):
    cluster = self._NewCluster(backup_descriptor_id='desc-456')
    with self.assertRaisesRegex(
        errors.Config.InvalidValue,
        'requires external backup storage to be configured',
    ):
      cluster.CheckPrerequisites()

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

  def testIsBootstrapJobCompleteReturnsFalseWhenNotFound(self):
    self._MockKubectl(
        return_value=('', 'Error from server (NotFound): jobs.batch', 1)
    )
    self.assertFalse(self._NewCluster(prepared=False)._IsBootstrapJobComplete())

  def testIsBootstrapJobCompleteRaisesOnFailedCondition(self):
    self._MockKubectl(
        return_value=(
            json.dumps({
                'status': {'conditions': [{'type': 'Failed', 'status': 'True'}]}
            }),
            '',
            0,
        )
    )
    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'bootstrap job failed'
    ):
      self._NewCluster(prepared=False)._IsBootstrapJobComplete()

  def testDeleteWaitsForBoundPvs(self):
    self.mock_run_kubectl.side_effect = lambda cmd, **_: (
        ('pvc-1 pvc-2', '', 0) if cmd[:2] == ['get', 'pvc'] else ('', '', 0)
    )

    cluster = self._NewCluster()
    cluster._Delete()

    self.mock_helm.assert_called_once_with(
        ['uninstall', 'spanner-omni', '--namespace', 'spanner-ns'],
        raise_on_failure=False,
        raise_on_timeout=False,
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
                '--timeout=300s',
                'pv/pvc-1',
                'pv/pvc-2',
            ],
            timeout=310,
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
    self.assertEqual(cluster._bound_pv_names, ['pvc-1', 'pvc-2'])

  def testDeleteLeavesRemainingPvsToIsDeleting(self):
    def _RunKubectl(cmd, **unused_kwargs):
      if cmd[:2] == ['get', 'pvc']:
        return ('pvc-1', '', 0)
      if cmd[:2] == ['get', 'pv']:
        return ('persistentvolume/pvc-1\n', '', 0)
      return ('', '', 0)

    self.mock_run_kubectl.side_effect = _RunKubectl
    cluster = self._NewCluster()

    cluster._Delete()

    self.assertTrue(cluster._IsDeleting())

  def testDeleteResourceLogsInsteadOfRaising(self):
    cluster = self._NewCluster()
    cluster._bound_pv_names = ['pvc-1']
    with mock.patch.object(
        resource.BaseResource,
        '_DeleteResource',
        autospec=True,
        side_effect=vm_util.RetryError('PVs not deleted'),
    ):
      with self.assertLogs(level='ERROR') as logs:
        cluster._DeleteResource()

    self.assertIn('pvc-1', logs.output[0])

  def testIsDeletingKeepsCheckingStoredPvNamesAfterPvcsDeleted(self):
    pvc_responses = [('pvc-1', '', 0), ('', '', 0), ('', '', 0)]
    pv_responses = [
        ('persistentvolume/pvc-1\n', '', 0),
        ('persistentvolume/pvc-1\n', '', 0),
        ('', '', 0),
    ]

    def _RunKubectl(cmd, **unused_kwargs):
      if cmd[:2] == ['get', 'pvc']:
        return pvc_responses.pop(0)
      if cmd[:2] == ['get', 'pv']:
        return pv_responses.pop(0)
      return ('', '', 0)

    self.mock_run_kubectl.side_effect = _RunKubectl
    cluster = self._NewCluster()

    # Attempt 1 discovers pvc-1, deletes PVCs, sees pv/pvc-1 still present.
    cluster._Delete()
    self.assertTrue(cluster._IsDeleting())
    # Attempt 2 sees no PVCs, but must still check the stored pvc-1.
    cluster._Delete()
    self.assertTrue(cluster._IsDeleting())
    # pv/pvc-1 is gone, which clears _bound_pv_names.
    self.assertFalse(cluster._IsDeleting())
    self.assertEqual(cluster._bound_pv_names, [])

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

  def testConfigureTlsCertificatesRemovesTempSecretFileAndKeys(self):
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
    cleanup_cmd = self.mock_client_vm.RemoteCommand.call_args_list[1][0][0]
    self.assertEqual(
        cleanup_cmd, 'rm -f /tmp/omni_tls/tls_secrets.json /tmp/omni_tls/*.key'
    )
    pulled_local_path, pulled_remote_path = (
        self.mock_client_vm.PullFile.call_args.args
    )
    self.assertTrue(
        pulled_local_path.startswith(
            os.path.join(self.temp_dir, 'omni_tls_secrets.json.')
        )
    )
    self.assertEqual(pulled_remote_path, '/tmp/omni_tls/tls_secrets.json')

  def testConfigureTlsCertificatesRestrictsLocalSecretFile(self):
    modes = []
    applied_paths = []

    def _Apply(cmd, **unused_kwargs):
      local_path = cmd[-1]
      applied_paths.append(local_path)
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

    self.assertLen(applied_paths, 1)
    mock_kubectl.assert_called_once_with(
        [
            'apply',
            '--server-side',
            '--force-conflicts',
            '-n',
            'spanner-ns',
            '-f',
            applied_paths[0],
        ],
        timeout=60,
    )
    self.assertEqual(modes, [0o600])
    self.assertFalse(os.path.exists(applied_paths[0]))

  def testConfigureTlsCertificatesRemovesKeysWhenCertScriptFails(self):
    self.mock_client_vm.RemoteCommand.side_effect = [
        errors.VirtualMachine.RemoteCommandError('cert creation failed'),
        ('', ''),
    ]
    cluster = self._NewCluster()
    cluster.omni_version = '2026.r5'

    with self.assertRaises(errors.VirtualMachine.RemoteCommandError):
      cluster._ConfigureTlsCertificatesAndSecrets()

    cleanup_cmd = self.mock_client_vm.RemoteCommand.call_args_list[1][0][0]
    self.assertEqual(
        cleanup_cmd, 'rm -f /tmp/omni_tls/tls_secrets.json /tmp/omni_tls/*.key'
    )

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


class PodEndpointsTest(SpannerOmniClusterTestBase):

  def testPrioritizesRootServerInClientZone(self):
    self.mock_cluster.zones = ['us-central1-a', 'us-central1-c']
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
    self.mock_cluster.zones = ['us-central1-a']
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
    self.mock_cluster.zones = []
    self.mock_cluster.zone = ''
    cluster = self._NewCluster()
    with self.assertRaisesRegex(
        errors.Config.InvalidValue, 'no zones configured'
    ):
      cluster._ValidateTopology()

  def testUsesDeduplicatedZoneShortName(self):
    self.mock_cluster.zones = ['us-central1-c', 'us-east1-b', 'us-west1-b']
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
    mock_kubectl = self._MockKubectl(
        return_value=(base64.b64encode(b'From-Secret-1').decode(), '', 0)
    )
    cluster = self._NewCluster()
    cluster.container_cluster = None  # pyrefly: ignore[bad-assignment]
    cluster._client_vm = None
    unpickled = pickle.loads(pickle.dumps(cluster))  # pylint: disable=g-unsafe-pickle-load
    self.assertIsNone(unpickled._admin_password)
    self.assertEqual(cluster._admin_password, 'Test-Password-123')
    self.assertEqual(unpickled._GetAdminPassword(), 'From-Secret-1')
    mock_kubectl.assert_called_once()

  def testFirstPodNameRaisesWhenEndpointsNotInitialized(self):
    cluster = self._NewCluster(prepared=False)
    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'pod endpoints are not initialized'
    ):
      _ = cluster.first_pod_name

  def testRestoreHandlesAlreadyExistsAndWaitsForStrictReadyState(self):
    mock_kubectl = self._MockKubectl(
        side_effect=[
            ('', 'ERROR: ALREADY_EXISTS: backup restored-backup', 1),
            ('state: CREATING', '', 0),
            ('state: READY', '', 0),
            ('', 'ERROR: ALREADY_EXISTS: database benchbase', 1),
            ('state: READY_OPTIMIZING', '', 0),
            ('state: READY', '', 0),
        ]
    )
    self._MockLogin()
    cluster = self._NewCluster(
        fake_bucket='test-bucket', backup_descriptor_id='desc-456'
    )

    cluster._RestoreDatabaseFromBackup('benchbase')

    self.assertTrue(cluster.spanner_restored)
    self.assertEqual(mock_kubectl.call_count, 6)
    cli_commands = [' '.join(c[0][0]) for c in mock_kubectl.call_args_list]
    self.assertIn('backups import restored-backup', cli_commands[0])
    self.assertIn('backups describe restored-backup', cli_commands[1])
    self.assertIn('backups describe restored-backup', cli_commands[2])
    self.assertIn('databases restore', cli_commands[3])
    self.assertIn('databases describe benchbase', cli_commands[4])
    self.assertIn('databases describe benchbase', cli_commands[5])

  def testRestoreRetriesOnTransientExecDisconnectAndRelogins(self):
    self.mock_cluster.zones = ['us-central1-a']
    self.mock_cluster.zone = 'us-central1-a'
    mock_kubectl = self._MockKubectl(
        side_effect=[
            ('Backup import in progress.', '', 0),
            ('state: READY', '', 0),
            ('', 'command terminated with exit code 137', 137),
            (_PodsJson(_Pod('spanner-a-rt-0', '10.68.0.5')), '', 0),
            ('', 'Error: failed to restore database: \n', 1),
            ('state: READY_OPTIMIZING', '', 0),
            ('state: READY_OPTIMIZING', '', 0),
            ('state: READY', '', 0),
        ]
    )
    mock_login = self._MockLogin()
    cluster = self._NewCluster(
        fake_bucket='test-bucket', backup_descriptor_id='desc-456'
    )

    cluster._RestoreDatabaseFromBackup('benchbase')

    self.assertTrue(cluster.spanner_restored)
    self.assertEqual(mock_kubectl.call_count, 8)
    self.assertEqual(mock_login.call_count, 2)

  def testRestoreHandlesActiveBackupAlreadyExistsOnImport(self):
    self._MockKubectl(
        side_effect=[
            (
                '',
                (
                    'ERROR: INVALID_ARGUMENT: backup_id is already being used'
                    ' by an active backup'
                ),
                1,
            ),
            ('state: READY', '', 0),
            ('Database restore in progress.', '', 0),
            ('state: READY', '', 0),
        ]
    )
    self._MockLogin()
    cluster = self._NewCluster(
        fake_bucket='test-bucket', backup_descriptor_id='desc-456'
    )

    cluster._RestoreDatabaseFromBackup('benchbase')

    self.assertTrue(cluster.spanner_restored)

  def testRestoreEmptyErrorRaisesWhenDatabaseDoesNotExist(self):
    self._MockKubectl(
        side_effect=[
            ('Backup import in progress.', '', 0),
            ('state: READY', '', 0),
            ('', 'Error: failed to restore database:\n', 1),
            ('', 'ERROR: NOT_FOUND: Database not found: benchbase', 1),
        ]
    )
    self._MockLogin()
    cluster = self._NewCluster(
        fake_bucket='test-bucket', backup_descriptor_id='desc-456'
    )

    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'failed to restore database'
    ):
      cluster._RestoreDatabaseFromBackup('benchbase')

    self.assertFalse(cluster.spanner_restored)

  def testWaitUntilResourceReadyTimesOutWithLastState(self):
    self._MockKubectl(return_value=('state: CREATING', '', 0))
    cluster = self._NewCluster()

    with self.assertRaisesRegex(
        errors.Resource.CreationError,
        r'Timed out after 1s.*benchbase.*last_state=CREATING',
    ):
      cluster._WaitUntilResourceReady(
          'databases', 'benchbase', timeout=1, poll_interval=1
      )

  def testWaitUntilResourceReadyRetriesTransientDescribeFailure(self):
    mock_kubectl = self._MockKubectl(
        side_effect=[
            ('', 'ERROR: INTERNAL: transient glitch', 1),
            ('state: READY', '', 0),
        ]
    )
    cluster = self._NewCluster()

    cluster._WaitUntilResourceReady(
        'databases', 'benchbase', timeout=10, poll_interval=1
    )

    self.assertEqual(mock_kubectl.call_count, 2)

  def testWaitUntilResourceReadyFailsFastOnPermanentDescribeError(self):
    mock_kubectl = self._MockKubectl(
        return_value=('', 'ERROR: PERMISSION_DENIED: forbidden', 1)
    )
    cluster = self._NewCluster()

    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'PERMISSION_DENIED'
    ):
      cluster._WaitUntilResourceReady(
          'databases', 'benchbase', timeout=60, poll_interval=1
      )

    self.assertEqual(mock_kubectl.call_count, 1)

  def testRestoreFailsFastOnPermanentError(self):
    mock_kubectl = self._MockKubectl(
        return_value=('', 'ERROR: INVALID_ARGUMENT: bad descriptor', 1)
    )
    self._MockLogin()
    cluster = self._NewCluster(
        fake_bucket='test-bucket', backup_descriptor_id='bad-desc'
    )

    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'INVALID_ARGUMENT'
    ):
      cluster._RestoreDatabaseFromBackup('benchbase')

    self.assertFalse(cluster.spanner_restored)
    self.assertEqual(mock_kubectl.call_count, 1)

  def testRestoreExhaustedRetriesRaisesCreationError(self):
    mock_kubectl = self._MockKubectl(
        return_value=('', 'ERROR: UNAVAILABLE: backend unavailable', 1)
    )
    self._MockLogin()
    cluster = self._NewCluster(
        fake_bucket='test-bucket', backup_descriptor_id='desc-456'
    )

    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'failed after 3 attempts'
    ):
      cluster._RestoreDatabaseFromBackup('benchbase')

    self.assertFalse(cluster.spanner_restored)
    self.assertEqual(mock_kubectl.call_count, 3)

  def testAuthenticatedCliRelogsInAndRetriesOnAuthError(self):
    mock_kubectl = self._MockKubectl(
        side_effect=[
            ('', 'Error: not authenticated', 1),
            ('state: READY', '', 0),
        ]
    )
    mock_login = self._MockLogin()

    _, _, retcode = self._NewCluster()._RunAuthenticatedCli(
        ['databases', 'describe', 'benchbase']
    )

    self.assertEqual(retcode, 0)
    self.assertEqual(mock_kubectl.call_count, 2)
    self.assertEqual(mock_kubectl.call_args.kwargs['timeout'], 120)
    mock_login.assert_called_once()

  def testAuthenticatedCliRaisesRetryableOnTimeout(self):
    self._MockKubectl(
        side_effect=errors.VmUtil.IssueCommandTimeoutError('timed out')
    )
    mock_login = self._MockLogin()

    with self.assertRaisesRegex(
        errors.Resource.RetryableCreationError, 'timed out'
    ):
      self._NewCluster()._RunAuthenticatedCli(
          ['databases', 'describe', 'benchbase']
      )
    mock_login.assert_not_called()

  def testAuthenticatedCliRefreshesPodsAndRetriesOnPodDisconnect(self):
    self.mock_cluster.zones = ['us-central1-a']
    self.mock_cluster.zone = 'us-central1-a'
    mock_kubectl = self._MockKubectl(
        side_effect=[
            (
                '',
                'Error from server (NotFound): pods "spanner-a-rt-0" not found',
                1,
            ),
            (_PodsJson(_Pod('spanner-a-rt-1', '10.68.0.6')), '', 0),
            ('state: READY', '', 0),
        ]
    )
    mock_login = self._MockLogin()
    cluster = self._NewCluster()

    stdout, _, retcode = cluster._RunAuthenticatedCli(
        ['databases', 'describe', 'benchbase']
    )

    self.assertEqual(retcode, 0)
    self.assertEqual(stdout, 'state: READY')
    self.assertEqual(cluster.first_pod_name, 'spanner-a-rt-1')
    self.assertEqual(mock_kubectl.call_count, 3)
    mock_login.assert_called_once()

  def testDatabaseExistsReturnsFalseOnTimeout(self):
    self._MockKubectl(
        side_effect=vm_util.TimeoutExceededRetryError('deadline exceeded')
    )
    self._MockLogin()

    self.assertFalse(self._NewCluster()._DatabaseExists('benchbase'))

  def testWaitUntilResourceReadyFailsFastOnTerminalFailure(self):
    self._MockKubectl(return_value=('state: FAILED\n', '', 0))
    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'failed.*state=FAILED'
    ):
      self._NewCluster()._WaitUntilResourceReady('databases', 'benchbase')

  def testWaitUntilResourceReadyFailsFastOnNonYamlOutput(self):
    self._MockKubectl(
        return_value=('state: CREATING\nerror: restore failed: [bad\n', '', 0)
    )
    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'Unexpected non-YAML output'
    ):
      self._NewCluster()._WaitUntilResourceReady('databases', 'benchbase')

  def testWaitUntilResourceReadyFailsFastOnNotFound(self):
    self._MockKubectl(
        return_value=('', 'ERROR: NOT_FOUND: database benchbase', 1)
    )
    with self.assertRaisesRegex(errors.Resource.CreationError, 'not found'):
      self._NewCluster()._WaitUntilResourceReady('databases', 'benchbase')

  def testWaitUntilResourceReadyRetriesOnKubectlPodOrContainerNotFound(self):
    self.mock_cluster.zones = ['us-central1-a']
    self.mock_cluster.zone = 'us-central1-a'
    mock_kubectl = self._MockKubectl(
        side_effect=[
            (
                '',
                'Error from server (NotFound): pods "spanner-a-rt-0" not found',
                1,
            ),
            (_PodsJson(_Pod('spanner-a-rt-0', '10.68.0.5')), '', 0),
            ('', 'unable to upgrade connection: container not found', 1),
            ('', 'unable to upgrade connection: container not found', 1),
            (_PodsJson(_Pod('spanner-a-rt-0', '10.68.0.5')), '', 0),
            ('state: READY\n', '', 0),
        ]
    )
    mock_login = self._MockLogin()

    self._NewCluster()._WaitUntilResourceReady('databases', 'benchbase')

    self.assertEqual(mock_kubectl.call_count, 6)
    self.assertEqual(mock_login.call_count, 2)

  def testWaitUntilResourceReadyRetriesOnKubectlTimeout(self):
    mock_kubectl = self._MockKubectl(
        side_effect=[
            vm_util.TimeoutExceededRetryError('context deadline exceeded'),
            ('state: READY\n', '', 0),
        ]
    )
    mock_login = self._MockLogin()

    self._NewCluster()._WaitUntilResourceReady('databases', 'benchbase')

    self.assertEqual(mock_kubectl.call_count, 2)
    mock_login.assert_not_called()

  def testCreateExternalStorageAndDatabaseIgnoreAlreadyExists(self):
    mock_kubectl = self._MockKubectl(
        return_value=('', 'ERROR: ALREADY_EXISTS: resource exists', 1)
    )
    cluster = self._NewCluster(fake_bucket='test-bucket')
    cluster._CreateExternalStorage()
    cluster.CreateDatabase('benchbase')

    storage_cmd = mock_kubectl.call_args_list[0][0][0]
    self.assertIn('external-storages', storage_cmd)
    self.assertIn('backup-storage', storage_cmd)
    self.assertIn('--fake-bucket=test-bucket', storage_cmd)

  def testCreateExternalStorageRetriesTransientErrorAndFailsOnPermanentError(
      self,
  ):
    mock_kubectl = self._MockKubectl(
        side_effect=[
            ('', 'ERROR: UNAVAILABLE: transient', 1),
            ('Created external storage', '', 0),
            ('', 'ERROR: PERMISSION_DENIED: forbidden', 1),
        ]
    )
    self._MockLogin()
    cluster = self._NewCluster(fake_bucket='test-bucket')

    cluster._CreateExternalStorage()
    self.assertEqual(mock_kubectl.call_count, 2)

    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'PERMISSION_DENIED'
    ):
      cluster._CreateExternalStorage()
    self.assertEqual(mock_kubectl.call_count, 3)

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

  def testCreateDatabaseRaisesOnUnexpectedError(self):
    self._MockKubectl(return_value=('', 'ERROR: PERMISSION_DENIED', 1))
    self._MockLogin()
    with self.assertRaisesRegex(
        errors.Resource.CreationError, 'Failed to create Spanner Omni database'
    ):
      self._NewCluster().CreateDatabase('benchbase')

  def testCreateDatabaseRetriesOnCommandTimeout(self):
    mock_kubectl = self._MockKubectl(
        side_effect=[
            errors.VmUtil.IssueCommandTimeoutError('kubectl exec timed out'),
            ('Database created', '', 0),
        ]
    )
    self._MockLogin()

    self._NewCluster().CreateDatabase('benchbase')

    self.assertEqual(mock_kubectl.call_count, 2)

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

    stdout, stderr = self._NewCluster().RunDDLQuery('ANALYZE;', timeout=1800)

    self.assertIn('_auto_op_1', stdout)
    self.assertEqual(stderr, '')
    self.assertIn('--ddl=ANALYZE;', mock_kubectl.call_args_list[0][0][0])
    self.assertIn('_auto_op_1', mock_kubectl.call_args_list[1][0][0])

  def testRunDDLQueryRaisesRunErrorOnSubmitTimeout(self):
    self._MockKubectl(
        side_effect=errors.VmUtil.IssueCommandTimeoutError('submit timed out')
    )
    self._MockLogin()
    with self.assertRaisesRegex(
        errors.Benchmarks.RunError, 'Failed to submit DDL'
    ):
      self._NewCluster().RunDDLQuery('ANALYZE;')

  def testRunDDLQueryRaisesRunErrorOnPollTimeout(self):
    times = iter([0.0, 1.0, 200.0])
    self.enter_context(
        mock.patch.object(time, 'time', side_effect=lambda: next(times, 200.0))
    )
    self._MockKubectl(
        side_effect=[
            (
                (
                    'Schema update in progress. Operation name:'
                    ' instances/default/databases/benchbase/operations/op_slow'
                ),
                '',
                0,
            ),
            ('done: false', '', 0),
        ]
    )
    self._MockLogin()
    with self.assertRaisesRegex(
        errors.Benchmarks.RunError,
        'Timed out waiting for Spanner Omni operation op_slow',
    ):
      self._NewCluster().RunDDLQuery('ANALYZE;', timeout=60)

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
        errors.Benchmarks.RunError, 'Failed to submit DDL'
    ):
      self._NewCluster().RunDDLQuery('ANALYZE;')

  def testRunDDLQueryRaisesWhenOperationIdMissing(self):
    self._MockKubectl(return_value=('Unexpected output', '', 0))
    self._MockLogin()
    with self.assertRaisesRegex(
        errors.Benchmarks.RunError, 'Failed to parse DDL operation ID'
    ):
      self._NewCluster().RunDDLQuery('ANALYZE;')

  def testRunDDLQuerySkipsAnalyzeWhenDisabled(self):
    mock_kubectl = self._MockKubectl()
    mock_login = self._MockLogin()

    self.assertEqual(
        self._NewCluster(run_analyze=False).RunDDLQuery('ANALYZE;'), ('', '')
    )

    mock_kubectl.assert_not_called()
    mock_login.assert_not_called()

  def testWaitForDatabaseOperationFailsFastOnPermanentCliError(self):
    mock_kubectl = self._MockKubectl(
        return_value=('', 'ERROR: NOT_FOUND: operation op not found', 1)
    )
    with self.assertRaisesRegex(
        errors.Benchmarks.RunError,
        'Failed to describe Spanner Omni operation op',
    ):
      self._NewCluster().WaitForDatabaseOperation('op', 'benchbase')
    self.assertEqual(mock_kubectl.call_count, 1)

  def testWaitForDatabaseOperationRetriesTransientCliError(self):
    mock_kubectl = self._MockKubectl(
        side_effect=[
            ('', 'ERROR: UNAVAILABLE: backend temporarily unavailable', 1),
            ('done: true\n', '', 0),
        ]
    )
    self._NewCluster().WaitForDatabaseOperation('op', 'benchbase')
    self.assertEqual(mock_kubectl.call_count, 2)

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

  def testWaitForDatabaseOperationRaisesOnNonYamlOutput(self):
    self._MockKubectl(return_value=('Progress: [100%\ndone: true\n', '', 0))
    with self.assertRaisesRegex(
        errors.Benchmarks.RunError,
        'Unexpected non-YAML output describing Spanner Omni operation op',
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
        errors.Benchmarks.RunError, 'Spanner Omni operation op failed'
    ):
      self._NewCluster().WaitForDatabaseOperation('op', 'benchbase')


if __name__ == '__main__':
  pkb_common_test_case.absltest.main()
