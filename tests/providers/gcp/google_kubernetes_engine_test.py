# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.gcp.google_kubernetes_engine."""

# pylint: disable=not-context-manager

import builtins
import os
import unittest
from unittest import mock

from absl import flags as flgs
from absl.testing import flagsaver
import contextlib2
from perfkitbenchmarker import container_service
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import container_spec
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import google_kubernetes_engine
from perfkitbenchmarker.providers.gcp import util
from tests import pkb_common_test_case

FLAGS = flgs.FLAGS

_COMPONENT = 'test_component'
_RUN_URI = 'abc9876'
_NVIDIA_DRIVER_SETUP_DAEMON_SET_SCRIPT = 'https://raw.githubusercontent.com/GoogleCloudPlatform/container-engine-accelerators/master/nvidia-driver-installer/cos/daemonset-preloaded.yaml'
_NVIDIA_UNRESTRICTED_PERMISSIONS_DAEMON_SET = (
    'nvidia_unrestricted_permissions_daemonset.yml'
)

_INSTANCE_GROUPS_LIST_OUTPUT = (
    '../../../tests/data/gcloud_compute_instance_groups_list_instances.json'
)
_NODE_POOLS_LIST_OUTPUT = (
    '../../../tests/data/gcloud_container_node_pools_list.json'
)
_KUBECTL_VERSION = """\
serverVersion:
  gitVersion: v1.2.3"""
_PVC_VOLUME = 'pvc-19f8e47d-6410-4edf-9320-5a243676e6e8'
_PVC_LIST = f"""
items:
- spec:
    volumeName: {_PVC_VOLUME}
"""


@contextlib2.contextmanager
def patch_critical_objects(stdout='', stderr='', return_code=0, flags=FLAGS):
  with contextlib2.ExitStack() as stack:
    flags.gcloud_path = 'gcloud'
    flags.run_uri = _RUN_URI
    flags.data_search_paths = ''

    stack.enter_context(mock.patch(builtins.__name__ + '.open'))
    stack.enter_context(mock.patch(vm_util.__name__ + '.PrependTempDir'))
    stack.enter_context(mock.patch(vm_util.__name__ + '.NamedTemporaryFile'))
    stack.enter_context(
        mock.patch(util.__name__ + '.GetDefaultUser', return_value='fakeuser')
    )
    stack.enter_context(
        mock.patch(
            util.__name__ + '.MakeFormattedDefaultTags',
            return_value='foo=bar,timeout=yesterday',
        )
    )
    stack.enter_context(
        mock.patch(
            gce_network.__name__ + '.GceFirewall.GetFirewall',
            return_value='fakefirewall',
        )
    )
    stack.enter_context(
        mock.patch(
            gce_network.__name__ + '.GceNetwork.GetNetwork',
            return_value=gce_network.GceNetwork(
                gce_network.GceNetworkSpec('fakeproject', zone='us-central1-a')
            ),
        )
    )

    retval = (stdout, stderr, return_code)
    issue_command = stack.enter_context(
        mock.patch(vm_util.__name__ + '.IssueCommand', return_value=retval)
    )
    yield issue_command


class GoogleContainerRegistryTestCase(pkb_common_test_case.PkbCommonTestCase):

  class FakeContainerImage(container_service.ContainerImage):

    def __init__(self, name):
      self.name = name
      self.directory = f'docker/{name}/Dockerfile'

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch.object(
            google_kubernetes_engine.container_service,
            'ContainerImage',
            self.FakeContainerImage,
        )
    )

  def testFullRegistryTag(self):
    spec = container_spec.ContainerRegistrySpec(
        'NAME',
        **{
            'cloud': 'GCP',
        },
    )
    spec.zone = 'us-west-1a'
    with patch_critical_objects():
      registry = google_kubernetes_engine.GoogleArtifactRegistry(spec)
      image = registry.GetFullRegistryTag('fakeimage')
    self.assertEqual(
        image, 'us-west-docker.pkg.dev/test_project/pkbabc9876/fakeimage'
    )

  def testRemoteBuildCreateSucceeds(self):
    spec = container_spec.ContainerRegistrySpec(
        'NAME',
        **{
            'cloud': 'GCP',
        },
    )
    spec.zone = 'us-west-1a'
    registry = google_kubernetes_engine.GoogleArtifactRegistry(spec)
    self.enter_context(
        mock.patch.object(util.GcloudCommand, 'Issue', return_value=('', '', 0))
    )
    registry._Build('fakeimage')


class GoogleKubernetesEngineCustomMachineTypeTestCase(
    pkb_common_test_case.PkbCommonTestCase
):

  @staticmethod
  def create_kubernetes_engine_spec():
    kubernetes_engine_spec = container_spec.ContainerClusterSpec(
        'NAME',
        **{
            'cloud': 'GCP',
            'vm_spec': {
                'GCP': {
                    'machine_type': {
                        'cpus': 4,
                        'memory': '1024MiB',
                    },
                    'zone': 'us-west1-a',
                },
            },
        },
    )
    return kubernetes_engine_spec

  def testCreate(self):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud container clusters create', command_string)
      self.assertIn('--machine-type custom-4-1024', command_string)


class GoogleKubernetesEngineTestCase(pkb_common_test_case.PkbCommonTestCase):

  @staticmethod
  def create_kubernetes_engine_spec():
    kubernetes_engine_spec = container_spec.ContainerClusterSpec(
        'NAME',
        **{
            'cloud': 'GCP',
            'vm_spec': {
                'GCP': {
                    'machine_type': 'fake-machine-type',
                    'zone': 'us-central1-a',
                    'min_cpu_platform': 'skylake',
                    'boot_disk_type': 'foo',
                    'boot_disk_size': 200,
                    'num_local_ssds': 2,
                    'project': 'fakeproject',
                },
            },
            'vm_count': 2,
        },
    )
    return kubernetes_engine_spec

  def testCreate(self):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud container clusters create', command_string)
      self.assertIn('--num-nodes 2', command_string)
      self.assertIn('--cluster-ipv4-cidr /19', command_string)
      self.assertIn('--machine-type fake-machine-type', command_string)
      self.assertIn('--project fakeproject', command_string)
      self.assertIn('--zone us-central1-a', command_string)
      self.assertIn('--min-cpu-platform skylake', command_string)
      self.assertIn('--disk-size 200', command_string)
      self.assertIn('--disk-type foo', command_string)
      self.assertIn('--local-ssd-count 2', command_string)
      self.assertIn('--no-enable-shielded-nodes', command_string)
      self.assertIn('--labels foo=bar,timeout=yesterday', command_string)

  def testCreateQuotaExceeded(self):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects(
        stderr="""
        message=Insufficient regional quota to satisfy request: resource "CPUS":
        request requires '6400.0' and is short '5820.0'""",
        return_code=1,
    ) as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      with self.assertRaises(errors.Benchmarks.QuotaFailure):
        cluster._Create()
      self.assertEqual(issue_command.call_count, 1)

  def testCreateResourcesExhausted(self):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects(
        stderr="""
        [ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS]:
        Instance 'test' creation failed: The zone
        'projects/artemis-prod/zones/us-central1-a' does not have enough
        resources available to fulfill the request.""",
        return_code=1,
    ) as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      with self.assertRaises(
          errors.Benchmarks.InsufficientCapacityCloudFailure
      ):
        cluster._Create()
      self.assertEqual(issue_command.call_count, 1)

  def testPostCreate(self):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects() as issue_command, mock.patch.object(
        container_service, 'RunKubectlCommand'
    ) as mock_kubectl_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._PostCreate()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn(
          'gcloud container clusters get-credentials pkb-{}'.format(_RUN_URI),
          command_string,
      )
      self.assertIn('KUBECONFIG', issue_command.call_args[1]['env'])

      self.assertEqual(mock_kubectl_command.call_count, 1)

  def testDelete(self):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Delete()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 4)
      self.assertIn(
          'gcloud container clusters delete pkb-{}'.format(_RUN_URI),
          command_string,
      )
      self.assertIn('--zone us-central1-a', command_string)

  def testExists(self):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Exists()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn(
          'gcloud container clusters describe pkb-{}'.format(_RUN_URI),
          command_string,
      )

  def testGetResourceMetadata(self):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects(stdout=_KUBECTL_VERSION) as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster.created = True
      metadata = cluster.GetResourceMetadata()
      self.assertEqual(issue_command.call_count, 1)
      expected = {
          'project': 'fakeproject',
          'gce_local_ssd_count': 2,
          'gce_local_ssd_interface': 'SCSI',
          'machine_type': 'fake-machine-type',
          'boot_disk_type': 'foo',
          'boot_disk_size': 200,
          'cloud': 'GCP',
          'cluster_type': 'Kubernetes',
          'zone': 'us-central1-a',
          'size': 2,
          'version': 'v1.2.3',
      }
      self.assertEqual(metadata, {**metadata, **expected})

  def testCidrCalculations(self):
    self.assertEqual(google_kubernetes_engine._CalculateCidrSize(1), 19)
    self.assertEqual(google_kubernetes_engine._CalculateCidrSize(16), 19)
    self.assertEqual(google_kubernetes_engine._CalculateCidrSize(17), 18)
    self.assertEqual(google_kubernetes_engine._CalculateCidrSize(48), 18)
    self.assertEqual(google_kubernetes_engine._CalculateCidrSize(49), 17)
    self.assertEqual(google_kubernetes_engine._CalculateCidrSize(250), 15)


class GoogleKubernetesEngineAutoscalingTestCase(
    pkb_common_test_case.PkbCommonTestCase
):

  @staticmethod
  def create_kubernetes_engine_spec():
    kubernetes_engine_spec = container_spec.ContainerClusterSpec(
        'NAME',
        **{
            'cloud': 'GCP',
            'vm_spec': {
                'GCP': {
                    'machine_type': 'fake-machine-type',
                    'project': 'fakeproject',
                    'zone': 'us-central1-a',
                },
            },
            'min_vm_count': 1,
            'vm_count': 2,
            'max_vm_count': 30,
        },
    )
    return kubernetes_engine_spec

  def testCreate(self):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud container clusters create', command_string)
      self.assertIn('--enable-autoscaling', command_string)
      self.assertIn('--min-nodes 1', command_string)
      self.assertIn('--num-nodes 2', command_string)
      self.assertIn('--max-nodes 30', command_string)
      self.assertIn('--cluster-ipv4-cidr /18', command_string)

  def testGetResourceMetadata(self):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects(stdout=_KUBECTL_VERSION) as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster.created = True
      metadata = cluster.GetResourceMetadata()
      self.assertEqual(issue_command.call_count, 1)
      expected = {
          'project': 'fakeproject',
          'cloud': 'GCP',
          'cluster_type': 'Kubernetes',
          'min_size': 1,
          'size': 2,
          'max_size': 30,
      }
      self.assertEqual(metadata, {**metadata, **expected})

  def testLabelDisks(self):
    with patch_critical_objects(stdout=_PVC_LIST) as issue_command:
      # spec must be createded inside context to get project
      spec = self.create_kubernetes_engine_spec()
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster.created = True
      cluster.LabelDisks()
      self.assertEqual(issue_command.call_count, 2)
      label_disk_cmd = ' '.join(issue_command.call_args_list[1][0][0])
      self.assertIn('disks add-label', label_disk_cmd)
      self.assertIn(_PVC_VOLUME, label_disk_cmd)
      self.assertIn('--project fakeproject', label_disk_cmd)
      self.assertIn('--zone us-central1-a', label_disk_cmd)


class GoogleKubernetesEngineVersionFlagTestCase(
    pkb_common_test_case.PkbCommonTestCase
):

  @staticmethod
  def create_kubernetes_engine_spec():
    kubernetes_engine_spec = container_spec.ContainerClusterSpec(
        'NAME',
        **{
            'cloud': 'GCP',
            'vm_spec': {
                'GCP': {
                    'machine_type': 'fake-machine-type',
                    'zone': 'us-west1-a',
                },
            },
        },
    )
    return kubernetes_engine_spec

  @flagsaver.flagsaver(container_cluster_version='fake-version')
  def testCreateCustomVersion(self):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertNotIn('--release-channel', command_string)
      self.assertIn('--cluster-version fake-version', command_string)
      self.assertIn('--no-enable-autoupgrade', command_string)

  def testCreateDefaultVersion(self):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertNotIn('--release-channel', command_string)
      self.assertNotIn('--cluster-version', command_string)
      self.assertIn('--no-enable-autoupgrade', command_string)

  @flagsaver.flagsaver(gke_release_channel='rapid')
  def testCreateRapidChannel(self):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('--release-channel rapid', command_string)
      self.assertNotIn('--cluster-version', command_string)
      self.assertNotIn('--no-enable-autoupgrade', command_string)


class GoogleKubernetesEngineGvnicFlagTestCase(
    pkb_common_test_case.PkbCommonTestCase
):

  @staticmethod
  def create_kubernetes_engine_spec():
    kubernetes_engine_spec = container_spec.ContainerClusterSpec(
        'NAME',
        **{
            'cloud': 'GCP',
            'vm_spec': {
                'GCP': {
                    'machine_type': 'fake-machine-type',
                    'zone': 'us-west1-a',
                },
            },
        },
    )
    return kubernetes_engine_spec

  def testCreateEnableGvnic(self):
    spec = self.create_kubernetes_engine_spec()
    FLAGS.gke_enable_gvnic = True
    with patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('--enable-gvnic', command_string)

  def testCreateDisableGvnic(self):
    spec = self.create_kubernetes_engine_spec()
    FLAGS.gke_enable_gvnic = False
    with patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('--no-enable-gvnic', command_string)


class GoogleKubernetesEngineWithGpusTestCase(
    pkb_common_test_case.PkbCommonTestCase
):

  @staticmethod
  def create_kubernetes_engine_spec():
    kubernetes_engine_spec = container_spec.ContainerClusterSpec(
        'NAME',
        **{
            'cloud': 'GCP',
            'vm_spec': {
                'GCP': {
                    'machine_type': 'fake-machine-type',
                    'gpu_type': 'k80',
                    'gpu_count': 2,
                    'zone': 'us-west1-a',
                },
            },
            'vm_count': 2,
        },
    )
    return kubernetes_engine_spec

  def testCreate(self):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud container clusters create', command_string)
      self.assertIn('--num-nodes 2', command_string)
      self.assertIn('--machine-type fake-machine-type', command_string)
      self.assertIn(
          '--accelerator type=nvidia-tesla-k80,count=2', command_string
      )

  @mock.patch('perfkitbenchmarker.kubernetes_helper.CreateFromFile')
  def testPostCreate(self, create_from_file_patch):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects() as issue_command, mock.patch.object(
        container_service, 'RunKubectlCommand'
    ) as mock_kubectl_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._PostCreate()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn(
          'gcloud container clusters get-credentials pkb-{}'.format(_RUN_URI),
          command_string,
      )
      self.assertIn('KUBECONFIG', issue_command.call_args[1]['env'])

      self.assertEqual(mock_kubectl_command.call_count, 1)

      expected_args_to_create_from_file = (
          _NVIDIA_DRIVER_SETUP_DAEMON_SET_SCRIPT,
          data.ResourcePath(_NVIDIA_UNRESTRICTED_PERMISSIONS_DAEMON_SET),
      )
      expected_calls = [
          mock.call(arg) for arg in expected_args_to_create_from_file
      ]

      # Assert that create_from_file was called twice,
      # and that the args were as expected (should be the NVIDIA
      # driver setup daemon set, followed by the
      # NVIDIA unrestricted permissions daemon set.
      create_from_file_patch.assert_has_calls(expected_calls)


class GoogleKubernetesEngineGetNodesTestCase(GoogleKubernetesEngineTestCase):

  def testGetInstanceGroups(self):
    path = os.path.join(os.path.dirname(__file__), _NODE_POOLS_LIST_OUTPUT)
    output = open(path).read()
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects(stdout=output) as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      instance_groups = cluster._GetInstanceGroups()

      command_string = ' '.join(issue_command.call_args[0][0])
      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud container node-pools list', command_string)
      self.assertIn('--cluster', command_string)

      expected = {
          'gke-pkb-0c47e6fa-default-pool-167d73ee-grp',
          'gke-pkb-0c47e6fa-test-efea7796-grp',
      }
      self.assertEqual(expected, set(instance_groups))  # order doesn't matter


class GoogleKubernetesEngineRegionalTestCase(
    pkb_common_test_case.PkbCommonTestCase
):

  @staticmethod
  def create_kubernetes_engine_spec(use_zonal_nodepools=False):
    kubernetes_engine_spec = container_spec.ContainerClusterSpec(
        'NAME',
        **{
            'cloud': 'GCP',
            'vm_spec': {
                'GCP': {
                    'machine_type': 'fake-machine-type',
                    'zone': 'us-west1',
                },
            },
            'nodepools': {
                'nodepool1': {
                    'vm_spec': {
                        'GCP': {
                            'machine_type': 'machine-type-1',
                            'zone': (
                                'us-west1-a,us-west1-b'
                                if use_zonal_nodepools
                                else None
                            ),
                        },
                    }
                },
                'nodepool2': {
                    'vm_spec': {
                        'GCP': {
                            'machine_type': 'machine-type-2',
                            'zone': (
                                'us-west1-c' if use_zonal_nodepools else None
                            ),
                        },
                    },
                    'sandbox_config': {
                        'type': 'gvisor',
                    },
                },
            },
        },
    )
    return kubernetes_engine_spec

  def testCreateRegionalCluster(self):
    spec = self.create_kubernetes_engine_spec(use_zonal_nodepools=False)
    with patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      create_cluster, create_nodepool1, create_nodepool2 = (
          call[0][0] for call in issue_command.call_args_list
      )
      self.assertNotIn('--zone', create_cluster)
      self.assertContainsSubsequence(create_cluster, ['--region', 'us-west1'])
      self.assertContainsSubsequence(
          create_cluster, ['--machine-type', 'fake-machine-type']
      )
      self.assertContainsSubsequence(
          create_cluster, ['--labels', 'foo=bar,timeout=yesterday']
      )

      self.assertContainsSubsequence(
          create_nodepool1,
          ['gcloud', 'container', 'node-pools', 'create', 'nodepool1'],
      )
      self.assertContainsSubsequence(
          create_nodepool1, ['--cluster', 'pkb-abc9876']
      )
      self.assertContainsSubsequence(
          create_nodepool1, ['--machine-type', 'machine-type-1']
      )
      self.assertContainsSubsequence(
          create_nodepool1, ['--node-labels', 'pkb_nodepool=nodepool1']
      )
      self.assertContainsSubsequence(
          create_nodepool1, ['--labels', 'foo=bar,timeout=yesterday']
      )
      self.assertNotIn('--node-locations', create_nodepool1)
      self.assertNotIn('--sandbox', create_nodepool1)
      self.assertContainsSubsequence(create_nodepool1, ['--region', 'us-west1'])

      self.assertContainsSubsequence(
          create_nodepool2,
          ['gcloud', 'container', 'node-pools', 'create', 'nodepool2'],
      )
      self.assertContainsSubsequence(
          create_nodepool2, ['--machine-type', 'machine-type-2']
      )
      self.assertContainsSubsequence(
          create_nodepool2, ['--sandbox', 'type=gvisor']
      )
      self.assertNotIn('--node-locations', create_nodepool2)

  def testCreateRegionalClusterZonalNodepool(self):
    spec = self.create_kubernetes_engine_spec(use_zonal_nodepools=True)
    with patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      create_cluster, create_nodepool1, create_nodepool2 = (
          call[0][0] for call in issue_command.call_args_list
      )
      self.assertNotIn('--zone', create_cluster)
      self.assertContainsSubsequence(create_cluster, ['--region', 'us-west1'])
      self.assertContainsSubsequence(
          create_cluster, ['--machine-type', 'fake-machine-type']
      )

      self.assertContainsSubsequence(
          create_nodepool1,
          ['gcloud', 'container', 'node-pools', 'create', 'nodepool1'],
      )
      self.assertContainsSubsequence(
          create_nodepool1, ['--cluster', 'pkb-abc9876']
      )
      self.assertContainsSubsequence(
          create_nodepool1, ['--node-labels', 'pkb_nodepool=nodepool1']
      )
      self.assertContainsSubsequence(
          create_nodepool1, ['--node-locations', 'us-west1-a,us-west1-b']
      )
      self.assertContainsSubsequence(create_nodepool1, ['--region', 'us-west1'])

      self.assertContainsSubsequence(
          create_nodepool2,
          ['gcloud', 'container', 'node-pools', 'create', 'nodepool2'],
      )
      self.assertContainsSubsequence(
          create_nodepool2, ['--node-locations', 'us-west1-c']
      )


class GoogleKubernetesEngineAutopilotTestCase(
    pkb_common_test_case.PkbCommonTestCase
):

  @staticmethod
  def create_kubernetes_engine_spec():
    kubernetes_engine_spec = container_spec.ContainerClusterSpec(
        'NAME',
        **{
            'cloud': 'GCP',
            'type': 'Autopilot',
            'vm_spec': {
                'GCP': {
                    'zone': 'us-central1-a',
                    'project': 'fakeproject',
                },
            },
        },
    )
    return kubernetes_engine_spec

  def testCreate(self):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeAutopilotCluster(spec)
      cluster._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud container clusters create-auto', command_string)
      self.assertIn('--project fakeproject', command_string)
      self.assertIn('--region us-central1', command_string)
      self.assertIn('--format json', command_string)
      self.assertIn('--labels foo=bar,timeout=yesterday', command_string)
      self.assertNotIn('--machine-type', command_string)
      self.assertNotIn('--num-nodes', command_string)

  def testGetResourceMetadata(self):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects():
      cluster = google_kubernetes_engine.GkeAutopilotCluster(spec)
      metadata = cluster.GetResourceMetadata()
      self.assertDictContainsSubset(
          {
              'project': 'fakeproject',
              'cloud': 'GCP',
              'cluster_type': 'Autopilot',
              'region': 'us-central1',
              'machine_type': 'Autopilot',
              'size': 'Autopilot',
              'nodepools': 'Autopilot',
          },
          metadata,
      )

  @flagsaver.flagsaver(gke_release_channel='rapid')
  def testGetResourceMetadataIncludesReleaseChannel(self):
    spec = self.create_kubernetes_engine_spec()
    with patch_critical_objects():
      cluster = google_kubernetes_engine.GkeAutopilotCluster(spec)
      metadata = cluster.GetResourceMetadata()
      self.assertDictContainsSubset(
          {
              'release_channel': 'rapid',
          },
          metadata,
      )


if __name__ == '__main__':
  unittest.main()
