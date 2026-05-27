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

# pylint: disable=not-context-manager,invalid-name,protected-access

import builtins
import contextlib
import json
import os
import tempfile
import unittest
from unittest import mock

from absl import flags as flgs
from absl.testing import flagsaver
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import container_spec
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import google_kubernetes_engine
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.resources.container_service import container
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from tests import pkb_common_test_case

FLAGS = flgs.FLAGS

_RUN_URI = 'abc9876'

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


class PatchedObjectsTestCase(pkb_common_test_case.PkbCommonTestCase):
  """Adds a patching context manager to the test case."""

  @contextlib.contextmanager
  def patch_critical_objects(
      self, stdout='', stderr='', return_code=0, flags=FLAGS
  ):
    """Patches common objects and yields a mock IssueCommand."""
    with contextlib.ExitStack() as stack:
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
                  gce_network.GceNetworkSpec(
                      'fakeproject', zone='us-central1-a'
                  )
              ),
          )
      )

      yield self.MockIssueCommand({'': [(stdout, stderr, return_code)]})


class GoogleContainerRegistryTestCase(PatchedObjectsTestCase):
  """Tests for the GoogleArtifactRegistry container registry."""

  class FakeContainerImage(container.ContainerImage):
    """Minimal ContainerImage stub for registry tests."""

    def __init__(self, name, directory=None):  # pylint: disable=super-init-not-called
      self.name = name
      self.directory = directory or f'docker/{name}/Dockerfile'

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch.object(
            container,
            'ContainerImage',
            self.FakeContainerImage,
        )
    )

  def testFullRegistryTag(self):
    """Tests that full registry tag is constructed correctly."""
    spec = container_spec.ContainerRegistrySpec(
        'NAME',
        **{
            'cloud': 'GCP',
        },
    )
    spec.zone = 'us-west-1a'
    with self.patch_critical_objects():
      registry = google_kubernetes_engine.GoogleArtifactRegistry(spec)
      image = registry.GetFullRegistryTag('fakeimage')
    self.assertEqual(
        image, 'us-west-docker.pkg.dev/test_project/pkbabc9876/fakeimage'
    )

  def testRemoteBuildCreateSucceeds(self):
    """Tests that _Build succeeds when gcloud Issue returns success."""
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


class GoogleKubernetesEngineCustomMachineTypeTestCase(PatchedObjectsTestCase):
  """Tests for GKE cluster creation with a custom machine type."""

  @staticmethod
  def create_kubernetes_engine_spec():
    """Creates a GKE spec with a custom CPU/memory machine type."""
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
            'poll_for_events': False,
        },
    )
    return kubernetes_engine_spec

  def testCreate(self):
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      self.assertIn(
          'gcloud container clusters create', issue_command.all_commands
      )
      self.assertIn('--machine-type custom-4-1024', issue_command.all_commands)


class GoogleKubernetesEngineTestCase(PatchedObjectsTestCase):
  """Tests for standard GKE cluster create/delete/exists operations."""

  @staticmethod
  def create_kubernetes_engine_spec():
    """Creates a standard GKE cluster spec with typical VM options."""
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
    """Tests that _Create issues the correct gcloud command with all flags."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      self.assertIn(
          'gcloud container clusters create', issue_command.all_commands
      )
      self.assertIn('--num-nodes 2', issue_command.all_commands)
      self.assertIn('--cluster-ipv4-cidr /19', issue_command.all_commands)
      self.assertIn(
          '--machine-type fake-machine-type', issue_command.all_commands
      )
      self.assertIn('--project fakeproject', issue_command.all_commands)
      self.assertIn('--zone us-central1-a', issue_command.all_commands)
      self.assertIn('--min-cpu-platform skylake', issue_command.all_commands)
      self.assertIn('--disk-size 200', issue_command.all_commands)
      self.assertIn('--disk-type foo', issue_command.all_commands)
      self.assertIn(
          '--ephemeral-storage-local-ssd count=2', issue_command.all_commands
      )
      self.assertIn('--no-enable-shielded-nodes', issue_command.all_commands)
      self.assertIn(
          '--labels foo=bar,timeout=yesterday', issue_command.all_commands
      )

  def testCreateQuotaExceeded(self):
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(
        stderr="""
        message=Insufficient regional quota to satisfy request: resource "CPUS":
        request requires '6400.0' and is short '5820.0'""",
        return_code=1,
    ):
      cluster = google_kubernetes_engine.GkeCluster(spec)
      with self.assertRaises(errors.Benchmarks.QuotaFailure):
        cluster._Create()

  def testCreateResourcesExhausted(self):
    """Tests _Create raises InsufficientCapacityCloudFailure on exhaustion."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(
        stderr="""
        [ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS]:
        Instance 'test' creation failed: The zone
        'projects/artemis-prod/zones/us-central1-a' does not have enough
        resources available to fulfill the request.""",
        return_code=1,
    ):
      cluster = google_kubernetes_engine.GkeCluster(spec)
      with self.assertRaises(
          errors.Benchmarks.InsufficientCapacityCloudFailure
      ):
        cluster._Create()

  def testGetCredentials(self):
    """Tests that _PostCreate issues get-credentials with KUBECONFIG set."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects() as issue_command, mock.patch.object(
        kubectl, 'RunKubectlCommand'
    ) as mock_kubectl_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      cluster._PostCreate()
      self.assertIn(
          f'gcloud container clusters get-credentials pkb-{_RUN_URI}',
          issue_command.all_commands,
      )
      self.assertIn(
          'KUBECONFIG', issue_command.func_to_mock.call_args[1]['env']
      )

      self.assertEqual(mock_kubectl_command.call_count, 1)

  def testDelete(self):
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Delete()
      self.assertEqual(issue_command.func_to_mock.call_count, 5)
      self.assertIn(
          f'gcloud container clusters delete pkb-{_RUN_URI}',
          issue_command.all_commands,
      )
      self.assertIn('--zone us-central1-a', issue_command.all_commands)

  def testExists(self):
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Exists()
      self.assertIn(
          f'gcloud container clusters describe pkb-{_RUN_URI}',
          issue_command.all_commands,
      )

  def testGetResourceMetadata(self):
    """Tests that GetResourceMetadata returns all expected fields."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(stdout=_KUBECTL_VERSION):
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster.created = True
      metadata = cluster.GetResourceMetadata()
      expected = {
          'project': 'fakeproject',
          'gce_local_ssd_count': 2,
          'gce_local_ssd_interface': 'NVME',
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


class GoogleKubernetesEngineAutoscalingTestCase(PatchedObjectsTestCase):
  """Tests for GKE cluster creation with cluster-level autoscaling."""

  @staticmethod
  def create_kubernetes_engine_spec():
    """Creates a GKE spec with cluster-level autoscaling enabled."""
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
    """Tests that _Create passes autoscaling flags to gcloud."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      self.assertIn(
          'gcloud container clusters create', issue_command.all_commands
      )
      self.assertIn('--enable-autoscaling', issue_command.all_commands)
      self.assertIn('--min-nodes 1', issue_command.all_commands)
      self.assertIn('--num-nodes 2', issue_command.all_commands)
      self.assertIn('--max-nodes 30', issue_command.all_commands)
      self.assertIn('--cluster-ipv4-cidr /18', issue_command.all_commands)

  def testGetResourceMetadata(self):
    """Tests that metadata includes autoscaling size fields."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(stdout=_KUBECTL_VERSION):
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster.created = True
      metadata = cluster.GetResourceMetadata()
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
    with self.patch_critical_objects(stdout=_PVC_LIST) as issue_command:
      # spec must be createded inside context to get project
      spec = self.create_kubernetes_engine_spec()
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster.created = True
      cluster.LabelDisks()
      self.assertIn('disks add-label', issue_command.all_commands)
      self.assertIn(_PVC_VOLUME, issue_command.all_commands)
      self.assertIn('--project fakeproject', issue_command.all_commands)
      self.assertIn('--zone us-central1-a', issue_command.all_commands)


class GoogleKubernetesEngineVersionFlagTestCase(PatchedObjectsTestCase):
  """Tests for GKE cluster creation with version and release-channel flags."""

  @staticmethod
  def create_kubernetes_engine_spec():
    """Creates a GKE spec for testing version and release-channel flags."""
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
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      self.assertNotIn('--release-channel', issue_command.all_commands)
      self.assertIn(
          '--cluster-version fake-version', issue_command.all_commands
      )
      self.assertIn('--no-enable-autoupgrade', issue_command.all_commands)

  def testCreateDefaultVersion(self):
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      self.assertNotIn('--release-channel', issue_command.all_commands)
      self.assertNotIn('--cluster-version', issue_command.all_commands)
      self.assertIn('--no-enable-autoupgrade', issue_command.all_commands)

  @flagsaver.flagsaver(gke_release_channel='rapid')
  def testCreateRapidChannel(self):
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      self.assertIn('--release-channel rapid', issue_command.all_commands)
      self.assertNotIn('--cluster-version', issue_command.all_commands)
      self.assertNotIn('--no-enable-autoupgrade', issue_command.all_commands)


class GoogleKubernetesEngineGvnicFlagTestCase(PatchedObjectsTestCase):
  """Tests for GKE cluster creation with gVNIC enable/disable flags."""

  @staticmethod
  def create_kubernetes_engine_spec():
    """Creates a GKE spec for testing the gVNIC flag."""
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
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      self.assertIn('--enable-gvnic', issue_command.all_commands)

  def testCreateDisableGvnic(self):
    spec = self.create_kubernetes_engine_spec()
    FLAGS.gke_enable_gvnic = False
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      self.assertIn('--no-enable-gvnic', issue_command.all_commands)


class GoogleKubernetesEngineWithGpusTestCase(PatchedObjectsTestCase):
  """Tests for GKE cluster creation with GPU accelerator configuration."""

  @staticmethod
  def create_kubernetes_engine_spec(gpu_type):
    """Creates a GKE spec with the given GPU type and 2 GPUs."""
    kubernetes_engine_spec = container_spec.ContainerClusterSpec(
        'NAME',
        **{
            'cloud': 'GCP',
            'vm_spec': {
                'GCP': {
                    'machine_type': 'fake-machine-type',
                    'gpu_type': gpu_type,
                    'gpu_count': 2,
                    'zone': 'us-west1-a',
                },
            },
            'vm_count': 2,
            'poll_for_events': False,
        },
    )
    return kubernetes_engine_spec

  @flagsaver.flagsaver(gke_gpu_driver_version='latest')
  def testCreate(self):
    """Tests that _Create includes the correct --accelerator flag for K80."""
    spec = self.create_kubernetes_engine_spec('k80')
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      self.assertIn(
          'gcloud container clusters create', issue_command.all_commands
      )
      self.assertIn('--num-nodes 2', issue_command.all_commands)
      self.assertIn(
          '--machine-type fake-machine-type', issue_command.all_commands
      )
      self.assertIn(
          '--accelerator '
          + 'type=nvidia-tesla-k80,count=2,gpu-driver-version=latest',
          issue_command.all_commands,
      )

  def testCreateGpuH100(self):
    spec = self.create_kubernetes_engine_spec('h100')
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      self.assertIn(
          '--accelerator '
          + 'type=nvidia-h100-80gb,count=2,gpu-driver-version=default',
          issue_command.all_commands,
      )


class GoogleKubernetesEngineGetNodesTestCase(GoogleKubernetesEngineTestCase):
  """Tests for GKE node/instance-group enumeration methods."""

  def testGetInstanceGroups(self):
    """Tests that _GetInstanceGroups parses node-pools list output."""
    path = os.path.join(os.path.dirname(__file__), _NODE_POOLS_LIST_OUTPUT)
    with open(path) as f:
      output = f.read()
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(stdout=output) as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      instance_groups = cluster._GetInstanceGroups()

      self.assertIn(
          'gcloud container node-pools list', issue_command.all_commands
      )
      self.assertIn('--cluster', issue_command.all_commands)

      expected = {
          'gke-pkb-0c47e6fa-default-pool-167d73ee-grp',
          'gke-pkb-0c47e6fa-test-efea7796-grp',
      }
      self.assertEqual(expected, set(instance_groups))  # order doesn't matter

  def testGetNodePoolNames(self):
    """Tests that GetNodePoolNames returns names from cluster describe."""
    pool_names = ['default-pool', 'nap-e2-standard-2-iu4vquho', 'test-pool']
    json_output = json.dumps(
        {'nodePools': [{'name': n} for n in pool_names]}
    )
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(stdout=json_output) as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      node_pools = cluster.GetNodePoolNames()

      self.assertIn(
          'gcloud container clusters describe ' + cluster.name,
          issue_command.all_commands,
      )
      self.assertIn('--format json', issue_command.all_commands)
      self.assertNotIn('--flatten', issue_command.all_commands)

      expected = {
          'default-pool',
          'nap-e2-standard-2-iu4vquho',
          'test-pool',
      }
      self.assertEqual(expected, set(node_pools))  # order doesn't matter


class GoogleKubernetesEngineRegionalTestCase(PatchedObjectsTestCase):
  """Tests for GKE regional cluster creation with multiple nodepools."""

  @staticmethod
  def create_kubernetes_engine_spec(use_zonal_nodepools=False):
    """Creates a regional GKE spec with two nodepools."""
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
    """Tests regional cluster creation with region-wide nodepools."""
    spec = self.create_kubernetes_engine_spec(use_zonal_nodepools=False)
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      create_cluster, _, create_nodepool1, create_nodepool2 = (
          call[0][0] for call in issue_command.func_to_mock.call_args_list
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
    """Tests regional cluster creation with zone-pinned nodepools."""
    spec = self.create_kubernetes_engine_spec(use_zonal_nodepools=True)
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      create_cluster, _, create_nodepool1, create_nodepool2 = (
          call[0][0] for call in issue_command.func_to_mock.call_args_list
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


class GoogleKubernetesEngineMachineFamiliesTestCase(PatchedObjectsTestCase):
  """Tests for GKE nodepool creation with machine-family constraints."""

  @staticmethod
  def create_kubernetes_engine_spec():
    """Creates a GKE spec with a nodepool using machine families."""
    kubernetes_engine_spec = container_spec.ContainerClusterSpec(
        'NAME',
        **{
            'cloud': 'GCP',
            'vm_spec': {
                'GCP': {
                    'machine_type': 'fake-machine-type',
                    'zone': 'us-central1-a',
                },
            },
            'nodepools': {
                'nodepool1': {
                    'vm_spec': {
                        'GCP': {
                            'machine_type': '',
                            'zone': 'us-central1-a',
                        },
                    },
                    'machine_families': ['n2'],
                },
            },
        },
    )
    return kubernetes_engine_spec

  def testCreateWithMachineFamilies(self):
    """Tests that machine-family nodepool issues a node-pools update command."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects() as issue_command, mock.patch.object(
        kubernetes_commands, 'ApplyYaml'
    ):
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      self.assertIn(
          'gcloud container node-pools update nodepool1',
          issue_command.all_commands,
      )
      self.assertIn(
          '--node-labels cloud.google.com/compute-class=nodepool1',
          issue_command.all_commands,
      )


class GoogleKubernetesEngineAutopilotTestCase(PatchedObjectsTestCase):
  """Tests for GKE Autopilot cluster creation and metadata."""

  @staticmethod
  def create_kubernetes_engine_spec():
    """Creates a GKE Autopilot cluster spec."""
    kubernetes_engine_spec = container_spec.ContainerClusterSpec(
        'NAME',
        **{
            'cloud': 'GCP',
            'type': 'Auto',
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
    """Tests Autopilot _Create uses create-auto without node flags."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeAutopilotCluster(spec)
      cluster._Create()
      self.assertIn(
          'gcloud container clusters create-auto', issue_command.all_commands
      )
      self.assertIn('--project fakeproject', issue_command.all_commands)
      self.assertIn('--region us-central1', issue_command.all_commands)
      self.assertIn('--format json', issue_command.all_commands)
      self.assertIn(
          '--labels foo=bar,timeout=yesterday', issue_command.all_commands
      )
      self.assertNotIn('--machine-type', issue_command.all_commands)
      self.assertNotIn('--num-nodes', issue_command.all_commands)

  def testGetResourceMetadata(self):
    """Tests that Autopilot metadata includes Auto values for size/type."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects():
      cluster = google_kubernetes_engine.GkeAutopilotCluster(spec)
      metadata = cluster.GetResourceMetadata()
      self.assertDictContainsSubset(
          {
              'project': 'fakeproject',
              'cloud': 'GCP',
              'cluster_type': 'Auto',
              'region': 'us-central1',
              'machine_type': 'Auto',
              'size': 'Auto',
              'nodepools': 'Auto',
          },
          metadata,
      )

  @flagsaver.flagsaver(gke_release_channel='rapid')
  def testGetResourceMetadataIncludesReleaseChannel(self):
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects():
      cluster = google_kubernetes_engine.GkeAutopilotCluster(spec)
      metadata = cluster.GetResourceMetadata()
      self.assertDictContainsSubset(
          {
              'release_channel': 'rapid',
          },
          metadata,
      )

  @flagsaver.flagsaver(gpu_type='h100', gpu_count=1, run_uri='123')
  def testApplyYamlGpusH100(self):
    """Tests Autopilot YAML generation for H100 GPU node selectors."""
    self.enter_context(
        mock.patch(
            gce_network.__name__ + '.GceFirewall.GetFirewall',
            return_value='fakefirewall',
        )
    )
    self.enter_context(
        mock.patch.object(
            vm_util,
            'GetTempDir',
            return_value=tempfile.gettempdir(),
        )
    )
    self.enter_context(
        mock.patch(
            gce_network.__name__ + '.GceNetwork.GetNetwork',
            return_value=gce_network.GceNetwork(
                gce_network.GceNetworkSpec('fakeproject', zone='us-central1-a')
            ),
        )
    )
    self.MockIssueCommand(
        {'apply -f': [('deployment.apps/test-deployment hello', '', 0)]}
    )
    self.enter_context(
        mock.patch.object(
            data,
            'ResourcePath',
            return_value=os.path.join(
                os.path.dirname(__file__),
                '..',
                '..',
                'data',
                'kube_apply.yaml.j2',
            ),
        )
    )
    spec = self.create_kubernetes_engine_spec()
    with self.assertLogs(level='INFO') as logs:
      cluster = google_kubernetes_engine.GkeAutopilotCluster(spec)
      yamls = kubernetes_commands.ConvertManifestToYamlDicts(
          'tests/data/kube_apply.yaml.j2',
          name='hello-world',
          command=[],
      )
      cluster.ModifyPodSpecPlacementYaml(
          yamls,
          'hello-world',
      )
      kubernetes_commands.ApplyYaml(
          yamls,
          should_log_file=True,
      )
    full_logs = ';'.join(logs.output)
    self.assertIn("cloud.google.com/gke-accelerator-count: '1'", full_logs)
    self.assertIn(
        'cloud.google.com/gke-accelerator: nvidia-h100-80gb', full_logs
    )
    self.assertIn('cloud.google.com/gke-gpu-driver-version: default', full_logs)
    self.assertIn('cloud.google.com/compute-class: Accelerator', full_logs)

  def testGetMachineTypeFromNodeName(self):
    """Tests GetMachineTypeFromNodeName queries kubectl for node type."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects():
      cluster = google_kubernetes_engine.GkeAutopilotCluster(spec)
    self.MockIssueCommand(
        {'get node': [('ek-standard-16', '', 0)]}
    )
    self.assertEqual(
        cluster.GetMachineTypeFromNodeName(
            'gke-pkb-cluster-default-pool-node-1'
        ),
        'ek-standard-16',
    )


class GoogleKubernetesEngineNodepoolAutoscalingTestCase(PatchedObjectsTestCase):
  """Tests GKE per-nodepool autoscaling overrides cluster-level settings."""

  def testCreateWithPerNodepoolAutoscaling(self):
    """Tests per-nodepool autoscaling settings override cluster defaults."""
    kubernetes_engine_spec = container_spec.ContainerClusterSpec(
        'NAME',
        **{
            'cloud': 'GCP',
            'vm_spec': {
                'GCP': {
                    'machine_type': 'fake-machine-type',
                    'zone': 'us-central1-a',
                },
            },
            'vm_count': 2,
            'min_vm_count': 1,
            'max_vm_count': 5,
            'nodepools': {
                'nodepool1': {
                    'vm_spec': {
                        'GCP': {
                            'machine_type': 'machine-type-1',
                        },
                    },
                    'vm_count': 3,
                    'min_vm_count': 2,
                    'max_vm_count': 10,
                },
            },
            'poll_for_events': False,
        },
    )
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(kubernetes_engine_spec)
      cluster._Create()

      create_cluster_cmd = issue_command.GetCommandWithSubstring(
          'gcloud container clusters create'
      )
      self.assertIn('--enable-autoscaling', create_cluster_cmd)
      self.assertIn('--min-nodes 1', create_cluster_cmd)
      self.assertIn('--max-nodes 5', create_cluster_cmd)
      self.assertIn('--num-nodes 2', create_cluster_cmd)

      nodepool_cmd = issue_command.GetCommandWithSubstring(
          'node-pools create nodepool1'
      )
      self.assertIn('--enable-autoscaling', nodepool_cmd)
      self.assertIn('--min-nodes 2', nodepool_cmd)
      self.assertIn('--max-nodes 10', nodepool_cmd)


class GkeManagementPlaneTestCase(PatchedObjectsTestCase):
  """Tests for GKE management-plane methods (k8s_management_benchmark)."""

  @staticmethod
  def create_kubernetes_engine_spec():
    """Creates a GKE spec for management-plane method tests."""
    return container_spec.ContainerClusterSpec(
        'NAME',
        **{
            'cloud': 'GCP',
            'vm_spec': {
                'GCP': {
                    'machine_type': 'fake-machine-type',
                    'zone': 'us-central1-a',
                },
            },
            'vm_count': 2,
            'poll_for_events': False,
        },
    )

  def _make_nodepool_config(self, name='pkbpool0'):
    """Returns a minimal BaseNodePoolConfig-like object."""
    cfg = mock.MagicMock()
    cfg.name = name
    cfg.num_nodes = 1
    cfg.machine_type = 'n1-standard-2'
    cfg.disk_size = 100
    cfg.max_local_disks = 0
    cfg.zone = None
    return cfg

  # ---- GetNodePoolNames (JSON format) ---------------------------------------

  def testGetNodePoolNamesJsonFormat(self):
    """GetNodePoolNames parses JSON cluster describe output."""
    cluster_json = (
        '{"nodePools": [{"name": "default-pool"}, {"name": "extra-pool"}]}'
    )
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(stdout=cluster_json) as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      names = cluster.GetNodePoolNames()

      self.assertIn(
          'gcloud container clusters describe ' + cluster.name,
          issue_command.all_commands,
      )
      self.assertIn('--format', issue_command.all_commands)
      # Must NOT use --flatten (old format)
      self.assertNotIn('--flatten', issue_command.all_commands)
      self.assertEqual({'default-pool', 'extra-pool'}, set(names))

  def testGetNodePoolNamesEmptyFallback(self):
    """GetNodePoolNames falls back to split() on non-JSON output."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(stdout='pool-a pool-b'):
      cluster = google_kubernetes_engine.GkeCluster(spec)
      names = cluster.GetNodePoolNames()
      self.assertEqual({'pool-a', 'pool-b'}, set(names))

  # ---- CreateNodePool -------------------------------------------------------

  def testCreateNodePool(self):
    """CreateNodePool issues gcloud node-pools create with cluster flag."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cfg = self._make_nodepool_config('mypool')
      cluster.CreateNodePool(cfg)

      cmd = issue_command.GetCommandWithSubstring('node-pools create mypool')
      self.assertIn('--cluster', cmd)
      self.assertNotIn('--node-version', cmd)

  def testCreateNodePoolWithVersion(self):
    """CreateNodePool passes --node-version when provided."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cfg = self._make_nodepool_config('mypool')
      cluster.CreateNodePool(cfg, node_version='1.34.1-gke.100')

      cmd = issue_command.GetCommandWithSubstring('node-pools create mypool')
      self.assertIn('--node-version 1.34.1-gke.100', cmd)

  # ---- DeleteNodePool -------------------------------------------------------

  def testDeleteNodePool(self):
    """DeleteNodePool issues gcloud node-pools delete with --quiet."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster.DeleteNodePool('old-pool')

      cmd = issue_command.GetCommandWithSubstring('node-pools delete old-pool')
      self.assertIn('--cluster', cmd)
      self.assertIn('--quiet', cmd)

  # ---- UpgradeNodePool ------------------------------------------------------

  def testUpgradeNodePool(self):
    """UpgradeNodePool issues gcloud clusters upgrade with --node-pool."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster.UpgradeNodePool('my-pool', '1.34.1-gke.200')

      cmd = issue_command.GetCommandWithSubstring('clusters upgrade')
      self.assertIn('--node-pool my-pool', cmd)
      self.assertIn('--cluster-version 1.34.1-gke.200', cmd)
      self.assertIn('--quiet', cmd)

  # ---- UpdateCluster --------------------------------------------------------

  def testUpdateCluster(self):
    """UpdateCluster issues gcloud clusters update with a timestamp label."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster.UpdateCluster()

      cmd = issue_command.GetCommandWithSubstring('clusters update')
      self.assertIn('--update-labels', cmd)
      self.assertIn('k8s-mgmt-ts=', cmd)

  # ---- Async variants -------------------------------------------------------

  def testCreateNodePoolAsyncReturnsOpName(self):
    """CreateNodePoolAsync returns the GKE operation name."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(
        stdout='extra line\noperation-1234\n'
    ) as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cfg = self._make_nodepool_config('asyncpool')
      handle = cluster.CreateNodePoolAsync(cfg)

      cmd = issue_command.GetCommandWithSubstring('node-pools create asyncpool')
      self.assertIn('--async', cmd)
      self.assertNotIn('--timeout', cmd)
      self.assertEqual('operation-1234', handle)

  def testCreateNodePoolAsyncWithVersion(self):
    """CreateNodePoolAsync passes --node-version when provided."""
    spec = self.create_kubernetes_engine_spec()
    stdout = 'operation-5678\n'
    with self.patch_critical_objects(stdout=stdout) as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cfg = self._make_nodepool_config('verpool')
      cluster.CreateNodePoolAsync(cfg, node_version='1.33.5-gke.1')

      cmd = issue_command.GetCommandWithSubstring('node-pools create verpool')
      self.assertIn('--node-version 1.33.5-gke.1', cmd)

  def testDeleteNodePoolAsyncReturnsOpName(self):
    """DeleteNodePoolAsync issues delete with --async."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(stdout='operation-del\n') as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      handle = cluster.DeleteNodePoolAsync('to-delete')

      cmd = issue_command.GetCommandWithSubstring('node-pools delete to-delete')
      self.assertIn('--async', cmd)
      self.assertIn('--quiet', cmd)
      self.assertEqual('operation-del', handle)

  def testUpgradeNodePoolAsyncReturnsOpName(self):
    """UpgradeNodePoolAsync issues upgrade with --async."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(stdout='operation-upg\n') as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      handle = cluster.UpgradeNodePoolAsync('my-pool', '1.34.2-gke.100')

      cmd = issue_command.GetCommandWithSubstring('clusters upgrade')
      self.assertIn('--async', cmd)
      self.assertIn('--node-pool my-pool', cmd)
      self.assertIn('--cluster-version 1.34.2-gke.100', cmd)
      self.assertEqual('operation-upg', handle)

  def testUpdateClusterAsyncReturnsOpName(self):
    """UpdateClusterAsync issues clusters update with --async."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(stdout='operation-upd\n') as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      handle = cluster.UpdateClusterAsync()

      cmd = issue_command.GetCommandWithSubstring('clusters update')
      self.assertIn('--async', cmd)
      self.assertIn('k8s-mgmt-ts=', cmd)
      self.assertEqual('operation-upd', handle)

  def testIssueAsyncRaisesOnNonZeroRetcode(self):
    """_IssueAsync raises CreationError when the command fails."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(stderr='boom', return_code=1):
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cfg = self._make_nodepool_config('failpool')
      with self.assertRaises(Exception):
        cluster.CreateNodePoolAsync(cfg)

  def testIssueAsyncRaisesOnEmptyOpName(self):
    """_IssueAsync raises CreationError when stdout produces no op name."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(stdout='   \n   '):
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cfg = self._make_nodepool_config('emptypool')
      with self.assertRaises(Exception):
        cluster.CreateNodePoolAsync(cfg)

  # ---- WaitForOperation -----------------------------------------------------

  def testWaitForOperationDone(self):
    """WaitForOperation returns immediately when status is DONE."""
    spec = self.create_kubernetes_engine_spec()
    done_json = '{"status": "DONE"}'
    with self.patch_critical_objects(stdout=done_json):
      cluster = google_kubernetes_engine.GkeCluster(spec)
      # Should not raise
      cluster.WaitForOperation('operation-xyz')

  def testWaitForOperationAbortingRaises(self):
    """WaitForOperation raises CreationError when status is ABORTING."""
    spec = self.create_kubernetes_engine_spec()
    aborted_json = '{"status": "ABORTING"}'
    with self.patch_critical_objects(stdout=aborted_json):
      cluster = google_kubernetes_engine.GkeCluster(spec)
      with self.assertRaises(errors.Resource.CreationError):
        cluster.WaitForOperation('operation-bad')

  # ---- ResolveNodePoolVersions ----------------------------------------------

  def testResolveNodePoolVersions(self):
    """ResolveNodePoolVersions returns (N-1 qualified, N qualified)."""
    server_config = {
        'validNodeVersions': [
            '1.34.5-gke.100',
            '1.34.3-gke.50',
            '1.33.8-gke.200',
            '1.33.5-gke.99',
            '1.32.1-gke.10',
        ]
    }
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(
        stdout=json.dumps(server_config)
    ) as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      initial, target = cluster.ResolveNodePoolVersions()

      cmd = issue_command.GetCommandWithSubstring('get-server-config')
      self.assertIn('--format', cmd)
      # target = newest overall = 1.34.5-gke.100
      self.assertEqual('1.34.5-gke.100', target)
      # initial = best version for minor 33 = 1.33.8-gke.200
      self.assertEqual('1.33.8-gke.200', initial)

  def testResolveNodePoolVersionsNoVersionsRaises(self):
    """ResolveNodePoolVersions raises GetError when versions list is empty."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(stdout='{"validNodeVersions": []}'):
      cluster = google_kubernetes_engine.GkeCluster(spec)
      with self.assertRaises(errors.Resource.GetError):
        cluster.ResolveNodePoolVersions()

  # ---- HasActiveUpgradeOperations -------------------------------------------

  def testHasActiveUpgradeOperationsTrue(self):
    """HasActiveUpgradeOperations returns True when an upgrade is running."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(stdout='operation-upgrade-123\n'):
      cluster = google_kubernetes_engine.GkeCluster(spec)
      self.assertTrue(cluster.HasActiveUpgradeOperations())

  def testHasActiveUpgradeOperationsFalse(self):
    """HasActiveUpgradeOperations returns False when no upgrade is running."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(stdout=''):
      cluster = google_kubernetes_engine.GkeCluster(spec)
      self.assertFalse(cluster.HasActiveUpgradeOperations())

  def testHasActiveUpgradeOperationsUsesCorrectFilter(self):
    """HasActiveUpgradeOperations queries for UPGRADE_NODES AND RUNNING."""
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(stdout='') as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster.HasActiveUpgradeOperations()

      self.assertIn('operations list', issue_command.all_commands)
      self.assertIn('UPGRADE_NODES', issue_command.all_commands)
      self.assertIn('RUNNING', issue_command.all_commands)


if __name__ == '__main__':
  unittest.main()
