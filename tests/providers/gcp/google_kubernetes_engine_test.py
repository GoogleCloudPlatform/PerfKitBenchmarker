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
import contextlib
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

  class FakeContainerImage(container.ContainerImage):

    def __init__(self, name, directory=None):
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
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects() as issue_command, mock.patch.object(
        kubectl, 'RunKubectlCommand'
    ) as mock_kubectl_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Create()
      cluster._PostCreate()
      self.assertIn(
          'gcloud container clusters get-credentials pkb-{}'.format(_RUN_URI),
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
          'gcloud container clusters delete pkb-{}'.format(_RUN_URI),
          issue_command.all_commands,
      )
      self.assertIn('--zone us-central1-a', issue_command.all_commands)

  def testExists(self):
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects() as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      cluster._Exists()
      self.assertIn(
          'gcloud container clusters describe pkb-{}'.format(_RUN_URI),
          issue_command.all_commands,
      )

  def testGetResourceMetadata(self):
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

  @staticmethod
  def create_kubernetes_engine_spec(gpu_type):
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
          'type=nvidia-h100-80gb,count=2,gpu-driver-version=default',
          issue_command.all_commands,
      )


class GoogleKubernetesEngineGetNodesTestCase(GoogleKubernetesEngineTestCase):

  def testGetInstanceGroups(self):
    path = os.path.join(os.path.dirname(__file__), _NODE_POOLS_LIST_OUTPUT)
    output = open(path).read()
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
    output = ['default-pool', 'nap-e2-standard-2-iu4vquho', 'test-pool']
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects(stdout='\n'.join(output)) as issue_command:
      cluster = google_kubernetes_engine.GkeCluster(spec)
      node_pools = cluster.GetNodePoolNames()

      self.assertIn(
          'gcloud container clusters describe ' + cluster.name,
          issue_command.all_commands,
      )
      self.assertIn('--flatten', issue_command.all_commands)
      self.assertIn('--format', issue_command.all_commands)

      expected = {
          'default-pool',
          'nap-e2-standard-2-iu4vquho',
          'test-pool',
      }
      self.assertEqual(expected, set(node_pools))  # order doesn't matter


class GoogleKubernetesEngineRegionalTestCase(PatchedObjectsTestCase):

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
    spec = self.create_kubernetes_engine_spec()
    with self.patch_critical_objects():
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


class GoogleKubernetesEngineNodepoolAutoscalingTestCase(PatchedObjectsTestCase):

  def testCreateWithPerNodepoolAutoscaling(self):
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


if __name__ == '__main__':
  unittest.main()
