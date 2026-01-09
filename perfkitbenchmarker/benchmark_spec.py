# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Container for all data required for a benchmark to run."""

import contextlib
import copy
import datetime
import importlib
import logging
import os
import pickle
import threading
import time
from typing import Any
import uuid

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_status
from perfkitbenchmarker import capacity_reservation
from perfkitbenchmarker import cloud_tpu
from perfkitbenchmarker import cluster
from perfkitbenchmarker import container_service
from perfkitbenchmarker import context
from perfkitbenchmarker import data_discovery_service
from perfkitbenchmarker import disk
from perfkitbenchmarker import dpb_constants
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import edw_compute_resource
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import flags as pkb_flags
from perfkitbenchmarker import key as cloud_key
from perfkitbenchmarker import lustre_service
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker import messaging_service
from perfkitbenchmarker import nfs_service
from perfkitbenchmarker import non_relational_db
from perfkitbenchmarker import os_types
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import providers
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import resource as resource_type
from perfkitbenchmarker import smb_service
from perfkitbenchmarker import stages
from perfkitbenchmarker import static_virtual_machine as static_vm
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import vpn_service
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.configs import freeze_restore_spec
from perfkitbenchmarker.resources import base_job
from perfkitbenchmarker.resources import example_resource
from perfkitbenchmarker.resources import managed_ai_model
from perfkitbenchmarker.resources.pinecone import pinecone as pinecone_resource
from perfkitbenchmarker.resources.vertex_vector_search import vvs as vvs_resource  # pylint: disable=line-too-long
import six
import six.moves._thread
import six.moves.copyreg


def PickleLock(lock: threading.Lock):
  return UnPickleLock, (lock.locked(),)


def UnPickleLock(locked: bool, *args):
  del args
  lock = threading.Lock()
  if locked:
    if not lock.acquire(False):
      raise pickle.UnpicklingError('Cannot acquire lock')
  return lock


six.moves.copyreg.pickle(six.moves._thread.LockType, PickleLock)

SUPPORTED = 'strict'
NOT_EXCLUDED = 'permissive'
SKIP_CHECK = 'none'
FLAGS = flags.FLAGS

flags.DEFINE_enum(
    'cloud',
    provider_info.GCP,
    provider_info.VALID_CLOUDS,
    'Name of the cloud to use.',
)
flags.DEFINE_string(
    'scratch_dir',
    None,
    'Base name for all scratch disk directories in the VM. '
    'Upon creation, these directories will have numbers '
    'appended to them (for example /scratch0, /scratch1, etc).',
)
flags.DEFINE_string(
    'startup_script', None, 'Script to run right after vm boot.'
)
flags.DEFINE_string(
    'postrun_script', None, 'Script to run right after run stage.'
)
flags.DEFINE_integer(
    'create_and_boot_post_task_delay',
    None,
    'Delay in seconds to delay in between boot tasks.',
)
_ENFORCE_DISK_MOUNT_POINT_OVERRIDE = flags.DEFINE_bool(
    'enforce_disk_mount_point_override',
    False,
    'Enforce the use of the --scratch_dir flag to override the default'
    ' mount_point in the disk spec.',
)
# pyformat: disable
# TODO(user): Delete this flag after fulling updating gcl.
flags.DEFINE_enum('benchmark_compatibility_checking', SUPPORTED,
                  [SUPPORTED, NOT_EXCLUDED, SKIP_CHECK],
                  'Method used to check compatibility between the benchmark '
                  ' and the cloud.  ' + SUPPORTED + ' runs the benchmark only'
                  ' if the cloud provider has declared it supported. ' +
                  NOT_EXCLUDED + ' runs the benchmark unless it has been'
                  ' declared not supported by the cloud provider. ' + SKIP_CHECK
                  + ' does not do the compatibility'
                  ' check.')
# pyformat: enable


class BenchmarkSpec:
  """Contains the various data required to make a benchmark run."""

  total_benchmarks = 0

  def __init__(
      self,
      benchmark_module: Any,
      benchmark_config: benchmark_config_spec.BenchmarkConfigSpec,
      benchmark_uid: str,
  ):
    """Initialize a BenchmarkSpec object.

    Args:
      benchmark_module: The benchmark module object.
      benchmark_config: BenchmarkConfigSpec. The configuration for the
        benchmark.
      benchmark_uid: An identifier unique to this run of the benchmark even if
        the same benchmark is run multiple times with different configs.
    """
    self.config = benchmark_config
    self.name = benchmark_module.BENCHMARK_NAME
    self.uid = benchmark_uid
    self.status = benchmark_status.SKIPPED
    self.failed_substatus = None
    self.status_detail = None
    BenchmarkSpec.total_benchmarks += 1
    self.sequence_number = BenchmarkSpec.total_benchmarks
    self.resources: list[resource_type.BaseResource] = []
    self.vms = []
    self.regional_networks = {}
    self.networks = {}
    self.custom_subnets = {
        k: {'cloud': v.cloud, 'cidr': v.cidr}
        for (k, v) in self.config.vm_groups.items()
    }
    self.firewalls = {}
    self.networks_lock = threading.Lock()
    self.firewalls_lock = threading.Lock()
    self.vm_groups = {}
    self.container_specs = benchmark_config.container_specs or {}
    self.container_registry = None
    self.deleted = False
    self.uuid = '%s-%s' % (FLAGS.run_uri, uuid.uuid4())
    self.always_call_cleanup = pkb_flags.ALWAYS_CALL_CLEANUP.value
    self.dpb_service: dpb_service.BaseDpbService = None
    self.container_cluster: container_service.BaseContainerCluster = None
    self.cluster: cluster.BaseCluster = None
    self.key = None
    self.relational_db = None
    self.non_relational_db = None
    self.tpus = []
    self.tpu_groups = {}
    self.base_job = None
    self.edw_service = None
    self.edw_compute_resource = None
    self.example_resource = None
    self.multi_attach_disk = None
    self.nfs_service = None
    self.lustre_service = None
    self.smb_service = None
    self.messaging_service = None
    self.ai_model = None
    self.pinecone = None
    self.vvs = None
    self.memory_store = None
    self.data_discovery_service = None
    self.app_groups = {}
    self._zone_index = 0
    self.capacity_reservations = []
    self.placement_group_specs = benchmark_config.placement_group_specs or {}
    self.placement_groups = {}
    # Benchmark configs for relational_db and vm_groups can both be included.
    # This is currently used for combo benchmarks.
    self.vms_to_boot = {}
    if self.config.relational_db:
      self.vms_to_boot.update(
          relational_db.VmsToBoot(self.config.relational_db.vm_groups)
      )
    if self.config.vm_groups:
      self.vms_to_boot.update(self.config.vm_groups)
    self.vpc_peering = self.config.vpc_peering

    self.vpn_service = None
    self.vpns = {}  # dict of vpn's
    self.vpn_gateways = {}  # dict of vpn gw's
    self.vpn_gateways_lock = threading.Lock()
    self.vpns_lock = threading.Lock()

    self.restore_spec = None
    self.freeze_path = None

    # Used by huggingface_bert_pretraining
    self.path: str
    self.run_dir: str
    self.data_dir: str
    self.ckpt_dir: str
    # Used by redis_memtier and keydb_memtier
    self.redis_endpoint_ip: str
    self.keydb_endpoint_ip: str
    # Used by mongodb_ycsb
    self.mongodb_url: str
    # Used by dino_benchmark
    self.imagenet_dir: str
    # Used by mlperf_inference_cpu_benchmark
    self.cm: str

    # Modules can't be pickled, but functions can, so we store the functions
    # necessary to run the benchmark.
    self.BenchmarkPrepare = getattr(benchmark_module, 'Prepare', None)
    self.BenchmarkPrepareSystem = getattr(
        benchmark_module, 'PrepareSystem', None
    )
    self.BenchmarkInstallPackages = getattr(
        benchmark_module, 'InstallPackages', None
    )
    self.BenchmarkStartServices = getattr(
        benchmark_module, 'StartServices', None
    )
    self.BenchmarkRun = benchmark_module.Run
    self.BenchmarkCleanup = benchmark_module.Cleanup
    # Set the current thread's BenchmarkSpec object to this one.
    context.SetThreadBenchmarkSpec(self)

  def __repr__(self):
    return '%s(%r)' % (self.__class__, self.__dict__)

  def __str__(self):
    return 'Benchmark name: {}\nFlags: {}'.format(self.name, self.config.flags)

  @contextlib.contextmanager
  def RedirectGlobalFlags(self):
    """Redirects flag reads and writes to the benchmark-specific flags object.

    Within the enclosed code block, reads and writes to the flags.FLAGS object
    are redirected to a copy that has been merged with config-provided flag
    overrides specific to this benchmark run.
    """
    with self.config.RedirectFlags(FLAGS):
      yield

  def _InitializeFromSpec(
      self,
      attribute_name: str,
      resource_spec: freeze_restore_spec.FreezeRestoreSpec,
  ) -> bool:
    """Initializes the BenchmarkSpec attribute from the restore_spec.

    Args:
      attribute_name: The attribute to restore.
      resource_spec: The spec class corresponding to the resource to be
        restored.

    Returns:
      True if successful, False otherwise.
    """
    if not hasattr(self, 'restore_spec'):
      return False
    if not self.restore_spec or not hasattr(self.restore_spec, attribute_name):
      return False
    if not resource_spec.enable_freeze_restore:
      return False
    logging.info('Getting %s instance from restore_spec', attribute_name)
    frozen_resource = copy.copy(getattr(self.restore_spec, attribute_name))
    setattr(self, attribute_name, frozen_resource)
    return True

  def ConstructResources(self):
    """Constructs the resources for the benchmark."""
    # Put registry first, as it can be needed by cluster.
    self.ConstructContainerRegistry()
    self.ConstructContainerCluster()
    # dpb service needs to go first, because it adds some vms.
    self.ConstructDpbService()
    self.ConstructCluster()
    self.ConstructVirtualMachines()
    self.ConstructRelationalDb()
    self.ConstructNonRelationalDb()
    self.ConstructKey()
    self.ConstructMessagingService()
    self.ConstructManagedAiModel()
    # CapacityReservations need to be constructed after VirtualMachines because
    # it needs information about the VMs (machine type, count, zone, etc). The
    # CapacityReservations will be provisioned before VMs.
    self.ConstructCapacityReservations()
    self.ConstructTpu()
    self.ConstructEdwService()
    self.ConstructEdwComputeResource()
    self.ConstructExampleResource()
    self.ConstructBaseJob()
    self.ConstructVPNService()
    self.ConstructNfsService()
    self.ConstructLustreService()
    self.ConstructSmbService()
    self.ConstructDataDiscoveryService()
    self.ConstructBaseJob()
    self.ConstructMemoryStore()
    self.ConstructPinecone()
    self.ConstructVertexVectorSearch()
    self.ConstructMultiAttachDisk()

  def ConstructContainerCluster(self):
    """Create the container cluster."""
    if self.config.container_cluster is None:
      return

    cloud = self.config.container_cluster.cloud
    cluster_type = self.config.container_cluster.type
    providers.LoadProvider(cloud)
    container_cluster_class = container_service.GetContainerClusterClass(
        cloud, cluster_type
    )
    self.container_cluster = container_cluster_class(
        self.config.container_cluster
    )

    if self.container_registry:
      self.container_cluster.SetContainerRegistry(self.container_registry)

    self.resources.append(self.container_cluster)

  def ConstructCluster(self):
    """Create the cluster."""
    if self.config.cluster is None:
      return
    cloud = self.config.cluster.cloud
    providers.LoadProvider(cloud)
    cluster_class = cluster.GetClusterClass(cloud)
    self.cluster = cluster_class(self.config.cluster)
    self.resources.append(self.cluster)
    self.vms_to_boot.update(self.cluster.ExportVmGroupsForUnmanagedProvision())

  def ConstructContainerRegistry(self):
    """Create the container registry."""
    if self.config.container_registry is None:
      return
    cloud = self.config.container_registry.cloud
    providers.LoadProvider(cloud)
    container_registry_class = container_service.GetContainerRegistryClass(
        cloud
    )
    self.container_registry = container_registry_class(
        self.config.container_registry
    )
    self.resources.append(self.container_registry)

  def ConstructDpbService(self):
    """Create the dpb_service object and create groups for its vms."""
    if self.config.dpb_service is None:
      return
    dpb_service_spec = self.config.dpb_service
    dpb_service_cloud = dpb_service_spec.worker_group.cloud
    dpb_service_spec.worker_group.vm_count = dpb_service_spec.worker_count
    providers.LoadProvider(dpb_service_cloud)

    dpb_service_type = dpb_service_spec.service_type
    dpb_service_class = dpb_service.GetDpbServiceClass(
        dpb_service_cloud, dpb_service_type
    )
    self.dpb_service = dpb_service_class(dpb_service_spec)

    # If the dpb service is un-managed, the provisioning needs to be handed
    # over to the vm creation module.
    if dpb_service_type in [
        dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER,
        dpb_constants.UNMANAGED_SPARK_CLUSTER,
    ]:
      # Ensure non cluster vms are not present in the spec.
      if self.vms_to_boot:
        raise errors.Benchmarks.UnsupportedConfigError(
            'Invalid Non cluster vm group {} when benchmarking '
            'unmanaged dpb service'.format(self.vms_to_boot)
        )

      base_vm_spec = dpb_service_spec.worker_group
      base_vm_spec.vm_spec.zone = self.dpb_service.dpb_service_zone

      if dpb_service_spec.worker_count:
        self.vms_to_boot['worker_group'] = dpb_service_spec.worker_group
      # else we have a single node cluster.

      master_group_spec = copy.copy(base_vm_spec)
      master_group_spec.vm_count = 1
      master_group_spec.disk_spec = None
      self.vms_to_boot['master_group'] = master_group_spec
    self.resources.append(self.dpb_service)

  def ConstructRelationalDb(self):
    """Creates the relational db and create groups for its vms."""
    if self.config.relational_db is None:
      return
    if self._InitializeFromSpec('relational_db', self.config.relational_db):
      return
    cloud = self.config.relational_db.cloud
    is_managed_db = self.config.relational_db.is_managed_db
    engine = self.config.relational_db.engine
    providers.LoadProvider(cloud)
    relational_db_class = relational_db.GetRelationalDbClass(
        cloud, is_managed_db, engine
    )
    if not self.config.relational_db.engine_version:
      self.config.relational_db.engine_version = (
          relational_db_class.GetDefaultEngineVersion(engine)
      )
    self.relational_db = relational_db_class(self.config.relational_db)
    self.resources.append(self.relational_db)

  def ConstructNonRelationalDb(self) -> None:
    """Initializes the non_relational db."""
    db_spec: non_relational_db.BaseNonRelationalDbSpec = (
        self.config.non_relational_db
    )
    if not db_spec:
      return
    # Initialization from restore spec
    if self._InitializeFromSpec('non_relational_db', db_spec):
      return
    # Initialization from benchmark config spec
    logging.info(
        'Constructing non_relational_db instance with spec: %s.', db_spec
    )
    service_type = db_spec.service_type
    non_relational_db_class = non_relational_db.GetNonRelationalDbClass(
        service_type
    )
    self.non_relational_db = non_relational_db_class.FromSpec(db_spec)

  def ConstructKey(self) -> None:
    """Initializes the cryptographic key."""
    key_spec: cloud_key.BaseKeySpec = self.config.key
    if not key_spec:
      return
    logging.info('Constructing key with spec: %s.', key_spec)
    key_class = cloud_key.GetKeyClass(key_spec.cloud)
    self.key = key_class(key_spec)
    self.resources.append(self.key)

  def ConstructTpuGroup(self, group_spec):
    """Constructs the BenchmarkSpec's cloud TPU objects."""
    if group_spec is None:
      return
    cloud = group_spec.cloud
    providers.LoadProvider(cloud)
    tpu_class = cloud_tpu.GetTpuClass(cloud)
    return tpu_class(group_spec)  # pytype: disable=not-instantiable

  def ConstructTpu(self):
    """Constructs the BenchmarkSpec's cloud TPU objects."""
    tpu_group_specs = self.config.tpu_groups

    for group_name, group_spec in sorted(tpu_group_specs.items()):
      tpu = self.ConstructTpuGroup(group_spec)

      self.tpu_groups[group_name] = tpu
      self.tpus.append(tpu)

  def ConstructEdwService(self):
    """Create the edw_service object."""
    if self.config.edw_service is None:
      return
    # Load necessary modules from the provider to account for dependencies
    # TODO(saksena): Replace with
    # providers.LoadProvider(string.lower(FLAGS.cloud))
    providers.LoadProvider(
        edw_service.TYPE_2_PROVIDER.get(self.config.edw_service.type)
    )
    # Load the module for the edw service based on type
    edw_service_type = self.config.edw_service.type
    edw_service_module = importlib.import_module(
        edw_service.TYPE_2_MODULE.get(edw_service_type)
    )
    # The edw_service_type in certain cases may be qualified with a hosting
    # cloud eg. snowflake_aws,snowflake_gcp, etc.
    # However the edw_service_class_name in all cases will still be cloud
    # agnostic eg. Snowflake.
    edw_service_class_name = edw_service_type.split('_')[0]
    edw_service_class = getattr(
        edw_service_module,
        edw_service_class_name[0].upper() + edw_service_class_name[1:],
    )
    # Check if a new instance needs to be created or restored from snapshot
    self.edw_service = edw_service_class(
        self.config.edw_service
    )  # pytype: disable=not-instantiable
    if self.container_cluster:
      assert isinstance(
          self.container_cluster, container_service.KubernetesCluster
      )
      self.edw_service.SetContainerCluster(self.container_cluster)

  def ConstructEdwComputeResource(self):
    """Create an edw_compute_resource object."""
    if self.config.edw_compute_resource is None:
      return
    edw_compute_resource_cloud = self.config.edw_compute_resource.cloud
    edw_compute_resource_type = self.config.edw_compute_resource.type
    providers.LoadProvider(edw_compute_resource_cloud)
    edw_compute_resource_class = (
        edw_compute_resource.GetEdwComputeResourceClass(
            edw_compute_resource_cloud, edw_compute_resource_type
        )
    )
    self.edw_compute_resource = edw_compute_resource_class(
        self.config.edw_compute_resource
    )  # pytype: disable=not-instantiable
    self.resources.append(self.edw_compute_resource)

  def ConstructExampleResource(self):
    """Create an example_resource object."""
    if self.config.example_resource is None:
      return
    example_resource_type = self.config.example_resource.example_type
    example_resource_class = example_resource.GetExampleResourceClass(
        example_resource_type
    )
    self.example_resource = example_resource_class(
        self.config.example_resource
    )  # pytype: disable=not-instantiable
    self.resources.append(self.example_resource)

  def ConstructBaseJob(self):
    """Create an instance of the base job.It is also called from pkb.py."""
    if self.config.base_job is None:
      return
    job_type = self.config.base_job.job_type
    cloud = self.config.base_job.CLOUD
    providers.LoadProvider(cloud)
    job_class = base_job.GetJobClass(job_type)

    self.base_job = job_class(
        self.config.base_job, self.container_registry
    )  # pytype: disable=not-instantiable
    self.resources.append(self.base_job)

  def ConstructManagedAiModel(self):
    """Create an example_resource object."""
    if self.config.ai_model is None:
      return
    cloud = self.config.ai_model.cloud
    providers.LoadProvider(cloud)
    model_class = managed_ai_model.GetManagedAiModelClass(
        cloud, self.config.ai_model.interface
    )
    assert self.vm_groups
    vm = self.vm_groups[
        'clients' if 'clients' in self.vm_groups else 'default'
    ][0]
    self.ai_model = model_class(
        vm, self.config.ai_model
    )  # pytype: disable=not-instantiable
    self.resources.append(self.ai_model)

  def ConstructPinecone(self):
    """Create an example_resource object."""
    if self.config.pinecone is None:
      return
    cloud = self.config.pinecone.cloud
    providers.LoadProvider(cloud)
    model_class = pinecone_resource.GetPineconeResourceClass(cloud)
    self.pinecone = model_class(
        self.config.pinecone
    )  # pytype: disable=not-instantiable
    self.pinecone.SetVms(self.vm_groups)
    self.resources.append(self.pinecone)

  def ConstructVertexVectorSearch(self):
    """Construct the Vertex Vector Search instance."""
    if self.config.vvs is None:
      return
    cloud = self.config.vvs.cloud
    providers.LoadProvider(cloud)
    model_class = vvs_resource.GetVVSResourceClass(cloud)
    self.vvs = model_class(self.config.vvs)  # pytype: disable=not-instantiable
    self.vvs.SetVms(self.vm_groups)
    self.resources.append(self.vvs)

  def ConstructMemoryStore(self):
    """Create the memory store instance."""
    if self.config.memory_store is None:
      return
    cloud = self.config.memory_store.cloud
    providers.LoadProvider(cloud)
    managed_memory_store_class = (
        managed_memory_store.GetManagedMemoryStoreClass(
            cloud,
            self.config.memory_store.service_type,
            self.config.memory_store.memory_store_type,
        )
    )
    self.memory_store = managed_memory_store_class(
        self.config.memory_store
    )  # pytype: disable=not-instantiable
    self.memory_store.SetVms(self.vm_groups)
    self.resources.append(self.memory_store)

  def ConstructNfsService(self):
    """Construct the NFS service object.

    Creates an NFS Service only if an NFS disk is found in the disk_specs.
    """
    if self.nfs_service:
      logging.info('NFS service already created: %s', self.nfs_service)
      return
    for group_spec in self.vms_to_boot.values():
      if not group_spec.disk_spec or not group_spec.vm_count:
        continue
      disk_spec = group_spec.disk_spec
      if disk_spec.disk_type != disk.NFS:
        continue
      # Choose which nfs_service to create.
      if disk_spec.nfs_ip_address:
        self.nfs_service = nfs_service.StaticNfsService(disk_spec)
      elif disk_spec.nfs_managed:
        cloud = group_spec.cloud
        providers.LoadProvider(cloud)
        nfs_class = nfs_service.GetNfsServiceClass(cloud)
        self.nfs_service = nfs_class(
            disk_spec, group_spec.vm_spec.zone
        )  # pytype: disable=not-instantiable
      else:
        self.nfs_service = nfs_service.UnmanagedNfsService(
            disk_spec, self.vms[0]
        )
      logging.debug('NFS service %s', self.nfs_service)
      break

  def ConstructLustreService(self):
    """Construct the Lustre service object.

    Creates an Lustre Service only if a Lustre disk is found in the disk_specs.
    """
    if self.lustre_service:
      logging.info('Lustre service already created: %s', self.lustre_service)
      return
    for group_spec in self.vms_to_boot.values():
      if not group_spec.disk_spec or not group_spec.vm_count:
        continue
      disk_spec = group_spec.disk_spec
      if disk_spec.disk_type != disk.LUSTRE:
        continue
      # Choose which lustre_service to create.
      cloud = group_spec.cloud
      providers.LoadProvider(cloud)
      lustre_class = lustre_service.GetLustreServiceClass(cloud)
      self.lustre_service = lustre_class(
          disk_spec, group_spec.vm_spec.zone
      )  # pytype: disable=not-instantiable
      break

  def ConstructSmbService(self):
    """Construct the SMB service object.

    Creates an SMB Service only if an SMB disk is found in the disk_specs.
    """
    if self.smb_service:
      logging.info('SMB service already created: %s', self.smb_service)
      return
    for group_spec in self.vms_to_boot.values():
      if not group_spec.disk_spec or not group_spec.vm_count:
        continue
      disk_spec = group_spec.disk_spec
      if disk_spec.disk_type != disk.SMB:
        continue

      cloud = group_spec.cloud
      providers.LoadProvider(cloud)
      smb_class = smb_service.GetSmbServiceClass(cloud)
      self.smb_service = smb_class(
          disk_spec, group_spec.vm_spec.zone
      )  # pytype: disable=not-instantiable
      logging.debug('SMB service %s', self.smb_service)
      break

  def ConstructMultiAttachDisk(self):
    """Construct the multi attach disk object."""
    # Hdml is constructed after VM creation. The class is included here
    # to enable multiple VMs in the same VM group to share the same HdML.
    # It is deleted during DeleteScratchDisks.
    if self.multi_attach_disk:
      return
    for group_spec in self.vms_to_boot.values():
      if not group_spec.disk_spec:
        continue
      disk_spec = group_spec.disk_spec
      if disk_spec.disk_type not in disk.MULTI_ATTACH_DISK_TYPES:
        continue
      if disk_spec.num_striped_disks > 1:
        raise errors.Benchmarks.UnsupportedConfigError(
            'Disk type %s allows only 1 striped disk.' % disk_spec.disk_type
        )
      cloud = group_spec.cloud
      providers.LoadProvider(cloud)
      disk_class = disk.GetMultiAttachDiskClass(cloud)
      self.multi_attach_disk = disk_class(
          disk_spec,
          name=f'pkb-{FLAGS.run_uri}-data-0-0',
          zone=group_spec.vm_spec.zone,
          project=None,
      )

  def ConstructVirtualMachineGroup(
      self, group_name, group_spec
  ) -> list[virtual_machine.VirtualMachine]:
    """Construct the virtual machine(s) needed for a group."""
    vms = []

    vm_count = group_spec.vm_count

    # First create the Static VM objects.
    if group_spec.static_vms:
      specs = [
          spec
          for spec in group_spec.static_vms
          if (FLAGS.static_vm_tags is None or spec.tag in FLAGS.static_vm_tags)
      ][:vm_count]
      for vm_spec in specs:
        static_vm_class = static_vm.GetStaticVmClass(vm_spec.os_type)
        vms.append(static_vm_class(vm_spec))  # pytype: disable=not-instantiable

    os_type = group_spec.os_type
    cloud = group_spec.cloud

    # This throws an exception if the benchmark is not
    # supported.
    self._CheckBenchmarkSupport(cloud)

    # Then create the remaining VM objects using VM and disk specs.

    if group_spec.disk_spec:
      disk_spec = group_spec.disk_spec
    else:
      disk_spec = None

    if group_spec.placement_group_name:
      group_spec.vm_spec.placement_group = self.placement_groups[
          group_spec.placement_group_name
      ]

    for _ in range(vm_count - len(vms)):
      # Assign a zone to each VM sequentially from the --zones flag.
      if FLAGS.zone:
        zone_list = FLAGS.zone
        group_spec.vm_spec.zone = zone_list[self._zone_index]
        self._zone_index = (
            self._zone_index + 1 if self._zone_index < len(zone_list) - 1 else 0
        )
      if group_spec.cidr:  # apply cidr range to all vms in vm_group
        group_spec.vm_spec.cidr = group_spec.cidr
      vm = self._CreateVirtualMachine(group_spec.vm_spec, os_type, cloud)
      vm.vm_group = group_name
      if disk_spec and not vm.is_static:
        if _ENFORCE_DISK_MOUNT_POINT_OVERRIDE.value:
          disk_spec.mount_point = FLAGS.scratch_dir
        vm.SetDiskSpec(disk_spec, group_spec.disk_count)
      vms.append(vm)

    return vms

  def ConstructCapacityReservations(self):
    """Construct capacity reservations for each VM group."""
    if not FLAGS.use_capacity_reservations:
      return
    for vm_group in self.vm_groups.values():
      cloud = vm_group[0].CLOUD
      providers.LoadProvider(cloud)
      capacity_reservation_class = capacity_reservation.GetResourceClass(cloud)
      self.capacity_reservations.append(
          capacity_reservation_class(vm_group)  # pytype: disable=not-instantiable
      )

  def _CheckBenchmarkSupport(self, cloud):
    """Throw an exception if the benchmark isn't supported."""

    if FLAGS.benchmark_compatibility_checking == SKIP_CHECK:
      return

    provider_info_class = provider_info.GetProviderInfoClass(cloud)
    benchmark_ok = provider_info_class.IsBenchmarkSupported(self.name)
    if FLAGS.benchmark_compatibility_checking == NOT_EXCLUDED:
      if benchmark_ok is None:
        benchmark_ok = True

    if not benchmark_ok:
      raise ValueError(
          'Provider {} does not support {}.  Use '
          '--benchmark_compatibility_checking=none '
          'to override this check.'.format(provider_info_class.CLOUD, self.name)
      )

  def _ConstructJujuController(
      self, group_spec
  ) -> virtual_machine.VirtualMachine:
    """Construct a VirtualMachine object for a Juju controller."""
    juju_spec = copy.copy(group_spec)
    juju_spec.vm_count = 1
    jujuvms = self.ConstructVirtualMachineGroup('juju', juju_spec)
    if len(jujuvms):
      jujuvm = jujuvms.pop()
      jujuvm.is_controller = True
      return jujuvm
    raise errors.Config.ParseError(
        'Attempted to construct Juju VM but none were created.'
    )

  def ConstructVirtualMachines(self):
    """Constructs the BenchmarkSpec's VirtualMachine objects."""

    self.ConstructPlacementGroups()

    vm_group_specs = self.vms_to_boot

    clouds = {}
    for group_name, group_spec in sorted(vm_group_specs.items()):
      vms = self.ConstructVirtualMachineGroup(group_name, group_spec)

      if group_spec.os_type.startswith('juju'):
        # The Juju VM needs to be created first, so that subsequent units can
        # be properly added under its control.
        if group_spec.cloud in clouds:
          jujuvm = clouds[group_spec.cloud]
        else:
          jujuvm = self._ConstructJujuController(group_spec)
          clouds[group_spec.cloud] = jujuvm

        for vm in vms:
          vm.controller = clouds[group_spec.cloud]

        jujuvm.units.extend(vms)  # pytype: disable=attribute-error
        if jujuvm and jujuvm not in self.vms:
          self.vms.extend([jujuvm])
          self.vm_groups['%s_juju_controller' % group_spec.cloud] = [jujuvm]

      self.vm_groups[group_name] = vms
      self.vms.extend(vms)

    # In the case of an un-managed yarn cluster, for hadoop software
    # installation, the dpb service instance needs access to constructed
    # master group and worker group.
    if self.config.dpb_service and self.config.dpb_service.service_type in [
        dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER,
        dpb_constants.UNMANAGED_SPARK_CLUSTER,
    ]:
      self.dpb_service.vms['master_group'] = self.vm_groups['master_group']
      if self.config.dpb_service.worker_count:
        self.dpb_service.vms['worker_group'] = self.vm_groups['worker_group']
      else:  # single node cluster
        self.dpb_service.vms['worker_group'] = []

  def ConstructPlacementGroups(self):
    for placement_group_name, placement_group_spec in (
        self.placement_group_specs
    ).items():
      self.placement_groups[placement_group_name] = self._CreatePlacementGroup(
          placement_group_spec, placement_group_spec.CLOUD
      )

  def ConstructVPNService(self):
    """Create the VPNService object."""
    if self.config.vpn_service is None:
      return
    self.vpn_service = vpn_service.VPNService(self.config.vpn_service)

  def ConstructMessagingService(self):
    """Create the messaging_service object.

    Assumes VMs are already constructed.
    """
    if self.config.messaging_service is None:
      return
    cloud = self.config.messaging_service.cloud
    delivery = self.config.messaging_service.delivery
    providers.LoadProvider(cloud)
    messaging_service_class = messaging_service.GetMessagingServiceClass(
        cloud, delivery
    )
    self.messaging_service = messaging_service_class.FromSpec(
        self.config.messaging_service
    )
    self.messaging_service.setVms(self.vm_groups)

  def ConstructDataDiscoveryService(self):
    """Create the data_discovery_service object."""
    if not self.config.data_discovery_service:
      return
    cloud = self.config.data_discovery_service.cloud
    service_type = self.config.data_discovery_service.service_type
    providers.LoadProvider(cloud)
    data_discovery_service_class = (
        data_discovery_service.GetDataDiscoveryServiceClass(cloud, service_type)
    )
    self.data_discovery_service = data_discovery_service_class.FromSpec(
        self.config.data_discovery_service
    )

  def Prepare(self):
    targets = [(vm.PrepareBackgroundWorkload, (), {}) for vm in self.vms]
    background_tasks.RunParallelThreads(targets, len(targets))

  def Provision(self):
    """Prepares the VMs and networks necessary for the benchmark to run."""
    should_restore = (
        hasattr(self, 'restore_spec') and self.restore_spec is not None
    )
    # Create capacity reservations if the cloud supports it. Note that the
    # capacity reservation class may update the VMs themselves. This is true
    # on AWS, because the VM needs to be aware of the capacity reservation id
    # before its Create() method is called. Furthermore, if the user does not
    # specify an AWS zone, but a region instead, the AwsCapacityReservation
    # class will make a reservation in a zone that has sufficient capacity.
    # In this case the VM's zone attribute, and the VMs network instance
    # need to be updated as well.
    if self.capacity_reservations:
      background_tasks.RunThreaded(
          lambda res: res.Create(), self.capacity_reservations
      )

    # Sort networks into a guaranteed order of creation based on dict key.
    # There is a finite limit on the number of threads that are created to
    # provision networks. Until support is added to provision resources in an
    # order based on dependencies, this key ordering can be used to avoid
    # deadlock by placing dependent networks later and their dependencies
    # earlier.
    networks = [self.networks[key] for key in sorted(self.networks.keys())]

    background_tasks.RunThreaded(lambda net: net.Create(), networks)

    # VPC peering is currently only supported for connecting 2 VPC networks
    if self.vpc_peering:
      if len(networks) > 2:
        raise errors.Error(
            'Networks of size %d are not currently supported.' % (len(networks))
        )
      # Ignore Peering for one network
      elif len(networks) == 2:
        networks[0].Peer(networks[1])

    if self.container_registry:
      self.container_registry.Create()
      for container_spec in self.container_specs.values():
        if container_spec.static_image:
          continue
        container_spec.image = self.container_registry.GetOrBuild(
            container_spec.image
        )

    if self.container_cluster:
      self.container_cluster.Create()

    # do after network setup but before VM created
    if self.nfs_service and self.nfs_service.CLOUD != nfs_service.UNMANAGED:
      self.nfs_service.Create()
    if self.lustre_service:
      self.lustre_service.Create()
    if self.smb_service:
      self.smb_service.Create()
    if self.multi_attach_disk:
      self.multi_attach_disk.Create()

    for placement_group_object in self.placement_groups.values():
      placement_group_object.Create()

    if self.vms:
      # Iterate through VM groups to apply potential provision_delay_seconds
      # and then call CreateAndBoot for each group.
      # Sorting by provision_order ensures a deterministic order if delays
      # are intended to create a sequence.
      for group_name in sorted(
          self.vm_groups.keys(),
          key=lambda x: self.vms_to_boot[x].provision_order,
      ):
        group_vms = self.vm_groups[group_name]
        if not group_vms:
          continue

        group_spec = self.vms_to_boot[group_name]
        provision_delay_seconds = getattr(
            group_spec, 'provision_delay_seconds', 0
        )
        if provision_delay_seconds:
          logging.info(
              'Delaying provisioning of VM group %s by %s seconds.',
              group_name,
              provision_delay_seconds,
          )
          time.sleep(provision_delay_seconds)

        post_task_delay = FLAGS.create_and_boot_post_task_delay
        logging.info(
            "Starting CreateAndBoot for VM group '%s' with"
            ' post_task_delay: %s.',
            group_name,
            post_task_delay,
        )
        background_tasks.RunThreaded(
            lambda vm: vm.CreateAndBoot(),
            group_vms,
            post_task_delay=post_task_delay,
        )

      if self.nfs_service and self.nfs_service.CLOUD == nfs_service.UNMANAGED:
        self.nfs_service.Create()
      if not FLAGS.skip_vm_preparation:
        background_tasks.RunThreaded(lambda vm: vm.PrepareAfterBoot(), self.vms)
      else:
        logging.info('Skipping VM preparation.')

      sshable_vms = [
          vm for vm in self.vms if vm.OS_TYPE not in os_types.WINDOWS_OS_TYPES
      ]
      sshable_vm_groups = {}
      for group_name, group_vms in self.vm_groups.items():
        sshable_vm_groups[group_name] = [
            vm
            for vm in group_vms
            if vm.OS_TYPE not in os_types.WINDOWS_OS_TYPES
        ]
      vm_util.GenerateSSHConfig(sshable_vms, sshable_vm_groups)
    if self.cluster:
      if self.cluster.unmanaged:
        self.cluster.ImportVmGroups(
            self.vm_groups['headnode'][0], self.vm_groups['workers']
        )
      self.cluster.Create()
    if self.dpb_service:
      self.dpb_service.Create()
    if hasattr(self, 'relational_db') and self.relational_db:
      self.relational_db.SetVms(self.vm_groups)
      self.relational_db.Create(restore=should_restore)
    if self.non_relational_db:
      self.non_relational_db.SetVms(self.vm_groups)
      self.non_relational_db.Create(restore=should_restore)
    if hasattr(self, 'key') and self.key:
      self.key.Create()
    if self.tpus:
      background_tasks.RunThreaded(lambda tpu: tpu.Create(), self.tpus)
    if self.ai_model:
      self.ai_model.Create()
    if self.pinecone:
      self.pinecone.Create()
    if self.vvs:
      self.vvs.Create()
    if self.edw_service:
      if (
          not self.edw_service.user_managed
          and self.edw_service.SERVICE_TYPE == 'redshift'
      ):
        # The benchmark creates the Redshift cluster's subnet group in the
        # already provisioned virtual private cloud (vpc).
        for network in networks:
          if network.__class__.__name__ == 'AwsNetwork':
            self.edw_service.cluster_subnet_group.subnet_id = network.subnet.id
      self.edw_service.Create()
    if self.edw_compute_resource:
      self.edw_compute_resource.Create()
    if hasattr(self, 'memory_store') and self.memory_store:
      self.memory_store.Create()
    if self.example_resource:
      self.example_resource.Create()

    if self.base_job:
      self.base_job.Create()
    if self.vpn_service:
      self.vpn_service.Create()
    if hasattr(self, 'messaging_service') and self.messaging_service:
      self.messaging_service.Create()
    if self.data_discovery_service:
      self.data_discovery_service.Create()

  def Delete(self):
    if self.deleted:
      return

    should_freeze = (
        hasattr(self, 'freeze_path') and self.freeze_path is not None
    )
    if should_freeze:
      self.Freeze()

    if self.container_registry:
      self.container_registry.Delete()
    if self.dpb_service:
      self.dpb_service.Delete()
    if hasattr(self, 'relational_db') and self.relational_db:
      self.relational_db.Delete(freeze=should_freeze)
    if hasattr(self, 'non_relational_db') and self.non_relational_db:
      self.non_relational_db.Delete(freeze=should_freeze)
    if hasattr(self, 'key') and self.key:
      self.key.Delete()
    if self.tpus:
      background_tasks.RunThreaded(lambda tpu: tpu.Delete(), self.tpus)
    if self.edw_service:
      self.edw_service.Delete()
    if hasattr(self, 'edw_compute_resource') and self.edw_compute_resource:
      self.edw_compute_resource.Delete()
    if self.example_resource:
      self.example_resource.Delete()
    if self.base_job:
      self.base_job.Delete()
    if self.nfs_service:
      self.nfs_service.Delete()
    if self.lustre_service:
      self.lustre_service.Delete()
    if self.smb_service:
      self.smb_service.Delete()
    if self.multi_attach_disk:
      self.multi_attach_disk.Delete()
    if hasattr(self, 'messaging_service') and self.messaging_service:
      self.messaging_service.Delete()
    if hasattr(self, 'memory_store') and self.memory_store:
      self.memory_store.Delete()
    if hasattr(self, 'ai_model') and self.ai_model:
      self.ai_model.Delete()
    if hasattr(self, 'data_discovery_service') and self.data_discovery_service:
      self.data_discovery_service.Delete()
    if hasattr(self, 'pinecone') and self.pinecone:
      self.pinecone.Delete()
    if hasattr(self, 'vvs') and self.vvs:
      self.vvs.Delete()

    # Note: It is ok to delete capacity reservations before deleting the VMs,
    # and will actually save money (mere seconds of usage).
    if self.capacity_reservations:
      try:
        background_tasks.RunThreaded(
            lambda reservation: reservation.Delete(), self.capacity_reservations
        )
      except Exception:  # pylint: disable=broad-except
        logging.exception(
            'Got an exception deleting CapacityReservations. '
            'Attempting to continue tearing down.'
        )

    if self.vms:
      try:
        # Delete VMs first to detach any multi-attached disks.
        background_tasks.RunThreaded(self.DeleteVm, self.vms)
        background_tasks.RunThreaded(
            lambda vm: vm.DeleteScratchDisks(), self.vms
        )
      except Exception:
        logging.exception(
            'Got an exception deleting VMs. '
            'Attempting to continue tearing down.'
        )
    if hasattr(self, 'placement_groups'):
      for placement_group_object in self.placement_groups.values():
        placement_group_object.Delete()

    for firewall in self.firewalls.values():
      try:
        firewall.DisallowAllPorts()
      except Exception:
        logging.exception(
            'Got an exception disabling firewalls. '
            'Attempting to continue tearing down.'
        )

    if self.container_cluster:
      self.container_cluster.DeleteServices()
      self.container_cluster.DeleteContainers()
      self.container_cluster.Delete()

    if self.cluster:
      self.cluster.Delete()

    for net in self.networks.values():
      try:
        net.Delete()
      except Exception:
        logging.exception(
            'Got an exception deleting networks. '
            'Attempting to continue tearing down.'
        )

    if hasattr(self, 'vpn_service') and self.vpn_service:
      self.vpn_service.Delete()

    self.deleted = True

  def GetSamples(self):
    """Returns samples created from benchmark resources."""
    samples = []
    for resource in self.resources:
      samples.extend(resource.GetSamples())
    return samples

  def StartBackgroundWorkload(self):
    targets = [(vm.StartBackgroundWorkload, (), {}) for vm in self.vms]
    background_tasks.RunParallelThreads(targets, len(targets))

  def StopBackgroundWorkload(self):
    targets = [(vm.StopBackgroundWorkload, (), {}) for vm in self.vms]
    background_tasks.RunParallelThreads(targets, len(targets))

  def _IsSafeKeyOrValueCharacter(self, char):
    return char.isalpha() or char.isnumeric() or char == '_'

  def _SafeLabelKeyOrValue(self, key):
    result = ''.join(
        c if self._IsSafeKeyOrValueCharacter(c) else '_' for c in key.lower()
    )

    # max length constraints on keys and values
    # https://cloud.google.com/resource-manager/docs/creating-managing-labels
    max_safe_length = 63
    # GCP labels are not allowed to start or end with '_'
    return result[:max_safe_length].strip('_')

  def _GetResourceDict(self, time_format, timeout_minutes=None):
    """Gets a list of tags to be used to tag resources."""
    now_utc = datetime.datetime.utcnow()

    if not timeout_minutes:
      timeout_minutes = FLAGS.timeout_minutes

    timeout_utc = now_utc + datetime.timedelta(minutes=timeout_minutes)

    tags = {
        resource_type.TIMEOUT_METADATA_KEY: timeout_utc.strftime(time_format),
        'create_time_utc': now_utc.strftime(time_format),
        'benchmark': self.name,
        'perfkit_uuid': self.uuid,
        'owner': FLAGS.owner,
        'benchmark_uid': self.uid,
    }

    # add metadata key value pairs
    metadata_dict = (
        flag_util.ParseKeyValuePairs(FLAGS.metadata)
        if hasattr(FLAGS, 'metadata')
        else dict()
    )
    for key, value in metadata_dict.items():
      tags[self._SafeLabelKeyOrValue(key)] = self._SafeLabelKeyOrValue(value)

    return tags

  def GetResourceTags(self, timeout_minutes=None):
    """Gets a list of tags to be used to tag resources."""
    return self._GetResourceDict(
        resource_type.METADATA_TIME_FORMAT, timeout_minutes
    )

  def _CreatePlacementGroup(self, placement_group_spec, cloud):
    """Create a placement group in zone.

    Args:
      placement_group_spec: A placement_group.BasePlacementGroupSpec object.
      cloud: The cloud for the placement group. See the flag of the same name
        for more information.

    Returns:
      A placement_group.BasePlacementGroup object.
    """

    placement_group_class = placement_group.GetPlacementGroupClass(cloud)
    if placement_group_class:
      return placement_group_class(
          placement_group_spec
      )  # pytype: disable=not-instantiable
    else:
      return None

  def _CreateVirtualMachine(self, vm_spec, os_type, cloud):
    """Create a vm in zone.

    Args:
      vm_spec: A virtual_machine.BaseVmSpec object.
      os_type: The type of operating system for the VM. See the flag of the same
        name for more information.
      cloud: The cloud for the VM. See the flag of the same name for more
        information.

    Returns:
      A virtual_machine.BaseVirtualMachine object.
    """
    vm = static_vm.StaticVirtualMachine.GetStaticVirtualMachine()
    if vm:
      return vm

    vm_class = virtual_machine.GetVmClass(cloud, os_type)
    if vm_class is None:
      raise errors.Error(
          'VMs of type %s" are not currently supported on cloud "%s".'
          % (os_type, cloud)
      )

    return vm_class(vm_spec)  # pytype: disable=not-instantiable

  def DeleteVm(self, vm):
    """Deletes a single vm and scratch disk if required.

    Args:
        vm: The BaseVirtualMachine object representing the VM.
    """
    if vm.is_static and vm.install_packages:
      vm.PackageCleanup()
    vm.Delete()

  @staticmethod
  def _GetPickleFilename(uid):
    """Returns the filename for the pickled BenchmarkSpec."""
    return os.path.join(vm_util.GetTempDir(), uid)

  def Pickle(self, filename=None):
    """Pickles the spec so that it can be unpickled on a subsequent run."""
    with open(
        filename or self._GetPickleFilename(self.uid), 'wb'
    ) as pickle_file:
      pickle.dump(self, pickle_file, 2)

  def Freeze(self):
    """Pickles the spec to a destination, defaulting to tempdir if not found."""
    if not self.freeze_path:
      return
    logging.info('Freezing benchmark_spec to %s', self.freeze_path)
    try:
      self.Pickle(self.freeze_path)
    except FileNotFoundError:
      default_path = f'{vm_util.GetTempDir()}/restore_spec.pickle'
      logging.exception(
          'Could not find file path %s, defaulting freeze to %s.',
          self.freeze_path,
          default_path,
      )
      self.Pickle(default_path)

  @classmethod
  def GetBenchmarkSpec(cls, benchmark_module, config, uid):
    """Unpickles or creates a BenchmarkSpec and returns it.

    Args:
      benchmark_module: The benchmark module object.
      config: BenchmarkConfigSpec. The configuration for the benchmark.
      uid: An identifier unique to this run of the benchmark even if the same
        benchmark is run multiple times with different configs.

    Returns:
      A BenchmarkSpec object.
    """
    if stages.PROVISION in FLAGS.run_stage:
      return cls(benchmark_module, config, uid)

    try:
      with open(cls._GetPickleFilename(uid), 'rb') as pickle_file:
        bm_spec = pickle.load(pickle_file)
    except Exception as e:  # pylint: disable=broad-except
      logging.error(
          'Unable to unpickle spec file for benchmark %s.',
          benchmark_module.BENCHMARK_NAME,
      )
      raise e
    # Always let the spec be deleted after being unpickled so that
    # it's possible to run cleanup even if cleanup has already run.
    bm_spec.deleted = False
    bm_spec.status = benchmark_status.SKIPPED
    context.SetThreadBenchmarkSpec(bm_spec)
    return bm_spec

  def CheckPrerequisites(self) -> None:
    """Checks preconditions for the benchmark_spec."""
    for app_group in self.app_groups:
      app_group.CheckPrerequisites()
    if self.dpb_service:
      self.dpb_service.CheckPrerequisites()
    if self.messaging_service:
      self.messaging_service.CheckPrerequisites()
    if hasattr(self, 'memory_store') and self.memory_store:
      self.memory_store.CheckPrerequisites()
