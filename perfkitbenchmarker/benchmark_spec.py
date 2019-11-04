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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import contextlib
import copy
import datetime
import importlib
import logging
import os
import pickle
import threading
import uuid

from perfkitbenchmarker import benchmark_status
from perfkitbenchmarker import capacity_reservation
from perfkitbenchmarker import cloud_tpu
from perfkitbenchmarker import container_service
from perfkitbenchmarker import context
from perfkitbenchmarker import disk
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import nfs_service
from perfkitbenchmarker import os_types
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import providers
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import smb_service
from perfkitbenchmarker import spark_service
from perfkitbenchmarker import stages
from perfkitbenchmarker import static_virtual_machine as static_vm
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
import six
from six.moves import range
import six.moves._thread
import six.moves.copyreg


def PickleLock(lock):
  return UnPickleLock, (lock.locked(),)


def UnPickleLock(locked, *args):
  lock = threading.Lock()
  if locked:
    if not lock.acquire(False):
      raise pickle.UnpicklingError('Cannot acquire lock')
  return lock

six.moves.copyreg.pickle(six.moves._thread.LockType, PickleLock)

SUPPORTED = 'strict'
NOT_EXCLUDED = 'permissive'
SKIP_CHECK = 'none'
# GCP labels only allow hyphens (-), underscores (_), lowercase characters, and
# numbers and International characters.
# metadata allow all characters and numbers.
METADATA_TIME_FORMAT = '%Y%m%dt%H%M%Sz'
FLAGS = flags.FLAGS

flags.DEFINE_enum('cloud', providers.GCP, providers.VALID_CLOUDS,
                  'Name of the cloud to use.')
flags.DEFINE_string('scratch_dir', None,
                    'Base name for all scratch disk directories in the VM. '
                    'Upon creation, these directories will have numbers '
                    'appended to them (for example /scratch0, /scratch1, etc).')
flags.DEFINE_string('startup_script', None,
                    'Script to run right after vm boot.')
flags.DEFINE_string('postrun_script', None,
                    'Script to run right after run stage.')
flags.DEFINE_integer('create_and_boot_post_task_delay', None,
                     'Delay in seconds to delay in between boot tasks.')
# pyformat: disable
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


class BenchmarkSpec(object):
  """Contains the various data required to make a benchmark run."""

  total_benchmarks = 0

  def __init__(self, benchmark_module, benchmark_config, benchmark_uid):
    """Initialize a BenchmarkSpec object.

    Args:
      benchmark_module: The benchmark module object.
      benchmark_config: BenchmarkConfigSpec. The configuration for the
          benchmark.
      benchmark_uid: An identifier unique to this run of the benchmark even
          if the same benchmark is run multiple times with different configs.
    """
    self.config = benchmark_config
    self.name = benchmark_module.BENCHMARK_NAME
    self.uid = benchmark_uid
    self.status = benchmark_status.SKIPPED
    self.failed_substatus = None
    self.status_detail = None
    BenchmarkSpec.total_benchmarks += 1
    self.sequence_number = BenchmarkSpec.total_benchmarks
    self.vms = []
    self.networks = {}
    self.firewalls = {}
    self.networks_lock = threading.Lock()
    self.firewalls_lock = threading.Lock()
    self.vm_groups = {}
    self.container_specs = benchmark_config.container_specs or {}
    self.container_registry = None
    self.deleted = False
    self.uuid = '%s-%s' % (FLAGS.run_uri, uuid.uuid4())
    self.always_call_cleanup = False
    self.spark_service = None
    self.dpb_service = None
    self.container_cluster = None
    self.relational_db = None
    self.tpus = []
    self.tpu_groups = {}
    self.edw_service = None
    self.nfs_service = None
    self.smb_service = None
    self.app_groups = {}
    self._zone_index = 0
    self.capacity_reservations = []
    self.placement_group_specs = benchmark_config.placement_group_specs or {}
    self.placement_groups = {}
    self.vms_to_boot = (
        self.config.vm_groups if self.config.relational_db is None else
        relational_db.VmsToBoot(self.config.relational_db.vm_groups))

    # Modules can't be pickled, but functions can, so we store the functions
    # necessary to run the benchmark.
    self.BenchmarkPrepare = benchmark_module.Prepare
    self.BenchmarkRun = benchmark_module.Run
    self.BenchmarkCleanup = benchmark_module.Cleanup

    # Set the current thread's BenchmarkSpec object to this one.
    context.SetThreadBenchmarkSpec(self)

  def __repr__(self):
    return '%s(%r)' % (self.__class__, self.__dict__)

  def __str__(self):
    return(
        'Benchmark name: {0}\nFlags: {1}'
        .format(self.name, self.config.flags))

  @contextlib.contextmanager
  def RedirectGlobalFlags(self):
    """Redirects flag reads and writes to the benchmark-specific flags object.

    Within the enclosed code block, reads and writes to the flags.FLAGS object
    are redirected to a copy that has been merged with config-provided flag
    overrides specific to this benchmark run.
    """
    with self.config.RedirectFlags(FLAGS):
      yield

  def ConstructContainerCluster(self):
    """Create the container cluster."""
    if self.config.container_cluster is None:
      return
    cloud = self.config.container_cluster.cloud
    cluster_type = self.config.container_cluster.type
    providers.LoadProvider(cloud)
    container_cluster_class = container_service.GetContainerClusterClass(
        cloud, cluster_type)
    self.container_cluster = container_cluster_class(
        self.config.container_cluster)

  def ConstructContainerRegistry(self):
    """Create the container registry."""
    if self.config.container_registry is None:
      return
    cloud = self.config.container_registry.cloud
    providers.LoadProvider(cloud)
    container_registry_class = container_service.GetContainerRegistryClass(
        cloud)
    self.container_registry = container_registry_class(
        self.config.container_registry)

  def ConstructDpbService(self):
    """Create the dpb_service object and create groups for its vms."""
    if self.config.dpb_service is None:
      return
    providers.LoadProvider(self.config.dpb_service.worker_group.cloud)
    dpb_service_class = dpb_service.GetDpbServiceClass(
        self.config.dpb_service.service_type)
    self.dpb_service = dpb_service_class(self.config.dpb_service)

  def ConstructRelationalDb(self):
    """Create the relational db and create groups for its vms."""
    if self.config.relational_db is None:
      return
    cloud = self.config.relational_db.cloud
    providers.LoadProvider(cloud)
    relational_db_class = (relational_db.GetRelationalDbClass(cloud))
    self.relational_db = relational_db_class(self.config.relational_db)

  def ConstructTpuGroup(self, group_spec):
    """Constructs the BenchmarkSpec's cloud TPU objects."""
    if group_spec is None:
      return
    cloud = group_spec.cloud
    providers.LoadProvider(cloud)
    tpu_class = cloud_tpu.GetTpuClass(cloud)
    return tpu_class(group_spec)

  def ConstructTpu(self):
    """Constructs the BenchmarkSpec's cloud TPU objects."""
    tpu_group_specs = self.config.tpu_groups

    for group_name, group_spec in sorted(six.iteritems(tpu_group_specs)):
      tpu = self.ConstructTpuGroup(group_spec)

      self.tpu_groups[group_name] = tpu
      self.tpus.append(tpu)

  def ConstructEdwService(self):
    """Create the edw_service object."""
    if self.config.edw_service is None:
      return
    # Load necessary modules from the provider to account for dependencies
    providers.LoadProvider(
        edw_service.TYPE_2_PROVIDER.get(self.config.edw_service.type))
    # Load the module for the edw service based on type
    edw_service_module = importlib.import_module(edw_service.TYPE_2_MODULE.get(
        self.config.edw_service.type))
    edw_service_class = getattr(edw_service_module,
                                self.config.edw_service.type[0].upper() +
                                self.config.edw_service.type[1:])
    # Check if a new instance needs to be created or restored from snapshot
    self.edw_service = edw_service_class(self.config.edw_service)

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
        self.nfs_service = nfs_class(disk_spec, group_spec.vm_spec.zone)
      else:
        self.nfs_service = nfs_service.UnmanagedNfsService(disk_spec,
                                                           self.vms[0])
      logging.debug('NFS service %s', self.nfs_service)
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
      self.smb_service = smb_class(disk_spec, group_spec.vm_spec.zone)
      logging.debug('SMB service %s', self.smb_service)
      break

  def ConstructVirtualMachineGroup(self, group_name, group_spec):
    """Construct the virtual machine(s) needed for a group."""
    vms = []

    vm_count = group_spec.vm_count
    disk_count = group_spec.disk_count

    # First create the Static VM objects.
    if group_spec.static_vms:
      specs = [
          spec for spec in group_spec.static_vms
          if (FLAGS.static_vm_tags is None or spec.tag in FLAGS.static_vm_tags)
      ][:vm_count]
      for vm_spec in specs:
        static_vm_class = static_vm.GetStaticVmClass(vm_spec.os_type)
        vms.append(static_vm_class(vm_spec))

    os_type = group_spec.os_type
    cloud = group_spec.cloud

    # This throws an exception if the benchmark is not
    # supported.
    self._CheckBenchmarkSupport(cloud)

    # Then create the remaining VM objects using VM and disk specs.

    if group_spec.disk_spec:
      disk_spec = group_spec.disk_spec
      # disk_spec.disk_type may contain legacy values that were
      # copied from FLAGS.scratch_disk_type into
      # FLAGS.data_disk_type at the beginning of the run. We
      # translate them here, rather than earlier, because here is
      # where we know what cloud we're using and therefore we're
      # able to pick the right translation table.
      disk_spec.disk_type = disk.WarnAndTranslateDiskTypes(
          disk_spec.disk_type, cloud)
    else:
      disk_spec = None

    if group_spec.placement_group_name:
      group_spec.vm_spec.placement_group = self.placement_groups[
          group_spec.placement_group_name]

    for _ in range(vm_count - len(vms)):
      # Assign a zone to each VM sequentially from the --zones flag.
      if FLAGS.zones or FLAGS.extra_zones or FLAGS.zone:
        zone_list = FLAGS.zones + FLAGS.extra_zones + FLAGS.zone
        group_spec.vm_spec.zone = zone_list[self._zone_index]
        self._zone_index = (self._zone_index + 1
                            if self._zone_index < len(zone_list) - 1 else 0)
      vm = self._CreateVirtualMachine(group_spec.vm_spec, os_type, cloud)
      if disk_spec and not vm.is_static:
        if disk_spec.disk_type == disk.LOCAL and disk_count is None:
          disk_count = vm.max_local_disks
        vm.disk_specs = [copy.copy(disk_spec) for _ in range(disk_count)]
        # In the event that we need to create multiple disks from the same
        # DiskSpec, we need to ensure that they have different mount points.
        if (disk_count > 1 and disk_spec.mount_point):
          for i, spec in enumerate(vm.disk_specs):
            spec.mount_point += str(i)
      vms.append(vm)

    return vms

  def ConstructCapacityReservations(self):
    """Construct capacity reservations for each VM group."""
    if not FLAGS.use_capacity_reservations:
      return
    for vm_group in six.itervalues(self.vm_groups):
      cloud = vm_group[0].CLOUD
      providers.LoadProvider(cloud)
      capacity_reservation_class = capacity_reservation.GetResourceClass(
          cloud)
      self.capacity_reservations.append(
          capacity_reservation_class(vm_group))

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
      raise ValueError('Provider {0} does not support {1}.  Use '
                       '--benchmark_compatibility_checking=none '
                       'to override this check.'.format(
                           provider_info_class.CLOUD, self.name))

  def _ConstructJujuController(self, group_spec):
    """Construct a VirtualMachine object for a Juju controller."""
    juju_spec = copy.copy(group_spec)
    juju_spec.vm_count = 1
    jujuvms = self.ConstructVirtualMachineGroup('juju', juju_spec)
    if len(jujuvms):
      jujuvm = jujuvms.pop()
      jujuvm.is_controller = True
      return jujuvm
    return None

  def ConstructVirtualMachines(self):
    """Constructs the BenchmarkSpec's VirtualMachine objects."""

    self.ConstructPlacementGroups()

    vm_group_specs = self.vms_to_boot

    clouds = {}
    for group_name, group_spec in sorted(six.iteritems(vm_group_specs)):
      vms = self.ConstructVirtualMachineGroup(group_name, group_spec)

      if group_spec.os_type == os_types.JUJU:
        # The Juju VM needs to be created first, so that subsequent units can
        # be properly added under its control.
        if group_spec.cloud in clouds:
          jujuvm = clouds[group_spec.cloud]
        else:
          jujuvm = self._ConstructJujuController(group_spec)
          clouds[group_spec.cloud] = jujuvm

        for vm in vms:
          vm.controller = clouds[group_spec.cloud]
          vm.vm_group = group_name

        jujuvm.units.extend(vms)
        if jujuvm and jujuvm not in self.vms:
          self.vms.extend([jujuvm])
          self.vm_groups['%s_juju_controller' % group_spec.cloud] = [jujuvm]

      self.vm_groups[group_name] = vms
      self.vms.extend(vms)
    # If we have a spark service, it needs to access the master_group and
    # the worker group.
    if (self.config.spark_service and
        self.config.spark_service.service_type == spark_service.PKB_MANAGED):
      for group_name in 'master_group', 'worker_group':
        self.spark_service.vms[group_name] = self.vm_groups[group_name]

  def ConstructPlacementGroups(self):
    for placement_group_name, placement_group_spec in six.iteritems(
        self.placement_group_specs):
      self.placement_groups[placement_group_name] = self._CreatePlacementGroup(
          placement_group_spec, placement_group_spec.CLOUD)

  def ConstructSparkService(self):
    """Create the spark_service object and create groups for its vms."""
    if self.config.spark_service is None:
      return

    spark_spec = self.config.spark_service
    # Worker group is required, master group is optional
    cloud = spark_spec.worker_group.cloud
    if spark_spec.master_group:
      cloud = spark_spec.master_group.cloud
    providers.LoadProvider(cloud)
    service_type = spark_spec.service_type
    spark_service_class = spark_service.GetSparkServiceClass(
        cloud, service_type)
    self.spark_service = spark_service_class(spark_spec)
    # If this is Pkb managed, the benchmark spec needs to adopt vms.
    if service_type == spark_service.PKB_MANAGED:
      for name, spec in [('master_group', spark_spec.master_group),
                         ('worker_group', spark_spec.worker_group)]:
        if name in self.vms_to_boot:
          raise Exception('Cannot have a vm group {0} with a {1} spark '
                          'service'.format(name, spark_service.PKB_MANAGED))
        self.vms_to_boot[name] = spec

  def Prepare(self):
    targets = [(vm.PrepareBackgroundWorkload, (), {}) for vm in self.vms]
    vm_util.RunParallelThreads(targets, len(targets))

  def Provision(self):
    """Prepares the VMs and networks necessary for the benchmark to run."""
    # Create capacity reservations if the cloud supports it. Note that the
    # capacity reservation class may update the VMs themselves. This is true
    # on AWS, because the VM needs to be aware of the capacity reservation id
    # before its Create() method is called. Furthermore, if the user does not
    # specify an AWS zone, but a region instead, the AwsCapacityReservation
    # class will make a reservation in a zone that has sufficient capacity.
    # In this case the VM's zone attribute, and the VMs network instance
    # need to be updated as well.
    if self.capacity_reservations:
      vm_util.RunThreaded(lambda res: res.Create(), self.capacity_reservations)

    # Sort networks into a guaranteed order of creation based on dict key.
    # There is a finite limit on the number of threads that are created to
    # provision networks. Until support is added to provision resources in an
    # order based on dependencies, this key ordering can be used to avoid
    # deadlock by placing dependent networks later and their dependencies
    # earlier. As an example, AWS stores both per-region and per-zone objects
    # in this dict, and each per-zone object depends on a corresponding
    # per-region object, so the per-region objects are given keys that come
    # first when sorted.
    networks = [self.networks[key]
                for key in sorted(six.iterkeys(self.networks))]
    vm_util.RunThreaded(lambda net: net.Create(), networks)
    if self.container_registry:
      self.container_registry.Create()
      for container_spec in six.itervalues(self.container_specs):
        if container_spec.static_image:
          continue
        container_spec.image = self.container_registry.GetOrBuild(
            container_spec.image)

    if self.container_cluster:
      self.container_cluster.Create()

    # do after network setup but before VM created
    if self.nfs_service and self.nfs_service.CLOUD != nfs_service.UNMANAGED:
      self.nfs_service.Create()
    if self.smb_service:
      self.smb_service.Create()

    for placement_group_object in self.placement_groups.values():
      placement_group_object.Create()

    if self.vms:

      # We separate out creating, booting, and preparing the VMs into two phases
      # so that we don't slow down the creation of all the VMs by running
      # commands on the VMs that booted.
      vm_util.RunThreaded(
          self.CreateAndBootVm,
          self.vms,
          post_task_delay=FLAGS.create_and_boot_post_task_delay)
      if self.nfs_service and self.nfs_service.CLOUD == nfs_service.UNMANAGED:
        self.nfs_service.Create()
      vm_util.RunThreaded(self.PrepareVmAfterBoot, self.vms)

      sshable_vms = [
          vm for vm in self.vms if vm.OS_TYPE not in os_types.WINDOWS_OS_TYPES
      ]
      sshable_vm_groups = {}
      for group_name, group_vms in six.iteritems(self.vm_groups):
        sshable_vm_groups[group_name] = [
            vm for vm in group_vms
            if vm.OS_TYPE not in os_types.WINDOWS_OS_TYPES
        ]
      vm_util.GenerateSSHConfig(sshable_vms, sshable_vm_groups)
    if self.spark_service:
      self.spark_service.Create()
    if self.dpb_service:
      self.dpb_service.Create()
    if hasattr(self, 'relational_db') and self.relational_db:
      self.relational_db.SetVms(self.vm_groups)
      self.relational_db.Create()
    if self.tpus:
      vm_util.RunThreaded(lambda tpu: tpu.Create(), self.tpus)
    if self.edw_service:
      if (not self.edw_service.user_managed and
          self.edw_service.SERVICE_TYPE == 'redshift'):
        # The benchmark creates the Redshift cluster's subnet group in the
        # already provisioned virtual private cloud (vpc).
        for network in networks:
          if network.__class__.__name__ == 'AwsNetwork':
            self.edw_service.cluster_subnet_group.subnet_id = network.subnet.id
      self.edw_service.Create()

  def Delete(self):
    if self.deleted:
      return

    if self.container_registry:
      self.container_registry.Delete()
    if self.spark_service:
      self.spark_service.Delete()
    if self.dpb_service:
      self.dpb_service.Delete()
    if hasattr(self, 'relational_db') and self.relational_db:
      self.relational_db.Delete()
    if self.tpus:
      vm_util.RunThreaded(lambda tpu: tpu.Delete(), self.tpus)
    if self.edw_service:
      self.edw_service.Delete()
    if self.nfs_service:
      self.nfs_service.Delete()
    if self.smb_service:
      self.smb_service.Delete()

    # Note: It is ok to delete capacity reservations before deleting the VMs,
    # and will actually save money (mere seconds of usage).
    if self.capacity_reservations:
      try:
        vm_util.RunThreaded(lambda reservation: reservation.Delete(),
                            self.capacity_reservations)
      except Exception:  # pylint: disable=broad-except
        logging.exception('Got an exception deleting CapacityReservations. '
                          'Attempting to continue tearing down.')

    if self.vms:
      try:
        vm_util.RunThreaded(self.DeleteVm, self.vms)
      except Exception:
        logging.exception('Got an exception deleting VMs. '
                          'Attempting to continue tearing down.')
    if hasattr(self, 'placement_groups'):
      for placement_group_object in self.placement_groups.values():
        placement_group_object.Delete()

    for firewall in six.itervalues(self.firewalls):
      try:
        firewall.DisallowAllPorts()
      except Exception:
        logging.exception('Got an exception disabling firewalls. '
                          'Attempting to continue tearing down.')

    if self.container_cluster:
      self.container_cluster.DeleteServices()
      self.container_cluster.DeleteContainers()
      self.container_cluster.Delete()

    for net in six.itervalues(self.networks):
      try:
        net.Delete()
      except Exception:
        logging.exception('Got an exception deleting networks. '
                          'Attempting to continue tearing down.')

    self.deleted = True

  def GetSamples(self):
    """Returns samples created from benchmark resources."""
    samples = []
    if self.container_cluster:
      samples.extend(self.container_cluster.GetSamples())
    if self.container_registry:
      samples.extend(self.container_registry.GetSamples())
    return samples

  def StartBackgroundWorkload(self):
    targets = [(vm.StartBackgroundWorkload, (), {}) for vm in self.vms]
    vm_util.RunParallelThreads(targets, len(targets))

  def StopBackgroundWorkload(self):
    targets = [(vm.StopBackgroundWorkload, (), {}) for vm in self.vms]
    vm_util.RunParallelThreads(targets, len(targets))

  def _GetResourceDict(self, time_format, timeout_minutes=None):
    """Gets a list of tags to be used to tag resources."""
    now_utc = datetime.datetime.utcnow()

    if not timeout_minutes:
      timeout_minutes = FLAGS.timeout_minutes

    timeout_utc = (
        now_utc +
        datetime.timedelta(minutes=timeout_minutes))

    tags = {
        'timeout_utc': timeout_utc.strftime(time_format),
        'create_time_utc': now_utc.strftime(time_format),
        'benchmark': self.name,
        'perfkit_uuid': self.uuid,
        'owner': FLAGS.owner,
        'benchmark_uid': self.uid,
    }

    return tags

  def GetResourceTags(self, timeout_minutes=None):
    """Gets a list of tags to be used to tag resources."""
    return self._GetResourceDict(METADATA_TIME_FORMAT, timeout_minutes)

  def _CreatePlacementGroup(self, placement_group_spec, cloud):
    """Create a placement group in zone.

    Args:
      placement_group_spec: A placement_group.BasePlacementGroupSpec object.
      cloud: The cloud for the placement group.
          See the flag of the same name for more information.
    Returns:
      A placement_group.BasePlacementGroup object.
    """

    placement_group_class = placement_group.GetPlacementGroupClass(cloud)
    if placement_group_class:
      return placement_group_class(placement_group_spec)
    else:
      return None

  def _CreateVirtualMachine(self, vm_spec, os_type, cloud):
    """Create a vm in zone.

    Args:
      vm_spec: A virtual_machine.BaseVmSpec object.
      os_type: The type of operating system for the VM. See the flag of the
          same name for more information.
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
          'VMs of type %s" are not currently supported on cloud "%s".' %
          (os_type, cloud))

    return vm_class(vm_spec)

  def CreateAndBootVm(self, vm):
    """Creates a single VM and waits for boot to complete.

    Args:
        vm: The BaseVirtualMachine object representing the VM.
    """
    vm.Create()
    logging.info('VM: %s', vm.ip_address)
    logging.info('Waiting for boot completion.')
    vm.AllowRemoteAccessPorts()
    vm.WaitForBootCompletion()

  def PrepareVmAfterBoot(self, vm):
    """Prepares a VM after it has booted.

    This function will prepare a scratch disk if required.

    Args:
        vm: The BaseVirtualMachine object representing the VM.

    Raises:
        Exception: If --vm_metadata is malformed.
    """
    vm.AddMetadata()
    vm.OnStartup()
    # Prepare vm scratch disks:
    if any((spec.disk_type == disk.LOCAL for spec in vm.disk_specs)):
      vm.SetupLocalDisks()
    for disk_spec in vm.disk_specs:
      if disk_spec.disk_type == disk.RAM:
        vm.CreateRamDisk(disk_spec)
      else:
        vm.CreateScratchDisk(disk_spec)
      # TODO(user): Simplify disk logic.
      if disk_spec.num_striped_disks > 1:
        # scratch disks has already been created and striped together.
        break
    # This must come after Scratch Disk creation to support the
    # Containerized VM case
    vm.PrepareVMEnvironment()

  def DeleteVm(self, vm):
    """Deletes a single vm and scratch disk if required.

    Args:
        vm: The BaseVirtualMachine object representing the VM.
    """
    if vm.is_static and vm.install_packages:
      vm.PackageCleanup()
    vm.Delete()
    vm.DeleteScratchDisks()

  @staticmethod
  def _GetPickleFilename(uid):
    """Returns the filename for the pickled BenchmarkSpec."""
    return os.path.join(vm_util.GetTempDir(), uid)

  def Pickle(self):
    """Pickles the spec so that it can be unpickled on a subsequent run."""
    with open(self._GetPickleFilename(self.uid), 'wb') as pickle_file:
      pickle.dump(self, pickle_file, 2)

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
        spec = pickle.load(pickle_file)
    except Exception as e:  # pylint: disable=broad-except
      logging.error('Unable to unpickle spec file for benchmark %s.',
                    benchmark_module.BENCHMARK_NAME)
      raise e
    # Always let the spec be deleted after being unpickled so that
    # it's possible to run cleanup even if cleanup has already run.
    spec.deleted = False
    spec.status = benchmark_status.SKIPPED
    context.SetThreadBenchmarkSpec(spec)
    return spec
