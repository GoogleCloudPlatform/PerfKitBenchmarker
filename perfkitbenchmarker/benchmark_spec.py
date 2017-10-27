# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
import copy_reg
import importlib
import logging
import os
import pickle
import thread
import threading
import uuid

from perfkitbenchmarker import benchmark_status
from perfkitbenchmarker import cloud_tpu
from perfkitbenchmarker import container_service
from perfkitbenchmarker import context
from perfkitbenchmarker import disk
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import managed_relational_db
from perfkitbenchmarker import os_types
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import providers
from perfkitbenchmarker import spark_service
from perfkitbenchmarker import stages
from perfkitbenchmarker import static_virtual_machine as static_vm
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util


def PickleLock(lock):
  return UnPickleLock, (lock.locked(),)


def UnPickleLock(locked, *args):
  lock = threading.Lock()
  if locked:
    if not lock.acquire(False):
      raise pickle.UnpicklingError('Cannot acquire lock')
  return lock


copy_reg.pickle(thread.LockType, PickleLock)

SUPPORTED = 'strict'
NOT_EXCLUDED = 'permissive'
SKIP_CHECK = 'none'

FLAGS = flags.FLAGS

flags.DEFINE_enum('cloud', providers.GCP, providers.VALID_CLOUDS,
                  'Name of the cloud to use.')
flags.DEFINE_string('scratch_dir', None,
                    'Base name for all scratch disk directories in the VM. '
                    'Upon creation, these directories will have numbers '
                    'appended to them (for example /scratch0, /scratch1, etc).')
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
    BenchmarkSpec.total_benchmarks += 1
    self.sequence_number = BenchmarkSpec.total_benchmarks
    self.vms = []
    self.networks = {}
    self.firewalls = {}
    self.networks_lock = threading.Lock()
    self.firewalls_lock = threading.Lock()
    self.vm_groups = {}
    self.deleted = False
    self.uuid = '%s-%s' % (FLAGS.run_uri, uuid.uuid4())
    self.always_call_cleanup = False
    self.spark_service = None
    self.dpb_service = None
    self.container_cluster = None
    self.managed_relational_db = None
    self.cloud_tpu = None
    self.edw_service = None
    self._zone_index = 0

    # Modules can't be pickled, but functions can, so we store the functions
    # necessary to run the benchmark.
    self.BenchmarkPrepare = benchmark_module.Prepare
    self.BenchmarkRun = benchmark_module.Run
    self.BenchmarkCleanup = benchmark_module.Cleanup

    # Set the current thread's BenchmarkSpec object to this one.
    context.SetThreadBenchmarkSpec(self)

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
    providers.LoadProvider(cloud)
    container_cluster_class = container_service.GetContainerClusterClass(cloud)
    self.container_cluster = container_cluster_class(
        self.config.container_cluster)

  def ConstructDpbService(self):
    """Create the dpb_service object and create groups for its vms."""
    if self.config.dpb_service is None:
      return
    providers.LoadProvider(self.config.dpb_service.worker_group.cloud)
    dpb_service_class = dpb_service.GetDpbServiceClass(
        self.config.dpb_service.service_type)
    self.dpb_service = dpb_service_class(self.config.dpb_service)

  def ConstructManagedRelationalDb(self):
    """Create the managed relational db and create groups for its vms."""
    if self.config.managed_relational_db is None:
      return
    cloud = self.config.managed_relational_db.cloud
    providers.LoadProvider(cloud)
    managed_relational_db_class = (
        managed_relational_db.GetManagedRelationalDbClass(cloud))
    self.managed_relational_db = managed_relational_db_class(
        self.config.managed_relational_db)

  def ConstructCloudTpu(self):
    """Constructs the BenchmarkSpec's cloud TPU objects."""
    if self.config.cloud_tpu is None:
      return
    cloud = self.config.cloud_tpu.cloud
    providers.LoadProvider(cloud)
    cloud_tpu_class = cloud_tpu.GetCloudTpuClass(cloud)
    self.cloud_tpu = cloud_tpu_class(self.config.cloud_tpu)

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

    for _ in xrange(vm_count - len(vms)):
      # Assign a zone to each VM sequentially from the --zones flag.
      if FLAGS.zones or FLAGS.extra_zones:
        zone_list = FLAGS.zones + FLAGS.extra_zones
        group_spec.vm_spec.zone = zone_list[self._zone_index]
        self._zone_index = (self._zone_index + 1
                            if self._zone_index < len(zone_list) - 1 else 0)
      vm = self._CreateVirtualMachine(group_spec.vm_spec, os_type, cloud)
      if disk_spec and not vm.is_static:
        if disk_spec.disk_type == disk.LOCAL and disk_count is None:
          disk_count = vm.max_local_disks
        vm.disk_specs = [copy.copy(disk_spec) for _ in xrange(disk_count)]
        # In the event that we need to create multiple disks from the same
        # DiskSpec, we need to ensure that they have different mount points.
        if (disk_count > 1 and disk_spec.mount_point):
          for i, spec in enumerate(vm.disk_specs):
            spec.mount_point += str(i)
      vms.append(vm)

    return vms

  def _CheckBenchmarkSupport(self, cloud):
    """ Throw an exception if the benchmark isn't supported."""

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
    vm_group_specs = self.config.vm_groups

    clouds = {}
    for group_name, group_spec in sorted(vm_group_specs.iteritems()):
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
        if name in self.config.vm_groups:
          raise Exception('Cannot have a vm group {0} with a {1} spark '
                          'service'.format(name, spark_service.PKB_MANAGED))
        self.config.vm_groups[name] = spec

  def Prepare(self):
    targets = [(vm.PrepareBackgroundWorkload, (), {}) for vm in self.vms]
    vm_util.RunParallelThreads(targets, len(targets))

  def Provision(self):
    """Prepares the VMs and networks necessary for the benchmark to run."""
    # Sort networks into a guaranteed order of creation based on dict key.
    # There is a finite limit on the number of threads that are created to
    # provision networks. Until support is added to provision resources in an
    # order based on dependencies, this key ordering can be used to avoid
    # deadlock by placing dependent networks later and their dependencies
    # earlier. As an example, AWS stores both per-region and per-zone objects
    # in this dict, and each per-zone object depends on a corresponding
    # per-region object, so the per-region objects are given keys that come
    # first when sorted.
    networks = [self.networks[key] for key in sorted(self.networks.iterkeys())]
    vm_util.RunThreaded(lambda net: net.Create(), networks)

    if self.container_cluster:
      self.container_cluster.Create()

    if self.vms:
      vm_util.RunThreaded(self.PrepareVm, self.vms)
      sshable_vms = [vm for vm in self.vms if vm.OS_TYPE != os_types.WINDOWS]
      sshable_vm_groups = {}
      for group_name, group_vms in self.vm_groups.iteritems():
        sshable_vm_groups[group_name] = [
            vm for vm in group_vms if vm.OS_TYPE != os_types.WINDOWS
        ]
      vm_util.GenerateSSHConfig(sshable_vms, sshable_vm_groups)
    if self.spark_service:
      self.spark_service.Create()
    if self.dpb_service:
      self.dpb_service.Create()
    if self.managed_relational_db:
      self.managed_relational_db.client_vm = self.vms[0]
      self.managed_relational_db.Create()
    if self.cloud_tpu:
      self.cloud_tpu.Create()
    if self.edw_service:
      self.edw_service.Create()

  def Delete(self):
    if self.deleted:
      return

    if self.spark_service:
      self.spark_service.Delete()
    if self.dpb_service:
      self.dpb_service.Delete()
    if self.managed_relational_db:
      self.managed_relational_db.Delete()
    if self.cloud_tpu:
      self.cloud_tpu.Delete()
    if self.edw_service:
      self.edw_service.Delete()

    if self.vms:
      try:
        vm_util.RunThreaded(self.DeleteVm, self.vms)
      except Exception:
        logging.exception('Got an exception deleting VMs. '
                          'Attempting to continue tearing down.')

    for firewall in self.firewalls.itervalues():
      try:
        firewall.DisallowAllPorts()
      except Exception:
        logging.exception('Got an exception disabling firewalls. '
                          'Attempting to continue tearing down.')

    for net in self.networks.itervalues():
      try:
        net.Delete()
      except Exception:
        logging.exception('Got an exception deleting networks. '
                          'Attempting to continue tearing down.')
    if self.container_cluster:
      self.container_cluster.Delete()

    self.deleted = True

  def StartBackgroundWorkload(self):
    targets = [(vm.StartBackgroundWorkload, (), {}) for vm in self.vms]
    vm_util.RunParallelThreads(targets, len(targets))

  def StopBackgroundWorkload(self):
    targets = [(vm.StopBackgroundWorkload, (), {}) for vm in self.vms]
    vm_util.RunParallelThreads(targets, len(targets))

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

  def PrepareVm(self, vm):
    """Creates a single VM and prepares a scratch disk if required.

    Args:
        vm: The BaseVirtualMachine object representing the VM.
    """
    vm_metadata = {
        'benchmark': self.name,
        'perfkit_uuid': self.uuid,
        'benchmark_uid': self.uid
    }
    for item in FLAGS.vm_metadata:
      if ':' not in item:
        raise Exception('"%s" not in expected key:value format' % item)
      key, value = item.split(':', 1)
      vm_metadata[key] = value

    vm.Create()

    logging.info('VM: %s', vm.ip_address)
    logging.info('Waiting for boot completion.')
    vm.AllowRemoteAccessPorts()
    vm.WaitForBootCompletion()
    vm.AddMetadata(**vm_metadata)
    vm.OnStartup()
    if any((spec.disk_type == disk.LOCAL for spec in vm.disk_specs)):
      vm.SetupLocalDisks()
    for disk_spec in vm.disk_specs:
      vm.CreateScratchDisk(disk_spec)

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
