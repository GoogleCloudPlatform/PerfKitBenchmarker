# Copyright 2014 Google Inc. All rights reserved.
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

import logging
import pickle
import copy_reg
import thread
import threading

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import static_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.aws import aws_network
from perfkitbenchmarker.aws import aws_virtual_machine
from perfkitbenchmarker.azure import azure_network
from perfkitbenchmarker.azure import azure_virtual_machine
from perfkitbenchmarker.deployment.config import config_reader
import perfkitbenchmarker.deployment.shared.ini_constants as ini_constants
from perfkitbenchmarker.digitalocean import digitalocean_network
from perfkitbenchmarker.digitalocean import digitalocean_virtual_machine
from perfkitbenchmarker.gcp import gce_network
from perfkitbenchmarker.gcp import gce_virtual_machine as gce_vm
from perfkitbenchmarker.openstack import os_network as openstack_network
from perfkitbenchmarker.openstack import os_virtual_machine as openstack_vm
from perfkitbenchmarker.rackspace import rackspace_network as rax_net
from perfkitbenchmarker.rackspace import rackspace_virtual_machine as rax_vm


def PickleLock(lock):
    return UnPickleLock, (lock.locked(),)


def UnPickleLock(locked, *args):
    lock = threading.Lock()
    if locked:
        if not lock.acquire(False):
            raise pickle.UnpicklingError("Cannot acquire lock")
    return lock


copy_reg.pickle(thread.LockType, PickleLock)

GCP = 'GCP'
AZURE = 'Azure'
AWS = 'AWS'
DIGITALOCEAN = 'DigitalOcean'
OPENSTACK = 'OpenStack'
RACKSPACE = 'Rackspace'
DEBIAN = 'debian'
RHEL = 'rhel'
WINDOWS = 'windows'
UBUNTU_CONTAINER = 'ubuntu_container'
IMAGE = 'image'
WINDOWS_IMAGE = 'windows_image'
MACHINE_TYPE = 'machine_type'
ZONE = 'zone'
VIRTUAL_MACHINE = 'virtual_machine'
NETWORK = 'network'
FIREWALL = 'firewall'
CLASSES = {
    GCP: {
        VIRTUAL_MACHINE: {
            DEBIAN: gce_vm.DebianBasedGceVirtualMachine,
            RHEL: gce_vm.RhelBasedGceVirtualMachine,
            UBUNTU_CONTAINER: gce_vm.ContainerizedGceVirtualMachine,
            WINDOWS: gce_vm.WindowsGceVirtualMachine
        },
        FIREWALL: gce_network.GceFirewall
    },
    AZURE: {
        VIRTUAL_MACHINE: {
            DEBIAN: azure_virtual_machine.DebianBasedAzureVirtualMachine,
            RHEL: azure_virtual_machine.RhelBasedAzureVirtualMachine,
            WINDOWS: azure_virtual_machine.WindowsAzureVirtualMachine
        },
        FIREWALL: azure_network.AzureFirewall
    },
    AWS: {
        VIRTUAL_MACHINE: {
            DEBIAN: aws_virtual_machine.DebianBasedAwsVirtualMachine,
            RHEL: aws_virtual_machine.RhelBasedAwsVirtualMachine,
            WINDOWS: aws_virtual_machine.WindowsAwsVirtualMachine
        },
        FIREWALL: aws_network.AwsFirewall
    },
    DIGITALOCEAN: {
        VIRTUAL_MACHINE: {
            DEBIAN:
            digitalocean_virtual_machine.DebianBasedDigitalOceanVirtualMachine,
            RHEL:
            digitalocean_virtual_machine.RhelBasedDigitalOceanVirtualMachine,
        },
        FIREWALL: digitalocean_network.DigitalOceanFirewall
    },
    OPENSTACK: {
        VIRTUAL_MACHINE: {
            DEBIAN: openstack_vm.DebianBasedOpenStackVirtualMachine,
            RHEL: openstack_vm.OpenStackVirtualMachine
        },
        FIREWALL: openstack_network.OpenStackFirewall
    },
    RACKSPACE: {
        VIRTUAL_MACHINE: {
            DEBIAN: rax_vm.DebianBasedRackspaceVirtualMachine,
            RHEL: rax_vm.RhelBasedRackspaceVirtualMachine
        },
        FIREWALL: rax_net.RackspaceSecurityGroup
    }
}

FLAGS = flags.FLAGS

flags.DEFINE_enum('cloud', GCP,
                  [GCP, AZURE, AWS, DIGITALOCEAN, OPENSTACK, RACKSPACE],
                  'Name of the cloud to use.')
flags.DEFINE_enum(
    'os_type', DEBIAN, [DEBIAN, RHEL, UBUNTU_CONTAINER, WINDOWS],
    'The VM\'s OS type. Ubuntu\'s os_type is "debian" because it is largely '
    'built on Debian and uses the same package manager. Likewise, CentOS\'s '
    'os_type is "rhel". In general if two OS\'s use the same package manager, '
    'and are otherwise very similar, the same os_type should work on both of '
    'them.')
flags.DEFINE_string('scratch_dir', '/scratch',
                    'Base name for all scratch disk directories in the VM.'
                    'Upon creation, these directories will have numbers'
                    'appended to them (for example /scratch0, /scratch1, etc).')


class BenchmarkSpec(object):
  """Contains the various data required to make a benchmark run."""

  def __init__(self, benchmark_info):
    if (FLAGS.benchmark_config_pair and
        benchmark_info['name'] in FLAGS.benchmark_config_pair.keys()):
      # TODO(user): Unify naming between config_reader and
      # perfkitbenchmarker.
      self.config = config_reader.ConfigLoader(
          FLAGS.benchmark_config_pair[benchmark_info['name']])
    self.vms = []
    self.vm_dict = {'default': []}
    self.benchmark_name = benchmark_info['name']
    if hasattr(self, 'config'):
      config_dict = {}
      for section in self.config._config.sections():
        config_dict[section] = self.config.GetSectionOptionsAsDictionary(
            section)
      self.cloud = config_dict['cluster']['type']
      self.project = config_dict['cluster']['project']
      self.zones = [config_dict['cluster']['zone']]
      self.image = []
      self.machine_type = []
      for node in self.config.node_sections:
        self.vm_dict[node.split(':')[1]] = []
      args = [((config_dict[node],
                node.split(':')[1]), {}) for node in self.config.node_sections]
      vm_util.RunThreaded(
          self.CreateVirtualMachineFromNodeSection, args)
      self.num_vms = len(self.vms)
      self.image = ','.join(self.image)
      self.zones = ','.join(self.zones)
      self.machine_type = ','.join(self.machine_type)
    else:
      self.cloud = FLAGS.cloud
      self.project = FLAGS.project
      self.zones = FLAGS.zones
      self.image = FLAGS.image
      self.machine_type = FLAGS.machine_type
      if benchmark_info['num_machines'] is None:
        self.num_vms = FLAGS.num_vms
      else:
        self.num_vms = benchmark_info['num_machines']
      self.scratch_disk = benchmark_info['scratch_disk']
      self.scratch_disk_size = FLAGS.scratch_disk_size
      self.scratch_disk_type = FLAGS.scratch_disk_type
      self.scratch_disk_iops = FLAGS.scratch_disk_iops

      self.vms = [
          self.CreateVirtualMachine(
              self.zones[min(index, len(self.zones) - 1)])
          for index in range(self.num_vms)]
      self.vm_dict['default'] = self.vms
      for vm in self.vms:
        # If we are using local disks and num_striped_disks has not been
        # set, then we want to set it to stripe all local disks together.
        if (FLAGS.scratch_disk_type == disk.LOCAL and
            benchmark_info['scratch_disk'] and
            not FLAGS['num_striped_disks'].present):
          num_striped_disks = (vm.max_local_disks //
                               benchmark_info['scratch_disk'])
          if num_striped_disks == 0:
            raise errors.Error(
                'Not enough local disks to run benchmark "%s". It requires at '
                'least %d local disk(s). The specified machine type has %d '
                'local disk(s).' % (benchmark_info['name'],
                                    int(benchmark_info['scratch_disk']),
                                    vm.max_local_disks))
        else:
          num_striped_disks = FLAGS.num_striped_disks
        for i in range(benchmark_info['scratch_disk']):
          mount_point = '%s%d' % (FLAGS.scratch_dir, i)
          disk_spec = disk.BaseDiskSpec(
              self.scratch_disk_size, self.scratch_disk_type,
              mount_point, self.scratch_disk_iops,
              num_striped_disks)
          vm.disk_specs.append(disk_spec)

    firewall_class = CLASSES[self.cloud][FIREWALL]
    self.firewall = firewall_class(self.project)
    self.file_name = '%s/%s' % (vm_util.GetTempDir(), benchmark_info['name'])
    self.deleted = False
    self.always_call_cleanup = False

  def Prepare(self):
    """Prepares the VMs and networks necessary for the benchmark to run."""
    prepare_args = network.BaseNetwork.networks.values()
    vm_util.RunThreaded(self.PrepareNetwork, prepare_args)

    if self.vms:
      prepare_args = [((vm, self.firewall), {}) for vm in self.vms]
      vm_util.RunThreaded(self.PrepareVm, prepare_args)
      if FLAGS.os_type != WINDOWS:
        vm_util.GenerateSSHConfig(self.vms)

  def Delete(self):
    if FLAGS.run_stage not in ['all', 'cleanup'] or self.deleted:
      return

    if self.vms:
      try:
        vm_util.RunThreaded(self.DeleteVm, self.vms)
      except Exception:
        logging.exception('Got an exception deleting VMs. '
                          'Attempting to continue tearing down.')
    try:
      self.firewall.DisallowAllPorts()
    except Exception:
      logging.exception('Got an exception disabling firewalls. '
                        'Attempting to continue tearing down.')
    for net in network.BaseNetwork.networks.itervalues():
      try:
        net.Delete()
      except Exception:
        logging.exception('Got an exception deleting networks. '
                          'Attempting to continue tearing down.')
    self.deleted = True

  def PrepareNetwork(self, network):
    """Initialize the network."""
    network.Create()

  def CreateVirtualMachine(self, zone):
    """Create a vm in zone.

    Args:
      zone: The zone in which the vm will be created. If zone is None,
        the VM class's DEFAULT_ZONE will be used instead.
    Returns:
      A vm object.
    """
    vm = static_virtual_machine.StaticVirtualMachine.GetStaticVirtualMachine()
    if vm:
      return vm

    vm_classes = CLASSES[self.cloud][VIRTUAL_MACHINE]
    if FLAGS.os_type not in vm_classes:
      raise errors.Error(
          'VMs of type %s" are not currently supported on cloud "%s".' %
          (FLAGS.os_type, self.cloud))
    vm_class = vm_classes[FLAGS.os_type]

    vm_spec = virtual_machine.BaseVirtualMachineSpec(
        self.project, zone, self.machine_type, self.image)
    vm_class.SetVmSpecDefaults(vm_spec)

    return vm_class(vm_spec)

  def CreateVirtualMachineFromNodeSection(self, node_section, node_name):
    """Create a VirtualMachine object from NodeSection.

    Args:
      node_section: A dictionary of (option name, option value) pairs.
      node_name: The name of node.
    """
    zone = node_section['zone'] if 'zone' in node_section else self.zones[0]
    if zone not in self.zones:
      self.zones.append(zone)
    if node_section['image'] not in self.image:
      self.image.append(node_section['image'])
    if node_section['vm_type'] not in self.machine_type:
      self.machine_type.append(node_section['vm_type'])
    if zone not in self.networks:
      network_class = CLASSES[self.cloud][NETWORK]
      self.networks[zone] = network_class(zone)
    vm_spec = virtual_machine.BaseVirtualMachineSpec(
        self.project,
        zone,
        node_section['vm_type'],
        node_section['image'],
        self.networks[zone])
    vm_class = CLASSES[self.cloud][VIRTUAL_MACHINE]
    vms = [vm_class(vm_spec) for _ in range(int(node_section['count']))]
    self.vms.extend(vms)
    self.vm_dict[node_name].extend(vms)
    # Create disk spec.
    for option in node_section:
      if option.startswith(ini_constants.OPTION_PD_PREFIX):
        # Create disk spec.
        disk_size, disk_type, mnt_point = node_section[option].split(':')
        disk_size = int(disk_size)
        disk_spec = disk.BaseDiskSpec(
            disk_size, disk_type, mnt_point)
        for vm in vms:
          vm.disk_specs.append(disk_spec)

  def PrepareVm(self, vm, firewall):
    """Creates a single VM and prepares a scratch disk if required.

    Args:
        vm: The BaseVirtualMachine object representing the VM.
        firewall: The BaseFirewall object representing the firewall.
    """
    vm.Create()
    logging.info('VM: %s', vm.ip_address)
    logging.info('Waiting for boot completion.')
    for port in vm.remote_access_ports:
      firewall.AllowPort(vm, port)
    vm.AddMetadata(benchmark=self.benchmark_name)
    vm.WaitForBootCompletion()
    vm.OnStartup()
    if FLAGS.scratch_disk_type == disk.LOCAL:
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

  def PickleSpec(self):
    """Pickles the spec so that it can be unpickled on a subsequent run."""
    self.networks = network.BaseNetwork.networks
    with open(self.file_name, 'wb') as pickle_file:
      pickle.dump(self, pickle_file, 2)

  @classmethod
  def GetSpecFromFile(cls, name):
    """Unpickles the spec and returns it.

    Args:
      name: The name of the benchmark (and the name of the pickled file).

    Returns:
      A BenchmarkSpec object.
    """
    file_name = '%s/%s' % (vm_util.GetTempDir(), name)
    try:
      with open(file_name, 'rb') as pickle_file:
        spec = pickle.load(pickle_file)
    except Exception as e:  # pylint: disable=broad-except
      logging.error('Unable to unpickle spec file for benchmark %s.', name)
      raise e
    network.BaseNetwork.networks = spec.networks
    # Always let the spec be deleted after being unpickled so that
    # it's possible to run cleanup even if cleanup has already run.
    spec.deleted = False
    return spec
