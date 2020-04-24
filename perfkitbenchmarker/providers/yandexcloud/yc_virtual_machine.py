# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Class to represent a YC Virtual Machine object.

Zones:
run 'yc compute zones list'
Images:
run 'yc compute images list'

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""

import collections
import copy
import itertools
import json
import logging
import posixpath
import re
import threading
import pprint

from perfkitbenchmarker import custom_virtual_machine_spec
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine as linux_vm
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_virtual_machine
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.providers.yandexcloud import flags as yc_flags
from perfkitbenchmarker.providers.yandexcloud import yc_disk
from perfkitbenchmarker.providers.yandexcloud import yc_network
from perfkitbenchmarker.providers.yandexcloud import util

import yaml

FLAGS = flags.FLAGS

UBUNTU_IMAGE = 'ubuntu-1804-lts'
_YC_VM_CREATE_TIMEOUT = 600


class YcUnexpectedWindowsAdapterOutputError(Exception):
  """Raised when querying the status of a windows adapter failed."""


class YcVmSpec(virtual_machine.BaseVmSpec):
  """Object containing the information needed to create a YcVirtualMachine.

  Attributes:
    cores: None or int. Number of vCPUs for custom VMs.
    core_fraction: None or int. If provided, specifies baseline performance for a core in percent.
    memory: None or int. For custom VMs, a string representation of the size
        of memory, expressed in GiB. Must be an integer number.
    preemptible: boolean. True if the VM should be preemptible, False otherwise.
    folder_id: string or None. The folder to create the VM in.
    image_family: string or None. The image family used to locate the image.
    image_folder_id: string or None. The image folder used to locate the
        specifed image.
    boot_disk_size: None or int. The size of the boot disk in GB.
    boot_disk_type: string or None. The type of the boot disk.
  """

  CLOUD = providers.YC

  def __init__(self, *args, **kwargs):
    super(YcVmSpec, self).__init__(*args, **kwargs)
    self.cores = None
    self.memory = None
    self.core_fraction = None

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. May
          be modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
          provided config values.
    """
    super(YcVmSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['machine_type'].present:
      config_values['machine_type'] = flag_values.machine_type
    if flag_values['yc_preemptible_vms'].present:
      config_values['preemptible'] = flag_values.yc_preemptible_vms
    if flag_values['yc_platform_id'].present:
      config_values['platform_id'] = flag_values.yc_platform_id
    if flag_values['yc_boot_disk_size'].present:
      config_values['boot_disk_size'] = flag_values.yc_boot_disk_size
    if flag_values['yc_boot_disk_type'].present:
      config_values['boot_disk_type'] = flag_values.yc_boot_disk_type
    if flag_values['yc_core_fraction'].present:
      config_values['core_fraction'] = flag_values.yc_core_fraction
    if flag_values['yc_folder_id'].present:
      config_values['folder_id'] = flag_values.yc_folder_id
    if flag_values['yc_image_family'].present:
      config_values['image_family'] = flag_values.yc_image_family
    if flag_values['yc_image_folder_id'].present:
      config_values['image_folder_id'] = flag_values.yc_image_folder_id

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(YcVmSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'preemptible': (option_decoders.BooleanDecoder, {'default': False}),
        'boot_disk_size': (option_decoders.IntDecoder, {'default': None}),
        'boot_disk_type': (option_decoders.StringDecoder, {'default': None}),
        'folder_id': (option_decoders.StringDecoder, {'default': None}),
        'image_family': (option_decoders.StringDecoder, {'default': None}),
        'image_folder_id': (option_decoders.StringDecoder, {'default': None}),
        'core_fraction': (option_decoders.IntDecoder, {'default': None}),
        'platform_id': (option_decoders.StringDecoder, {'default': None}),
    })
    return result


class YcVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a Yandex Cloud Virtual Machine."""

  CLOUD = providers.YC
  # Subclasses should override the default image OR
  # both the image family and image_folder_id.
  DEFAULT_IMAGE = None
  DEFAULT_IMAGE_FAMILY = None
  DEFAULT_IMAGE_FOLDER_ID = "standard-images"
  DEFAULT_USER_NAME = "yc-user"
  BOOT_DISK_SIZE_GB = 10
  BOOT_DISK_TYPE = yc_disk.NET_HDD

  _host_lock = threading.Lock()
  deleted_hosts = set()
  host_map = collections.defaultdict(list)

  def __init__(self, vm_spec):
    """Initialize a YC virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVmSpec object of the vm.

    Raises:
      errors.Config.MissingOption: If the spec does not include both "cores" and "memory"
    """
    super(YcVirtualMachine, self).__init__(vm_spec)
    self.user_name = self.DEFAULT_USER_NAME
    self.boot_metadata = {}
    self.image = self.image or self.DEFAULT_IMAGE
    self.preemptible = vm_spec.preemptible
    self.core_fraction = vm_spec.core_fraction
    self.platform_id = vm_spec.platform_id
    self.machine_type = vm_spec.machine_type
    self.memory, self.cores = util.ReturnFlavor(self.machine_type)
    self.early_termination = False
    self.preemptible_status_code = None
    self.folder_id = vm_spec.folder_id or util.GetDefaultFolderId()
    self.image_family = vm_spec.image_family or self.DEFAULT_IMAGE_FAMILY
    self.image_folder_id = vm_spec.image_folder_id or self.DEFAULT_IMAGE_FOLDER_ID
    self.backfill_image = False
    self.network = yc_network.YcNetwork.GetNetwork(self)
    self.boot_disk_size = vm_spec.boot_disk_size
    self.boot_disk_type = vm_spec.boot_disk_type
    self.id = None

    if not self.cores or not self.memory:
        raise errors.Config.MissingOption

    if self.preemptible and not self.core_fraction:
        raise errors.Config.MissingOption

  @property
  def host_list(self):
    """Returns the list of hosts that are compatible with this VM."""
    return self.host_map[(self.folder_id, self.zone)]

  def _GenerateCreateCommand(self, public_key):
    """Generates a command to create the VM instance.

    Args:
      ssh_keys_path: string. Path to a file containing the ssh-keys metadata.

    Returns:
      YcCommand. yc command to issue in order to create the VM instance.
    """
    boot_disk = {
        'auto-delete': True
    }
    boot_disk['name'] = self.name
    boot_disk['size'] = self.boot_disk_size or self.BOOT_DISK_SIZE_GB
    boot_disk['type'] = self.boot_disk_type or self.BOOT_DISK_TYPE
    if self.image:
      boot_disk['image-name'] = self.image_name
    if self.image_family:
      boot_disk['image-family'] = self.image_family
    if self.image_folder_id:
      boot_disk['image-folder-id'] = self.image_folder_id

    args = []
    args.extend(['compute', 'instances', 'create'])

    cmd = util.YcCommand(self, *args)
    cmd.flags['name'] = self.name
    cmd.flags['network-interface'] = util.FormatTags({
        'subnet-name': self.network.subnet_resource.name,
        'nat-ip-version': 'ipv4',
    })
    if boot_disk:
      cmd.flags['create-boot-disk'] = util.FormatTags(boot_disk)
    if self.platform_id:
      cmd.flags['platform-id'] = self.platform_id
    cmd.flags['cores'] = self.cores
    cmd.flags['memory'] = self.memory
    cmd.flags['zone'] = self.zone
    cmd.flags['ssh-key'] = public_key

    metadata_from_file = flag_util.ParseKeyValuePairs(
        FLAGS.yc_instance_metadata_from_file)
    cmd.flags['metadata-from-file'] = util.FormatTags(metadata_from_file)

    metadata = {'owner': FLAGS.owner} if FLAGS.owner else {}
    metadata.update(self.boot_metadata)
    parsed_metadata = flag_util.ParseKeyValuePairs(FLAGS.yc_instance_metadata)
    for key, value in parsed_metadata.items():
      if key in metadata:
        logging.warning('Metadata "%s" is set internally. Cannot be overridden '
                        'from command line.', key)
        continue
      metadata[key] = value
    cmd.flags['metadata'] = util.FormatTags(metadata)

    if self.preemptible:
      cmd.flags['preemptible'] = True
    return cmd

  def _Create(self):
    """Create a YC VM instance."""
    with open(self.ssh_public_key) as f:
      public_key = f.read().rstrip('\n')
    with vm_util.NamedTemporaryFile(mode='w', dir=vm_util.GetTempDir(),
                                    prefix='key-metadata') as tf:
      tf.write(public_key)
      tf.close()
      create_cmd = self._GenerateCreateCommand(tf.name)
      _, stderr, retcode = create_cmd.Issue(timeout=_YC_VM_CREATE_TIMEOUT)

    util.CheckYcResponseKnownFailures(stderr, retcode)

  def _CreateDependencies(self):
    super(YcVirtualMachine, self)._CreateDependencies()

  def _ParseDescribeResponse(self, describe_response):
    """Sets the ID and IP addresses from a response to the describe command.

    Args:
      describe_response: JSON-loaded response to the describe yc command.

    Raises:
      KeyError, IndexError: If the ID and IP addresses cannot be parsed.
    """
    self.id = describe_response['id']
    network_interface = describe_response['network_interfaces'][0]
    primary_address = network_interface['primary_v4_address']
    self.internal_ip = primary_address['address']
    self.ip_address = primary_address['one_to_one_nat']['address']

  def _NeedsToParseDescribeResponse(self):
    """Returns whether the ID and IP addresses still need to be set."""
    return not self.id or not self.internal_ip or not self.ip_address

  @vm_util.Retry()
  def _PostCreate(self):
    """Get the instance's data."""
    if self._NeedsToParseDescribeResponse():
      getinstance_cmd = util.YcCommand(self, 'compute', 'instances',
                                       'describe', self.name)
      stdout, _, _ = getinstance_cmd.Issue()
      response = json.loads(stdout)
      self._ParseDescribeResponse(response)
    if not self.image:
      getdisk_cmd = util.YcCommand(
          self, 'compute', 'disks', 'describe', self.name)
      stdout, _, _ = getdisk_cmd.Issue()
      response = json.loads(stdout)
      self.image = response['source_image_id']
      self.backfill_image = True

  def _Delete(self):
    """Delete a YC VM instance."""
    delete_cmd = util.YcCommand(self, 'compute', 'instances', 'delete',
                                self.name)
    delete_cmd.Issue()

  def _Exists(self):
    """Returns true if the VM exists."""
    getinstance_cmd = util.YcCommand(self, 'compute', 'instances',
                                     'describe', self.name)
    stdout, _, _ = getinstance_cmd.Issue(suppress_warning=True, raise_on_failure=False)
    try:
      response = json.loads(stdout)
    except ValueError:
      return False
    try:
      # The VM may exist before we can fully parse the describe response for the
      # IP address or ID of the VM. For example, if the VM has a status of
      # provisioning, we can't yet parse the IP address. If this is the case, we
      # will continue to invoke the describe command in _PostCreate above.
      # However, if we do have this information now, it's better to stash it and
      # avoid invoking the describe command again.
      self._ParseDescribeResponse(response)
    except (KeyError, IndexError):
      pass
    return True

  def ShouldDownloadPreprovisionedData(self, module_name, filename):
    """Returns whether or not preprovisioned data is available."""
    return False

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    disks = []

    for i in range(disk_spec.num_striped_disks):
      name = '%s-data-%d-%d' % (self.name, len(self.scratch_disks), i)
      data_disk = yc_disk.YcDisk(disk_spec, name, self.zone, self.folder_id)
      data_disk.disk_number = self.remote_disk_counter + 1
      self.remote_disk_counter += 1
      disks.append(data_disk)

    self._CreateScratchDiskFromDisks(disk_spec, disks)

  def AddMetadata(self, **kwargs):
    """Adds metadata to the VM and disk."""
    if not kwargs:
      return
    cmd = util.YcCommand(self, 'compute', 'instances', 'add-metadata',
                         self.name)
    cmd.flags['metadata'] = util.MakeFormattedDefaultTags()
    if kwargs:
      cmd.flags['metadata'] = '{metadata},{kwargs}'.format(
          metadata=cmd.flags['metadata'],
          kwargs=util.FormatTags(kwargs))
    cmd.Issue()

    cmd = util.YcCommand(
        self, 'compute', 'disks', 'add-labels',
        '--labels={}'.format(util.MakeFormattedDefaultTags()), self.name)
    cmd.Issue()

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the VM.

    Returns:
      dict mapping string property key to value.
    """
    result = super(YcVirtualMachine, self).GetResourceMetadata()
    for attr_name in 'cores', 'memory', 'preemptible', 'folder_id':
      attr_value = getattr(self, attr_name)
      if attr_value:
        result[attr_name] = attr_value
    # Only record image_family flag when it is used in vm creation command.
    # Note, when using non-debian/ubuntu based custom images, user will need
    # to use --os_type flag. In that case, we do not want to
    # record image_family in metadata.
    if self.backfill_image and self.image_family:
      result['image_family'] = self.image_family
    if self.image_folder_id:
      result['image_folder_id'] = self.image_folder_id
    return result

  def SimulateMaintenanceEvent(self):
    """Simulates a maintenance event on the VM."""
    cmd = util.YcCommand(self, 'alpha', 'compute', 'instances',
                         'simulate-maintenance-event', self.name)
    _, _, retcode = cmd.Issue()
    if retcode:
      raise errors.VirtualMachine.VirtualMachineError(
          'Unable to simulate maintenance event.')

  @vm_util.Retry(max_retries=5)
  def UpdateInterruptibleVmStatus(self):
    """Updates the interruptible status if the VM was preempted."""
    if self.preemptible:
      yc_command = util.YcCommand(
          'compute', 'instance', 'list-operations',
          '--filter=zone:%s targetLink.scope():%s' % (self.zone, self.name))
      stdout, _, _ = yc_command.Issue()
      self.early_termination = any(
          operation['operationType'] == 'compute.instances.preempted'
          for operation in json.loads(stdout))

  def IsInterruptible(self):
    """Returns whether this vm is an interruptible vm (spot vm).

    Returns: True if this vm is an interruptible vm (spot vm).
    """
    return self.preemptible

  def WasInterrupted(self):
    """Returns whether this spot vm was terminated early by YC.

    Returns: True if this vm was terminated early by YC.
    """
    return self.early_termination

  def GetVmStatusCode(self):
    """Returns the early termination code if any.

    Returns: Early termination code.
    """
    return self.preemptible_status_code


class ContainerizedYcVirtualMachine(YcVirtualMachine,
                                    linux_vm.ContainerizedDebianMixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE


class CentOs7BasedYcVirtualMachine(YcVirtualMachine,
                                   linux_vm.CentOs7Mixin):
  DEFAULT_IMAGE_FAMILY = 'centos-7'


class CentOs8BasedYcVirtualMachine(YcVirtualMachine,
                                   linux_vm.CentOs8Mixin):
  DEFAULT_IMAGE_FAMILY = 'centos-8'


class Debian9BasedYcVirtualMachine(YcVirtualMachine,
                                   linux_vm.Debian9Mixin):
  DEFAULT_IMAGE_FAMILY = 'debian-9'


class Debian10BasedYcVirtualMachine(YcVirtualMachine,
                                    linux_vm.Debian10Mixin):
  DEFAULT_IMAGE_FAMILY = 'debian-10'


class Ubuntu1604BasedYcVirtualMachine(YcVirtualMachine,
                                      linux_vm.Ubuntu1604Mixin):
  DEFAULT_IMAGE_FAMILY = 'ubuntu-1604-lts'


class Ubuntu1804BasedYcVirtualMachine(YcVirtualMachine,
                                      linux_vm.Ubuntu1804Mixin):
  DEFAULT_IMAGE_FAMILY = 'ubuntu-1804-lts'
