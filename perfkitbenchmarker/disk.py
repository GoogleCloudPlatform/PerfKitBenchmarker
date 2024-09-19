# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing abstract classes related to disks.

Disks can be created, deleted, attached to VMs, and detached from VMs.
"""

import abc
import time
from typing import List

from absl import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec

flags.DEFINE_boolean(
    'nfs_timeout_hard', True, 'Whether to use hard or soft for NFS mount.'
)
flags.DEFINE_integer('nfs_rsize', 1048576, 'NFS read size.')
flags.DEFINE_integer('nfs_wsize', 1048576, 'NFS write size.')
flags.DEFINE_integer('nfs_timeout', 60, 'NFS timeout.')
flags.DEFINE_integer('nfs_retries', 2, 'NFS Retries.')
flags.DEFINE_integer(
    'nfs_nconnect',
    None,
    'Number of connections that each NFS client should '
    'establish to the server.',
)
flags.DEFINE_boolean(
    'nfs_noresvport',
    False,
    'Whether the NFS client should use a non-privileged '
    'source port. Suggested to use with EFS',
)
flags.DEFINE_boolean(
    'nfs_managed',
    True,
    'Use a managed NFS service if using NFS disks. Otherwise '
    'start an NFS server on the first VM.',
)
flags.DEFINE_string(
    'nfs_ip_address',
    None,
    'If specified, PKB will target this ip address when '
    'mounting NFS "disks" rather than provisioning an NFS '
    'Service for the corresponding cloud.',
)
flags.DEFINE_string(
    'nfs_directory',
    None,
    'Directory to mount if using a StaticNfsService. This '
    'corresponds to the "VOLUME_NAME" of other NfsService '
    'classes.',
)
flags.DEFINE_string('smb_version', '3.0', 'SMB version.')
flags.DEFINE_list(
    'mount_options', [], 'Additional arguments to supply when mounting.'
)
flags.DEFINE_list(
    'fstab_options', [], 'Additional arguments to supply to fstab.'
)
flags.DEFINE_integer(
    'provisioned_iops',
    None,
    'Iops to provision, if applicable. Defaults to None.',
)
flags.DEFINE_bool('multi_writer_mode', False, 'Multi-writer mode.')
flags.DEFINE_integer(
    'provisioned_throughput',
    None,
    'Throughput to provision, if applicable. Defaults to None.',
)


FLAGS = flags.FLAGS

# These are the (deprecated) old disk type names
STANDARD = 'standard'
REMOTE_SSD = 'remote_ssd'
PIOPS = 'piops'  # Provisioned IOPS (SSD) in AWS and Alicloud
REMOTE_ESSD = 'remote_essd'  # Enhanced Cloud SSD in Alicloud

# 'local' refers to disks that come attached to VMs. It is the only
# "universal" disk type that is not associated with a provider. It
# exists because we can provision a local disk without creating a disk
# spec. The Aerospike benchmarks use this fact in their config
# methods, and they need to be able to tell when a disk is local. So
# until that changes, 'local' is a special disk type.
LOCAL = 'local'

RAM = 'ram'

# refers to disks that come from a cloud/unmanaged NFS or SMB service
NFS = 'nfs'
SMB = 'smb'

# FUSE mounted object storage bucket
OBJECT_STORAGE = 'object_storage'

# Standard metadata keys relating to disks
MEDIA = 'media'
REPLICATION = 'replication'
# And some possible values
HDD = 'hdd'
SSD = 'ssd'
NONE = 'none'
ZONE = 'zone'
REGION = 'region'

DEFAULT_MOUNT_OPTIONS = 'discard'
DEFAULT_FSTAB_OPTIONS = 'defaults'


def GetDiskSpecClass(cloud, disk_type):
  """Get the DiskSpec class corresponding to 'cloud'."""
  spec_class = spec.GetSpecClass(BaseDiskSpec, CLOUD=cloud, DISK_TYPE=disk_type)
  # GetSpecClass will return base spec if failed
  if spec_class is BaseDiskSpec:
    return spec.GetSpecClass(BaseDiskSpec, CLOUD=cloud)
  return spec_class


def IsRemoteDisk(disk_type):
  return disk_type not in [LOCAL, NFS, SMB, RAM]


class BaseDiskSpec(spec.BaseSpec):
  """Stores the information needed to create a disk.

  Attributes:
    device_path: None or string. Path on the machine where the disk is located.
    disk_number: None or int. Optional disk identifier unique within the current
      machine.
    disk_size: None or int. Size of the disk in GB.
    disk_type: None or string. See cloud specific disk classes for more
      information about acceptable values.
    mount_point: None or string. Directory of mount point.
    num_striped_disks: int. The number of disks to stripe together. If this is
      1, it means no striping will occur. This must be >= 1.
  """

  SPEC_TYPE = 'BaseDiskSpec'
  CLOUD = None

  def __init__(self, *args, **kwargs):
    self.device_path: str = None
    self.mount_point: str = None
    self.disk_type: str = None
    self.disk_size: int = None
    self.num_striped_disks: int = None
    self.num_partitions = None
    self.partition_size = None
    self.provisioned_iops = None
    self.provisioned_throughput = None
    self.multi_writer_mode: bool = False
    self.multi_writer_group_name: str = None
    super().__init__(*args, **kwargs)

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Overrides config values with flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. Is
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.

    Returns:
      dict mapping config option names to values derived from the config
      values or flag values.
    """
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['data_disk_size'].present:
      config_values['disk_size'] = flag_values.data_disk_size
    if flag_values['data_disk_type'].present:
      config_values['disk_type'] = flag_values.data_disk_type
    if flag_values['num_striped_disks'].present:
      config_values['num_striped_disks'] = flag_values.num_striped_disks
    if flag_values['scratch_dir'].present:
      config_values['mount_point'] = flag_values.scratch_dir
    if flag_values['num_partitions'].present:
      config_values['num_partitions'] = flag_values.num_partitions
    if flag_values['partition_size'].present:
      config_values['partition_size'] = flag_values.partition_size
    if flag_values['provisioned_iops'].present:
      config_values['provisioned_iops'] = flag_values.provisioned_iops
    if flag_values['provisioned_throughput'].present:
      config_values['provisioned_throughput'] = (
          flag_values.provisioned_throughput
      )
    if flag_values['multi_writer_mode'].present:
      config_values['multi_writer_mode'] = flag_values.multi_writer_mode

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Can be overridden by derived classes to add options or impose additional
    requirements on existing options.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'device_path': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'disk_number': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True},
        ),
        'disk_size': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True},
        ),
        'disk_type': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'mount_point': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'num_striped_disks': (
            option_decoders.IntDecoder,
            {'default': 1, 'min': 1},
        ),
        'num_partitions': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True},
        ),
        'partition_size': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'provisioned_iops': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True},
        ),
        'provisioned_throughput': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True},
        ),
        'multi_writer_mode': (
            option_decoders.BooleanDecoder,
            {'default': False, 'none_ok': True},
        ),
        'multi_writer_group_name': (
            option_decoders.StringDecoder,
            {'default': False, 'none_ok': True},
        ),
    })
    return result


class BaseNFSDiskSpec(BaseDiskSpec):
  """Stores the information needed to create a base NFT Disk."""

  SPEC_TYPE = 'BaseDiskSpec'
  CLOUD = None
  DISK_TYPE = NFS
  SPEC_ATTRS = ['CLOUD', 'DISK_TYPE']

  def __init__(self, *args, **kwargs):
    self.device_path: str = None
    self.mount_point: str = None
    super().__init__(*args, **kwargs)
    self.disk_type = NFS

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Overrides config values with flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. Is
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.

    Returns:
      dict mapping config option names to values derived from the config
      values or flag values.
    """
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['nfs_version'].present:
      config_values['nfs_version'] = flag_values.nfs_version
    if flag_values['nfs_timeout_hard'].present:
      config_values['nfs_timeout_hard'] = flag_values.nfs_timeout_hard
    if flag_values['nfs_rsize'].present:
      config_values['nfs_rsize'] = flag_values.nfs_rsize
    if flag_values['nfs_wsize'].present:
      config_values['nfs_wsize'] = flag_values.nfs_wsize
    if flag_values['nfs_timeout'].present:
      config_values['nfs_timeout'] = flag_values.nfs_timeout
    if flag_values['nfs_retries'].present:
      config_values['nfs_retries'] = flag_values.nfs_retries
    if flag_values['nfs_nconnect'].present:
      config_values['nfs_nconnect'] = flag_values.nfs_nconnect
    if flag_values['nfs_ip_address'].present:
      config_values['nfs_ip_address'] = flag_values.nfs_ip_address
    if flag_values['nfs_managed'].present:
      config_values['nfs_managed'] = flag_values.nfs_managed
    if flag_values['nfs_directory'].present:
      config_values['nfs_directory'] = flag_values.nfs_directory

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Can be overridden by derived classes to add options or impose additional
    requirements on existing options.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'nfs_version': (option_decoders.StringDecoder, {'default': None}),
        'nfs_ip_address': (option_decoders.StringDecoder, {'default': None}),
        'nfs_managed': (option_decoders.BooleanDecoder, {'default': True}),
        'nfs_directory': (option_decoders.StringDecoder, {'default': None}),
        'nfs_rsize': (option_decoders.IntDecoder, {'default': 1048576}),
        'nfs_wsize': (option_decoders.IntDecoder, {'default': 1048576}),
        'nfs_timeout': (option_decoders.IntDecoder, {'default': 60}),
        'nfs_timeout_hard': (option_decoders.BooleanDecoder, {'default': True}),
        'nfs_retries': (option_decoders.IntDecoder, {'default': 2}),
        'nfs_nconnect': (option_decoders.IntDecoder, {'default': None}),
    })
    return result


class BaseSMBDiskSpec(BaseDiskSpec):
  """Stores the information needed to create a SMB Disk."""

  SPEC_TYPE = 'BaseDiskSpec'
  CLOUD = None
  DISK_TYPE = SMB
  SPEC_ATTRS = ['CLOUD', 'DISK_TYPE']

  def __init__(self, *args, **kwargs):
    self.device_path: str = None
    self.mount_point: str = None
    super().__init__(*args, **kwargs)

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Overrides config values with flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. Is
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.

    Returns:
      dict mapping config option names to values derived from the config
      values or flag values.
    """
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['smb_version'].present:
      config_values['smb_version'] = flag_values.smb_version

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Can be overridden by derived classes to add options or impose additional
    requirements on existing options.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'smb_version': (option_decoders.StringDecoder, {'default': '3.0'}),
    })
    return result


class BaseDisk(resource.BaseResource):
  """Object representing a Base Disk.

  attach_start_time: time when we start the disk attach to vm
  attach_end_time: time when disk attach to vm is done
  detach_start_time: time when we start the disk detach from vm
  detach_end_time: time when disk detach from vm is done
  create_disk_start_time: time when start disk creation command
  create_disk_end_time: time when we end disk creation command
  """

  is_striped = False

  def __init__(self, disk_spec):
    super().__init__()
    self.disk_size = disk_spec.disk_size
    self.disk_type = disk_spec.disk_type
    self.mount_point = disk_spec.mount_point
    self.num_striped_disks = disk_spec.num_striped_disks
    self.num_partitions = disk_spec.num_partitions
    self.partition_size = disk_spec.partition_size
    self.multi_writer_disk: bool = disk_spec.multi_writer_mode
    self.metadata.update({
        'type': self.disk_type,
        'size': self.disk_size,
        'num_stripes': self.num_striped_disks,
    })

    # Set in derived classes by Attach()
    self.vm = None

    # Linux related attributes.
    self.device_path = disk_spec.device_path

    # Windows related attributes.

    # The disk number corresponds to the order in which disks were attached to
    # the instance. The System Disk has a disk number of 0. Any local disks
    # have disk numbers ranging from 1 to the number of local disks on the
    # system. Any additional disks that were attached after boot will have
    # disk numbers starting at the number of local disks + 1. These disk
    # numbers are used in diskpart scripts in order to identify the disks
    # that we want to operate on.
    self.disk_number = disk_spec.disk_number
    self.attach_start_time: float = None
    self.attach_end_time: float = None
    self.detach_start_time: float = None
    self.detach_end_time: float = None
    self.create_disk_start_time: float = None
    self.create_disk_end_time: float = None
    self.disk_create_time: float = None
    self.disk_detach_time: float = None
    self.time_to_visibility: float = None

  @property
  def mount_options(self):
    """Returns options to mount a disk.

    The default value 'discard' is from the linux VM's MountDisk method.

    See `man 8 mount` for usage.  For example, returning "ro" will cause the
    mount command to be "mount ... -o ro ..." mounting the disk as read only.
    """
    opts = DEFAULT_MOUNT_OPTIONS
    if FLAGS.mount_options:
      opts = ','.join(FLAGS.mount_options)
    self.metadata.update({'mount_options': opts})
    return opts

  @property
  def fstab_options(self):
    """Returns options to use in the /etc/fstab entry for this drive.

    The default value 'defaults' is from the linux VM's MountDisk method.

    See `man fstab` for usage.
    """
    opts = DEFAULT_FSTAB_OPTIONS
    if FLAGS.fstab_options:
      opts = ','.join(FLAGS.fstab_options)
    self.metadata.update({'fstab_options': opts})
    return opts

  def Attach(self, vm):
    """Attaches the disk to a VM and calculates the attach time.

    Args:
      vm: The BaseVirtualMachine instance to which the disk will be attached.
    """
    self._Attach(vm)

  def _Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: The BaseVirtualMachine instance to which the disk will be attached.
    """
    pass

  def Detach(self):
    """Detaches the disk from a VM."""
    self.detach_start_time = time.time()
    self._Detach()
    self.detach_end_time = self.detach_end_time or time.time()

  def _Detach(self):
    """Detaches the disk from a VM."""
    pass

  def GetDevicePath(self):
    """Returns the path to the device inside a Linux VM."""
    return self.device_path

  def GetDeviceId(self):
    """Return the Windows DeviceId of this disk."""
    return r'\\.\PHYSICALDRIVE%s' % self.disk_number

  def _Create(self):
    # handled by disk
    pass

  def _Delete(self):
    # handled by disk
    pass

  def GetAttachTime(self):
    if self.attach_start_time and self.attach_end_time:
      return self.attach_end_time - self.attach_start_time

  def GetCreateTime(self):
    if self.create_disk_start_time and self.create_disk_end_time:
      return self.create_disk_end_time - self.create_disk_start_time

  def GetDetachTime(self):
    if self.detach_start_time and self.detach_end_time:
      return self.detach_end_time - self.detach_start_time

  def GetSamples(self) -> List[sample.Sample]:
    samples = super().GetSamples()
    metadata = self.GetResourceMetadata()
    metadata['resource_class'] = self.__class__.__name__
    if self.GetAttachTime():
      samples.append(
          sample.Sample(
              'Time to Attach Disk',
              self.GetAttachTime(),
              'seconds',
              metadata,
          )
      )
    if self.GetDetachTime():
      samples.append(
          sample.Sample(
              'Time to Detach Disk',
              self.GetDetachTime(),
              'seconds',
              metadata,
          )
      )
    if self.GetCreateTime():
      samples.append(
          sample.Sample(
              'Time to Create Disk',
              self.GetCreateTime(),
              'seconds',
              metadata,
          )
      )
    return samples

  def IsNvme(self):
    raise NotImplementedError()


class StripedDisk(BaseDisk):
  """Object representing several disks striped together."""

  is_striped = True

  def __init__(self, disk_spec, disks):
    """Initializes a StripedDisk object.

    Args:
      disk_spec: A BaseDiskSpec containing the desired mount point.
      disks: A list of BaseDisk objects that constitute the StripedDisk.
    """
    super().__init__(disk_spec)
    self.disks = disks
    self.metadata = disks[0].metadata.copy()
    if self.disk_size:
      self.metadata['size'] = self.disk_size * self.num_striped_disks
    if self.metadata.get('iops'):
      self.metadata['iops'] = self.metadata['iops'] * self.num_striped_disks
    if self.metadata.get('throughput'):
      total_throughput = self.metadata['throughput'] * self.num_striped_disks
      self.metadata['throughput'] = total_throughput

  def _Create(self):
    for disk in self.disks:
      disk.Create()

  def _Delete(self):
    for disk in self.disks:
      disk.Delete()

  def _Attach(self, vm):
    for disk in self.disks:
      disk.Attach(vm)

  def _Detach(self):
    for disk in self.disks:
      disk.Detach()

  def GetAttachTime(self):
    disk_attach_time = None
    for disk_details in self.disks:
      disk_details_attach_time = disk_details.GetAttachTime()
      if not disk_details_attach_time:
        raise ValueError(
            'No attach time found for disk %s' % disk_details.GetDeviceId()
        )
      if not disk_attach_time:
        disk_attach_time = disk_details_attach_time
      else:
        disk_attach_time = max(
            disk_attach_time, disk_details_attach_time
        )
    return disk_attach_time

  def GetCreateTime(self):
    if self.disk_create_time:
      return self.disk_create_time
    for disk_details in self.disks:
      disk_details_create_time = disk_details.GetCreateTime()
      if not disk_details_create_time:
        raise ValueError(
            'No create time found for disk %s' % disk_details.GetDeviceId()
        )
      if not self.disk_create_time:
        self.disk_create_time = disk_details_create_time
      else:
        self.disk_create_time = max(
            self.disk_create_time, disk_details_create_time
        )
    return self.disk_create_time

  def GetDetachTime(self):
    if self.disk_detach_time:
      return self.disk_detach_time
    for disk_details in self.disks:
      disk_details_detach_time = disk_details.GetDetachTime()
      if not disk_details_detach_time:
        raise ValueError(
            'No create time found for disk %s' % disk_details.GetDeviceId()
        )
      if not self.disk_detach_time:
        self.disk_detach_time = disk_details_detach_time
      else:
        self.disk_detach_time = max(
            self.disk_detach_time, disk_details_detach_time
        )
    return self.disk_detach_time

  def IsNvme(self):
    for d in self.disks:
      if not d.IsNvme():
        return False
    return True


class NetworkDisk(BaseDisk):
  """Object representing a Network Disk."""

  @abc.abstractmethod
  def _GetNetworkDiskMountOptionsDict(self):
    """Default mount options as a dict."""
    pass

  @property
  def mount_options(self):
    opts = []
    for key, value in sorted(
        self._GetNetworkDiskMountOptionsDict().items()
    ):
      opts.append('%s' % key if value is None else '%s=%s' % (key, value))
    return ','.join(opts)

  @property
  def fstab_options(self):
    return self.mount_options

  @abc.abstractmethod
  def Attach(self, vm):
    """Attached NetworkDisk to a VM.  Must set self.vm."""
    raise NotImplementedError()

  def _Create(self):
    # handled by the Network Disk service
    pass

  def _Delete(self):
    # handled by the Network Disk service
    pass


class NfsDisk(NetworkDisk):
  """Provides options for mounting NFS drives.

  NFS disk should be ready to mount at the time of creation of this disk.

  Args:
    disk_spec: The disk spec.
    remote_mount_address: The host_address:/volume path to the NFS drive.
    nfs_tier: The NFS tier / performance level of the server.
  """

  def __init__(
      self,
      disk_spec,
      remote_mount_address,
      default_nfs_version=None,
      nfs_tier=None,
  ):
    super().__init__(disk_spec)
    self.nfs_version = disk_spec.nfs_version or default_nfs_version
    self.nfs_timeout_hard = disk_spec.nfs_timeout_hard
    self.nfs_rsize = disk_spec.nfs_rsize
    self.nfs_wsize = disk_spec.nfs_wsize
    self.nfs_timeout = disk_spec.nfs_timeout
    self.nfs_retries = disk_spec.nfs_retries
    self.nfs_nconnect = disk_spec.nfs_nconnect
    self.device_path = remote_mount_address
    for key, value in self._GetNetworkDiskMountOptionsDict().items():
      self.metadata['nfs_{}'.format(key)] = value
    if nfs_tier:
      self.nfs_tier = nfs_tier
      self.metadata['nfs_tier'] = nfs_tier
    super().GetResourceMetadata()

  def _GetNetworkDiskMountOptionsDict(self):
    """Default NFS mount options as a dict."""
    options = {
        'hard' if self.nfs_timeout_hard else 'soft': None,
        'rsize': self.nfs_rsize,
        'wsize': self.nfs_wsize,
        'timeo': self.nfs_timeout * 10,  # in decaseconds
        'retrans': self.nfs_retries,
    }
    # the client doesn't have to specify an NFS version to use (but should)
    if self.nfs_version:
      options['nfsvers'] = self.nfs_version
    if self.nfs_nconnect:
      options['nconnect'] = self.nfs_nconnect
    if FLAGS.nfs_noresvport:
      options['noresvport'] = None
    return options

  def Attach(self, vm):
    self.vm = vm
    self.vm.Install('nfs_utils')


class SmbDisk(NetworkDisk):
  """Provides options for mounting SMB drives.

  SMB disk should be ready to mount at the time of creation of this disk.

  Args:
    disk_spec: The disk spec.
    remote_mount_address: The host_address:/volume path to the SMB drive.
    smb_tier: The SMB tier / performance level of the server.
  """

  def __init__(
      self,
      disk_spec,
      remote_mount_address,
      storage_account_and_key,
      default_smb_version=None,
      smb_tier=None,
  ):
    super().__init__(disk_spec)
    self.smb_version = disk_spec.smb_version
    self.device_path = remote_mount_address
    self.storage_account_and_key = storage_account_and_key
    if smb_tier:
      self.metadata['smb_tier'] = smb_tier

  def _GetNetworkDiskMountOptionsDict(self):
    """Default SMB mount options as a dict."""
    options = {
        'vers': self.smb_version,
        'username': self.storage_account_and_key['user'],
        'password': self.storage_account_and_key['pw'],
        'dir_mode': '0777',
        'file_mode': '0777',
        'serverino': None,
        # the following mount option is a suggestion from
        # https://docs.microsoft.com/en-us/azure/storage/files/storage-troubleshooting-files-performance#throughput-on-linux-clients-is-significantly-lower-when-compared-to-windows-clients
        'nostrictsync': None,
    }
    return options

  def Attach(self, vm):
    self.vm = vm
    self.vm.InstallPackages('cifs-utils')
