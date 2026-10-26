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
"""Resource for AWS FSx for NetApp ONTAP service."""

import json

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import netapp_service
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS

# Second-generation Single-AZ file system with one HA pair:
# https://docs.aws.amazon.com/fsx/latest/ONTAPGuide/high-availability-AZ.html
_DEPLOYMENT_TYPE = 'SINGLE_AZ_2'
_HA_PAIRS = 1

# Performance specs:
# https://docs.aws.amazon.com/fsx/latest/ONTAPGuide/performance.html
_VALID_PROVISIONED_THROUGHPUTS = [384, 768, 1536, 3072, 6144]
_DEFAULT_PROVISIONED_THROUGHPUT = 1536

# FSx for ONTAP requires at least 1024 GiB of SSD storage per HA pair:
# https://docs.aws.amazon.com/fsx/latest/APIReference/API_CreateFileSystem.html
_MIN_FILE_SYSTEM_CAPACITY_GIB = 1024 * _HA_PAIRS
# FSx uses FlexVol volumes by default with one HA pair and FlexGroup volumes
# with more than one:
# https://docs.aws.amazon.com/fsx/latest/ONTAPGuide/managing-volumes.html
_VOLUME_STYLE = 'FLEXVOL'
# 0 turns off automatic daily backups.
_AUTOMATIC_BACKUP_RETENTION_DAYS = 0
# False turns off deduplication, compression and compaction.
_STORAGE_EFFICIENCY_STATUS = False

_BYTES_PER_GIB = 1024**3
# Upper bound in seconds to wait for a file system or SVM create or delete.
_FSX_WAIT_TIMEOUT = 3600


class AwsNetAppError(errors.Error):
  """Raised when an AWS FSx CLI command fails."""


class AwsNetAppDiskSpec(disk.BaseNetAppDiskSpec):
  """Object holding the information needed to create an FSx ONTAP volume.

  Attributes:
    provisioned_throughput: int. FSx throughput capacity in MBps. Must be one
      of _VALID_PROVISIONED_THROUGHPUTS. The --provisioned_throughput flag
      overrides the config value.
  """

  provisioned_throughput: int

  CLOUD = provider_info.AWS
  DISK_TYPE = disk.NETAPP_VOLUMES

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for configurable options."""
    result = super()._GetOptionDecoderConstructions()
    result.update({
        # Narrows BaseDiskSpec's IntDecoder to the FSx throughput tiers.
        'provisioned_throughput': (
            option_decoders.EnumDecoder,
            {
                'valid_values': _VALID_PROVISIONED_THROUGHPUTS,
                'default': _DEFAULT_PROVISIONED_THROUGHPUT,
            },
        ),
    })
    return result


class AwsNetAppService(netapp_service.BaseNetAppService):
  """Resource for AWS FSx for NetApp ONTAP service.

  The file system and storage virtual machine (SVM) are dependencies of the
  volume, which VMs mount over NFS.
  """

  disk_spec: AwsNetAppDiskSpec

  CLOUD = provider_info.AWS

  def __init__(self, disk_spec: AwsNetAppDiskSpec, zone):
    super().__init__(disk_spec, zone)
    self.region = util.GetRegionFromZone(zone)
    # SVM and volume names allow only letters, digits and underscores. See
    # https://docs.aws.amazon.com/fsx/latest/ONTAPGuide/creating-svms.html and
    # https://docs.aws.amazon.com/fsx/latest/ONTAPGuide/creating-volumes.html
    self.svm_name = f'pkb_svm_{FLAGS.run_uri}'
    self.volume_name = f'pkb_vol_{FLAGS.run_uri}'
    # Junction path of the volume in the SVM namespace.
    self.server_directory = f'/{self.volume_name}'
    self.file_system_id: str | None = None
    self.svm_id: str | None = None
    self.volume_id: str | None = None

  @property
  def network(self):
    spec = aws_network.AwsNetworkSpec(self.zone)
    return aws_network.AwsNetwork.GetNetworkFromNetworkSpec(spec)

  @property
  def file_system_capacity_gib(self) -> int:
    """Calculates file system SSD capacity (minimum 1024 GiB per HA pair)."""
    return max(self.disk_spec.disk_size, _MIN_FILE_SYSTEM_CAPACITY_GIB)

  def _GetRemoteAddress(self):
    """Gets the NFS IP address of the SVM."""
    svm = self._DescribeStorageVirtualMachine()
    ip_addresses = svm.get('Endpoints', {}).get('Nfs', {}).get('IpAddresses')
    if ip_addresses:
      return ip_addresses[0]
    raise errors.Error(
        f'Could not find NFS IP for SVM {self.svm_name}. SVM details: {svm}'
    )

  def GetResourceMetadata(self):
    result = super().GetResourceMetadata()
    result['netapp_deployment_type'] = _DEPLOYMENT_TYPE
    result['netapp_throughput_capacity'] = (
        self.disk_spec.provisioned_throughput
    )
    result['netapp_file_system_capacity_gib'] = self.file_system_capacity_gib
    return result

  def _CreateDependencies(self):
    self._CreateFileSystem()
    self._CreateStorageVirtualMachine()

  def _CreateFileSystem(self):
    """Creates the file system and waits until it is AVAILABLE."""
    network = self.network
    security_group_id = network.regional_network.vpc.default_security_group_id
    ontap_config = {
        'DeploymentType': _DEPLOYMENT_TYPE,
        'HAPairs': _HA_PAIRS,
        'ThroughputCapacity': self.disk_spec.provisioned_throughput,
        'AutomaticBackupRetentionDays': _AUTOMATIC_BACKUP_RETENTION_DAYS,
    }
    cmd = [
        'create-file-system',
        '--file-system-type=ONTAP',
        f'--storage-capacity={self.file_system_capacity_gib}',
        '--storage-type=SSD',
        # Same subnet and security group as the VMs.
        f'--subnet-ids={network.subnet.id}',
        f'--security-group-ids={security_group_id}',
        '--ontap-configuration',
        json.dumps(ontap_config),
        '--tags',
    ] + util.MakeFormattedDefaultTags()
    try:
      response = self._FsxCommand(*cmd)
    except AwsNetAppError as ex:
      raise errors.Resource.CreationError(ex)
    self.file_system_id = response['FileSystem']['FileSystemId']
    self._WaitUntilFileSystemReady()

  def _CreateStorageVirtualMachine(self):
    """Creates the SVM and waits until it is CREATED."""
    cmd = [
        'create-storage-virtual-machine',
        f'--file-system-id={self.file_system_id}',
        f'--name={self.svm_name}',
        # AWS recommends the UNIX security style for volumes that Linux clients
        # mount. Volumes inherit it from the SVM root volume. See
        # https://docs.aws.amazon.com/fsx/latest/ONTAPGuide/attach-linux-client.html
        '--root-volume-security-style=UNIX',
        '--tags',
    ] + util.MakeFormattedDefaultTags()
    try:
      response = self._FsxCommand(*cmd)
    except AwsNetAppError as ex:
      raise errors.Resource.CreationError(ex)
    self.svm_id = response['StorageVirtualMachine']['StorageVirtualMachineId']
    self._WaitUntilSvmReady()

  def _Create(self):
    ontap_config = {
        'StorageVirtualMachineId': self.svm_id,
        'JunctionPath': self.server_directory,
        'SizeInBytes': self.disk_spec.disk_size * _BYTES_PER_GIB,
        'VolumeStyle': _VOLUME_STYLE,
        # Required for read-write volumes.
        'StorageEfficiencyEnabled': _STORAGE_EFFICIENCY_STATUS,
    }
    cmd = [
        'create-volume',
        '--volume-type=ONTAP',
        f'--name={self.volume_name}',
        '--ontap-configuration',
        json.dumps(ontap_config),
        '--tags',
    ] + util.MakeFormattedDefaultTags()
    try:
      response = self._FsxCommand(*cmd)
    except AwsNetAppError as ex:
      raise errors.Resource.RetryableCreationError(ex)
    self.volume_id = response['Volume']['VolumeId']

  def _Delete(self):
    try:
      self._FsxCommand(
          'delete-volume',
          f'--volume-id={self.volume_id}',
          '--ontap-configuration',
          # Without this, FSx takes a final backup of the volume.
          json.dumps({'SkipFinalBackup': True}),
      )
    except AwsNetAppError:
      pass

  def _DeleteDependencies(self):
    # FSx deletes an SVM only after its volumes are gone, and a file system
    # only after its SVMs are gone, so wait for each delete to finish.
    if self.svm_id:
      self._DeleteAndWait(
          self._DescribeStorageVirtualMachine,
          'delete-storage-virtual-machine',
          f'--storage-virtual-machine-id={self.svm_id}',
      )
    if self.file_system_id:
      # Waiting also makes sure the file system's network interfaces are gone
      # before PKB deletes the subnet.
      self._DeleteAndWait(
          self._DescribeFileSystem,
          'delete-file-system',
          f'--file-system-id={self.file_system_id}',
      )
    super()._DeleteDependencies()

  def _Exists(self):
    """Returns True if the volume exists."""
    return bool(self._DescribeVolume())

  def _IsReady(self):
    """Returns True if the volume is ready to mount."""
    return self._IsLifecycleReady(
        f'Volume {self.volume_id}',
        self._DescribeVolume(),
        ('CREATED', 'AVAILABLE'),
    )

  def _IsDeleting(self):
    """Returns True while FSx is deleting the volume."""
    return self._DescribeVolume().get('Lifecycle') == 'DELETING'

  @staticmethod
  def _IsLifecycleReady(name, details, ready_lifecycles) -> bool:
    """Returns True if an FSx resource has reached one of ready_lifecycles.

    Args:
      name: str. Resource name to use in error messages.
      details: dict. FSx description of a file system, SVM or volume.
      ready_lifecycles: Lifecycle values that mean the resource is ready.

    Raises:
      errors.Benchmarks.InsufficientCapacityCloudFailure: FSx failed to create
        the resource due to insufficient capacity.
      errors.Resource.CreationError: FSx failed to create the resource.
    """
    lifecycle = details.get('Lifecycle')
    if lifecycle in ready_lifecycles:
      return True
    if lifecycle in ('FAILED', 'MISCONFIGURED'):
      reason = (
          details.get('FailureDetails', {}).get('Message')
          or details.get('LifecycleTransitionReason', {}).get('Message')
          or ''
      )
      error_msg = f'{name} failed with lifecycle {lifecycle}: {reason}'
      if 'insufficient capacity' in reason.lower():
        raise errors.Benchmarks.InsufficientCapacityCloudFailure(error_msg)
      raise errors.Resource.CreationError(error_msg)
    return False

  @vm_util.Retry(
      timeout=_FSX_WAIT_TIMEOUT,
      retryable_exceptions=(errors.Resource.RetryableCreationError,),
  )
  def _WaitUntilFileSystemReady(self):
    file_system = self._DescribeFileSystem()
    name = f'File system {self.file_system_id}'
    if self._IsLifecycleReady(name, file_system, ('AVAILABLE',)):
      return
    raise errors.Resource.RetryableCreationError(
        f'{name} lifecycle is {file_system.get("Lifecycle")}'
    )

  @vm_util.Retry(
      timeout=_FSX_WAIT_TIMEOUT,
      retryable_exceptions=(errors.Resource.RetryableCreationError,),
  )
  def _WaitUntilSvmReady(self):
    svm = self._DescribeStorageVirtualMachine()
    name = f'SVM {self.svm_id}'
    if self._IsLifecycleReady(name, svm, ('CREATED',)):
      return
    raise errors.Resource.RetryableCreationError(
        f'{name} lifecycle is {svm.get("Lifecycle")}'
    )

  @vm_util.Retry(
      timeout=_FSX_WAIT_TIMEOUT,
      retryable_exceptions=(errors.Resource.RetryableDeletionError,),
  )
  def _DeleteAndWait(self, describe_fn, *delete_cmd):
    """Runs delete_cmd until describe_fn no longer finds the resource."""
    details = describe_fn()
    if not details:
      return
    lifecycle = details.get('Lifecycle')
    if lifecycle != 'DELETING':
      try:
        self._FsxCommand(*delete_cmd)
      except AwsNetAppError:
        pass
    raise errors.Resource.RetryableDeletionError(
        f'{delete_cmd[0]} not finished, lifecycle is {lifecycle}'
    )

  def _DescribeFileSystem(self):
    """Describes the file system. Returns {} if it does not exist."""
    return self._Describe(
        'FileSystems',
        'describe-file-systems',
        f'--file-system-ids={self.file_system_id}',
    )

  def _DescribeStorageVirtualMachine(self):
    """Describes the SVM. Returns {} if it does not exist."""
    return self._Describe(
        'StorageVirtualMachines',
        'describe-storage-virtual-machines',
        f'--storage-virtual-machine-ids={self.svm_id}',
    )

  def _DescribeVolume(self):
    """Describes the volume. Returns {} if it does not exist."""
    return self._Describe(
        'Volumes', 'describe-volumes', f'--volume-ids={self.volume_id}'
    )

  def _Describe(self, response_key, *args):
    """Runs an FSx describe command and returns the first item found.

    Args:
      response_key: str. Key of the resource list in the describe response,
        e.g. 'Volumes'.
      *args: The describe command and its arguments.

    Returns:
      dict. The first resource in the response, or {} if it does not exist or
      the command fails.
    """
    try:
      response = self._FsxCommand(*args)
    except AwsNetAppError:
      return {}
    items = response.get(response_key) or []
    return items[0] if items else {}

  def _FsxCommand(self, *args):
    """Runs an AWS FSx CLI command and returns its parsed JSON output."""
    cmd = util.AWS_PREFIX + ['fsx', *args, f'--region={self.region}']
    stdout, stderr, retcode = vm_util.IssueCommand(
        cmd, raise_on_failure=False, timeout=1800
    )
    if retcode:
      raise AwsNetAppError(f'Error running aws fsx command: {stderr}')
    return json.loads(stdout) if stdout else {}
