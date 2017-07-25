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
"""IOPS Storage Utility class.

This class is used to translate an {IOPS, Cloud Provider} requirement
to {core, number of disks, storage size} machine requirements necessary
to meet the IOPS level using the Cloud Provider declared.

 - On AWS, we will use "ebs-gp2" storage type.
 - On GCP, we will use PD-SSD storage type.

In future versions, we will support Azure as well.

The data used to make these Conversions was acquired in May 2017 from the
following resources:

GCP Storage Ratings: https://cloud.google.com/compute/docs/disks/. Storage can
          go as high as 64TB per disk but IOPS maxes out at 30,000 iops/disk
          or (1000GB).
AWS Storage Ratings: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/
          EBSVolumeTypes.html#EBSVolumeTypes_gp2. Storage can go as
          high as 16TiB per disk but IOPS maxes out at 10000 IOPS/volume size.
          Reference gives volume size in GiB so converted to GB below.
GCP CPU Ratings: https://cloud.google.com/compute/docs/disks/
          performance#ssd-pd-performance
AWS CPU Ratings: https://aws.amazon.com/ebs/details/
GCP Disk Number Ratings: https://cloud.google.com/compute/docs/disks/performance
AWS Disk Number Ratings: https://aws.amazon.com/ebs/details/


Note: These conversions will require updating as performance and resources
change.

To utilize this class, initialize an instance of the DiskIOPSToCapacity class
with the IOPS level desired and the provider you wish to use. The machine
requirement attributes will be immediately populated.
"""
import math
import numpy


class InvalidProviderError(Exception):
  pass


class InvalidIOPSError(Exception):
  pass


class InvalidStorageTypeError(Exception):
  pass


# GLOBAL STRINGS
AWS = 'AWS'
GCP = 'GCP'
MAX_IOPS = 'MAX_IOPS'
DEFAULT_STORAGE_TYPE = 'DEFAULT_STORAGE_TYPE'
VALID_STORAGE_TYPES = 'VALID_STORAGE_TYPES'
# Cloud providers info dictionary. Will be updated when support more cloud
# providers. VALID_STORAGE_TYPES are storage types that are currently
# supported by disk_iops_to_capacity converter.
# MAX IOPS Sources:
#   GCP: Increasing vCPUs increases IOPS performance. Top IOPS performance is
#         per instance is averaged to 30000.
#         https://cloud.google.com/compute/docs/disks/performance
#   AWS: Can add additional volumes to increase IOPS performance. Top IOPS
#         performance per CPU is 75000.
#        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumeTypes.html
CLOUD_PROVIDERS_INFO = {
    AWS: {
        MAX_IOPS: 75000,
        DEFAULT_STORAGE_TYPE: 'ebs-gp2',
        VALID_STORAGE_TYPES: ['ebs-gp2']
    },
    GCP: {
        MAX_IOPS: 30000,
        DEFAULT_STORAGE_TYPE: 'pd-ssd',
        VALID_STORAGE_TYPES: ['pd-ssd'],
    }
}


class DiskIOPSToCapacity(object):
  """Given iops and service provider requirements, return disk configurations.

  This class is used to translate an {IOPS, Cloud Provider} requirement
  to {core, number of disks, storage size} machine requirements necessary
  to meet the IOPS level using the Cloud MySQL Provider declared.

  Currently assumes SSD persistent disks.
  TODO:
    - Implement Azure calculations. Add other cloud providers as applicable.
    - Support other storage types such as HDD and/or EBS-piops.
      Add a further parameter of Disk Type (default SSD PD) and update
      calculations to include HDD disk iops levels.

  Attributes:
    _iops: Number of IOPS required.
    _provider: 'AWS' or 'GCP'.
    _size: Minimum size (GB) required for _iops level with _provider.
    _number_disks: Disk number required to meet _iops level with _provider.
    _cpu_count: vCPUs per instance required to meet _iops level with _provider.
  """

  def __init__(self, iops, provider=GCP, storage_type=None):
    self._size = None
    self._cpu_count = None
    self._number_disks = None
    self._iops = iops
    self._provider = provider.upper()
    self._storage_type = storage_type
    self._ValidateProvider()
    self._ValidateIOPS()
    self._ValidateStorageType()
    self._PopulateConfigs()

  def _ValidateStorageType(self):
    """Validate storage type for given _provider, set to default if not given.

       Raises:
        InvalidStorageTypeError: Incorrect storage type given.

    TODO: When support other types of storage types (i.e. when this class
    supports ebs-piops for AWS or pd-hhd for gcp), will need to update
    VALID_STORAGE_TYPES in CLOUD_PROVIDERS_INFO dictionary.

    """
    if self._storage_type:
      self._storage_type = self._storage_type.lower()
      if (self._storage_type is
          not CLOUD_PROVIDERS_INFO[self._provider][DEFAULT_STORAGE_TYPE]):
        raise InvalidStorageTypeError()
    else:
      self._storage_type = CLOUD_PROVIDERS_INFO[self._provider][
          DEFAULT_STORAGE_TYPE]

  def _ValidateProvider(self):
    """Validate provider to be GCP or AWS, throw exception if invalid.

    Raises:
      InvalidProviderError: Incorrect provider type given.
    """
    if self._provider not in CLOUD_PROVIDERS_INFO.keys():
      raise InvalidProviderError('Provider given is not supported by '
                                 'storage_utility.')

  def _ValidateIOPS(self):
    """Validate IOPS to be within valid limits, throw exception if invalid.

    If IOPS parameter is less than 1 or greater than provider maximum IOPS
    throw InvalidIOPSError.

    Raises:
      InvalidIOPSError: Invalid IOPS parameter given.
    """
    if (self._iops < 1 or
        self._iops > CLOUD_PROVIDERS_INFO[self._provider][MAX_IOPS]):
      raise InvalidIOPSError(
          'Invalid IOPS parameter, must be positive number less than '
          'the maximum achievable for given cloud provider. '
          'The maximum for {} is {}.'.format(
              self._provider, CLOUD_PROVIDERS_INFO[self._provider][MAX_IOPS]))

  def _PopulateConfigs(self):
    """Populate Storage Configurations."""
    self._SetSize()
    self._SetCPUCount()
    self._SetNumberDisks()

  def PrintConfigs(self):
    """Print out necessary configs."""
    vm_config = ('For {} IOPS using {}, the following is required:\n\tStorage '
                 'Size (GB): {}\n\tCPU Count: {}\n\tNumber of '
                 'Disks: {}').format(self._iops,
                                     self._provider.upper(), self._size,
                                     self._cpu_count, self._number_disks)
    print vm_config

  def _SetSize(self):
    """Set minimum size (GB) necessary to achieve _iops level.
    Rating performance levels as of May 2017, sources found below.
    GCP: ratings from https://cloud.google.com/compute/docs/disks/. Storage can
          go as high as 64TB per disk but IOPS maxes out at 30,000 iops/disk
          or (1000GB).
    AWS: ratings from
          http://docs.aws.amazon.com/AWSEC2/latest/
          UserGuide/EBSVolumeTypes.html#EBSVolumeTypes_gp2. Storage can go as
          high as 16TiB per disk but IOPS maxes out at 10000 IOPS/volume size.
          Reference gives volume size in GiB so converted to GB below.
    """
    if self._provider == GCP:
      self._size = min(int(math.ceil(self._iops / 30.0)), 30000 / 30)
    elif self._provider == AWS:
      value = self._iops
      value = numpy.array(value)
      self._size = int(
          numpy.piecewise(value, [[value <= 100], [(value > 100) & (
              value <= 9999)], [value > 9999]], [
                  lambda x: int(math.ceil(1.07374)),
                  lambda x: int(math.ceil(3 * value)),
                  lambda x: int(math.ceil(3579.855))]))

  def GetSize(self):
    """Return storage size.
    Returns:
      __size: Storage size (GB).
    """
    return self._size

  def _SetCPUCount(self):
    """Set cpu count.

    GCP: ratings from
    https://cloud.google.com/compute/docs/disks/performance#ssd-pd-performance
    AWS: to achieve good performance on EBS, one needs to use an
    EBS-optimized VM instance, and the smallest VM instance that can be EBS
    optimized is *.large VM types (e.g., c4.large), those comes with 2 cores.
    ratings from
    http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSOptimized.html

    """
    if self._provider == GCP:
      value = self._iops
      self._cpu_count = int(
          numpy.piecewise(value, [[value <= 15000], [
              (value > 15000) & (value <= 25000)
          ], [value > 25000]], [lambda x: 1, lambda x: 16, lambda x: 32]))
    elif self._provider == AWS:
      self._cpu_count = 2

  def GetCPUCount(self):
    """Return CPU count.

    Returns:
      _cpu_count: CPU count.
    """
    return self._cpu_count

  def _SetNumberDisks(self):
    """Set number of disks.

    GCP: Adding disks does not increase IOPS for GCP.
    AWS: ratings from https://aws.amazon.com/ebs/details/
    """
    if self._provider == GCP:
      self._number_disks = 1
    elif self._provider == AWS:
      self._number_disks = max(int(math.ceil(self._iops / 10000.0)), 1)

  def GetNumberDisks(self):
    """Return Number of Disks.

    Returns:
      _number_disks: Number of disks.
    """
    return self._number_disks
