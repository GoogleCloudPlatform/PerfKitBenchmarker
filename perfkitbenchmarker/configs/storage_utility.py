#!/usr/bin/env python

# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

import math
import numpy


class SQLProviderError(Exception):
  pass


class IOPSError(Exception):
  pass


class StorageConfig(object):
  """Given iops and service provider requirements, return necessary parameters.

  Assumes SSD persistent disks.

  Args:
    iops: Number of iops required.
    provider: Provider: 'AWS' or 'GCP'.
  """

  AWS = 'AWS'
  GCP = 'GCP'

  def __init__(self, iops, provider=GCP):
    self._storage_size = None
    self._cpu_count = None
    self._number_disks = None
    self._iops = iops
    self._provider = provider.upper()
    self.ValidateProvider()
    self.ValidateIOPS()
    self.PopulateConfigs()

  def ValidateProvider(self):
    """Validate provider to be GCP or AWS, throw exception if invalid.

    Raises:
      SQLProviderError: Incorrect provider type given.
    """
    if self._provider != self.AWS and self._provider != self.GCP:
      raise SQLProviderError('Provider given is not supported by '
                             'storage_utility.')

  def ValidateIOPS(self):
    """Validate IOPS to be positive integer, throw exception if invalid.

    Raises:
      IOPSError: Invalid IOPS parameter given.
    """
    if self._iops < 1:
      raise IOPSError()

  def PopulateConfigs(self):
    """Poplulate Storage Configurations."""
    self.SetStorageSize()
    self.SetCPUCount()
    self.SetNumberDisks()

  def PrintConfigs(self):
    """Print out necessary configs."""
    vm_config = ("For {} IOPS using {}, the following is required:\n\tStorage "
                 "Size (GB): {}\n\tCPU Count: {}\n\tNumber of "
                 "Disks: {}").format(self._iops,
                                     self._provider.upper(),
                                     self._storage_size,
                                     self._cpu_count,
                                     self._number_disks)
    print vm_config

  def SetStorageSize(self):
    """Set Storage size.

    GCP: ratings from https://cloud.google.com/compute/docs/disks/. Storage can
          go as high as 64TB per disk but IOPS maxes out at 30,000 iops/disk
          or (1000GB).
    AWS: ratings from
          http://docs.aws.amazon.com/AWSEC2/latest/
          UserGuide/EBSVolumeTypes.html#EBSVolumeTypes_gp2. Storage can go as
          high as 16TiB per disk but IOPS maxes out at 10000 IOPS/volume size.
          Reference gives volume size in GiB so converted to GB below.
    """
    if self._provider == self.GCP:
      self._storage_size = min(self._iops / 30, 30000 / 30)
    elif self._provider == self.AWS:
      value = self._iops
      value = numpy.array(value)
      self._storage_size = int(numpy.piecewise(value,
                                               [[value <= 100],
                                                [(value > 100) &
                                                 (value <= 9999)],
                                                [value > 9999]],
                                               [lambda x:
                                                int(math.ceil(1.07374)),
                                                lambda x:
                                                int(math.ceil(3 * value)),
                                                lambda x:
                                                int(math.ceil(3579.855))]))

  def GetStorageSize(self):
    """Return storage size.
    Returns:
      _storage_size: Storage size (GB).
    """
    return self._storage_size

  def SetCPUCount(self):
    """Set cpu count.

    GCP: ratings from
    https://cloud.google.com/compute/docs/disks/performance#ssd-pd-performance
    AWS: ratings from https://aws.amazon.com/ebs/details/
    """
    if self._provider == self.GCP:
      value = self._iops
      self._cpu_count = int(numpy.piecewise(value,
                                            [[value <= 15000],
                                             [(value > 15000) &
                                              (value <= 25000)],
                                             [value > 25000]],
                                            [lambda x: 1,
                                             lambda x: 16,
                                             lambda x: 32]))
    elif self._provider == self.AWS:
      self._cpu_count = 1

  def GetCPUCount(self):
    """Return CPU count.

    Returns:
      _cpu_count: CPU count.
    """
    return self._cpu_count

  def SetNumberDisks(self):
    """Set number of disks.

    GCP: ratings from https://cloud.google.com/compute/docs/disks/performance
    AWS: ratings from https://aws.amazon.com/ebs/details/
    """
    if self._provider == self.GCP:
      if self._iops >= 30000:
        self._number_disks = 1000
      else:
        self._number_disks = max(int(round(self._iops / 60) * 2), 1)

    elif self._provider == self.AWS:
      self._number_disks = max(self._iops / 10000, 1)

  def GetNumberDisks(self):
    """Return Number of Disks.

    Returns:
      _number_disks: Number of disks.
    """
    return self._number_disks
