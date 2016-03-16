# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

"""Integration tests for Azure scratch disks."""

import os
import unittest

from perfkitbenchmarker import pkb
from perfkitbenchmarker import test_util

MOUNT_POINT = '/scratch'


@unittest.skipUnless('PERFKIT_INTEGRATION' in os.environ,
                     'PERFKIT_INTEGRATION not in environment')
class AzureScratchDiskIntegrationTest(unittest.TestCase):
  """Integration tests for Azure disks.

  Please see the section on integration testing in the README.
  """

  def setUp(self):
    pkb.SetUpPKB()

  def testPremiumStorage(self):
    test_util.assertDiskMounts({
        'flags': {
            'azure_storage_type': 'PLRS'
        },
        'vm_groups': {
            'vm_group_1': {
                'cloud': 'Azure',
                'vm_spec': {
                    'Azure': {
                        'machine_type': 'Standard_DS2',
                        'zone': 'East US 2'
                    }
                },
                'disk_spec': {
                    'Azure': {
                        'disk_type': 'premium-storage',
                        'disk_size': 10,  # disk size must be between
                                          # 10 and 1024 GB.
                        'mount_point': MOUNT_POINT
                    }
                }
            }
        }
    }, MOUNT_POINT)

  def testStandardDisk(self):
    test_util.assertDiskMounts({
        'flags': {
            'azure_storage_type': 'LRS'
        },
        'vm_groups': {
            'vm_group_1': {
                'cloud': 'Azure',
                'vm_spec': {
                    'Azure': {
                        'machine_type': 'Standard_D2',
                        'zone': 'East US 2'
                    }
                },
                'disk_spec': {
                    'Azure': {
                        'disk_type': 'standard-disk',
                        'disk_size': 2,
                        'mount_point': MOUNT_POINT
                    }
                }
            }
        }
    }, MOUNT_POINT)

  def testLocalSSD(self):
    test_util.assertDiskMounts({
        'vm_groups': {
            'vm_group_1': {
                'cloud': 'Azure',
                'vm_spec': {
                    'Azure': {
                        'machine_type': 'Standard_G1',
                        'zone': 'East US 2'
                    }
                },
                'disk_spec': {
                    'Azure': {
                        'disk_type': 'local',
                        'mount_point': MOUNT_POINT
                    }
                }
            }
        }
    }, MOUNT_POINT)
