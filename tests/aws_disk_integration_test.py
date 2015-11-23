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

"""Integration tests for AWS scratch disks."""

import os
import unittest

from perfkitbenchmarker import pkb
from perfkitbenchmarker import test_util

MOUNT_POINT = '/scratch'


@unittest.skipUnless('PERFKIT_INTEGRATION' in os.environ,
                     'PERFKIT_INTEGRATION not in environment')
class AwsScratchDiskIntegrationTest(unittest.TestCase):
  """Integration tests for AWS disks.

  Please see the section on integration testing in the README.
  """

  def setUp(self):
    pkb.SetUpPKB()

  def testEBSStandard(self):
    test_util.assertDiskMounts({
        'vm_groups': {
            'vm_group_1': {
                'cloud': 'AWS',
                'vm_spec': {
                    'AWS': {
                        'machine_type': 'm4.large',
                        'zone': 'us-east-1a'
                    }
                },
                'disk_spec': {
                    'AWS': {
                        'disk_type': 'standard',
                        'disk_size': 2,
                        'mount_point': MOUNT_POINT
                    }
                }
            }
        }
    }, MOUNT_POINT)

  def testEBSGP(self):
    test_util.assertDiskMounts({
        'vm_groups': {
            'vm_group_1': {
                'cloud': 'AWS',
                'vm_spec': {
                    'AWS': {
                        'machine_type': 'm4.large',
                        'zone': 'us-east-1a'
                    }
                },
                'disk_spec': {
                    'AWS': {
                        'disk_type': 'gp2',
                        'disk_size': 2,
                        'mount_point': MOUNT_POINT
                    }
                }
            }
        }
    }, MOUNT_POINT)

  def testEBSPIOPS(self):
    test_util.assertDiskMounts({
        'vm_groups': {
            'vm_group_1': {
                'cloud': 'AWS',
                'vm_spec': {
                    'AWS': {
                        'machine_type': 'm4.large',
                        'zone': 'us-east-1a'
                    }
                },
                'disk_spec': {
                    'AWS': {
                        'disk_type': 'io1',
                        'disk_size': 35,  # maximum size/IOPS ratio is 30
                        'iops': 100,  # minimum value is 100 IOPS
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
                'cloud': 'AWS',
                'vm_spec': {
                    'AWS': {
                        'machine_type': 'm3.medium',
                        'zone': 'us-east-1a'
                    }
                },
                'disk_spec': {
                    'AWS': {
                        'disk_type': 'local',
                        'mount_point': MOUNT_POINT
                    }
                }
            }
        }
    }, MOUNT_POINT)
