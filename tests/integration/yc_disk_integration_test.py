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

"""Integration tests for YandexCloud scratch disks."""

import os
import unittest

from perfkitbenchmarker import pkb
from perfkitbenchmarker import test_util

MOUNT_POINT = '/scratch'


@unittest.skipUnless('PERFKIT_INTEGRATION' in os.environ,
                     'PERFKIT_INTEGRATION not in environment')
class YcScratchDiskIntegrationTest(unittest.TestCase):
  """Integration tests for YandexCloud disks.

  Please see the section on integration testing in the README.
  """

  def setUp(self):
    pkb.SetUpPKB()

  def testPDStandard(self):
    test_util.assertDiskMounts({
        'vm_groups': {
            'vm_group_1': {
                'cloud': 'YandexCloud',
                'vm_spec': {
                    'YandexCloud': {
                        'machine_type': 'Medium',
                        'zone': 'ru-central1-a'
                    }
                },
                'disk_spec': {
                    'YandexCloud': {
                        'disk_type': 'network-hdd',
                        'disk_size': 2,
                        'mount_point': MOUNT_POINT
                    }
                }
            }
        }
    }, MOUNT_POINT)

  def testPDSSD(self):
    test_util.assertDiskMounts({
        'vm_groups': {
            'vm_group_1': {
                'cloud': 'YandexCloud',
                'vm_spec': {
                    'YandexCloud': {
                        'machine_type': 'Medium',
                        'zone': 'ru-central1-a'
                    }
                },
                'disk_spec': {
                    'YandexCloud': {
                        'disk_type': 'network-ssd',
                        'disk_size': 2,
                        'mount_point': MOUNT_POINT
                    }
                }
            }
        }
    }, MOUNT_POINT)


if __name__ == '__main__':
  unittest.main()
