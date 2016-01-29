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

"""Test the translation of disk type names."""

import unittest

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import context
from perfkitbenchmarker import os_types
from perfkitbenchmarker.configs import benchmark_config_spec
from tests import mock_flags


_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'uid'


class _DiskTypeRenamingTestCase(unittest.TestCase):

  def setUp(self):
    self.mocked_flags = mock_flags.PatchTestCaseFlags(self)
    self.mocked_flags.os_type = os_types.DEBIAN
    self.addCleanup(context.SetThreadBenchmarkSpec, None)

  def _CreateBenchmarkSpec(self, config_dict):
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        _BENCHMARK_NAME, flag_values=self.mocked_flags, **config_dict)
    spec = benchmark_spec.BenchmarkSpec(config_spec, _BENCHMARK_NAME,
                                        _BENCHMARK_UID)
    spec.ConstructVirtualMachines()
    return spec


class GcpDiskTypeRenamingTest(_DiskTypeRenamingTestCase):
  """Test that the disk type renaming works for GCP.
  """

  def testPDStandard(self):
    config = {
        'vm_groups': {
            'vm_group_1': {
                'cloud': 'GCP',
                'vm_spec': {
                    'GCP': {
                        'machine_type': 'test_machine_type',
                    }
                },
                'disk_spec': {
                    'GCP': {
                        'disk_type': 'standard',
                        'disk_size': 2,
                    }
                }
            }
        }
    }
    spec = self._CreateBenchmarkSpec(config)
    self.assertEquals(spec.vms[0].disk_specs[0].disk_type, 'pd-standard')

  def testPDSSD(self):
    config = {
        'vm_groups': {
            'vm_group_1': {
                'cloud': 'GCP',
                'vm_spec': {
                    'GCP': {
                        'machine_type': 'test_machine_type',
                    }
                },
                'disk_spec': {
                    'GCP': {
                        'disk_type': 'remote_ssd',
                        'disk_size': 2,
                    }
                }
            }
        }
    }
    spec = self._CreateBenchmarkSpec(config)
    self.assertEquals(spec.vms[0].disk_specs[0].disk_type, 'pd-ssd')


class AwsDiskTypeRenamingTest(_DiskTypeRenamingTestCase):
  def testEBSStandard(self):
    config = {
        'vm_groups': {
            'vm_group_1': {
                'cloud': 'AWS',
                'vm_spec': {
                    'AWS': {
                        'zone': 'us-east-1a'
                    }
                },
                'disk_spec': {
                    'AWS': {
                        'disk_type': 'standard',
                        'disk_size': 2
                    }
                }
            }
        }
    }
    spec = self._CreateBenchmarkSpec(config)
    self.assertEquals(spec.vms[0].disk_specs[0].disk_type, 'standard')

  def testEBSGP(self):
    config = {
        'vm_groups': {
            'vm_group_1': {
                'cloud': 'AWS',
                'vm_spec': {
                    'AWS': {
                        'zone': 'us-east-1a'
                    }
                },
                'disk_spec': {
                    'AWS': {
                        'disk_type': 'remote_ssd',
                        'disk_size': 2
                    }
                }
            }
        }
    }
    spec = self._CreateBenchmarkSpec(config)
    self.assertEquals(spec.vms[0].disk_specs[0].disk_type, 'gp2')

  def testEBSPIOPS(self):
    config = {
        'vm_groups': {
            'vm_group_1': {
                'cloud': 'AWS',
                'vm_spec': {
                    'AWS': {
                        'zone': 'us-east-1a'
                    }
                },
                'disk_spec': {
                    'AWS': {
                        'disk_type': 'piops',
                        'disk_size': 2
                    }
                }
            }
        }
    }
    spec = self._CreateBenchmarkSpec(config)
    self.assertEquals(spec.vms[0].disk_specs[0].disk_type, 'io1')
