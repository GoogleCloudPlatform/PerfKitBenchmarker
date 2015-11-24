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


class GcpDiskTypeRenamingTest(unittest.TestCase):
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

    spec = benchmark_spec.BenchmarkSpec(config, 'name', 'uid')
    spec.ConstructVirtualMachines()

    self.assertEquals(spec.vms[0].disk_specs[0].disk_type,
                      'pd-standard')

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

    spec = benchmark_spec.BenchmarkSpec(config, 'name', 'uid')
    spec.ConstructVirtualMachines()

    self.assertEquals(spec.vms[0].disk_specs[0].disk_type,
                      'pd-ssd')


class AwsDiskTypeRenamingTest(unittest.TestCase):
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

    spec = benchmark_spec.BenchmarkSpec(config, 'name', 'uid')
    spec.ConstructVirtualMachines()

    self.assertEquals(spec.vms[0].disk_specs[0].disk_type,
                      'standard')

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

    spec = benchmark_spec.BenchmarkSpec(config, 'name', 'uid')
    spec.ConstructVirtualMachines()

    self.assertEquals(spec.vms[0].disk_specs[0].disk_type,
                      'gp2')

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

    spec = benchmark_spec.BenchmarkSpec(config, 'name', 'uid')
    spec.ConstructVirtualMachines()

    self.assertEquals(spec.vms[0].disk_specs[0].disk_type,
                      'io1')
