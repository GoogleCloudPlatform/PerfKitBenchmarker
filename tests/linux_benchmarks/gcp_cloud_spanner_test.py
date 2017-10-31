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

"""Tests for perfkitbenchmarker.linux_packages.cloud_spanner_ycsb_benchmark"""

import mock
import unittest
import logging

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.providers.gcp import gcp_spanner
from perfkitbenchmarker.linux_benchmarks import cloud_spanner_ycsb_benchmark

class GcpCloudSpannerTestCase(unittest.TestCase):
  def setUp(self):
    self.spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    vm0 = mock.MagicMock()
    vm0.RemoteCommand.side_effect = [(' ', ' ')]
    self.spec.vms = [vm0]

  # Setup flags and mock cloud spanner instance and database existence status
  # Check Delete and Create calls
  def prepareHelper(self, idx, flag_values_, side_effects_, ndelete_, ncreate_, nexist_, params_create_):
      print 'Testcase', idx
      flag_values = flag_values_
      p = mock.patch(cloud_spanner_ycsb_benchmark.__name__ + '.FLAGS')
      flags_mock = p.start()
      flags_mock.configure_mock(**flag_values)
      self.addCleanup(p.stop)
      side_effect=lambda **kwargs: [(yield x) for x in side_effects_]
      with mock.patch.object(gcp_spanner.GcpSpannerInstance, '_Exists',
           side_effect=side_effect()) as mock_exist, mock.patch.object(
           gcp_spanner.GcpSpannerInstance, 'Delete', return_value=None) as mock_delete, mock.patch.object(
                   gcp_spanner.GcpSpannerInstance, '_Create',
                   return_value=None) as mock_create:
        cloud_spanner_ycsb_benchmark.Prepare(self.spec)
        self.assertEquals(mock_delete.call_count, ndelete_)
        self.assertEquals(mock_create.call_count, ncreate_)
        self.assertEquals(mock_exist.call_count, nexist_)
        mock_create.assert_called_once_with(**params_create_)

  def testPrepareAnonymousInstance(self):
    for idx, [flag_values_, side_effects_, [ndelete_, ncreate_, nexist_], params_create_] in enumerate([
        # Normal: create new instance and database
        [ {'run_uri': '123', 'cloud_spanner_instance': None},
          [False, False, True],
          [0, 1, 3],
          {'create_instance': True, 'create_database': True}
        ],
        # Delete previous anonymoust instance and create new instance and database
        [ {'run_uri': '123', 'cloud_spanner_instance': None},
          [True, False, True],
          [1, 1, 3],
          {'create_instance': True, 'create_database': True}
        ],
        # Failure to create instance or database result in instance delete
        [ {'run_uri': '123', 'cloud_spanner_instance': None},
          [True, False, False],
          [2, 1, 3],
          {'create_instance': True, 'create_database': True}
        ],
        [ {'run_uri': '123', 'cloud_spanner_instance': None},
          [False, False, False],
          [1, 1, 3],
          {'create_instance': True, 'create_database': True}
        ],
    ]):
      self.prepareHelper(idx, flag_values_, side_effects_, ndelete_, ncreate_, nexist_, params_create_)

  def testPrepareNamedInstanceDatabase(self):
    # Important not to delete any instance
    # Failure to create instance or database will keep instance in tact
    for idx, [flag_values_, side_effects_, [ndelete_, ncreate_, nexist_], params_create_] in enumerate([
        # Normal: create new instance and database
        [ {'run_uri': '123', 'cloud_spanner_instance': 'test-instance'},
          [False, True],
          [0, 1, 2],
          {'create_instance': True, 'create_database': True}
        ],
        # Reuse provided instance and database
        [ {'run_uri': '123', 'cloud_spanner_instance': 'test-instance'},
          [True, True],
          [0, 1, 2],
          {'create_instance': False, 'create_database': False}
        ],
        # Reuse provided instance and create database
        [ {'run_uri': '123', 'cloud_spanner_instance': 'test-instance'},
          [True, False],
          [0, 1, 2],
          {'create_instance': False, 'create_database': True}
        ],
    ]):
      self.prepareHelper(idx, flag_values_, side_effects_, ndelete_, ncreate_, nexist_, params_create_)

if __name__ == '__main__':
  unittest.main()
