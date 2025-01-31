# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.gcp.gce_windows_virtual_machine."""

import typing
import unittest

from absl import flags
from absl.testing import parameterized
import mock
from perfkitbenchmarker import os_types
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import gce_windows_virtual_machine  # pylint:disable=unused-import
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class GceWindowsVirtualMachineTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.spec = gce_virtual_machine.GceVmSpec(
        'test_component', machine_type='test_machine_type', project='p'
    )
    p = mock.patch(
        gce_virtual_machine.__name__ + '.gce_network.GceNetwork.GetNetwork'
    )
    self.mock_get_network = p.start()
    self.addCleanup(p.stop)
    p = mock.patch(
        gce_virtual_machine.__name__ + '.gce_network.GceFirewall.GetFirewall'
    )
    self.mock_get_firewall = p.start()
    self.addCleanup(p.stop)

    get_tmp_dir_mock = mock.patch(
        vm_util.__name__ + '.GetTempDir', return_value='TempDir'
    )
    get_tmp_dir_mock.start()
    self.addCleanup(get_tmp_dir_mock.stop)

  @parameterized.named_parameters(
      (
          'WINDOWS2022_DESKTOP',
          os_types.WINDOWS2022_DESKTOP,
          True,
          'windows-2022',
          'windows-cloud',
      ),
      (
          'WINDOWS2022_SQLSERVER_2019_ENTERPRISE',
          os_types.WINDOWS2022_SQLSERVER_2019_ENTERPRISE,
          True,
          'sql-ent-2019-win-2022',
          'windows-sql-cloud',
      ),
  )
  def testWindowsConfig(self, os_type, gvnic, family, project):
    vm_class = typing.cast(
        type(gce_windows_virtual_machine.WindowsGceVirtualMachine),
        virtual_machine.GetVmClass(provider_info.GCP, os_type),
    )
    vm = vm_class(self.spec)
    self.assertEqual(vm.OS_TYPE, os_type)
    self.assertEqual(vm.SupportGVNIC(), gvnic)
    self.assertEqual(vm.GetDefaultImageFamily(False), family)
    self.assertEqual(vm.GetDefaultImageProject(), project)


if __name__ == '__main__':
  unittest.main()
