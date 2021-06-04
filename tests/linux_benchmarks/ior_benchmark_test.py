# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for ior_benchmark."""

import unittest
from absl.testing import flagsaver
import mock

from perfkitbenchmarker import disk
from perfkitbenchmarker import hpc_util
from perfkitbenchmarker import pkb  # pylint:disable=unused-import
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import ior_benchmark
from perfkitbenchmarker.linux_packages import ior
from tests import pkb_common_test_case


def MockVm():
  vm = mock.Mock()
  vm.scratch_disks = [mock.Mock(mount_point='/ior', data_disk_type='nfs')]
  return vm


runior_sample = sample.Sample('runior', 0, '')
runmdtest_sample = sample.Sample('runmdtest', 0, '')


class IorBenchmarkTestCase(pkb_common_test_case.PkbCommonTestCase,
                           test_util.SamplesTestMixin):

  def setUp(self):
    super().setUp()
    self.headnode = MockVm()
    self.vms = [self.headnode, MockVm()]
    self.spec = mock.Mock(vms=self.vms)
    self.mock_runior = self.enter_context(mock.patch.object(ior, 'RunIOR'))
    self.mock_runmdtest = self.enter_context(
        mock.patch.object(ior, 'RunMdtest'))
    self.mock_runior.return_value = [runior_sample]
    self.mock_runmdtest.return_value = [runmdtest_sample]

  @mock.patch.object(hpc_util, 'CreateMachineFile')
  def testPrepare(self, mock_create_machine_file):
    ior_benchmark.Prepare(self.spec)
    for vm in self.vms:
      vm.Install.assert_called_with('ior')
      vm.AuthenticateVm.assert_called()
    mock_create_machine_file.assert_called_with(self.vms)

  def testRunDefaults(self):
    samples = ior_benchmark.Run(self.spec)

    self.headnode.PushDataFile.assert_called_with(
        'default_ior_script',
        '/ior/default_ior_script',
        should_double_copy=False)
    self.mock_runior.assert_called_with(self.headnode, 256,
                                        '/ior/default_ior_script')
    # one RunIOR and 3 MdTest based on mdtest_args
    expected_samples = [
        runior_sample, runmdtest_sample, runmdtest_sample, runmdtest_sample
    ]
    self.assertSampleListsEqualUpToTimestamp(expected_samples, samples)
    self.mock_runmdtest.assert_has_calls([
        mock.call(self.headnode, 32, '-n 1000 -u -C'),
        mock.call(self.headnode, 32, '-n 1000 -u -T'),
        mock.call(self.headnode, 32, '-n 1000 -u -r')
    ])
    for vm in self.vms:
      vm.DropCaches.assert_called()

  @flagsaver.flagsaver(mdtest_drop_caches=False)
  def testNoDropCache(self):
    samples = ior_benchmark.Run(self.spec)

    # only 1 RunMdTest
    expected_samples = [runior_sample, runmdtest_sample]
    self.assertSampleListsEqualUpToTimestamp(expected_samples, samples)
    self.mock_runmdtest.assert_has_calls([
        mock.call(self.headnode, 32, '-n 1000 -u -C -T -r'),
    ])
    for vm in self.vms:
      vm.DropCaches.assert_not_called()

  @flagsaver.flagsaver(ior_script=None)
  def testNoIorScript(self):
    ior_benchmark.Run(self.spec)

    self.headnode.PushDataFile.assert_not_called()
    self.mock_runior.assert_not_called()

  @flagsaver.flagsaver(
      ior_script='myior.sh',
      ior_num_procs=4,
      mdtest_num_procs=24,
      mdtest_args=['a', 'b'],
      data_disk_type=disk.SMB)
  def testNonDefaultFlags(self):
    samples = ior_benchmark.Run(self.spec)

    self.headnode.PushDataFile.assert_called_with(
        'myior.sh', '/ior/myior.sh', should_double_copy=True)

    expected_mdtest_args = ['a -C', 'a -T', 'a -r', 'b -C', 'b -T', 'b -r']
    expected_calls = [
        mock.call(self.headnode, 24, args) for args in expected_mdtest_args
    ]
    self.mock_runmdtest.assert_has_calls(expected_calls)
    self.mock_runior.assert_called_with(self.headnode, 4, '/ior/myior.sh')
    self.assertLen(samples, 7)


if __name__ == '__main__':
  unittest.main()
