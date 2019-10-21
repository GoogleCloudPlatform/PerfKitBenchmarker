# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for linux_virtual_machine.py."""

import unittest

import mock

from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import os_types
from perfkitbenchmarker import pkb
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from tests import pkb_common_test_case
import six

FLAGS = flags.FLAGS


# Need to provide implementations for all of the abstract methods in
# order to instantiate linux_virtual_machine.BaseLinuxMixin.
class LinuxVM(linux_virtual_machine.BaseLinuxMixin):

  def Install(self):
    pass

  def Uninstall(self):
    pass


class LinuxVMResource(virtual_machine.BaseVirtualMachine,
                      linux_virtual_machine.BaseLinuxMixin):

  CLOUD = 'fake_cloud'
  OS_TYPE = 'fake_os_type'
  BASE_OS_TYPE = 'debian'

  def __init__(self, _):
    super(LinuxVMResource, self).__init__(virtual_machine.BaseVmSpec('test'))

  def Install(self):
    pass

  def Uninstall(self):
    pass

  def _Create(self):
    pass

  def _Delete(self):
    pass


class TestSetFiles(pkb_common_test_case.PkbCommonTestCase):

  def runTest(self, set_files, calls):
    """Run a SetFiles test.

    Args:
      set_files: the value of FLAGS.set_files
      calls: a list of mock.call() objects giving the expected calls to
        vm.RemoteCommand() for the test.
    """
    FLAGS['set_files'].parse(set_files)

    vm = LinuxVM()

    with mock.patch.object(vm, 'RemoteCommand') as remote_command:
      vm.SetFiles()

    six.assertCountEqual(  # use assertCountEqual because order is undefined
        self,
        remote_command.call_args_list,
        calls)

  def testNoFiles(self):
    self.runTest([],
                 [])

  def testOneFile(self):
    self.runTest(['/sys/kernel/mm/transparent_hugepage/enabled=always'],
                 [mock.call('echo "always" | sudo tee '
                            '/sys/kernel/mm/transparent_hugepage/enabled')])

  def testMultipleFiles(self):
    self.runTest(['/sys/kernel/mm/transparent_hugepage/enabled=always',
                  '/sys/kernel/mm/transparent_hugepage/defrag=never'],
                 [mock.call('echo "always" | sudo tee '
                            '/sys/kernel/mm/transparent_hugepage/enabled'),
                  mock.call('echo "never" | sudo tee '
                            '/sys/kernel/mm/transparent_hugepage/defrag')])


class TestSysctl(pkb_common_test_case.PkbCommonTestCase):

  def runTest(self, sysctl, calls):
    FLAGS['sysctl'].parse(sysctl)
    vm = LinuxVM()

    with mock.patch.object(vm, 'RemoteCommand') as remote_command:
      vm.DoSysctls()

    self.assertEqual(sorted(remote_command.call_args_list), sorted(calls))

  def testSysctl(self):
    self.runTest(
        ['vm.dirty_background_ratio=10', 'vm.dirty_ratio=25'],
        [mock.call('sudo bash -c \'echo "vm.dirty_background_ratio=10" >> '
                   '/etc/sysctl.conf\''),
         mock.call('sudo bash -c \'echo "vm.dirty_ratio=25" >> '
                   '/etc/sysctl.conf\'')])

  def testNoSysctl(self):
    self.runTest([],
                 [])


class TestDiskOperations(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(TestDiskOperations, self).setUp()
    FLAGS['default_timeout'].parse(0)  # due to @retry
    patcher = mock.patch.object(LinuxVM, 'RemoteHostCommand')
    self.remote_command = patcher.start()
    self.addCleanup(patcher.stop)
    self.remote_command.side_effect = [('', None, 0), ('', None, 0)]
    self.vm = LinuxVM()

  def assertRemoteHostCalled(self, *calls):
    self.assertEqual([mock.call(call) for call in calls],
                     self.remote_command.call_args_list)

  def testMountDisk(self):
    mkdir_cmd = ('sudo mkdir -p mp;'
                 'sudo mount -o discard dp mp && '
                 'sudo chown $USER:$USER mp;')
    fstab_cmd = 'echo "dp mp ext4 defaults" | sudo tee -a /etc/fstab'
    self.vm.MountDisk('dp', 'mp')
    self.assertRemoteHostCalled(mkdir_cmd, fstab_cmd)

  def testFormatDisk(self):
    expected_command = ('[[ -d /mnt ]] && sudo umount /mnt; '
                        'sudo mke2fs -F -E lazy_itable_init=0,discard '
                        '-O ^has_journal -t ext4 -b 4096 dp')
    self.vm.FormatDisk('dp')
    self.assertRemoteHostCalled(expected_command)
    self.assertEqual('ext4', self.vm.os_metadata['disk_filesystem_type'])
    self.assertEqual(4096, self.vm.os_metadata['disk_filesystem_blocksize'])

  def testNfsMountDisk(self):
    mkdir_cmd = ('sudo mkdir -p mp;'
                 'sudo mount -t nfs -o hard,ro dp mp && '
                 'sudo chown $USER:$USER mp;')
    fstab_cmd = 'echo "dp mp nfs ro" | sudo tee -a /etc/fstab'
    self.vm.MountDisk('dp', 'mp',
                      disk_type='nfs', mount_options='hard,ro',
                      fstab_options='ro')
    self.assertRemoteHostCalled(mkdir_cmd, fstab_cmd)

  def testNfsFormatDisk(self):
    self.vm.FormatDisk('dp', disk_type='nfs')
    self.assertRemoteHostCalled()  # no format disk command executed


class LogDmesgTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(LogDmesgTestCase, self).setUp()
    self.vm = LinuxVMResource(None)

  def testPreDeleteDoesNotCallDmesg(self):
    FLAGS.log_dmesg = False
    with mock.patch.object(self.vm, 'RemoteCommand') as remote_command:
      self.vm._PreDelete()
    remote_command.assert_not_called()

  def testPreDeleteCallsDmesg(self):
    FLAGS.log_dmesg = True
    with mock.patch.object(self.vm, 'RemoteCommand') as remote_command:
      self.vm._PreDelete()
    remote_command.assert_called_once_with('hostname && dmesg', should_log=True)


class TestLsCpu(unittest.TestCase):

  LSCPU_DATA = {
      'NUMA node(s)': '1',
      'Core(s) per socket': '2',
      'Socket(s)': '3',
      'a': 'b',
  }

  def LsCpuText(self, data):
    return '\n'.join(['%s:%s' % entry for entry in data.items()])

  def CreateVm(self, os_type, remote_command_text):
    vm = LinuxVMResource(None)
    vm.OS_TYPE = os_type  # pylint: disable=invalid-name
    vm.RemoteCommand = mock.Mock()  # pylint: disable=invalid-name
    vm.RemoteCommand.return_value = remote_command_text, ''
    vm.name = 'pkb-test'
    return vm

  def testRecordLscpuOutputLinux(self):
    vm = self.CreateVm(os_types.UBUNTU1604, self.LsCpuText(self.LSCPU_DATA))
    samples = pkb._CreateLscpuSamples([vm])
    vm.RemoteCommand.assert_called_with('lscpu')
    self.assertEqual(1, len(samples))
    metadata = {'node_name': vm.name}
    metadata.update(self.LSCPU_DATA)
    expected = sample.Sample('lscpu', 0, '', metadata, samples[0].timestamp)
    self.assertEqual(expected, samples[0])

  def testRecordLscpuOutputNonLinux(self):
    vm = self.CreateVm(os_types.WINDOWS, '')
    samples = pkb._CreateLscpuSamples([vm])
    self.assertEqual(0, len(samples))
    vm.RemoteCommand.assert_not_called()

  def testMissingRequiredLsCpuEntries(self):
    with self.assertRaises(ValueError):
      linux_virtual_machine.LsCpuResults('')

  def testLsCpuParsing(self):
    vm = self.CreateVm(os_types.UBUNTU1604,
                       self.LsCpuText(self.LSCPU_DATA) + '\nThis Line=Invalid')
    results = vm.CheckLsCpu()
    self.assertEqual(1, results.numa_node_count)
    self.assertEqual(2, results.cores_per_socket)
    self.assertEqual(3, results.socket_count)
    self.assertEqual(
        {
            'NUMA node(s)': '1',
            'Core(s) per socket': '2',
            'Socket(s)': '3',
            'a': 'b'
        }, results.data)


class TestPartitionTable(unittest.TestCase):

  def CreateVm(self, remote_command_text):
    vm = LinuxVMResource(None)
    vm.RemoteCommand = mock.Mock()  # pylint: disable=invalid-name
    vm.RemoteCommand.return_value = remote_command_text, ''
    vm.name = 'pkb-test'
    vm._partition_table = {}
    return vm

  def testFdiskParsingBootDiskOnly(self):
    vm = self.CreateVm("""
Disk /dev/sda: 10.7 GB, 10737418240 bytes
4 heads, 32 sectors/track, 163840 cylinders, total 20971520 sectors
Units = sectors of 1 * 512 = 512 bytes
Sector size (logical/physical): 512 bytes / 4096 bytes
I/O size (minimum/optimal): 4096 bytes / 4096 bytes
Disk identifier: 0x00067934

   Device Boot      Start         End      Blocks   Id  System
/dev/sda1   *        2048    20971519    10484736   83  Linux
    """)
    results = vm.partition_table
    self.assertEqual(
        {'/dev/sda': 10737418240}, results)

  def testFdiskParsingWithRaidDisk(self):
    vm = self.CreateVm("""
Disk /dev/sda: 10 GiB, 10737418240 bytes, 20971520 sectors
Units: sectors of 1 * 512 = 512 bytes
Sector size (logical/physical): 512 bytes / 4096 bytes
I/O size (minimum/optimal): 4096 bytes / 4096 bytes
Disklabel type: dos
Disk identifier: 0x8c87e63b

Device     Boot Start      End  Sectors Size Id Type
/dev/sda1  *     2048 20971486 20969439  10G 83 Linux


Disk /dev/sdb: 375 GiB, 402653184000 bytes, 98304000 sectors
Units: sectors of 1 * 4096 = 4096 bytes
Sector size (logical/physical): 4096 bytes / 4096 bytes
I/O size (minimum/optimal): 4096 bytes / 4096 bytes


Disk /dev/sdc: 375 GiB, 402653184000 bytes, 98304000 sectors
Units: sectors of 1 * 4096 = 4096 bytes
Sector size (logical/physical): 4096 bytes / 4096 bytes
I/O size (minimum/optimal): 4096 bytes / 4096 bytes


Disk /dev/md0: 749.8 GiB, 805037932544 bytes, 196542464 sectors
Units: sectors of 1 * 4096 = 4096 bytes
Sector size (logical/physical): 4096 bytes / 4096 bytes
I/O size (minimum/optimal): 524288 bytes / 1048576 bytes
    """)
    results = vm.partition_table
    self.assertEqual(
        {'/dev/sda': 10737418240,
         '/dev/sdb': 402653184000,
         '/dev/sdc': 402653184000,
         '/dev/md0': 805037932544}, results)


if __name__ == '__main__':
  unittest.main()
