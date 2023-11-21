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

from typing import Dict, Union
import unittest

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized

import mock

from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import os_types
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker import vm_util
from tests import matchers
from tests import pkb_common_test_case


FLAGS = flags.FLAGS


def CreateTestLinuxVm():
  vm_spec = pkb_common_test_case.CreateTestVmSpec()
  return pkb_common_test_case.TestLinuxVirtualMachine(vm_spec=vm_spec)


def CreateCentos7Vm():
  vm_spec = pkb_common_test_case.CreateTestVmSpec()
  return TestCentos7VirtualMachine(vm_spec)


# /proc/cmdline on a GCP CentOS7 vm
_CENTOS7_KERNEL_COMMAND_LINE = (
    'BOOT_IMAGE=/boot/vmlinuz-3.10.0-1127.13.1.el7.x86_64 '
    'root=UUID=1-2-3-4-5 ro crashkernel=auto console=ttyS0,38400n8')

_DISABLE_YUM_CRON = mock.call(
    'sudo systemctl disable yum-cron.service', ignore_failure=True)


class TestCentos7VirtualMachine(linux_virtual_machine.CentOs7Mixin,
                                pkb_common_test_case.TestVirtualMachine):
  user_name = 'perfkit'


class TestSetFiles(pkb_common_test_case.PkbCommonTestCase):

  def runTest(self, set_files, calls):
    """Run a SetFiles test.

    Args:
      set_files: the value of FLAGS.set_files
      calls: a list of mock.call() objects giving the expected calls to
        vm.RemoteCommand() for the test.
    """
    FLAGS['set_files'].parse(set_files)

    vm = CreateTestLinuxVm()

    with mock.patch.object(vm, 'RemoteCommand') as remote_command:
      vm.SetFiles()

    self.assertCountEqual(  # use assertCountEqual because order is undefined
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
    vm = CreateTestLinuxVm()

    with mock.patch.object(vm, 'RemoteCommand') as remote_command:
      vm.DoSysctls()

    self.assertCountEqual(remote_command.call_args_list, calls)

  def testSysctl(self):
    self.runTest(
        ['vm.dirty_background_ratio=10', 'vm.dirty_ratio=25'],
        [mock.call('sudo bash -c \'echo "vm.dirty_background_ratio=10" >> '
                   '/etc/sysctl.conf\''),
         mock.call('sudo bash -c \'echo "vm.dirty_ratio=25" >> '
                   '/etc/sysctl.conf\''),
         mock.call('sudo sysctl -p')])

  def testNoSysctl(self):
    self.runTest([],
                 [])


class TestDiskOperations(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(TestDiskOperations, self).setUp()
    FLAGS['default_timeout'].parse(0)  # due to @retry
    patcher = mock.patch.object(pkb_common_test_case.TestLinuxVirtualMachine,
                                'RemoteHostCommand')
    self.remote_command = patcher.start()
    self.addCleanup(patcher.stop)
    self.remote_command.side_effect = [('', None, 0), ('', None, 0)]
    self.vm = CreateTestLinuxVm()

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
    self.vm = CreateTestLinuxVm()

  def testPreDeleteDoesNotCallDmesg(self):
    FLAGS.log_dmesg = False
    with mock.patch.object(self.vm, 'RemoteCommand') as remote_command:
      self.vm._PreDelete()
    remote_command.assert_not_called()

  def testPreDeleteCallsDmesg(self):
    FLAGS.log_dmesg = True
    with mock.patch.object(self.vm, 'RemoteCommand') as remote_command:
      self.vm._PreDelete()
    remote_command.assert_called_once_with('hostname && dmesg')


class TestLsCpu(unittest.TestCase, test_util.SamplesTestMixin):

  LSCPU_DATA = {
      'NUMA node(s)': '1',
      'Core(s) per socket': '2',
      'Thread(s) per core': '2',
      'Socket(s)': '3',
      'a': 'b',
  }

  PROC_CPU_TEXT = """
  processor: 29
  cpu family: 6
  core id: 13
  oddkey: v29
  apicid: 27

  processor: 30
  cpu family: 6
  core id: 14
  oddkey: v30
  apicid:29

  processor: 31
  cpu family: 6
  core id: 15
  apicid: 31
  """

  def LsCpuText(self, data):
    return '\n'.join(['%s:%s' % entry for entry in data.items()])

  def CreateVm(self, os_type, remote_command_text):
    vm = CreateTestLinuxVm()
    vm.OS_TYPE = os_type  # pylint: disable=invalid-name
    vm.RemoteCommand = mock.Mock()  # pylint: disable=invalid-name
    vm.RemoteCommand.return_value = remote_command_text, ''
    vm.name = 'pkb-test'
    return vm

  def testRecordLscpuOutputLinux(self):
    vm = self.CreateVm(os_types.DEFAULT, self.LsCpuText(self.LSCPU_DATA))
    samples = linux_virtual_machine.CreateLscpuSamples([vm])
    vm.RemoteCommand.assert_called_with('lscpu')
    self.assertEqual(1, len(samples))
    metadata = {'node_name': vm.name}
    metadata.update(self.LSCPU_DATA)
    expected = sample.Sample('lscpu', 0, '', metadata, samples[0].timestamp)
    self.assertEqual(expected, samples[0])

  def testRecordLscpuOutputNonLinux(self):
    vm = self.CreateVm(os_types.WINDOWS, '')
    samples = linux_virtual_machine.CreateLscpuSamples([vm])
    self.assertEqual(0, len(samples))
    vm.RemoteCommand.assert_not_called()

  def testMissingRequiredLsCpuEntries(self):
    with self.assertRaises(ValueError):
      linux_virtual_machine.LsCpuResults('')

  def testLsCpuParsing(self):
    vm = self.CreateVm(os_types.DEFAULT,
                       self.LsCpuText(self.LSCPU_DATA) + '\nThis Line=Invalid')
    results = vm.CheckLsCpu()
    self.assertEqual(1, results.numa_node_count)
    self.assertEqual(2, results.cores_per_socket)
    self.assertEqual(2, results.threads_per_core)
    self.assertEqual(3, results.socket_count)
    self.assertEqual(
        {
            'NUMA node(s)': '1',
            'Core(s) per socket': '2',
            'Thread(s) per core': '2',
            'Socket(s)': '3',
            'a': 'b'
        }, results.data)

  def testProcCpuParsing(self):
    vm = self.CreateVm(os_types.DEFAULT, self.PROC_CPU_TEXT)
    results = vm.CheckProcCpu()
    expected_mappings = {}
    expected_mappings[29] = {'apicid': '27', 'core id': '13'}
    expected_mappings[30] = {'apicid': '29', 'core id': '14'}
    expected_mappings[31] = {'apicid': '31', 'core id': '15'}
    expected_common = {
        'cpu family': '6',
        'oddkey': 'v29;v30',
        'proccpu': 'cpu family,oddkey'
    }
    self.assertEqual(expected_mappings, results.mappings)
    self.assertEqual(expected_common, results.GetValues())

  def testProcCpuSamples(self):
    vm = self.CreateVm(os_types.DEFAULT, self.PROC_CPU_TEXT)
    samples = linux_virtual_machine.CreateProcCpuSamples([vm])
    proccpu_metadata = {
        'cpu family': '6',
        'node_name': 'pkb-test',
        'oddkey': 'v29;v30',
        'proccpu': 'cpu family,oddkey',
    }
    proccpu_mapping_metadata = {
        'node_name': 'pkb-test',
        'proc_29': 'apicid=27;core id=13',
        'proc_30': 'apicid=29;core id=14',
        'proc_31': 'apicid=31;core id=15'
    }
    expected_samples = [
        sample.Sample('proccpu', 0, '', proccpu_metadata),
        sample.Sample('proccpu_mapping', 0, '', proccpu_mapping_metadata)
    ]
    self.assertSampleListsEqualUpToTimestamp(expected_samples, samples)


class TestRemoteCommand(pkb_common_test_case.PkbCommonTestCase):
  """Test class for testing linux_virtual_machine.RemoteCommand internals."""

  def setUp(self):
    super().setUp()
    self.vm = CreateTestLinuxVm()
    self.vm.name = 'pkb-test'
    self.issue_cmd_mock = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', autospec=True)
    )
    self.issue_cmd_mock.return_value = ('output', '', 0)
    self.vm.ip_address = 'localhost:4000'

  def testSshAppendedToCommandButNotLog(self):
    with self.assertLogs() as logs:
      self.vm.RemoteCommand('foo')
    self.assertEqual(
        logs.output,
        ['INFO:root:Running on %s via ssh: %s' % ('pkb-test', 'foo')],
    )
    self.issue_cmd_mock.assert_called_once_with(
        matchers.HASALLOF('ssh', 'PasswordAuthentication=no', 'foo'),
        timeout=None,
        should_pre_log=False,
        raise_on_failure=False,
        stack_level=mock.ANY,
    )

  def testIssueCommanndCalledWithStackLevel(self):
    self.vm.RemoteCommand('foo')
    self.issue_cmd_mock.assert_called_once_with(
        mock.ANY,
        timeout=None,
        should_pre_log=False,
        raise_on_failure=False,
        stack_level=4,
    )

  @parameterized.parameters(
      'RemoteCommand',
      'RemoteCommandWithReturnCode',
      'RemoteHostCommand',
      'RemoteHostCommandWithReturnCode',
  )
  def testLogComesFromRightFile(self, method_name):
    method = getattr(self.vm, method_name)
    with self.assertLogs() as logs:
      method('foo')
    self.assertLen(logs.output, 1)
    self.assertIn('linux_virtual_machine_test', logs.records[0].pathname)

  def testLoginShellAppendsBash(self):
    self.vm.RemoteCommand('foo', login_shell=True)
    self.issue_cmd_mock.assert_called_once_with(
        matchers.HASALLOF('-t', 'bash -l -c "foo"'),
        timeout=None,
        should_pre_log=False,
        raise_on_failure=False,
        stack_level=mock.ANY,
    )

  def testNonZeroReturnCodeRaises(self):
    self.issue_cmd_mock.return_value = ('output', 'err', 1)
    with self.assertRaises(errors.VirtualMachine.RemoteCommandError):
      self.vm.RemoteCommand('foo')

  def testNonZeroReturnCodeIgnored(self):
    self.issue_cmd_mock.return_value = ('output', 'err', 1)
    stdout, stderr, ret = self.vm.RemoteCommandWithReturnCode(
        'foo', ignore_failure=True)
    self.assertEqual(stdout, 'output')
    self.assertEqual(stderr, 'err')
    self.assertEqual(ret, 1)

  def testRetriesInLimitSucceeds(self):
    self.issue_cmd_mock.side_effect = [(
        'o',
        '',
        linux_virtual_machine.RETRYABLE_SSH_RETCODE,
    )] * 2 + [
        ('o', '', 0),
    ]
    _, _, ret_val = self.vm.RemoteCommandWithReturnCode('foo', retries=3)
    self.assertEqual(ret_val, 0)

  def testRetriesOutOfLimitFails(self):
    self.issue_cmd_mock.side_effect = [(
        'o',
        '',
        linux_virtual_machine.RETRYABLE_SSH_RETCODE,
    )] * 2 + [
        ('o', '', 0),
    ]
    with self.assertRaises(errors.VirtualMachine.RemoteCommandError):
      self.vm.RemoteCommand('foo', retries=2)


class TestPartitionTable(unittest.TestCase):

  def CreateVm(self, remote_command_text):
    vm = CreateTestLinuxVm()
    vm.RemoteCommand = mock.Mock()  # pylint: disable=invalid-name
    vm.RemoteCommand.return_value = remote_command_text, ''
    vm.name = 'pkb-test'
    vm._partition_table = {}
    return vm

  def testFdiskNoPartitonTable(self):
    vm = self.CreateVm('')
    results = vm.partition_table
    self.assertEqual({}, results)

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


class LinuxVirtualMachineTestCase(pkb_common_test_case.PkbCommonTestCase):
  os_info = 'Ubuntu 18.04.1 LTS'
  kernel_release = '5.3.0-1026'
  cpu_arch = 'x86_64'
  partition_table = 'Disk /dev/sda: 1 GiB, 1073741824 bytes, 2097152 sectors'
  lscpu_output = '\n'.join([
      'NUMA node(s): 1',
      'Core(s) per socket: 1',
      'Thread(s) per core: 1',
      'Socket(s): 1',
  ])
  normal_boot_responses = {
      'cat /proc/sys/net/ipv4/tcp_congestion_control': 'cubic',
      'grep PRETTY_NAME /etc/os-release': f'PRETTY_NAME="{os_info}"',
      'uname -r': kernel_release,
      'uname -m': cpu_arch,
      'sudo fdisk -l': partition_table,
      'PATH="${PATH}":/usr/sbin ip link show up':
          pkb_common_test_case.IP_LINK_TEXT,
      'cat /proc/cpuinfo | grep processor | wc -l': '16'
  }

  def CreateVm(self, run_cmd_response: Union[str, Dict[str, str]]):
    vm = CreateTestLinuxVm()

    def FakeRemoteHostCommandWithReturnCode(cmd, **_):
      if isinstance(run_cmd_response, str):
        stdout = run_cmd_response
      elif isinstance(run_cmd_response, dict):
        # NOTE: @vm_util.Retry would infinitely retry a KeyError on this map so
        # we raise SystemExit which does not inherit from Exception.
        # Unfortunately other issues, like a typo can raise
        # AttributeError which is retried until the test times out.
        # See b/271465182 for more discussion.
        if cmd not in run_cmd_response:
          raise SystemExit(f'Define response for {cmd}')
        stdout = run_cmd_response[cmd]
      else:
        raise NotImplementedError()
      return stdout, '', 0

    # Re-sets the VM's boot time so TestReboot doesn't time out.
    def FakeWaitForBootCompletion():
      vm.bootable_time = 10

    vm.RemoteHostCommandWithReturnCode = mock.Mock(
        side_effect=FakeRemoteHostCommandWithReturnCode)
    vm.CheckLsCpu = mock.Mock(
        return_value=linux_virtual_machine.LsCpuResults(self.lscpu_output))
    vm.WaitForBootCompletion = mock.Mock(side_effect=FakeWaitForBootCompletion)
    return vm

  @parameterized.named_parameters(
      ('has_smt_centos7', _CENTOS7_KERNEL_COMMAND_LINE, True),
      ('no_smt_centos7', _CENTOS7_KERNEL_COMMAND_LINE + ' noht nosmt nr_cpus=1',
       False))
  def testIsSmtEnabled(self, proc_cmdline, is_enabled):
    vm = self.CreateVm(proc_cmdline)
    self.assertEqual(is_enabled, vm.IsSmtEnabled())

  @parameterized.named_parameters(
      ('hasSMT_want_real', 32, 'regular', 16),
      ('noSMT_want_real', 32, 'nosmt', 32),
  )
  def testNumCpusForBenchmarkNoSmt(self, vcpus, kernel_command_line,
                                   expected_num_cpus):
    FLAGS['use_numcpu_multi_files'].parse(True)
    responses = {
        'ls /sys/fs/cgroup/cpuset.cpus.effective >> /dev/null 2>&1 || echo file_not_exist': (
            ''
        ),
        'cat /sys/fs/cgroup/cpuset.cpus.effective': f'0-{vcpus-1}',
        'cat /proc/cmdline': kernel_command_line,
    }
    vm = self.CreateVm(responses)
    self.assertEqual(expected_num_cpus, vm.NumCpusForBenchmark(True))

  def testNumCpusForBenchmarkDefaultCall(self):
    # shows that IsSmtEnabled is not called unless new optional parameter used
    FLAGS['use_numcpu_multi_files'].parse(True)
    responses = {
        'ls /sys/fs/cgroup/cpuset.cpus.effective >> /dev/null 2>&1 || echo file_not_exist': (
            ''
        ),
        'cat /sys/fs/cgroup/cpuset.cpus.effective': '0-31',
    }
    vm = self.CreateVm(responses)
    vm.IsSmtEnabled = mock.Mock()
    self.assertEqual(32, vm.NumCpusForBenchmark())
    vm.IsSmtEnabled.assert_not_called()
    self.assertEqual(32, vm.NumCpusForBenchmark(False))
    vm.IsSmtEnabled.assert_not_called()

  def testNumCpusFromDifferentSources(self):
    # Shows the extraction of number of CPUs from different sources.
    FLAGS['use_numcpu_multi_files'].parse(True)
    responses = {
        'ls /sys/fs/cgroup/cpuset.cpus.effective >> /dev/null 2>&1 || echo file_not_exist': (
            ''
        ),
        'cat /sys/fs/cgroup/cpuset.cpus.effective': '0-15,17-32',
    }
    vm = self.CreateVm(responses)
    self.assertEqual(32, vm.NumCpusForBenchmark())
    responses = {
        'ls /sys/fs/cgroup/cpuset.cpus.effective >> /dev/null 2>&1 || echo file_not_exist': (
            'file_not_exist'
        ),
        'ls /dev/cgroup/cpuset.cpus.effective >> /dev/null 2>&1 || echo file_not_exist': (
            'file_not_exist'
        ),
        'ls /proc/self/status >> /dev/null 2>&1 || echo file_not_exist': '',
        'cat /proc/self/status | grep Cpus_allowed_list': (
            'Cpus_allowed_list:\t0-3,7-8'
        ),
    }
    vm = self.CreateVm(responses)
    self.assertEqual(6, vm.NumCpusForBenchmark())
    responses = {
        'ls /sys/fs/cgroup/cpuset.cpus.effective >> /dev/null 2>&1 || echo file_not_exist': (
            'file_not_exist'
        ),
        'ls /dev/cgroup/cpuset.cpus.effective >> /dev/null 2>&1 || echo file_not_exist': (
            'file_not_exist'
        ),
        'ls /proc/self/status >> /dev/null 2>&1 || echo file_not_exist': (
            'file_not_exist'
        ),
        'ls /proc/cpuinfo >> /dev/null 2>&1 || echo file_not_exist': '',
        'cat /proc/cpuinfo | grep processor | wc -l': '16',
    }
    vm = self.CreateVm(responses)
    self.assertEqual(16, vm.NumCpusForBenchmark())

  def testMemCapFromDifferentSources(self):
    # Shows the extraction of number of CPUs from different sources.
    FLAGS['use_cgroup_memory_limits'].parse(True)
    responses = {
        'ls /proc/self/cgroup >> /dev/null 2>&1 || echo file_not_exist': '',
        "grep memory /proc/self/cgroup |cut -d ':' -f 3 |sed -e 's:^/::'": (
            'container\n'
        ),
        'ls /sys/fs/cgroup/memory/container/memory.limit_in_bytes >> /dev/null 2>&1 || echo file_not_exist': (
            ''
        ),
        'cat /sys/fs/cgroup/memory/container/memory.limit_in_bytes': '1024',
        "cat /proc/meminfo | grep MemTotal | awk '{print $2}'": '3',
    }
    vm = self.CreateVm(responses)
    self.assertEqual(1, vm.total_memory_kb)
    responses = {
        'ls /proc/self/cgroup >> /dev/null 2>&1 || echo file_not_exist': '',
        "grep memory /proc/self/cgroup |cut -d ':' -f 3 |sed -e 's:^/::'": (
            'container\n'
        ),
        'ls /sys/fs/cgroup/memory/container/memory.limit_in_bytes >> /dev/null 2>&1 || echo file_not_exist': (
            'file_not_exist'
        ),
        'ls /sys/fs/cgroup/memory/memory.limit_in_bytes >> /dev/null 2>&1 || echo file_not_exist': (
            ''
        ),
        'cat /sys/fs/cgroup/memory/memory.limit_in_bytes': '2048',
        "cat /proc/meminfo | grep MemTotal | awk '{print $2}'": '3',
    }
    vm = self.CreateVm(responses)
    self.assertEqual(2, vm.total_memory_kb)
    FLAGS['use_cgroup_memory_limits'].parse(False)
    responses = {
        "cat /proc/meminfo | grep MemTotal | awk '{print $2}'": '3',
    }
    vm = self.CreateVm(responses)
    self.assertEqual(3, vm.total_memory_kb)

  def testBoot(self):
    vm = self.CreateVm(self.normal_boot_responses)
    vm.WaitForBootCompletion()
    vm.RecordAdditionalMetadata()
    expected_os_metadata = {
        '/dev/sda': 1073741824,
        'kernel_release': self.kernel_release,
        'mtu': '1500',
        'os_info': self.os_info,
        'cpu_arch': self.cpu_arch,
        'threads_per_core': 1,
    }
    self.assertEqual(expected_os_metadata, vm.os_metadata)

  def testReboot(self):
    responses = self.normal_boot_responses.copy()
    vm = self.CreateVm(responses)
    vm.RecordAdditionalMetadata()
    os_info_new = 'Ubuntu 18.04.1b LTS'
    kernel_release_new = '5.3.0-1027'
    responses.update({
        'sudo reboot': 'bye bye',
        'hostname': 'hello there',
        'grep PRETTY_NAME /etc/os-release': f'PRETTY_NAME="{os_info_new}"',
        'uname -r': kernel_release_new,
        'stat -c %z /proc/': '',
        'sudo mkdir -p /opt/pkb; sudo chmod a+rwxt /opt/pkb': '',
        'mkdir -p /tmp/pkb': '',
        'cat /proc/cpuinfo | grep processor | wc -l': '8'
    })
    vm.Reboot()
    self.assertEqual(os_info_new, vm.os_metadata['os_info'])
    self.assertEqual(kernel_release_new, vm.os_metadata['kernel_release'])
    self.assertEqual(8, vm.NumCpusForBenchmark())

  def testGetNetworkDeviceNames(self):
    responses = self.normal_boot_responses.copy()
    vm = self.CreateVm(responses)
    names = vm._get_network_device_mtus()
    self.assertEqual({'eth0': '1500', 'eth1': '1500'}, names)
    mock_cmd = vm.RemoteHostCommandWithReturnCode
    mock_cmd.assert_called_with('PATH="${PATH}":/usr/sbin ip link show up',
                                stack_level=3)

  def testCpuVulnerabilitiesEmpty(self):
    self.assertEqual({}, self.CreateVm('').cpu_vulnerabilities.asdict)

  def testCpuVulnerabilities(self):
    # lines returned from running "grep . .../cpu/vulnerabilities/*"
    cpu_vuln_lines = [
        '.../itlb_multihit:KVM: Vulnerable',
        '.../l1tf:Mitigation: PTE Inversion',
        '.../mds:Vulnerable: Clear CPU buffers attempted, no microcode',
        '.../meltdown:Mitigation: PTI',
        '.../spec_store_bypass:Mitigation: Speculative Store Bypass disabled',
        '.../spectre_v1:Mitigation: usercopy/swapgs barriers',
        '.../spectre_v2:Mitigation: Full generic retpoline, IBPB: conditional',
        '.../srbds:Not affected',
        '.../tsx_async_abort:Not affected',
        # Not actually seen, shows that falls into "unknowns"
        '.../made_up:Unknown Entry',
    ]
    cpu_vuln = self.CreateVm('\n'.join(cpu_vuln_lines)).cpu_vulnerabilities
    expected_mitigation = {
        'l1tf': 'PTE Inversion',
        'meltdown': 'PTI',
        'spec_store_bypass': 'Speculative Store Bypass disabled',
        'spectre_v1': 'usercopy/swapgs barriers',
        'spectre_v2': 'Full generic retpoline, IBPB: conditional',
    }
    self.assertEqual(expected_mitigation, cpu_vuln.mitigations)
    expected_vulnerability = {
        'itlb_multihit': 'KVM',
        'mds': 'Clear CPU buffers attempted, no microcode'
    }
    self.assertEqual(expected_vulnerability, cpu_vuln.vulnerabilities)
    expected_notaffecteds = set(['srbds', 'tsx_async_abort'])
    self.assertEqual(expected_notaffecteds, cpu_vuln.notaffecteds)
    expected_unknowns = {'made_up': 'Unknown Entry'}
    self.assertEqual(expected_unknowns, cpu_vuln.unknowns)
    expected_asdict = {
        'mitigations': 'l1tf,meltdown,spec_store_bypass,spectre_v1,spectre_v2',
        'mitigation_l1tf': 'PTE Inversion',
        'mitigation_meltdown': 'PTI',
        'mitigation_spec_store_bypass': 'Speculative Store Bypass disabled',
        'mitigation_spectre_v1': 'usercopy/swapgs barriers',
        'mitigation_spectre_v2': 'Full generic retpoline, IBPB: conditional',
        'notaffecteds': 'srbds,tsx_async_abort',
        'unknown_made_up': 'Unknown Entry',
        'unknowns': 'made_up',
        'vulnerabilities': 'itlb_multihit,mds',
        'vulnerability_itlb_multihit': 'KVM',
        'vulnerability_mds': 'Clear CPU buffers attempted, no microcode',
    }
    self.assertEqual(expected_asdict, cpu_vuln.asdict)

  @parameterized.named_parameters(('flag_true', True, _DISABLE_YUM_CRON),
                                  ('default_flag', None, _DISABLE_YUM_CRON),
                                  ('flag_false', False, None))
  def testCentos7OnStartup(self, flag_disable_yum_cron, additional_command):
    vm = CreateCentos7Vm()
    mock_remote = mock.Mock(return_value=('', ''))
    vm.RemoteHostCommand = mock_remote  # pylint: disable=invalid-name

    if flag_disable_yum_cron is not None:
      with flagsaver.flagsaver(disable_yum_cron=flag_disable_yum_cron):
        vm.OnStartup()
    else:
      # tests the default value of the flag
      vm.OnStartup()

    common_call = ("echo 'Defaults:perfkit !requiretty' | "
                   'sudo tee /etc/sudoers.d/pkb')
    calls = [mock.call(common_call, login_shell=True)]
    if additional_command:
      calls.append(additional_command)
    mock_remote.assert_has_calls(calls)

  def testRebootCommandNonRebootable(self):
    vm = CreateTestLinuxVm()
    vm.IS_REBOOTABLE = False
    with self.assertRaises(errors.VirtualMachine.VirtualMachineError):
      vm._Reboot()

  @parameterized.named_parameters(
      ('regular', CreateTestLinuxVm, False))
  def testRebootCommand(self, vm_create_function, ignore_ssh_error):
    vm = vm_create_function()
    mock_remote = mock.Mock(return_value=('', ''))
    vm.RemoteCommand = mock_remote  # pylint: disable=invalid-name

    vm._Reboot()

    if ignore_ssh_error:
      mock_remote.assert_called_with(
          'sudo reboot', ignore_failure=True, ignore_ssh_error=True)
    else:
      mock_remote.assert_called_with('sudo reboot', ignore_failure=True)

  def testUnreachableVm(self):
    vm = self.CreateVm(self.normal_boot_responses.copy())
    vm.WaitForBootCompletion.side_effect = vm_util.TimeoutExceededRetryError

    with self.assertRaises(vm_util.TimeoutExceededRetryError):
      vm.WaitForBootCompletion()
    self.assertIsNone(vm.bootable_time)

    vm.RecordAdditionalMetadata()
    self.assertEqual({}, vm.os_metadata)


if __name__ == '__main__':
  unittest.main()
