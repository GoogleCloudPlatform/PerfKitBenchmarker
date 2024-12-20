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
"""Common base class for PKB unittests."""

import pathlib
from typing import Any, Dict

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import command_interface
from perfkitbenchmarker import configs
from perfkitbenchmarker import context
from perfkitbenchmarker import linux_benchmarks
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import os_mixin
from perfkitbenchmarker import pkb  # pylint:disable=unused-import
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import util
from tests import mock_command

FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()


# Tests Docker and IB filtered out and having multiple eth with same MTU
IP_LINK_TEXT = """\
1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN mode DEFAULT group default qlen 1000
    link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
2: eth0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc mq state UP mode DEFAULT group default qlen 1000
    link/ether 00:0d:3a:ed:2d:65 brd ff:ff:ff:ff:ff:ff
3: ib0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 2044 qdisc mq state UP mode DEFAULT group default qlen 256
    link/infiniband 00:00:01:49:fe:80:00:00:00:00:00:00:00:15:5d:ff:fd:34:02:db brd 00:ff:ff:ff:ff:12:40:1b:80:3a:00:00:00:00:00:00:ff:ff:ff:ff
4: ib1: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 2044 qdisc mq state UP mode DEFAULT group default qlen 256
    link/infiniband 00:00:01:49:fe:80:00:00:00:00:00:00:00:15:5d:ff:fd:34:02:dc brd 00:ff:ff:ff:ff:12:40:1b:80:3a:00:00:00:00:00:00:ff:ff:ff:ff
5: ib2: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 2044 qdisc mq state UP mode DEFAULT group default qlen 256
    link/infiniband 00:00:01:49:fe:80:00:00:00:00:00:00:00:15:5d:ff:fd:34:02:dd brd 00:ff:ff:ff:ff:12:40:1b:80:3a:00:00:00:00:00:00:ff:ff:ff:ff
6: ib3: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 2044 qdisc mq state UP mode DEFAULT group default qlen 256
    link/infiniband 00:00:01:49:fe:80:00:00:00:00:00:00:00:15:5d:ff:fd:34:02:de brd 00:ff:ff:ff:ff:12:40:1b:80:3a:00:00:00:00:00:00:ff:ff:ff:ff
7: ib4: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 2044 qdisc mq state UP mode DEFAULT group default qlen 256
    link/infiniband 00:00:01:49:fe:80:00:00:00:00:00:00:00:15:5d:ff:fd:34:02:df brd 00:ff:ff:ff:ff:12:40:1b:80:3a:00:00:00:00:00:00:ff:ff:ff:ff
8: ib5: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 2044 qdisc mq state UP mode DEFAULT group default qlen 256
    link/infiniband 00:00:01:49:fe:80:00:00:00:00:00:00:00:15:5d:ff:fd:34:02:e0 brd 00:ff:ff:ff:ff:12:40:1b:80:3a:00:00:00:00:00:00:ff:ff:ff:ff
9: ib6: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 2044 qdisc mq state UP mode DEFAULT group default qlen 256
    link/infiniband 00:00:01:49:fe:80:00:00:00:00:00:00:00:15:5d:ff:fd:34:02:e1 brd 00:ff:ff:ff:ff:12:40:1b:80:3a:00:00:00:00:00:00:ff:ff:ff:ff
10: ib7: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 2044 qdisc mq state UP mode DEFAULT group default qlen 256
    link/infiniband 00:00:01:49:fe:80:00:00:00:00:00:00:00:15:5d:ff:fd:34:02:e2 brd 00:ff:ff:ff:ff:12:40:1b:80:3a:00:00:00:00:00:00:ff:ff:ff:ff
11: docker0: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 qdisc noqueue state DOWN mode DEFAULT group default
    link/ether 02:42:8a:7d:93:c8 brd ff:ff:ff:ff:ff:ff
12: eth1: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc mq state UP mode DEFAULT group default qlen 1000
    link/ether 00:0d:3a:ed:2d:67 brd ff:ff:ff:ff:ff:ff
"""


class TestVmSpec(virtual_machine.BaseVmSpec):
  CLOUD = 'test_vm_spec_cloud'


def CreateTestVmSpec() -> TestVmSpec:
  return TestVmSpec('test_component_name')


class TestOsMixin(os_mixin.BaseOsMixin):
  """Test class that provides dummy implementations of abstract functions."""

  OS_TYPE = 'test_os_type'
  BASE_OS_TYPE = 'debian'

  def Install(self, pkg):
    pass

  def PackageCleanup(self):
    pass

  def RemoteCommand(
      self, command, ignore_failure=False, timeout=None, **kwargs
  ):
    pass

  def RemoteCopy(self, file_path, remote_path='', copy_to=True):
    pass

  def SetReadAhead(self, num_sectors, devices):
    pass

  def Uninstall(self, package_name):
    pass

  def _Suspend(self):
    pass

  def _Resume(self):
    pass

  def VMLastBootTime(self):
    pass

  def WaitForBootCompletion(self):
    pass

  def _WaitForSSH(self, ip_address=None):
    pass

  def _Start(self):
    pass

  def _Stop(self):
    pass

  def _CreateScratchDiskFromDisks(self, disk_spec, disks):
    pass

  def _PrepareScratchDisk(self, scratch_disk, disk_spec):
    pass

  def _GetNumCpus(self):
    pass

  def _GetTotalFreeMemoryKb(self):
    pass

  def _GetTotalMemoryKb(self):
    pass

  def _Reboot(self):
    pass

  def _TestReachable(self, ip):
    pass

  def _IsSmtEnabled(self):
    return True

  def GetConnectionIp(self):
    pass


class TestResource(resource.BaseResource):

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.created = True

  def _Create(self):
    pass

  def _Delete(self):
    pass


class TestVirtualMachine(
    TestResource, TestOsMixin, virtual_machine.BaseVirtualMachine
):
  """Test class that has dummy methods for a base virtual machine."""

  CLOUD = provider_info.UNIT_TEST

  def _Start(self):
    pass

  def _Stop(self):
    pass

  def _Suspend(self):
    pass

  def _Resume(self):
    pass

  def GetConnectionIp(self):
    pass


class UbuntuTestVirtualMachine(
    TestVirtualMachine
):
  """Test class that has dummy methods for a base virtual machine."""

  CLOUD = provider_info.UNIT_TEST
  OS_TYPE = 'ubuntu2404'


# Need to provide implementations for all of the abstract methods in
# order to instantiate linux_virtual_machine.BaseLinuxMixin.
class TestLinuxVirtualMachine(
    linux_virtual_machine.BaseLinuxVirtualMachine, TestVirtualMachine
):

  def InstallPackages(self, packages):
    pass


class TestGceLinuxVirtualMachine(  # pytype: disable=signature-mismatch  # overriding-return-type-checks
    gce_virtual_machine.GceVirtualMachine, TestLinuxVirtualMachine
):
  """Test class that has VM methods for a GCE virtual machine."""


class TestGceVirtualMachine(TestOsMixin, gce_virtual_machine.GceVirtualMachine):  # pytype: disable=signature-mismatch  # overriding-return-type-checks
  pass

  def _PreemptibleMetadataKeyValue(self):
    return '', ''


SIMPLE_CONFIG = """
cluster_boot:
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
          zone: us-central1-c
          project: my-project
"""


def CreateBenchmarkSpecFromYaml(
    yaml_string: str = SIMPLE_CONFIG, benchmark_name: str = 'cluster_boot'
) -> benchmark_spec.BenchmarkSpec:
  config = configs.LoadConfig(yaml_string, {}, benchmark_name)
  return CreateBenchmarkSpecFromConfigDict(config, benchmark_name)


def CreateBenchmarkSpecFromConfigDict(
    config_dict: Dict[str, Any], benchmark_name: str
) -> benchmark_spec.BenchmarkSpec:
  config_spec = benchmark_config_spec.BenchmarkConfigSpec(
      benchmark_name, flag_values=FLAGS, **config_dict
  )
  benchmark_module = next(
      b
      for b in linux_benchmarks.BENCHMARKS
      if b.BENCHMARK_NAME == benchmark_name
  )
  return benchmark_spec.BenchmarkSpec(benchmark_module, config_spec, 'name0')


def GetTestDir() -> pathlib.Path:
  """Returns the PKB base directory for tests."""
  return pathlib.Path(__file__).parent


class PkbCommonTestCase(parameterized.TestCase, absltest.TestCase):
  """Test case class for PKB.

  Contains common functions shared by PKB test cases.
  """

  def setUp(self):
    super().setUp()
    saved_flag_values = flagsaver.save_flag_values()
    self.addCleanup(flagsaver.restore_flag_values, saved_flag_values)

    # Functions that create a benchmark_spec.BenchmarkSpec attach the
    # benchmark spec to the running thread in __init__(). If this isn't
    # cleaned up, it creates problems for tests run using unittest.
    self.addCleanup(context.SetThreadBenchmarkSpec, None)

    p = mock.patch(
        util.__name__ + '.GetDefaultProject', return_value='test_project'
    )
    self.enter_context(p)

  def MockIssueCommand(
      self,
      call_to_response: dict[str, list[tuple[str, str, int]]],
  ) -> mock_command.MockIssueCommand:
    """Mocks IssueCommand, returning response for the given call.

    Args:
      call_to_response: A dictionary of commands to a list of responses.
        Commands just need to be a substring of the actual command. Each
        response is given in order, like with mock's normal iterating
        side_effect.

    Returns:
      The mocked function object with call dict.
    """
    return mock_command.MockIssueCommand(call_to_response, self)

  def MockRemoteCommand(
      self,
      call_to_response: dict[str, list[tuple[str, str]]],
      vm: virtual_machine.BaseVirtualMachine | None = None,
  ) -> virtual_machine.BaseVirtualMachine:
    """Mocks vm.RemoteCommand, returning response for the given call.

    Args:
      call_to_response: A dictionary of commands to a list of responses.
        Commands just need to be a substring of the actual command. Each
        response is given in order, like with mock's normal iterating
        side_effect.
      vm: A mocked vm. If None, a mock is created.

    Returns:
      The vm, now with a mocked RemoteCommand.
    """
    if vm is None:
      vm = mock.create_autospec(virtual_machine.BaseVirtualMachine)
    vm.RemoteCommand.mock_command = mock_command.MockRemoteCommand(
        call_to_response, vm
    )
    return vm

  def MockRunCommand(
      self,
      call_to_response: dict[str, list[tuple[str, str, int]]],
      cli: command_interface.CommandInterface | None = None,
  ) -> command_interface.CommandInterface:
    """Mocks RunCommand, returning response for the given call.

    Args:
      call_to_response: A dictionary of commands to a list of responses.
        Commands just need to be a substring of the actual command. Each
        response is given in order, like with mock's normal iterating
        side_effect.
      cli: A mocked CLI. If None, a mock is created.

    Returns:
      The mocked cli.
    """
    if cli is None:
      cli = mock.create_autospec(command_interface.CommandInterface)
    cli.RunCommand.mock_command = mock_command.MockCommand(
        call_to_response, cli.RunCommand, ('', '', 0)
    )
    return cli
