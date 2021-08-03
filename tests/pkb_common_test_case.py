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
import subprocess
from typing import Any, Dict

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import context
from perfkitbenchmarker import linux_benchmarks
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import pkb  # pylint:disable=unused-import
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()


class TestVmSpec(virtual_machine.BaseVmSpec):
  CLOUD = 'test_vm_spec_cloud'


def CreateTestVmSpec() -> TestVmSpec:
  return TestVmSpec('test_component_name')


class TestOsMixin(virtual_machine.BaseOsMixin):
  """Test class that provides dummy implementations of abstract functions."""
  OS_TYPE = 'test_os_type'
  BASE_OS_TYPE = 'debian'

  def Install(self, pkg):
    pass

  def PackageCleanup(self):
    pass

  def RemoteCommand(self, command, should_log, ignore_failure, suppress_warning,
                    timeout, **kwargs):
    pass

  def RemoteCopy(self, file_path, remote_path, copy_to):
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

  def _WaitForSSH(self):
    pass

  def _Start(self):
    pass

  def _Stop(self):
    pass

  def _CreateScratchDiskFromDisks(self, disk_spec, disks):
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


class TestVirtualMachine(TestOsMixin, virtual_machine.BaseVirtualMachine):
  """Test class that has dummy methods for a base virtual machine."""
  CLOUD = 'test_vm_cloud'

  def _Create(self):
    pass

  def _Delete(self):
    pass

  def _Start(self):
    pass

  def _Stop(self):
    pass

  def _Suspend(self):
    pass

  def _Resume(self):
    pass


# Need to provide implementations for all of the abstract methods in
# order to instantiate linux_virtual_machine.BaseLinuxMixin.
class TestLinuxVirtualMachine(linux_virtual_machine.BaseLinuxVirtualMachine,
                              TestVirtualMachine):

  def InstallPackages(self, packages):
    pass


class TestGceVirtualMachine(TestOsMixin, gce_virtual_machine.GceVirtualMachine):
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
    yaml_string: str = SIMPLE_CONFIG,
    benchmark_name: str = 'cluster_boot') -> benchmark_spec.BenchmarkSpec:
  config = configs.LoadConfig(yaml_string, {}, benchmark_name)
  return CreateBenchmarkSpecFromConfigDict(config, benchmark_name)


def CreateBenchmarkSpecFromConfigDict(
    config_dict: Dict[str, Any],
    benchmark_name: str) -> benchmark_spec.BenchmarkSpec:
  config_spec = benchmark_config_spec.BenchmarkConfigSpec(
      benchmark_name, flag_values=FLAGS, **config_dict)
  benchmark_module = next((b for b in linux_benchmarks.BENCHMARKS
                           if b.BENCHMARK_NAME == benchmark_name))
  return benchmark_spec.BenchmarkSpec(benchmark_module, config_spec, 'name0')


def GetTestDir() -> pathlib.Path:
  """Returns the PKB base directory for tests."""
  return pathlib.Path(__file__).parent


class PkbCommonTestCase(parameterized.TestCase, absltest.TestCase):
  """Test case class for PKB.

  Contains common functions shared by PKB test cases.
  """

  def setUp(self):
    super(PkbCommonTestCase, self).setUp()
    saved_flag_values = flagsaver.save_flag_values()
    self.addCleanup(flagsaver.restore_flag_values, saved_flag_values)

    # Functions that create a benchmark_spec.BenchmarkSpec attach the
    # benchmark spec to the running thread in __init__(). If this isn't
    # cleaned up, it creates problems for tests run using unittest.
    self.addCleanup(context.SetThreadBenchmarkSpec, None)

    p = mock.patch(
        util.__name__ + '.GetDefaultProject',
        return_value='test_project')
    self.enter_context(p)

  # TODO(user): Extend MockIssueCommand to support multiple calls to
  # vm_util.IssueCommand
  def MockIssueCommand(self, stdout: str, stderr: str, retcode: int) -> None:
    """Mocks function calls inside vm_util.IssueCommand.

    Mocks subproccess.Popen and _ReadIssueCommandOutput in IssueCommand.
    This allows the logic of IssueCommand to run and returns the given
    stdout, stderr when IssueCommand is called.

    Args:
      stdout: String. Output from standard output
      stderr: String. Output from standard error
      retcode: Int. Return code from running the command.
    """

    p = mock.patch(
        'subprocess.Popen', spec=subprocess.Popen)
    cmd_output = mock.patch.object(vm_util, '_ReadIssueCommandOutput')

    self.addCleanup(p.stop)
    self.addCleanup(cmd_output.stop)

    p.start().return_value.returncode = retcode
    cmd_output.start().side_effect = [(stdout, stderr)]
