"""Tests for pip."""

from typing import Dict, List
import unittest
from absl.testing import parameterized
import mock

from perfkitbenchmarker.linux_packages import pip
from perfkitbenchmarker.linux_packages import python
from tests import pkb_common_test_case

# executed remote commands
NEED_PIP_27 = [
    'curl https://bootstrap.pypa.io/pip/2.7/get-pip.py | sudo python -',
    'pip --version',
    'mkdir -p /opt/pkb && pip freeze | tee /opt/pkb/requirements.txt'
]
NEED_PIP_38 = [
    'curl https://bootstrap.pypa.io/pip/get-pip.py | sudo python3 -',
    'pip3 --version',
    'mkdir -p /opt/pkb && pip3 freeze | tee /opt/pkb/requirements.txt'
]
EXISTING_PIP_27 = [
    'echo \'exec python -m pip "$@"\'| sudo tee /usr/bin/pip && '
    'sudo chmod 755 /usr/bin/pip',
    'pip --version',
    'mkdir -p /opt/pkb && pip freeze | tee /opt/pkb/requirements.txt',
]
EXISTING_PIP_38 = [
    'echo \'exec python3 -m pip "$@"\'| sudo tee /usr/bin/pip3 && '
    'sudo chmod 755 /usr/bin/pip3',
    'pip3 --version',
    'mkdir -p /opt/pkb && pip3 freeze | tee /opt/pkb/requirements.txt',
]
PYTHON_38_KWARGS = {'pip_cmd': 'pip3', 'python_cmd': 'python3'}


class PipTest(pkb_common_test_case.PkbCommonTestCase):

  @parameterized.named_parameters(
      ('need_pip_27', False, '2.7', NEED_PIP_27, {}),
      ('need_pip_38', False, '3.8', NEED_PIP_38, PYTHON_38_KWARGS),
      ('existing_pip_27', True, '2.7', EXISTING_PIP_27, {}),
      ('existing_pip_38', True, '3.8', EXISTING_PIP_38, PYTHON_38_KWARGS),
  )
  def testInstall(self, need_pip: bool, python_version: str,
                  expected_commands: List[str], install_kwargs: Dict[str, str]):
    self.enter_context(
        mock.patch.object(
            python, 'GetPythonVersion', return_value=python_version))
    vm = mock.Mock()
    vm.TryRemoteCommand.return_value = need_pip

    pip.Install(vm, **install_kwargs)

    vm.RemoteCommand.assert_has_calls(
        [mock.call(cmd) for cmd in expected_commands])


if __name__ == '__main__':
  unittest.main()
