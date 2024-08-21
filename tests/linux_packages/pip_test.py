"""Tests for pip."""

import unittest
from absl.testing import parameterized
import mock
from perfkitbenchmarker.linux_packages import pip
from perfkitbenchmarker.linux_packages import python
from tests import pkb_common_test_case
import requests

# executed remote commands
NEED_PIP_37 = [
    'sudo rm -f /usr/lib/python3*/EXTERNALLY-MANAGED',
    (
        'curl https://bootstrap.pypa.io/pip/3.7/get-pip.py -o get_pip.py && '
        'sudo python3 get_pip.py'
    ),
    'sudo pip --version',
    'sudo pip3 --version',
]
NEED_PIP_38 = [
    'sudo rm -f /usr/lib/python3*/EXTERNALLY-MANAGED',
    (
        'curl https://bootstrap.pypa.io/pip/get-pip.py -o get_pip.py && '
        'sudo python3 get_pip.py'
    ),
    'sudo pip --version',
    'sudo pip3 --version',
]
EXISTING_PIP_38 = [
    'sudo rm -f /usr/lib/python3*/EXTERNALLY-MANAGED',
    (
        'echo \'exec python3 -m pip "$@"\'| sudo tee /usr/bin/pip && '
        'sudo chmod 755 /usr/bin/pip'
    ),
    (
        'echo \'exec python3 -m pip "$@"\'| sudo tee /usr/bin/pip3 && '
        'sudo chmod 755 /usr/bin/pip3'
    ),
    'sudo pip --version',
    'sudo pip3 --version',
]


def _TestResponse(status_code: int) -> requests.Response:
  response = requests.Response()
  response.status_code = status_code
  return response


VERSIONED_PYPI_RESPONSES = {
    'https://bootstrap.pypa.io/pip/3.7/get-pip.py': _TestResponse(200),
    'https://bootstrap.pypa.io/pip/3.8/get-pip.py': _TestResponse(404),
}

_INSTALL_PIP_FROM_PYPI = {
    'python3 -m pip --version': False,
    'sudo pip --version': True,
    'sudo pip3 --version': True,
}

_ALREADY_HAS_PIP = {
    'python3 -m pip --version': True,
    'sudo pip --version': False,
    'sudo pip3 --version': False,
}


class PipTest(pkb_common_test_case.PkbCommonTestCase):

  @parameterized.named_parameters(
      ('need_pip_37', _INSTALL_PIP_FROM_PYPI, '3.7', NEED_PIP_37),
      ('need_pip_38', _INSTALL_PIP_FROM_PYPI, '3.8', NEED_PIP_38),
      ('existing_pip_38', _ALREADY_HAS_PIP, '3.8', EXISTING_PIP_38),
  )
  @mock.patch.object(requests, 'get', side_effect=VERSIONED_PYPI_RESPONSES.get)
  def testInstall(
      self,
      try_remote_command_side_effects: dict[str, bool],
      python_version: str,
      expected_commands: list[str],
      requests_get: mock.Mock,
  ):
    self.enter_context(
        mock.patch.object(
            python, 'GetPythonVersion', return_value=python_version
        )
    )
    vm = mock.Mock()
    vm.TryRemoteCommand.side_effect = try_remote_command_side_effects.get

    pip.Install(vm)

    vm.RemoteCommand.assert_has_calls(
        [mock.call(cmd) for cmd in expected_commands]
    )

    if not try_remote_command_side_effects['python3 -m pip --version']:
      requests_get.assert_called_once()

  @mock.patch.object(
      requests,
      'get',
      side_effect=[
          ConnectionResetError('unable to connect'),
          _TestResponse(200),
      ],
  )
  def testNetworkRetries(
      self,
      requests_get: mock.Mock,
  ):
    self.enter_context(
        mock.patch.object(python, 'GetPythonVersion', return_value='3.7')
    )
    vm = mock.Mock()
    vm.TryRemoteCommand.side_effect = _INSTALL_PIP_FROM_PYPI.get

    pip.Install(vm)

    vm.RemoteCommand.assert_has_calls(
        [mock.call(cmd) for cmd in NEED_PIP_37]
    )

    requests_get.assert_called()


if __name__ == '__main__':
  unittest.main()
