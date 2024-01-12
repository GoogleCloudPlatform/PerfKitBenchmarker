"""Tests for pip."""

from typing import Dict, List
import unittest
from absl.testing import parameterized
import mock
from perfkitbenchmarker.linux_packages import pip
from perfkitbenchmarker.linux_packages import python
from tests import pkb_common_test_case
import requests

# executed remote commands
NEED_PIP_27 = [
    (
        'curl https://bootstrap.pypa.io/pip/2.7/get-pip.py -o get_pip.py && '
        'sudo python get_pip.py'
    ),
    'pip --version',
    'mkdir -p /opt/pkb && pip freeze | tee /opt/pkb/requirements.txt',
]
NEED_PIP_38 = [
    (
        'curl https://bootstrap.pypa.io/pip/get-pip.py -o get_pip.py && '
        'sudo python3 get_pip.py'
    ),
    'pip3 --version',
    'mkdir -p /opt/pkb && pip3 freeze | tee /opt/pkb/requirements.txt',
]
EXISTING_PIP_27 = [
    (
        'echo \'exec python -m pip "$@"\'| sudo tee /usr/bin/pip && '
        'sudo chmod 755 /usr/bin/pip'
    ),
    'pip --version',
    'mkdir -p /opt/pkb && pip freeze | tee /opt/pkb/requirements.txt',
]
EXISTING_PIP_38 = [
    (
        'echo \'exec python3 -m pip "$@"\'| sudo tee /usr/bin/pip3 && '
        'sudo chmod 755 /usr/bin/pip3'
    ),
    'pip3 --version',
    'mkdir -p /opt/pkb && pip3 freeze | tee /opt/pkb/requirements.txt',
]
PYTHON_38_KWARGS = {'pip_cmd': 'pip3', 'python_cmd': 'python3'}


def _TestResponse(status_code: int) -> requests.Response:
  response = requests.Response()
  response.status_code = status_code
  return response


VERSIONED_PYPI_RESPONSES = {
    'https://bootstrap.pypa.io/pip/2.7/get-pip.py': _TestResponse(200),
    'https://bootstrap.pypa.io/pip/3.8/get-pip.py': _TestResponse(404),
}


class PipTest(pkb_common_test_case.PkbCommonTestCase):

  @parameterized.named_parameters(
      ('need_pip_27', False, '2.7', NEED_PIP_27, {}),
      ('need_pip_38', False, '3.8', NEED_PIP_38, PYTHON_38_KWARGS),
      ('existing_pip_27', True, '2.7', EXISTING_PIP_27, {}),
      ('existing_pip_38', True, '3.8', EXISTING_PIP_38, PYTHON_38_KWARGS),
  )
  @mock.patch.object(requests, 'get', side_effect=VERSIONED_PYPI_RESPONSES.get)
  def testInstall(
      self,
      already_has_pip: bool,
      python_version: str,
      expected_commands: List[str],
      install_kwargs: Dict[str, str],
      requests_get: mock.Mock,
  ):
    self.enter_context(
        mock.patch.object(
            python, 'GetPythonVersion', return_value=python_version
        )
    )
    vm = mock.Mock()
    vm.TryRemoteCommand.return_value = already_has_pip

    pip.Install(vm, **install_kwargs)

    vm.RemoteCommand.assert_has_calls(
        [mock.call(cmd) for cmd in expected_commands]
    )

    if not already_has_pip:
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
        mock.patch.object(python, 'GetPythonVersion', return_value='2.7')
    )
    vm = mock.Mock()
    vm.TryRemoteCommand.return_value = False

    pip.Install(vm)

    vm.RemoteCommand.assert_has_calls(
        [mock.call(cmd) for cmd in NEED_PIP_27]
    )

    requests_get.assert_called()


if __name__ == '__main__':
  unittest.main()
