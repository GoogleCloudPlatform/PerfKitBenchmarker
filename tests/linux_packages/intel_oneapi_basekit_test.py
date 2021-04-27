"""Tests for Intel oneAPI package."""

import unittest
import mock
from perfkitbenchmarker.linux_packages import intel_oneapi_basekit
from tests import pkb_common_test_case


class IntelHpcBaseKitTestCase(pkb_common_test_case.PkbCommonTestCase):

  @mock.patch.object(intel_oneapi_basekit, '_VerifyInstall')
  def testInstall(self, mock_verify):
    vm = mock.Mock()

    intel_oneapi_basekit.Install(vm)

    vm.RemoteCommand.assert_called_with(
        'chmod +x 2021.2.0.sh; '
        'sudo ./2021.2.0.sh -a -s --eula accept; '
        'rm 2021.2.0.sh')
    mock_verify.assert_called_with(vm)

  def testVerifyInstall(self):
    vm = mock.Mock()
    mpi_root = '/opt/intel/oneapi/mpi/2021.2.0'
    env_1 = ['PATH=/usr/bin']
    env_2 = [
        'PATH=/usr/bin:/opt/intel/oneapi/vtune/2021.2.0/bin64',
        f'I_MPI_ROOT={mpi_root}'
    ]
    vm.RemoteCommand.side_effect = [
        ('\n'.join(env_1), ''),
        ('\n'.join(env_2), ''),
        (mpi_root, ''),
    ]

    intel_oneapi_basekit._VerifyInstall(vm)

    vm.RemoteCommand.assert_has_calls([
        mock.call('env'),
        mock.call('. /opt/intel/oneapi/setvars.sh >/dev/null; env')
    ])

  def testVerifyInstallNoMpiRoot(self):
    vm = mock.Mock()
    vm.RemoteCommand.return_value = [('A=B', '')]

    with self.assertRaises(ValueError):
      intel_oneapi_basekit._VerifyInstall(vm)

  def testGetVariables(self):
    vm = mock.Mock()
    input_vars = {'A': 'B', 'XDG_SESSION_ID': '4'}
    vars_as_text = '\n'.join(
        f'{key}={value}' for key, value in sorted(input_vars.items()))
    vm.RemoteCommand.return_value = vars_as_text, ''

    env_vars = intel_oneapi_basekit._GetVariables(vm)

    # skipped over the ignored variable XDG_SESSION_ID
    expected_vars = {'A': ['B']}
    self.assertEqual(expected_vars, env_vars)


if __name__ == '__main__':
  unittest.main()

if __name__ == '__main__':
  unittest.main()
