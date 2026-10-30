# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
import stat
import unittest
from unittest import mock

from perfkitbenchmarker.resources.container_service import kubernetes_pod_utils
from tests import pkb_common_test_case


class KubernetesPodUtilsTest(pkb_common_test_case.PkbCommonTestCase):

  @mock.patch(
      'perfkitbenchmarker.resources.container_service.kubectl.RunKubectlCommand'
  )
  def test_run_pod_command_with_namespace(self, mock_kubectl):
    kubernetes_pod_utils._RunPodCommand('my-pod', 'my-ns', 'echo hello')
    mock_kubectl.assert_called_once_with(
        ['exec', 'my-pod', '-n', 'my-ns', '--', 'bash', '-c', 'echo hello'],
        raise_on_failure=False,
    )

  @mock.patch(
      'perfkitbenchmarker.resources.container_service.kubectl.RunKubectlCommand'
  )
  def test_run_pod_command_without_namespace(self, mock_kubectl):
    kubernetes_pod_utils._RunPodCommand('my-pod', '', 'echo hello')
    mock_kubectl.assert_called_once_with(
        ['exec', 'my-pod', '--', 'bash', '-c', 'echo hello'],
        raise_on_failure=False,
    )

  @mock.patch(
      'perfkitbenchmarker.resources.container_service.kubernetes_pod_utils._RunPodCommand'
  )
  @mock.patch('perfkitbenchmarker.vm_util.IssueCommand')
  @mock.patch('os.path.getsize')
  @mock.patch('os.stat')
  def test_remote_host_copy_to(
      self, mock_stat, mock_getsize, mock_issue, mock_run
  ):
    mock_issue.return_value = ('', '', 0)
    mock_run.side_effect = [('', '', 0), ('100', '', 0)]
    mock_stat_obj = mock.MagicMock()
    mock_stat_obj.st_mode = stat.S_IFREG
    mock_stat.return_value = mock_stat_obj
    mock_getsize.return_value = 100

    kubernetes_pod_utils.RemoteHostCopy(
        'my-pod', 'my-ns', '/local/path/file.txt', '/remote/path/file.txt'
    )

    mock_run.assert_any_call(
        'my-pod',
        'my-ns',
        'mv file.txt /remote/path/file.txt; chmod 755 /remote/path/file.txt',
    )

  @mock.patch(
      'perfkitbenchmarker.resources.container_service.kubernetes_pod_utils._RunPodCommand'
  )
  @mock.patch('perfkitbenchmarker.vm_util.IssueCommand')
  @mock.patch('os.path.getsize')
  @mock.patch('os.stat')
  def test_remote_host_copy_to_empty_remote_path(
      self, mock_stat, mock_getsize, mock_issue, mock_run
  ):
    mock_issue.return_value = ('', '', 0)
    mock_run.side_effect = [('', '', 0), ('100', '', 0)]
    mock_stat_obj = mock.MagicMock()
    mock_stat_obj.st_mode = stat.S_IFREG
    mock_stat.return_value = mock_stat_obj
    mock_getsize.return_value = 100

    kubernetes_pod_utils.RemoteHostCopy(
        'my-pod', 'my-ns', '/local/path/file.txt', ''
    )

    mock_run.assert_any_call(
        'my-pod', 'my-ns', 'mv file.txt file.txt; chmod 755 file.txt'
    )

  def test_remote_host_copy_retries_zero(self):
    # Tests that when retries=0, the loop does not run and the default
    # retcode -1 is raised. This kills the mutant that changes the
    # initial retcode to 1.
    with self.assertRaisesRegex(
        kubernetes_pod_utils.errors.VmUtil.IssueCommandError,
        r'Got non-zero return code \(-1\)',
    ):
      kubernetes_pod_utils.RemoteHostCopy(
          'my-pod', 'my-ns', '/local/path/file.txt', '', retries=0
      )

  @mock.patch(
      'perfkitbenchmarker.resources.container_service.kubernetes_pod_utils._RunPodCommand'
  )
  @mock.patch('perfkitbenchmarker.vm_util.IssueCommand')
  @mock.patch('os.path.getsize')
  @mock.patch('os.stat')
  def test_remote_host_copy_size_mismatch(
      self, mock_stat, mock_getsize, mock_issue, mock_run
  ):
    mock_issue.return_value = ('', '', 0)
    # The first _RunPodCommand is the 'mv', the second is the 'stat -c %s'.
    # Return 101 for the remote size to simulate a mismatch.
    mock_run.side_effect = [('', '', 0), ('101', '', 0)]
    mock_stat_obj = mock.MagicMock()
    mock_stat_obj.st_mode = stat.S_IFREG
    mock_stat.return_value = mock_stat_obj
    mock_getsize.return_value = 100

    with self.assertRaisesRegex(
        kubernetes_pod_utils.errors.VmUtil.IssueCommandError,
        r'Remote size 101 != local size 100',
    ):
      kubernetes_pod_utils.RemoteHostCopy(
          'my-pod', 'my-ns', '/local/path/file.txt', ''
      )


if __name__ == '__main__':
  unittest.main()
