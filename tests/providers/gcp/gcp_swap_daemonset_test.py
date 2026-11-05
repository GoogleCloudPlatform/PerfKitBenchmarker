import unittest
from unittest import mock

from perfkitbenchmarker.providers.gcp import gcp_swap_daemonset
from tests import pkb_common_test_case


class GcpSwapDaemonsetTest(pkb_common_test_case.PkbCommonTestCase):

  @mock.patch.object(gcp_swap_daemonset.GcpSwapDaemonSet, 'PodExec')
  def test_setup_gke_lssd_stateful_loop_swap(self, mock_pod_exec):
    def _exec(*args, **kwargs):
      cmd = args[0] if args else kwargs.get('cmd', '')
      if 'losetup -j' in cmd:
        return ('/dev/loop0\n', '')
      if 'df -k' in cmd:
        return (
            (
                'Filesystem 1K-blocks Used Available Use% Mounted on\n/dev/sda1'
                ' 100000 0 100000 0% /host/mnt/stateful_partition'
            ),
            '',
        )
      return ('', '')

    mock_pod_exec.side_effect = _exec
    ds = gcp_swap_daemonset.GcpSwapDaemonSet(
        name='test-ds', namespace='default', label='app=test', nodepool='test'
    )

    ds._SetupGkeLssdStatefulLoopSwap(enable_dmcrypt=True)
    self.assertGreater(mock_pod_exec.call_count, 0)
    # Verify ignore_failure=True is used to ignore missing utilities
    # (kills mutant at 412)
    self.assertTrue(
        mock_pod_exec.call_args_list[0].kwargs.get('ignore_failure')
    )
    calls = [c[0][0] for c in mock_pod_exec.call_args_list]
    self.assertTrue(any('fallocate' in c for c in calls))
    self.assertTrue(any('dmsetup' in c for c in calls))
    # Verify fallback to stateful_partition (kills mutant at 414)
    self.assertTrue(
        any('df -P /host/mnt/stateful_partition' in c for c in calls)
    )

  @mock.patch.object(gcp_swap_daemonset.GcpSwapDaemonSet, 'PodExec')
  @mock.patch.object(
      gcp_swap_daemonset.GcpSwapDaemonSet, '_SetupGkeLssdStatefulLoopSwap'
  )
  def test_provision_lssd_fallback_to_loop(
      self, mock_setup_loop, mock_pod_exec
  ):
    mock_pod_exec.return_value = ('', '')  # No devices returned
    ds = gcp_swap_daemonset.GcpSwapDaemonSet(
        name='test-ds', namespace='default', label='app=test', nodepool='test'
    )

    ds._SetupGkeLssdSwap(enable_dmcrypt=True, swap_size_gb=10)
    mock_setup_loop.assert_called_once_with(True)


if __name__ == '__main__':
  unittest.main()
