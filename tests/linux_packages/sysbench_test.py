"""Tests for sysbench."""

import unittest

from absl.testing import flagsaver
import mock
from perfkitbenchmarker.linux_packages import sysbench
from tests import pkb_common_test_case


class SysbenchTest(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(sysbench_ignore_concurrent_modification=True)
  def testInstallIgnoresConcurrentMods(self):
    vm = mock.Mock()
    sysbench._Install(vm)
    self.assertIn('P0001', vm.RemoteCommand.call_args_list[1][0][0])

if __name__ == '__main__':
  unittest.main()
