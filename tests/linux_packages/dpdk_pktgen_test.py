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


from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import mock
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker.linux_packages import dpdk_pktgen

FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()


class DpdkPktgenInstallTest(absltest.TestCase):

  def _InstallAndCollectCommands(self) -> list[str]:
    vm = mock.create_autospec(
        linux_virtual_machine.BaseLinuxVirtualMachine,
        instance=True,
        spec_set=True,
    )
    vm.RemoteCommand.return_value = ('', '')
    vm.RobustRemoteCommand.return_value = ('', '')
    dpdk_pktgen._Install(vm)
    return [call.args[0] for call in vm.RemoteCommand.call_args_list]

  @flagsaver.flagsaver(dpdk_pktgen_mbuf_cache_size=128)
  def testCacheSizeIsRewrittenAndPatchedDefineIsChecked(self):
    cmds = self._InstallAndCollectCommands()
    cache_cmds = [cmd for cmd in cmds if 'MBUF_CACHE_SIZE' in cmd]
    self.assertLen(cache_cmds, 1)
    cmd = cache_cmds[0]
    # Value-agnostic rewrite of the constant the pools are sized from.
    self.assertIn(
        r'sed -i -E "s/(MBUF_CACHE_SIZE *= *)[0-9]+/\1128/"'
        f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/app/pktgen-constants.h',
        cmd,
    )
    # Guard that the rewrite happened.
    self.assertIn(r'grep -Eq "MBUF_CACHE_SIZE *= *128\b"', cmd)
    # Guard that pktgen.patch redirected MEMPOOL_CACHE_SIZE to MBUF_CACHE_SIZE;
    # without that hunk the flag never reaches rte_pktmbuf_pool_create.
    self.assertIn(
        r'grep -Eq "define MEMPOOL_CACHE_SIZE +MBUF_CACHE_SIZE\b"'
        f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/app/l2p.h',
        cmd,
    )

  def testPatchIsAppliedBeforeConstantsAreRewritten(self):
    cmds = self._InstallAndCollectCommands()
    patch_idx = next(i for i, cmd in enumerate(cmds) if 'patch -l -p1' in cmd)
    rewrite_idx = next(
        i for i, cmd in enumerate(cmds) if 'MBUF_CACHE_SIZE' in cmd
    )
    make_idx = next(i for i, cmd in enumerate(cmds) if cmd.endswith('make'))
    self.assertLess(patch_idx, rewrite_idx)
    self.assertLess(rewrite_idx, make_idx)


class DpdkPktgenFlagTest(parameterized.TestCase):

  @parameterized.parameters(0, 256, 512)
  def testCacheSizeAcceptsDpdkRange(self, value: int):
    with flagsaver.flagsaver(dpdk_pktgen_mbuf_cache_size=value):
      self.assertEqual(dpdk_pktgen.DPDK_PKTGEN_MBUF_CACHE_SIZE.value, value)

  @parameterized.named_parameters(
      ('above_dpdk_max', 1024),
      ('negative', -1),
  )
  def testCacheSizeRejectsOutOfRange(self, value: int):
    # DPDK's rte_mempool_create_empty rejects cache_size > 512 (EINVAL), which
    # pktgen turns into rte_exit at start-up; fail at flag parsing instead.
    with self.assertRaises(flags.IllegalFlagValueError):
      with flagsaver.flagsaver(dpdk_pktgen_mbuf_cache_size=value):
        pass


if __name__ == '__main__':
  absltest.main()
