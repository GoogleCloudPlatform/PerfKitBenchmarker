"""Tests for perfkitbenchmarker.linux_packages.numactl."""

import unittest
from absl.testing import parameterized
import mock
from perfkitbenchmarker.linux_packages import numactl

SINGLE_NUMA_NODE = """
available: 1 nodes (0)
node 0 cpus: 0 1 2 3 4 5 6 7
node 0 size: 32116 MB
node 0 free: 13798 MB
node distances:
node   0
  0:  10
"""

TWO_NUMA_NODES = """
available: 2 nodes (0-1)
node 0 cpus: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44
node 0 size: 120889 MB
node 0 free: 120312 MB
node 1 cpus: 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59
node 1 size: 120931 MB
node 1 free: 120242 MB
node distances:
node   0   1
  0:  10  20
  1:  20  10
"""


def MockVm(
    run_cmd_response: dict[str, str], allowed_cpus_arg: set[int] | None = None
):
  vm = mock.Mock()

  def FakeRemoteHostCommand(cmd, **_):
    if isinstance(run_cmd_response, dict):
      if cmd not in run_cmd_response:
        raise SystemExit(f'Define response for {cmd}')
      stdout = run_cmd_response[cmd]
    else:
      raise NotImplementedError()
    return stdout, ''

  allowed_cpus = set()
  if allowed_cpus_arg is not None:
    allowed_cpus = allowed_cpus_arg

  vm.RemoteCommand = mock.Mock(side_effect=FakeRemoteHostCommand)
  vm.GetCpusAllowedSet = mock.Mock(return_value=allowed_cpus)
  return vm


class NumactlTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('singlenode', SINGLE_NUMA_NODE, {0: 8}, '0', None, set(range(8))),
      ('twonode', TWO_NUMA_NODES, {0: 30, 1: 30}, '0-1', None, set(range(60))),
      (
          'twonode_with_subset_cpus',
          TWO_NUMA_NODES,
          {0: 4, 1: 4},
          '0-1',
          '0-3,15-18',
          set([0, 1, 2, 3, 15, 16, 17, 18]),
      ),
      (
          'twonode_with_cpus_allowed_in_only_one_numa_node',
          TWO_NUMA_NODES,
          {1: 4},
          '1',
          '15-18',
          set([15, 16, 17, 18]),
      ),
  )
  def testGetNuma(
      self,
      numactl_text,
      expected_cpus,
      mems_allowed_list,
      cpus_allowed_list,
      cpu_allowed_set,
  ):
    responses = {'numactl --hardware': numactl_text}
    if mems_allowed_list is not None:
      responses['cat /proc/self/status | grep Mems_allowed_list'] = (
          f'Mems_allowed_list:\t{mems_allowed_list}'
      )
    if cpus_allowed_list is not None:
      responses['cat /sys/fs/cgroup/cpuset.cpus.effective'] = cpus_allowed_list
    mock_vm = MockVm(responses, cpu_allowed_set)
    actual_cpus = numactl.GetNuma(mock_vm)
    self.assertEqual(expected_cpus, actual_cpus)

  @parameterized.named_parameters(
      ('singlenode', SINGLE_NUMA_NODE, {0: 32116}, '0', None, None),
      ('twonode', TWO_NUMA_NODES, {0: 120889, 1: 120931}, '0-1', None, None),
      (
          'twonode_with_subset_cpus',
          TWO_NUMA_NODES,
          {0: 120889, 1: 120931},
          '0-1',
          '0-3,15-18',
          set([0, 1, 2, 3, 15, 16, 17, 18]),
      ),
      (
          'twonode_with_cpus_allowed_in_only_one_numa_node',
          TWO_NUMA_NODES,
          {0: 120889, 1: 120931},
          '1',
          '15-18',
          set([15, 16, 17, 18]),
      ),
  )
  def testGetNumaMemory(
      self,
      numactl_text,
      mems,
      mems_allowed_list,
      cpus_allowed_list,
      cpu_allowed_set,
  ):
    responses = {'numactl --hardware': numactl_text}
    if mems_allowed_list is None:
      responses[
          'ls /proc/self/status >> /dev/null 2>&1 || echo file_not_exist'
      ] = 'file_not_exist'
    else:
      responses['cat /proc/self/status | grep Mems_allowed_list'] = (
          f'Mems_allowed_list:\t{mems_allowed_list}'
      )
    if cpus_allowed_list is None:
      responses[
          'ls /sys/fs/cgroup/cpuset.cpus.effective >> /dev/null 2>&1 || echo'
          ' file_not_exist'
      ] = 'file_not_exist'
    else:
      responses['cat /sys/fs/cgroup/cpuset.cpus.effective'] = cpus_allowed_list
    actual_mems = numactl.GetNumaMemory(MockVm(responses, cpu_allowed_set))
    self.assertEqual(mems, actual_mems)


if __name__ == '__main__':
  unittest.main()
