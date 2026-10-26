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

# Two sockets with NPS=2, i.e. two NUMA nodes per socket: nodes 0-1 are near
# each other and far from nodes 2-3.
FOUR_NUMA_NODES = """
available: 4 nodes (0-3)
node 0 cpus: 0 1 2 3
node 0 size: 120889 MB
node 0 free: 120312 MB
node 1 cpus: 4 5 6 7
node 1 size: 120889 MB
node 1 free: 120312 MB
node 2 cpus: 8 9 10 11
node 2 size: 120889 MB
node 2 free: 120312 MB
node 3 cpus: 12 13 14 15
node 3 size: 120889 MB
node 3 free: 120312 MB
node distances:
node   0   1   2   3
  0:  10  12  32  32
  1:  12  10  32  32
  2:  32  32  10  12
  3:  32  32  12  10
"""

NO_DISTANCE_MATRIX = """
available: 1 nodes (0)
node 0 cpus: 0 1 2 3 4 5 6 7
node 0 size: 32116 MB
node 0 free: 13798 MB
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
      ('singlenode', SINGLE_NUMA_NODE, {0: set(range(8))}, '0', set(range(8))),
      (
          'twonode',
          TWO_NUMA_NODES,
          {
              0: set(range(0, 15)) | set(range(30, 45)),
              1: set(range(15, 30)) | set(range(45, 60)),
          },
          '0-1',
          set(range(60)),
      ),
      (
          'twonode_with_subset_cpus',
          TWO_NUMA_NODES,
          {0: {0, 1, 2, 3}, 1: {15, 16, 17, 18}},
          '0-1',
          {0, 1, 2, 3, 15, 16, 17, 18},
      ),
      (
          'twonode_with_cpus_allowed_in_only_one_numa_node',
          TWO_NUMA_NODES,
          {1: {15, 16, 17, 18}},
          '1',
          {15, 16, 17, 18},
      ),
  )
  def testGetNumaCpus(
      self, numactl_text, expected_cpus, mems_allowed_list, cpu_allowed_set
  ):
    responses = {
        'numactl --hardware': numactl_text,
        'cat /proc/self/status | grep Mems_allowed_list': (
            f'Mems_allowed_list:\t{mems_allowed_list}'
        ),
    }
    actual_cpus = numactl.GetNumaCpus(MockVm(responses, cpu_allowed_set))
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

  @parameterized.named_parameters(
      ('singlenode', SINGLE_NUMA_NODE, {0: {0: 10}}),
      (
          'twonode',
          TWO_NUMA_NODES,
          {0: {0: 10, 1: 20}, 1: {0: 20, 1: 10}},
      ),
      (
          'four_nodes_two_sockets',
          FOUR_NUMA_NODES,
          {
              0: {0: 10, 1: 12, 2: 32, 3: 32},
              1: {0: 12, 1: 10, 2: 32, 3: 32},
              2: {0: 32, 1: 32, 2: 10, 3: 12},
              3: {0: 32, 1: 32, 2: 12, 3: 10},
          },
      ),
      ('no_distance_matrix', NO_DISTANCE_MATRIX, {}),
  )
  def testGetNumaDistances(self, numactl_text, expected_distances):
    mock_vm = MockVm({'numactl --hardware': numactl_text})
    self.assertEqual(expected_distances, numactl.GetNumaDistances(mock_vm))

  @parameterized.named_parameters(
      ('empty', {}, ''),
      ('singlenode', {0: 10}, '10:[0]'),
      ('twonode', {0: 10, 1: 20}, '10:[0], 20:[1]'),
      (
          'four_nodes_two_sockets',
          {0: 10, 1: 12, 2: 32, 3: 32},
          '10:[0], 12:[1], 32:[2-3]',
      ),
      (
          'sixteen_nodes_four_sockets',
          {0: 10, 1: 12, 2: 12, 3: 12, **{n: 32 for n in range(4, 16)}},
          '10:[0], 12:[1-3], 32:[4-15]',
      ),
      (
          'non_contiguous',
          {0: 32, 1: 10, 2: 12, 3: 32},
          '10:[1], 12:[2], 32:[0,3]',
      ),
  )
  def testFormatNumaDistances(self, node_distances, expected):
    self.assertEqual(expected, numactl.FormatNumaDistances(node_distances))


if __name__ == '__main__':
  unittest.main()
