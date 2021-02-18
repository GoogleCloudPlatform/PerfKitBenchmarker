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


def MockVm(text):
  vm = mock.Mock()
  vm.RemoteCommand.return_value = text, ''
  return vm


class NumactlTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('singlenode', SINGLE_NUMA_NODE, {
          0: 8
      }),
      ('twonode', TWO_NUMA_NODES, {
          0: 30,
          1: 30
      }),
  )
  def testGetNuma(self, numactl_text, cpus):
    self.assertEqual(cpus, numactl.GetNuma(MockVm(numactl_text)))

  @parameterized.named_parameters(
      ('singlenode', SINGLE_NUMA_NODE, {
          0: 32116
      }),
      ('twonode', TWO_NUMA_NODES, {
          0: 120889,
          1: 120931
      }),
  )
  def testGetNumaMemory(self, numactl_text, cpus):
    self.assertEqual(cpus, numactl.GetNumaMemory(MockVm(numactl_text)))


if __name__ == '__main__':
  unittest.main()
