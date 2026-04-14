import collections
import unittest

from perfkitbenchmarker.linux_benchmarks import kubernetes_redis_memtier_benchmark
from tests import pkb_common_test_case


class KubernetesRedisMemtierBenchmarkTest(
    pkb_common_test_case.PkbCommonTestCase
):

  def test__CreateRunConfigMatrix(self):
    cls = collections.namedtuple('cls', ['field_a', 'field_b', 'field_c'])
    run_configs = kubernetes_redis_memtier_benchmark._CreateRunConfigMatrix(
        cls,
        field_a=[1, 2],
        field_b=['x', 'y'],
        field_c=[True, False],
    )
    self.assertLen(run_configs, 2**3)
    self.assertIsInstance(run_configs[0], cls)
    print(run_configs)
    self.assertEqual(
        run_configs,
        [
            cls(field_a=1, field_b='x', field_c=True),
            cls(field_a=1, field_b='x', field_c=False),
            cls(field_a=1, field_b='y', field_c=True),
            cls(field_a=1, field_b='y', field_c=False),
            cls(field_a=2, field_b='x', field_c=True),
            cls(field_a=2, field_b='x', field_c=False),
            cls(field_a=2, field_b='y', field_c=True),
            cls(field_a=2, field_b='y', field_c=False),
        ],
    )


if __name__ == '__main__':
  unittest.main()
