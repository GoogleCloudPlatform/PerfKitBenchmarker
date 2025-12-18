import tempfile
import threading
import time
import unittest
from unittest import mock
from perfkitbenchmarker import benchmark_spec as bm_spec_lib
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import container_spec
from perfkitbenchmarker.linux_benchmarks import kubernetes_hpa_benchmark
from tests import container_service_mock
from tests import pkb_common_test_case


class KubernetesMetricsCollectorTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.stop = threading.Event()
    self.wait_mock = self.enter_context(
        mock.patch.object(self.stop, 'wait', autospec=True)
    )
    self.samples: list[sample.Sample] = []
    self.kmc = kubernetes_hpa_benchmark.KubernetesMetricsCollector(
        self.samples, self.stop
    )

  def testObserveSuccess(self):
    self.kmc._Observe(lambda: [_sample()])

  def testObserve1PercentFailureRateIsSuccess(self):
    sampler = Sampler(num_successes=99, num_failures=1)

    self.wait_mock.side_effect = (
        lambda timeout: sampler.get_samples_remaining() <= 0
    )
    self.kmc._Observe(sampler.get_sample)
    self.assertLen(self.samples, 99)

  def testObserve99PercentFailureRateIsFailure(self):
    sampler = Sampler(num_successes=1, num_failures=99)

    self.wait_mock.side_effect = (
        lambda timeout: sampler.get_samples_remaining() <= 0
    )
    with self.assertRaises(AssertionError):
      self.kmc._Observe(sampler.get_sample)


class Sampler:

  def __init__(self, num_successes: int, num_failures: int):
    self._num_success = num_successes
    self._num_failures = num_failures

  def get_sample(self) -> list[sample.Sample]:
    # arbitrarily emit failures first
    if self._num_failures > 0:
      self._num_failures -= 1
      raise errors.VmUtil.IssueCommandError('A failure occurred')
    elif self._num_success > 0:
      self._num_success -= 1
      return [_sample()]
    else:
      raise RuntimeError('Sampler ran out of samples')

  def get_samples_remaining(self) -> int:
    return self._num_success + self._num_failures


def _sample() -> sample.Sample:
  return sample.Sample(
      metric='some_metric',
      value=42,
      unit='',
      metadata={},
      timestamp=time.time(),
  )


class HpaBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    container_service_mock.MockContainerInit(self)
    self.kubernetes_cluster = (
        container_service_mock.CreateTestKubernetesCluster()
    )
    self.bm_spec = mock.create_autospec(
        bm_spec_lib.BenchmarkSpec, instance=True
    )
    self.bm_spec.container_cluster = self.kubernetes_cluster
    self.bm_spec.container_specs = {
        'kubernetes_fib': container_spec.ContainerSpec(
            'kubernetes_fib',
            **{
                'image': 'ubuntu:latest',
            },
        )
    }

  def testPrepareClusterManifestApplied(self):
    self.enter_context(
        mock.patch.object(
            vm_util,
            'GetTempDir',
            return_value=tempfile.gettempdir(),
        )
    )
    manifest_path = 'container/kubernetes_hpa/fib.yaml.j2'
    self.MockIssueCommand({'apply -f': [('deployment.apps/fib fib', '', 0)]})
    with self.assertLogs(level='INFO') as logs:
      kubernetes_hpa_benchmark.PrepareCluster(self.bm_spec, manifest_path)
    full_logs = ';'.join(logs.output)
    self.assertIn('image: ubuntu:latest', full_logs)
    self.assertNotIn('template: {}', full_logs)


if __name__ == '__main__':
  unittest.main()
