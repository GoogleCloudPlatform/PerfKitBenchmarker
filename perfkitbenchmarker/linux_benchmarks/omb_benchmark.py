"""Runs the OSU MPI benchmarks.

Source:
  https://mvapich.cse.ohio-state.edu/benchmarks/

Uses IntelMPI
"""

import logging
from typing import Any, Dict, Iterator, List
from absl import flags

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import omb

BENCHMARK_NAME = 'omb'
BENCHMARK_CONFIG = """
omb:
  description: OSU MPI micro-benchmarks
  vm_groups:
    default:
      vm_count: 2
      vm_spec: *default_single_core
"""

_BENCHMARKS = [
    'acc_latency', 'allgather', 'allgatherv', 'allreduce', 'alltoall',
    'alltoallv', 'barrier', 'bcast', 'bibw', 'bw', 'cas_latency', 'fop_latency',
    'gather', 'gatherv', 'get_acc_latency', 'get_bw', 'get_latency',
    'iallgather', 'iallgatherv', 'iallreduce', 'ialltoall', 'ialltoallv',
    'ialltoallw', 'ibarrier', 'ibcast', 'igather', 'igatherv', 'ireduce',
    'iscatter', 'iscatterv', 'latency', 'latency_mp', 'latency_mt', 'mbw_mr',
    'multi_lat', 'put_bibw', 'put_bw', 'put_latency', 'reduce',
    'reduce_scatter', 'scatter', 'scatterv'
]

_BENCHMARKS_ARG = flags.DEFINE_multi_enum(
    'omb_benchmarks', None, _BENCHMARKS,
    'OSU micro-bencmarks to run.  Default is to run all')
_RUN_LONG_LATENCY = flags.DEFINE_bool(
    'omb_run_long_latency', False,
    'Whether to run the very long latency test get_acc_latency and latency_mt.')
FLAGS = flags.FLAGS


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Installs omb on the VM.

  Args:
    bm_spec: The benchmark specification. Contains all data that is required to
      run the benchmark.
  """
  vms = bm_spec.vms
  vms[0].Install('omb')
  omb.PrepareWorkers(vms)


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Run omb.

  Args:
    bm_spec: The benchmark specification. Contains all data that is required to
      run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = bm_spec.vms
  samples = []
  for benchmark in _GetBenchmarks():
    samples.extend(_CreateSamples(omb.RunBenchmark(vms, benchmark)))
  return samples


def _GetBenchmarks() -> List[str]:
  """Returns the list of OSU benchmarks to run."""
  if _BENCHMARKS_ARG.value:
    return _BENCHMARKS_ARG.value
  if _RUN_LONG_LATENCY.value:
    return _BENCHMARKS
  benchmarks = _BENCHMARKS.copy()
  # unless explicitly told to run the long latency tests skip them
  for long_test in omb.LONG_RUNNING_TESTS:
    logging.info('Skipping long benchmark %s', long_test)
    benchmarks.remove(long_test)
  return benchmarks


def _CreateSamples(result: omb.RunResult) -> Iterator[sample.Sample]:
  """Generates samples from the RunResult."""
  for row in result.data:
    row = row.copy()
    entry = sample.Sample(result.name, row.pop('value'), result.units)
    entry.metadata.update(row)
    for key, value in result.metadata.items():
      entry.metadata[f'metadata_{key}'] = value
    for key, value in result.params.items():
      entry.metadata[f'param_{key}'] = value
    entry.metadata.update({
        'cmd': result.full_cmd,
        'omb_version': omb.VERSION,
        'mpi_vendor': result.mpi_vendor,
        'mpi_version': result.mpi_version,
    })
    yield entry


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Cleanup omb.

  Args:
    bm_spec: The benchmark specification. Contains all data that is required to
      run the benchmark.
  """
  del bm_spec
