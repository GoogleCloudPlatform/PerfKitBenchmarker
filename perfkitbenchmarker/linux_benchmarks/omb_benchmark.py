"""Runs the OSU MPI benchmarks.

Source:
  https://mvapich.cse.ohio-state.edu/benchmarks/

Uses IntelMPI
"""

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

_BENCHMARKS_ARG = flags.DEFINE_multi_enum(
    'omb_benchmarks', None, sorted(omb.BENCHMARKS),
    'OSU micro-bencmarks to run.  Default is to run all')
_RUN_LONG_LATENCY = flags.DEFINE_bool(
    'omb_run_long_latency', False,
    'Whether to run the very long latency test get_acc_latency and latency_mt.')
_MESSAGE_SIZES = flags.DEFINE_list(
    'omb_message_sizes', None, '--message-size values to pass in.  Value of '
    '"1:8,1024" will run sizes 1,2,4,8,1024.  Default is to run all sizes')
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
    for message_size in _MESSAGE_SIZES.value or [None]:
      # Passing in message_size=None means to run all message sizes
      request = omb.RunRequest(benchmark, vms, message_size)
      for result in omb.RunBenchmark(request):
        samples.extend(_CreateSamples(result))
  return samples


def _GetBenchmarks() -> List[str]:
  """Returns the list of OSU benchmarks to run."""
  if _BENCHMARKS_ARG.value:
    return _BENCHMARKS_ARG.value
  if _RUN_LONG_LATENCY.value:
    return sorted(omb.BENCHMARKS)
  return sorted(name for name, run_type in sorted(omb.BENCHMARKS.items())
                if not run_type.long_running)


def _CreateSamples(result: omb.RunResult) -> Iterator[sample.Sample]:
  """Generates samples from the RunResult."""
  for row in result.data:
    entry = sample.Sample(result.name, row[result.value_column], result.units)
    entry.metadata.update(row)
    for key, value in result.metadata.items():
      entry.metadata[f'metadata_{key}'] = value
    for key, value in result.params.items():
      entry.metadata[f'param_{key}'] = value
    pinning = ';'.join(sorted(result.pinning))
    entry.metadata.update({
        'cmd': result.full_cmd,
        'omb_version': omb.VERSION,
        'mpi_vendor': result.mpi_vendor,
        'mpi_version': result.mpi_version,
        'number_processes': result.number_processes,
        'run_time': round(result.run_time, 1),
        'pinning': pinning,
        'perhost': result.perhost,
        'mpi_env': ';'.join(f'{k}={v}' for k, v in result.mpi_env.items()),
    })
    yield entry


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Cleanup omb.

  Args:
    bm_spec: The benchmark specification. Contains all data that is required to
      run the benchmark.
  """
  del bm_spec
