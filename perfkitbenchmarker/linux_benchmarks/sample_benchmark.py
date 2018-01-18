"""Sample benchmark.

This benchmark demonstrates the use of preprovisioned data.

TODO(deitz): Expand this benchmark so that it describes how to add new
benchmarks to PerfKitBenchmarker and demonstrates more features.
"""

import posixpath

from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

BENCHMARK_NAME = 'sample'
BENCHMARK_CONFIG = """
sample:
  description: Runs a sample benchmark.
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: null
"""

# This benchmark uses preprovisioned data. Before running this benchmark on a
# given cloud, you must add the file 'sample_preprovisioned_data.txt' to that
# cloud by following the cloud-specific instructions. To create this file, run
# the following command:
#   echo "1234567890" > sample_preprovisioned_data.txt
#
# The value in this dict is the md5sum hash for the file.
#
# The benchmark defines the constant BENCHMARK_DATA to contain the name of the
# file (which is prepended by the benchmark name followed by an underscore)
# mapped to the md5sum hash. This ensures that when we change versions of the
# benchmark data or binaries, we update the code.
BENCHMARK_DATA = {'preprovisioned_data.txt': '7c12772809c1c0c3deda6103b10fdfa0'}


def GetConfig(user_config):
  """Returns the configuration of a benchmark."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Prepares the VMs and other resources for running the benchmark.

  This is a good place to download binaries onto the VMs, create any data files
  needed for a benchmark run, etc.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.
  """
  vm = benchmark_spec.vms[0]
  vm.InstallPreprovisionedBenchmarkData(BENCHMARK_NAME, BENCHMARK_DATA,
                                        vm_util.VM_TMP_DIR)

  stdout, _ = vm.RemoteCommand('cat %s' % (posixpath.join(
      vm_util.VM_TMP_DIR, 'preprovisioned_data.txt')))
  assert stdout.strip() == '1234567890'


def Run(benchmark_spec):
  """Runs the benchmark and returns a dict of performance data.

  It must be possible to run the benchmark multiple times after the Prepare
  stage.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.

  Returns:
    A list of performance samples.
  """
  del benchmark_spec
  # This is a sample benchmark that produces no meaningful data, but we return
  # a single sample as an example.
  metadata = dict()
  metadata['sample_metadata'] = 'sample'
  return [sample.Sample('sample_metric', 123.456, 'sec', metadata)]


def Cleanup(benchmark_spec):
  """Cleans up after the benchmark completes.

  The state of the VMs should be equivalent to the state before Prepare was
  called.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.
  """
  del benchmark_spec
