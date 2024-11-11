"""Runs a timed compilation of the open-source chromium browser.

Resulting Samples include metadata tags for the specific chromium tag which
was built as well as the filesystem type of the scratch volume on which the
build occurred.
"""

import os.path

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'chromium_compile'
BENCHMARK_CONFIG = """
chromium_compile:
  description: Download and compile Google Chromium
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n1-standard-8
          zone: us-central1-c
        AWS:
          machine_type: c4.2xlarge
          zone: us-east-1a
        Azure:
          machine_type: Standard_F8
          zone: eastus
      disk_spec:
        # Standardize with 720 MB/s bandwidth to minimize I/O impact.
        GCP:
          disk_size: 1000
          disk_type: pd-ssd
          mount_point: /scratch
        AWS:
          num_striped_disks: 3
          disk_size: 333
          disk_type: gp3
          provisioned_iops: 12000
          provisioned_throughput: 240
          mount_point: /scratch
        Azure:
          num_striped_disks: 2
          disk_size: 500
          disk_type: PremiumV2_LRS
          provisioned_iops: 18000
          provisioned_throughput: 360
          mount_point: /scratch
      vm_count: 1
  flags:
    gcp_preprovisioned_data_bucket: 'p3rf-preprovisioned-data'
"""

WORKING_DIRECTORY_NAME = BENCHMARK_NAME + '_working_directory'
# Compilation fails when exceeding kMaxNumberOfWorkers (hardcoded to 256):
# https://github.com/chromium/chromium/blob/34135a338b8690ee89937e8d7b5abab01d6a404a/base/task/thread_pool/thread_group_impl.cc#L54  # pylint: disable=line-too-long
_MAX_NUMBER_OF_WORKERS = 512


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Clone chromium source and prepare a temporary directory.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  vm.Install('chromium_compile')


def _BuildAndCleanTarget(vm, requested_target, metadata):
  """Run a full build and clean of the provided git target.

  Args:
    vm: virtual_machine, where to run the build.
    requested_target: string, a valid ninja build target. Probably 'chrome' or
      'base_unittests'.
    metadata: dict, metadata to include in the resulting Samples.

  Returns:
    A list of sample.Sample instances.
  """
  if requested_target == 'ALL':
    build_target = ''
  else:
    build_target = requested_target

  local_working_directory = os.path.join(
      vm.GetScratchDir(), WORKING_DIRECTORY_NAME
  )
  depot_tools_path = os.path.join(local_working_directory, 'depot_tools')
  src_path = os.path.join(local_working_directory, 'src')
  gn_path = os.path.join(depot_tools_path, 'gn')
  thread_pool_impl_path = os.path.join(
      src_path, 'base/task/thread_pool/thread_group_impl.cc'
  )
  # Chromium hardcode max number of workers, but set thread pool size
  # dynamically based on number of processors.
  # Since we are only measuring compilation time, modify max worker count
  # so compilation doesn't fail.
  vm.RemoteCommand(
      'sed -i "s/constexpr size_t kMaxNumberOfWorkers = 256;/'
      f'constexpr size_t kMaxNumberOfWorkers = {_MAX_NUMBER_OF_WORKERS};/g" '
      f'{thread_pool_impl_path}'
  )
  returnme = []

  _, build_result = vm.RobustRemoteCommand(
      'cd {} && PATH="$PATH:{}" '
      'bash -c "time ninja -j {} -C out/Default {}"'.format(
          src_path, depot_tools_path, vm.NumCpusForBenchmark(), build_target
      )
  )

  returnme.append(
      sample.Sample(
          metric='{} build time'.format(requested_target),
          value=vm_util.ParseTimeCommandResult(build_result),
          unit='seconds',
          metadata=metadata,
      )
  )

  _, clean_result = vm.RemoteCommand(
      'cd {} && PATH="$PATH:{}" bash -c "time {} clean out/Default"'.format(
          src_path, depot_tools_path, gn_path
      )
  )

  returnme.append(
      sample.Sample(
          metric='{} clean time'.format(requested_target),
          value=vm_util.ParseTimeCommandResult(clean_result),
          unit='seconds',
          metadata=metadata,
      )
  )

  return returnme


def Run(benchmark_spec):
  """Run a timed compile of the Chromium source.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  vm = benchmark_spec.vms[0]
  returnme = []

  local_working_directory = os.path.join(
      vm.GetScratchDir(), WORKING_DIRECTORY_NAME
  )
  src_path = os.path.join(local_working_directory, 'src')

  df_stdout, _ = vm.RemoteCommand('df -T {}'.format(src_path))

  src_folder_filesystem = df_stdout.splitlines()[1].split()[1]

  metadata = {
      'chromium_compile_checkout': FLAGS.chromium_compile_checkout,
      'chromium_compile_tools_checkout': FLAGS.chromium_compile_tools_checkout,
      'src_folder_filesystem': src_folder_filesystem,
  }

  for target in FLAGS.chromium_compile_targets:
    samples = _BuildAndCleanTarget(vm, target, metadata)
    returnme += samples

  return returnme


def Cleanup(benchmark_spec):
  """Cleanup the VM to its original state.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  vm.RemoteCommand(
      'rm -rf {}'.format(
          os.path.join(vm.GetScratchDir(), WORKING_DIRECTORY_NAME)
      )
  )
