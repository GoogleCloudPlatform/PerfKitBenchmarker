"""Runs Nvidia HPC benchmark.

Source:
https://catalog.ngc.nvidia.com/orgs/nvidia/containers/hpc-benchmarks
"""

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import nvidia_driver
from perfkitbenchmarker.linux_packages import slurm


NVIDIA_HPC = 'nvcr.io#nvidia/hpc-benchmarks:23.10'
THROUGHPUT_REGEX = (
    r'Final Summary::HPCG result is VALID with a GFLOP/s rating of=(.*)')


BENCHMARK_NAME = 'nvidia_hpc'
BENCHMARK_CONFIG = """
nvidia_hpc:
  description: Runs Nvidia HPC.
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: g2-standard-4
          gpu_count: 1
          gpu_type: l4
          zone: us-east1-d
          boot_disk_size: 1000
        AWS:
          machine_type: g5.xlarge
          zone: us-east-1
          boot_disk_size: 1000
        Azure:
          machine_type: Standard_NC6
          zone: eastus
          boot_disk_size: 1000
      disk_spec: *default_500_gb
      vm_count: null
  flags:
    placement_group_style: closest_supported
    scratch_dir: /mnt/localssd
    data_disk_type: local
"""

FLAGS = flags.FLAGS


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _PrepareNvidiaHPL(vm):
  """Install packages and configure VM."""
  vm.Install('nvidia_hpc')
  nvidia_driver.EnablePersistenceMode(vm)
  vm.RemoteCommand('sudo mount -o remount,size=75% /run')


def Prepare(benchmark_spec):
  """Install and setup Nvidia HPL benchmark.

  Args:
    benchmark_spec: The benchmark specification.
  """
  vms = benchmark_spec.vms
  background_tasks.RunThreaded(_PrepareNvidiaHPL, vms)
  slurm.ConfigureSlurm(vms)


def _CreateMetadata(vms):
  """Constructing benchmark metadata."""
  metadata = dict()
  metadata.update(cuda_toolkit.GetMetadata(vms[0]))
  metadata['num_nodes'] = len(vms)
  gpus_per_node = FLAGS.hpcg_gpus_per_node or nvidia_driver.QueryNumberOfGpus(
      vms[0]
  )
  metadata['cpus_per_rank'] = int(vms[0].NumCpusForBenchmark() / gpus_per_node)
  metadata['gpus_per_node'] = gpus_per_node
  metadata['total_gpus'] = gpus_per_node * len(vms)
  metadata['runtime'] = FLAGS.hpcg_runtime
  metadata['problem_size'] = ','.join(str(n) for n in FLAGS.hpcg_problem_size)
  return metadata


def Run(benchmark_spec):
  """Runs Nvidia HPL benchmark.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects.
  """
  samples = []
  controller = benchmark_spec.vms[0]
  gpus_per_node = FLAGS.hpcg_gpus_per_node or nvidia_driver.QueryNumberOfGpus(
      benchmark_spec.vms[0])
  nx, ny, nz = FLAGS.hpcg_problem_size
  stdout, _ = controller.RobustRemoteCommand(
      'srun '
      f'-N {len(benchmark_spec.vms)} '
      f'--ntasks-per-node {gpus_per_node} '
      '--cpus-per-task '
      f'{int(controller.NumCpusForBenchmark() / gpus_per_node)} '
      # Public instruction uses pmix, but need extra work.
      '--cpu-bind=none --mpi=pmi2 '
      f'--container-image="{NVIDIA_HPC}" '
      f'./hpcg.sh --nx {nx} --ny {ny} --nz {nz} --rt {FLAGS.hpcg_runtime}')
  samples.append(
      sample.Sample(
          'HPCG Throughput',
          regex_util.ExtractFloat(THROUGHPUT_REGEX, stdout),
          'Gflops',
          _CreateMetadata(benchmark_spec.vms)))
  if gpus_per_node > 1 and len(benchmark_spec.vms) == 1:
    # measure per gpu performance
    metadata = _CreateMetadata(benchmark_spec.vms)
    metadata['cpus_per_task'] = controller.NumCpusForBenchmark()
    metadata['gpus_per_node'] = 1
    stdout, _ = controller.RobustRemoteCommand(
        'srun '
        f'-N {len(benchmark_spec.vms)} '
        f'--ntasks-per-node {1} '
        f'--cpus-per-task {controller.NumCpusForBenchmark()} '
        '--cpu-bind=none --mpi=pmi2 '
        f'--container-image="{NVIDIA_HPC}" '
        f'./hpcg.sh --nx {nx} --ny {ny} --nz {nz} --rt {FLAGS.hpcg_runtime}')
    samples.append(
        sample.Sample(
            'HPCG Throughput',
            regex_util.ExtractFloat(THROUGHPUT_REGEX, stdout),
            'Gflops', metadata))
  return samples


def Cleanup(_):
  """Cleanup Nvidia HPL."""
  pass
