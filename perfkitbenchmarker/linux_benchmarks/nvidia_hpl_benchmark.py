"""Runs Nvidia HPL benchmark.

The test uses the sample data files within the Nvidia HPC image:
HPL-dgx-{nnodes}N.dat, which requires each node having 8xH100 GPUs.

Source:
https://catalog.ngc.nvidia.com/orgs/nvidia/containers/hpc-benchmarks
"""

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import nvidia_driver
from perfkitbenchmarker.linux_packages import optimize_gpu
from perfkitbenchmarker.linux_packages import slurm


BENCHMARK_NAME = 'nvidia_hpl'
BENCHMARK_CONFIG = """
nvidia_hpl:
  description: Runs nvidia hpl benchmark.
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: a3-megagpu-8g
          gpu_count: 8
          gpu_type: h100
          zone: us-east1-d
          boot_disk_size: 1000
        AWS:
          machine_type: p5.48xlarge
          zone: us-east-1
          boot_disk_size: 1000
      disk_spec: *default_500_gb
      vm_count: null
  flags:
    placement_group_style: closest_supported
    scratch_dir: /mnt/localssd
    data_disk_type: local
    preprovision_ignore_checksum: True
    gce_num_local_ssds: 16
    gce_ssd_interface: NVME
    gcloud_scopes: https://www.googleapis.com/auth/devstorage.read_write,cloud-platform
"""

FLAGS = flags.FLAGS


def CheckPrerequisites(_):
  """Perform flag checks."""
  if FLAGS.cloud == 'GCP' and not FLAGS.image_project:
    raise errors.Benchmarks.UnsupportedConfigError(
        '--image_project is required. Please follow'
        ' https://cloud.google.com/cluster-toolkit/docs/deploy/deploy-a3-mega-cluster'
        ' to build your own image.'
    )


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
  vm.RemoteCommand(
      'echo "FROM nvcr.io/nvidia/hpc-benchmarks:24.06" >> Dockerfile')
  vm.RemoteCommand(
      'echo "WORKDIR /workspace" >> Dockerfile')
  vm.UpdateDockerfile('Dockerfile')
  vm.RemoteCommand('docker build --network=host -t pkb-hpc-image .')


def Prepare(benchmark_spec):
  """Install and setup Nvidia HPL benchmark.

  Args:
    benchmark_spec: The benchmark specification.
  """
  vms = benchmark_spec.vms
  background_tasks.RunThreaded(_PrepareNvidiaHPL, vms)
  slurm.ConfigureSlurm(vms)
  background_tasks.RunThreaded(optimize_gpu.Install, vms)
  optimize_gpu.BuildHostFile(vms[0], len(benchmark_spec.vms))


def _CreateMetadata(vms, result_line_parts):
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
  metadata['N'] = float(result_line_parts[1])
  metadata['NB'] = float(result_line_parts[2])
  metadata['P'] = float(result_line_parts[3])
  metadata['Q'] = float(result_line_parts[4])
  metadata['time'] = float(result_line_parts[5])
  metadata['gflops_per_gpu'] = float(result_line_parts[8][:-1])
  return metadata


def Run(benchmark_spec):
  """Runs Nvidia HPL benchmark.

  Sample output:
  ==============================================================================
  T/V              N    NB     P     Q         Time          Gflops (   per GPU)
  ------------------------------------------------------------------------------
  WC0         376832  1024     4     4        85.42       4.176e+05 ( 2.610e+04)

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects.
  """
  samples = []
  controller = benchmark_spec.vms[0]
  gpus_per_node = FLAGS.hpcg_gpus_per_node or nvidia_driver.QueryNumberOfGpus(
      benchmark_spec.vms[0])
  provider_env = optimize_gpu.SetContainerEnv(controller)
  hpl_command = (
      './hpl.sh --dat /workspace/hpl-linux-x86_64/sample-dat/'
      f'HPL-dgx-{len(benchmark_spec.vms)}N.dat'
  )
  # pylint: disable=protected-access
  hostfile = controller._RemoteFileExists('/var/tmp/hostfile')
  if hostfile:
    hostfile_arg = 'export SLURM_HOSTFILE=/var/tmp/hostfile; '
    slurm_args = ''
  else:
    hostfile_arg = ''
    slurm_args = f'-N {len(benchmark_spec.vms)} '
  mount_args = ','.join(optimize_gpu.GetContainerMounts(controller))
  if mount_args:
    slurm_args += f'--container-mounts="{mount_args}" '
  stdout, _ = controller.RemoteCommand(
      f'{hostfile_arg}'
      'export TMPDIR=/tmp; '
      'export NCCL_DEBUG=INFO; '
      'export HPL_FCT_COMM_POLICY=1; '
      'export HPL_P2P_AS_BCAST=0; '
      'export HPL_USE_NVSHMEM=0; '
      'export NVSHMEM_DISABLE_CUDA_VMM=1; '
      'export OMPI_MCA_pml="ucx"; '
      'export UCX_MAX_RNDV_RAILS=8; '
      # environment variables to use
      f'srun '
      f'--ntasks-per-node {gpus_per_node} '
      '--cpus-per-task '
      f'{int(controller.NumCpusForBenchmark() / gpus_per_node)} '
      '--cpu-bind=none --mpi=pmi2 '
      '--container-image="dockerd://pkb-hpc-image" '
      f'{slurm_args} bash -c "{provider_env} {hpl_command}"'
  )
  lines = stdout.splitlines()
  result_line_idx = None
  for line_idx, line in enumerate(lines):
    if line.startswith(
        'T/V                N    NB     P     Q         Time          Gflops '
        '(   per GPU)'):
      result_line_idx = line_idx + 2
      break
  if not result_line_idx:
    raise ValueError('Failed to find result line.')
  else:
    result_line = lines[result_line_idx]
    result_line_parts = result_line.split()
    samples.append(
        sample.Sample(
            'HPL Throughput',
            float(result_line_parts[6]),
            'Gflops',
            _CreateMetadata(benchmark_spec.vms, result_line_parts),
        )
    )
  return samples


def Cleanup(_):
  """Cleanup Nvidia HPL."""
  pass
