"""Runs Nvidia HPL benchmark.

The test uses the sample data files within the Nvidia HPC image:
HPL-dgx-{nnodes}N.dat, which requires each node having 8xH100 GPUs.

Source:
https://catalog.ngc.nvidia.com/orgs/nvidia/containers/hpc-benchmarks
"""

import posixpath
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import nvidia_driver


BENCHMARK_NAME = 'nvidia_hpl'
BENCHMARK_CONFIG = """
nvidia_hpl:
  description: Runs nvidia hpl benchmark.
  cluster:
    headnode:
      vm_spec:
        GCP:
          machine_type: c3-standard-88
          zone: us-central1-a
          boot_disk_size: 2000
        AWS:
          machine_type: m7i.16xlarge
          zone: us-east-2a
          boot_disk_size: 2000
      vm_count: 1
    workers:
      vm_count: null
      os_type: ubuntu2004
      vm_spec:
        GCP:
          machine_type: a4-highgpu-8g
          zone: us-central1-a
        AWS:
          machine_type: p6.48xlarge
          zone: us-east-2a
  flags:
    placement_group_style: closest_supported
    preprovision_ignore_checksum: True
"""

BENCHMARK_DATA = {
    'a4.sqsh':
        'f1697f356da43c6c3c1ff2888349158c175a3393bfa107c659ba90610b326b21',
    'p6.sqsh':
        '02dc6376608348de27f78c8ed4d65cfc1b78731dc42681384d4f968a25864cb1'
}

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
  nvidia_driver.EnablePersistenceMode(vm)
  vm.RemoteCommand('sudo mount -o remount,size=75% /run')


def Prepare(benchmark_spec):
  """Install and setup Nvidia HPL benchmark.

  Args:
    benchmark_spec: The benchmark specification.
  """
  vms = benchmark_spec.vms
  background_tasks.RunThreaded(_PrepareNvidiaHPL, vms)
  benchmark_spec.cluster.InstallSquashImage(
      BENCHMARK_NAME,
      f'{benchmark_spec.cluster.TYPE}.sqsh',
      benchmark_spec.cluster.nfs_path,
      posixpath.join(BENCHMARK_NAME,
                     f'{benchmark_spec.cluster.TYPE}.dockerfile')
  )


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
  vms = benchmark_spec.cluster.worker_vms
  ngpus = nvidia_driver.QueryNumberOfGpus(vms[0])
  cpus_per_task = vms[0].NumCpusForBenchmark() // ngpus
  image = posixpath.join(
      benchmark_spec.cluster.nfs_path, f'{benchmark_spec.cluster.TYPE}.sqsh')
  hpl_command = ''
  if nvidia_driver.GetGpuType(vms[0]) == nvidia_driver.NVIDIA_H100:
    hpl_dat = f'HPL-dgx-{len(vms)}N.dat'
  elif nvidia_driver.GetGpuType(vms[0]) == nvidia_driver.NVIDIA_H200:
    hpl_dat = f'HPL-H200-{len(vms) * 8}GPUs.dat'
    if len(vms) > 4 and len(vms) != 16:
      raise ValueError(
          f'Unsupported number of nodes: {len(benchmark_spec.vms)}'
      )
  elif nvidia_driver.GetGpuType(vms[0]) == nvidia_driver.NVIDIA_B200:
    hpl_dat = f'HPL-B200-{len(vms) * 8}GPUs.dat'
  else:
    raise ValueError(
        f'Unsupported GPU type: {nvidia_driver.GetGpuType(vms[0])}'
    )
  hpl_command += (
      f'./hpl.sh --dat /workspace/hpl-linux-x86_64/sample-dat/{hpl_dat}'
  )
  stdout, _ = benchmark_spec.cluster.RemoteCommand(
      '--mpi=pmix --cpu-bind=none '
      f'--gpus-per-node={ngpus} --ntasks-per-node={ngpus} '
      f'--cpus-per-task={cpus_per_task} '
      f'--container-image {image} '
      '--container-writable '
      f'--wait=60 --kill-on-bad-exit=1 {hpl_command}',
      env=('export HPL_CUSOLVER_MP_TESTS=0; export HPL_USE_NVSHMEM=0; '),
      login_shell=True)
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
            _CreateMetadata(vms, result_line_parts),
        )
    )
  return samples


def Cleanup(_):
  """Cleanup Nvidia HPL."""
  pass
