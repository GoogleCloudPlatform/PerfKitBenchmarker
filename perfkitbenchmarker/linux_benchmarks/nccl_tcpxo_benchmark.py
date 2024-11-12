"""Run NCCL benchmarks with GPUDirect-TCPXO framework."""

import time
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks import nccl_benchmark
from perfkitbenchmarker.linux_packages import optimize_gpu
from perfkitbenchmarker.linux_packages import slurm


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'nccl_tcpxo'
BENCHMARK_CONFIG = """
nccl_tcpxo:
  description: Runs NCCL Benchmark with TCPXO.
  vm_groups:
    default:
      vm_count: null
      disk_spec: *default_500_gb  # Store enroot images
      os_type: debian12_dl
      vm_spec:
        GCP:
          machine_type: a3-megagpu-8g
          zone: us-central1-a
          # User should build their own image following:
          # https://github.com/GoogleCloudPlatform/cluster-toolkit/blob/main/examples/machine-learning/a3-megagpu-8g/slurm-a3mega-image.yaml
          image_family: slurm-a3mega
          boot_disk_size: 500
  flags:
    placement_group_style: closest_supported
    gcloud_scopes: cloud-platform
    scratch_dir: /mnt/localssd

"""

_IMAGE = 'docker://nvcr.io#nvidia/pytorch:24.04-py3'
_SQSH_IMAGE = './nvidia+pytorch+24.04-py3.sqsh'
_GPUS_PER_NODE = 8  # This benchmark works specifically for a3-megagpu-8g


def CheckPrerequisites(_):
  """Perform flag checks."""
  if not FLAGS.image_project:
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


def Prepare(benchmark_spec):
  """Install and set up NCCL on the target vm.

  Args:
    benchmark_spec: The benchmark specification
  """
  benchmark_spec.always_call_cleanup = True
  nnodes = len(benchmark_spec.vms)
  for vm in benchmark_spec.vms:
    vm.AuthenticateVm()
    vm.Install('optimize_gpu')
  slurm.ConfigureSlurm(benchmark_spec.vms)
  controller = benchmark_spec.vms[0]
  controller.RemoteCommand(f'srun -N {nnodes} enroot import {_IMAGE}')
  controller.RemoteCommand(
      f'srun -N {nnodes} git clone https://github.com/NVIDIA/nccl-tests.git'
  )
  controller.RemoteCommand(
      f'srun -N {nnodes} '
      '--container-mounts="$PWD:/nccl" '
      f'--container-image="{_SQSH_IMAGE}" '
      'bash -c "cd /nccl/nccl-tests/ && MPI=1 CC=mpicc CXX=mpicxx make -j"'
  )
  optimize_gpu.BuildHostFile(controller, len(benchmark_spec.vms))


def Run(benchmark_spec):
  """Run NCCL on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  controller = benchmark_spec.vms[0]

  samples = []
  mount_path = ','.join(
      ['$PWD:/nccl'] + optimize_gpu.GetContainerMounts(benchmark_spec.vms[0])
  )
  for operation in FLAGS.nccl_operations:
    stdout, _ = controller.RemoteCommand(
        'export SLURM_HOSTFILE=/var/tmp/hostfile; '
        'srun '
        '--mpi=pmi2 '
        f'--ntasks-per-node={_GPUS_PER_NODE} '
        f'--container-image="{_SQSH_IMAGE}" '
        f'--container-mounts="{mount_path}" '
        'bash -c "'
        'export NCCL_DEBUG=INFO; '
        f'{optimize_gpu.SetContainerEnv(benchmark_spec.vms[0])} '
        f'/nccl/nccl-tests/build/{operation}_perf '
        f'--minbytes {FLAGS.nccl_minbytes} '
        f'--maxbytes {FLAGS.nccl_maxbytes} '
        f'--stepfactor {FLAGS.nccl_stepfactor} '
        f'--ngpus {FLAGS.nccl_ngpus} '
        f'--check {int(FLAGS.nccl_check)} '
        f'--nthreads {FLAGS.nccl_nthreads} '
        f'--iters {FLAGS.nccl_iters};"'
    )
    samples.extend(
        nccl_benchmark.MakeSamplesFromOutput(
            {
                'slots': _GPUS_PER_NODE,
                'operation': operation,
                'minbytes': FLAGS.nccl_minbytes,
                'maxbytes': FLAGS.nccl_maxbytes,
                'nccl_net_plugin': 'TCPFastrak',
            },
            stdout,
        )[0]
    )
    time.sleep(FLAGS.nccl_seconds_between_runs)
  return samples


def Cleanup(unused_benchmark_spec):
  """Cleanup NCCL on the cluster.

  Args:
    unused_benchmark_spec: The benchmark specification. Contains all data that
      is required to run the benchmark.
  """
  pass
