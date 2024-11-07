"""Run NCCL benchmarks with GPUDirect-TCPXO framework."""

import time
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks import nccl_benchmark
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


def BuildTopoAwareHostFile(controller, nnodes):
  """Build topo aware hostfile.

  https://cloud.google.com/cluster-toolkit/docs/machine-learning/a3-mega-enable-gpudirect-tcpxo#run_nccl_test
  Args:
    controller: The controller VM
    nnodes: The number of nodes in the cluster
  """
  controller.RemoteCommand(
      'srun --mpi=pmi2 -n'
      f' {nnodes * _GPUS_PER_NODE} --ntasks-per-node={_GPUS_PER_NODE} bash -c'
      " 'curl -s"
      ' "http://metadata.google.internal/computeMetadata/v1/instance/attributes/physical_host"'
      ' -H "Metadata-Flavor: Google"; echo /$SLURMD_NODENAME\' | sort -t / -s'
      ' -k 1,4 | awk -F "/" \'{print $NF}\' >/var/tmp/topo_sorted_hostfile'
  )


def Prepare(benchmark_spec):
  """Install and set up NCCL on the target vm.

  Args:
    benchmark_spec: The benchmark specification
  """
  benchmark_spec.always_call_cleanup = True
  nnodes = len(benchmark_spec.vms)
  for vm in benchmark_spec.vms:
    vm.AuthenticateVm()
  slurm.ConfigureSlurm(benchmark_spec.vms)
  controller = benchmark_spec.vms[0]
  controller.RemoteCommand(f'srun -N {nnodes} enroot import {_IMAGE}')
  controller.RemoteCommand(
      f'srun -N {nnodes} git clone https://github.com/NVIDIA/nccl-tests.git'
  )
  # controller.RemoteCommand(
  #     'git clone https://github.com/NVIDIA/nccl-tests.git')
  controller.RemoteCommand(
      f'srun -N {nnodes} '
      '--container-mounts="$PWD:/nccl" '
      f'--container-image="{_SQSH_IMAGE}" '
      'bash -c "cd /nccl/nccl-tests/ && MPI=1 CC=mpicc CXX=mpicxx make -j"'
  )
  BuildTopoAwareHostFile(controller, len(benchmark_spec.vms))


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
  for operation in FLAGS.nccl_operations:
    stdout, _ = controller.RemoteCommand(
        'export NCCL_DEBUG=INFO; '
        'export NCCL_NET=FasTrak; '  # enforce using FasTrak
        'export NCCL_LIB_DIR="/var/lib/tcpxo/lib64"; '
        'export SLURM_HOSTFILE=/var/tmp/topo_sorted_hostfile; '
        'source /var/lib/tcpxo/lib64/nccl-env-profile.sh; '
        # pylint: disable=anomalous-backslash-in-string
        'HOST_VARS=$(sed \'s/ \{1,\}/,/g\' <<<"${!NCCL*}"); '
        'srun '
        '--mpi=pmi2 '
        f'--ntasks-per-node={_GPUS_PER_NODE} '
        f'--container-image="{_SQSH_IMAGE}" '
        '--container-env="${HOST_VARS}" '
        '--container-mounts="/var/tmp:/var/tmp,$PWD:/nccl,/var/lib/tcpxo/lib64/" '
        'sh -c "'
        'export LD_LIBRARY_PATH=/var/lib/tcpxo/lib64:/usr/lib/x86_64-linux-gnu:'
        '\$LD_LIBRARY_PATH; '  # pylint: disable=anomalous-backslash-in-string
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
