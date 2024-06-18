"""Module installing nccl-tests."""

from absl import flags

FLAGS = flags.FLAGS


def Install(vm):
  """Installs nccl-test on the VM."""
  env = ''
  if FLAGS.aws_efa:
    env = (
        'export LD_LIBRARY_PATH=/opt/amazon/efa/lib:/opt/amazon/efa/lib64:'
        '$LD_LIBRARY_PATH &&'
    )
  if FLAGS.azure_infiniband:
    vm.Install('mofed')
  vm.Install('cuda_toolkit')
  vm.Install('nccl')
  vm.Install('openmpi')
  vm.RemoteCommand('rm -rf nccl-tests')
  vm.RemoteCommand('git clone https://github.com/NVIDIA/nccl-tests.git')
  vm.RemoteCommand(
      'cd nccl-tests && {env} make MPI=1 MPI_HOME={mpi} '
      'NCCL_HOME={nccl} CUDA_HOME={cuda}'.format(
          env=env,
          mpi=FLAGS.nccl_mpi_home,
          nccl=FLAGS.nccl_home,
          cuda='/usr/local/cuda-{}'.format(FLAGS.cuda_toolkit_version),
      )
  )
