"""GCS FUSE based disk implementation."""

from absl import flags
from perfkitbenchmarker import disk

FLAGS = flags.FLAGS


class GcsFuseDisk(disk.MountableDisk):
  """GCS FUSE based disk implementation.

  First subdirectory of mount point is buckets then object paths under that.
  """

  def Attach(self, vm):
    vm.Install('gcsfuse')

  def Mount(self, vm):
    vm.RemoteCommand(
        f'sudo mkdir -p {self.mount_point} && '
        f'sudo chmod a+w {self.mount_point}')
    vm.RemoteCommand(f'gcsfuse {FLAGS.gcsfuse_options} {self.mount_point}')

