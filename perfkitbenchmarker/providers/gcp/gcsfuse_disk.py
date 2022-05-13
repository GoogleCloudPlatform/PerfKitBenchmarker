"""GCS FUSE based disk implementation."""

from absl import flags
from perfkitbenchmarker import disk

FLAGS = flags.FLAGS

DEFAULT_MOUNT_OPTIONS = [
    'allow_other',
    'dir_mode=777',
    'file_mode=777',
    'implicit_dirs',
]


class GcsFuseDisk(disk.MountableDisk):
  """GCS FUSE based disk implementation.

  Mount the bucket specified by flag gcsfuse_bucket at the mount_point. If not
  specified, all the buckets are mounted as subdirectories.
  """

  def Attach(self, vm):
    vm.Install('gcsfuse')

  def Mount(self, vm):
    vm.RemoteCommand(
        f'sudo mkdir -p {self.mount_point} && '
        f'sudo chmod a+w {self.mount_point}')

    opts = ','.join(DEFAULT_MOUNT_OPTIONS + FLAGS.mount_options)
    bucket = FLAGS.gcsfuse_bucket
    target = self.mount_point
    vm.RemoteCommand(f'sudo mount -t gcsfuse -o {opts} {bucket} {target}')
