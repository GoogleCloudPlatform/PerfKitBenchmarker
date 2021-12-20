"""Install gcsfuse package."""

from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_string('gcsfuse_version', '0.37.0', 'The version of the gcsfuse.')
flags.DEFINE_string(
    'gcsfuse_bucket', '', 'The GCS bucket to be mounted. '
    'If not set, all buckets are mounted as subdirectories.')

PACKAGE_LOCAL = '/tmp/gcsfuse.deb'
MNT = '/gcs'


def _PackageUrl():
  return 'https://github.com/GoogleCloudPlatform/gcsfuse/releases/download/v{v}/gcsfuse_{v}_amd64.deb'.format(
      v=FLAGS.gcsfuse_version)


def AptInstall(vm):
  """Installs the gcsfuse package and mounts gcsfuse.

  Args:
    vm: BaseVirtualMachine. VM to receive the scripts.
  """
  vm.InstallPackages('wget')
  vm.RemoteCommand('wget -O {local} {url}'.format(
      local=PACKAGE_LOCAL, url=_PackageUrl()))

  vm.RemoteCommand(
      f'sudo apt-get install -y --allow-downgrades {PACKAGE_LOCAL}')
