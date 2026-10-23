"""Module for installing chromium_compile_benchmark dependencies."""

import logging
import os.path
from absl import flags
from perfkitbenchmarker import vm_util

CHROMIUM_COMPILE_TARGETS = flags.DEFINE_list(
    'chromium_compile_targets',
    ['base_unittests', 'chrome'],
    'Build (and clean) these build targets. base_unittests is a '
    'minimal target which is very quick to build and should be '
    'closer to the build overhead. chrome builds the browser '
    'itself. Passing ALL will result in ninja building '
    'everything (an empty target) which, per the chromium docs '
    'will take hours.',
)

# TODO(andytzhu): Upgrade periodically.
DEFAULT_CHROMIUM_COMPILE_CHECKOUT = '154.0.8037.97'
CHROMIUM_COMPILE_CHECKOUT = flags.DEFINE_string(
    'chromium_compile_checkout',
    DEFAULT_CHROMIUM_COMPILE_CHECKOUT,
    'What (tag, commit, branch) to check out from the chromium repo. This will '
    'be included in the metadata so builds for the same tag can be correlated.',
)

DEFAULT_CHROMIUM_COMPILE_TOOLS_CHECKOUT = (
    '3e9862aec41915d8c39589f05a0380a6c12c525a'
)
CHROMIUM_COMPILE_TOOLS_CHECKOUT = flags.DEFINE_string(
    'chromium_compile_tools_checkout',
    DEFAULT_CHROMIUM_COMPILE_TOOLS_CHECKOUT,
    'What (tag, commit, branch) to check out from the chromium tools repo. '
    'This should be coordinated with --chromium_compile_checkout and will '
    'be included in the metadata so builds for the same tag can be correlated.',
)

SNAPSHOT_WORKING_DIRECTORY = flags.DEFINE_bool(
    'chromium_compile_snapshot_working_directory',
    False,
    'If true, create a tarball of the working directory at the end of the '
    'prepare stage and copy it to the runner VM.',
)

FLAGS = flags.FLAGS
BENCHMARK_NAME = 'chromium_compile'
WORKING_DIRECTORY_NAME = BENCHMARK_NAME + '_working_directory'
DEPOT_TOOLS_GIT_REPO = (
    'https://chromium.googlesource.com/chromium/tools/depot_tools.git'
)

# Tarballs are generated with --chromium_compile_snapshot_working_directory.
PREPROVISIONED_DATA = {
    f'chromium_compile_working_directory_{DEFAULT_CHROMIUM_COMPILE_CHECKOUT}_ubuntu2404.tar.gz': (
        '4885b7166f90c4cb0735d6e32ff0ed36104e7fd4f620032f4075d0dd2e2da93d'
    ),
    f'chromium_compile_working_directory_{DEFAULT_CHROMIUM_COMPILE_CHECKOUT}_ubuntu2604.tar.gz': (
        '07779c010c11bb9b0b86693cfffede5fe4cf2404fb01c664ea0723f1463b83e4'
    ),

}


def _GetTarballName(vm):
  return (
      f'chromium_compile_working_directory_{CHROMIUM_COMPILE_CHECKOUT.value}'
      f'_{vm.OS_TYPE}.tar.gz'
  )


def AptInstall(vm):
  """Install the chromium_compile package on the VM using aptitude.

  Args:
    vm: VM to install chromium_compile on.
  """
  scratch_dir = vm.GetScratchDir()
  local_working_directory = os.path.join(scratch_dir, WORKING_DIRECTORY_NAME)
  depot_tools_path = os.path.join(local_working_directory, 'depot_tools')
  src_path = os.path.join(local_working_directory, 'src')

  vm.Install('pip')
  # Required for Python 3.6 and earlier
  vm.RemoteCommand('sudo pip3 install importlib-metadata')
  # It's not clear why install-build-deps.sh does not pull these in.
  vm.InstallPackages('git ninja-build')

  is_preprovisioned = _InstallViaPreprovisionedData(vm, scratch_dir)
  if not is_preprovisioned:
    logging.info('Not using preprovisioned data.')

    vm.RemoteCommand('mkdir -p {}'.format(local_working_directory))

    vm.RemoteCommand(
        'cd {} && git clone {} && cd depot_tools && git checkout {}'.format(
            local_working_directory,
            DEPOT_TOOLS_GIT_REPO,
            CHROMIUM_COMPILE_TOOLS_CHECKOUT.value,
        )
    )

    # Check out the actual source tree at the target tag without git history.
    # Using gclient config + sync -r src@<tag> clones src and all sub-repos
    # directly at the pinned revision with --depth=1 (whereas `fetch` clones
    # HEAD first and a second `gclient sync` unshallows sub-repos on update).
    vm.RemoteCommand(
        f'cd {local_working_directory} && PATH="$PATH:{depot_tools_path}" '
        'gclient config https://chromium.googlesource.com/chromium/src.git '
        '--unmanaged && '
        f'PATH="$PATH:{depot_tools_path}" gclient sync --nohooks --no-history '
        '--with_branch_heads --jobs 16 '
        f'-r src@{CHROMIUM_COMPILE_CHECKOUT.value}'
    )

  vm.RemoteCommand(
      'echo ttf-mscorefonts-installer '
      'msttcorefonts/accepted-mscorefonts-eula select true '
      '| sudo debconf-set-selections'
  )

  vm_util.Retry()(vm.RemoteCommand)(
      f'cd {local_working_directory} && PATH="$PATH:{depot_tools_path}" '
      './src/build/install-build-deps.sh --no-prompt'
  )

  if not is_preprovisioned:
    vm.RemoteCommand(
        f'cd {local_working_directory} && PATH="$PATH:{depot_tools_path}" '
        'gclient runhooks'
    )
    vm.RemoteCommand(f'cd {src_path} && rm -rf .git')

  vm.RemoteCommand(
      'cd {} && PATH="$PATH:{}" gn gen out/Default'.format(
          src_path, depot_tools_path
      )
  )
  if SNAPSHOT_WORKING_DIRECTORY.value:
    _SnapshotWorkingDirectory(vm, scratch_dir)


def _InstallViaPreprovisionedData(vm, scratch_dir='.'):
  """Returns whether Chromium_compile was installed via preprovisioned data."""
  tarball = _GetTarballName(vm)
  # if the tarball is not in preprovisioned data return False
  if tarball not in PREPROVISIONED_DATA:
    return False
  if (
      CHROMIUM_COMPILE_TOOLS_CHECKOUT.value
      != DEFAULT_CHROMIUM_COMPILE_TOOLS_CHECKOUT
  ):
    return False
  if not vm.ShouldDownloadPreprovisionedData('chromium_compile', tarball):
    return False
  if SNAPSHOT_WORKING_DIRECTORY.value:
    logging.info(
        'Not using preprovisioned data because'
        ' --chromium_compile_snapshot_working_directory is set to True.'
    )
    return False
  vm.InstallPreprovisionedPackageData(
      'chromium_compile', [tarball], scratch_dir, timeout=1800
  )
  vm.RemoteCommand(f'cd {scratch_dir} && tar xfz {tarball} && rm {tarball}')

  return True


def _SnapshotWorkingDirectory(vm, scratch_dir='.'):
  """Snapshots the working directory tarball back to the runner VM."""
  tarball = _GetTarballName(vm)
  vm.RemoteCommand(
      f'cd {scratch_dir} && tar czf {tarball} {WORKING_DIRECTORY_NAME}'
  )
  vm.PullFile(vm_util.GetTempDir(), f'{scratch_dir}/{tarball}')
  vm.RemoteCommand(f'rm -rf {scratch_dir}/{tarball}')


def YumInstall(_):
  """Install the chromium_compile package on the VM using yum."""
  raise NotImplementedError(
      'Installation of chromium_compile is only supported on Debian.'
  )
