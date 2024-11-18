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
CHROMIUM_COMPILE_CHECKOUT = flags.DEFINE_string(
    'chromium_compile_checkout',
    '127.0.6533.88',
    'What (tag, commit, branch) to check out from the chromium repo. This will '
    'be included in the metadata so builds for the same tag can be correlated.',
)

CHROMIUM_COMPILE_TOOLS_CHECKOUT = flags.DEFINE_string(
    'chromium_compile_tools_checkout',
    '6e1a586bf2eb140e22caaf971ab133853e2c0c04',
    'What (tag, commit, branch) to check out from the chromium tools repo. '
    'This should be coordinated with --chromium_compile_checkout and will '
    'be included in the metadata so builds for the same tag can be correlated.',
)

FLAGS = flags.FLAGS
BENCHMARK_NAME = 'chromium_compile'
WORKING_DIRECTORY_NAME = BENCHMARK_NAME + '_working_directory'
DEPOT_TOOLS_GIT_REPO = (
    'https://chromium.googlesource.com/chromium/tools/depot_tools.git'
)

# Using 'chromium_compile_checkout'='127.0.6533.88' and
# 'chromium_compile_tools_checkout'='6e1a586bf2eb140e22caaf971ab133853e2c0c04',
# the provision,prepare stages were run to install
# 'chromium_compile_working_directory'. Run 'tar cvzf <dest> <src>' to create a
# tarball. Move tarball to a preprovisioned data bucket in GCS.
PREPROVISIONED_DATA = {
    'chromium_compile_working_directory.tar.gz': (
        '215d1270e24632368b7b334280b9e708025ae1ec30a58cdcfe1753c8ab264eda'
    ),
    'chromium_compile_working_directory_2024.tar.gz': (
        '22ad11eb4a86a496942873a3875464a4da950781e38b9e9b893afe983d0cc6a2'
    ),
}
TARBALL = 'chromium_compile_working_directory_2024.tar.gz'


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
    logging.info('Unable to use preprovisioned data.')

    vm.RemoteCommand('mkdir -p {}'.format(local_working_directory))

    vm.RemoteCommand(
        'cd {} && git clone {} && cd depot_tools && git checkout {}'.format(
            local_working_directory,
            DEPOT_TOOLS_GIT_REPO,
            CHROMIUM_COMPILE_TOOLS_CHECKOUT.value,
        )
    )

    # Check out the actual source tree. This will create src_path used below.
    vm.RemoteCommand(
        'cd {} && PATH="$PATH:{}" fetch --nohooks chromium'.format(
            local_working_directory, depot_tools_path
        )
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
        'cd {} && PATH="$PATH:{}" gclient runhooks'.format(
            local_working_directory, depot_tools_path
        )
    )

    vm.RemoteCommand(
        'cd {} && git checkout {}'.format(
            src_path, CHROMIUM_COMPILE_CHECKOUT.value
        )
    )

    vm.RemoteCommand(
        'cd {} && PATH="$PATH:{}" '
        'gclient sync --with_branch_heads --jobs 16'.format(
            local_working_directory, depot_tools_path
        )
    )

  vm.RemoteCommand(
      'cd {} && PATH="$PATH:{}" gn gen out/Default'.format(
          src_path, depot_tools_path
      )
  )


def _InstallViaPreprovisionedData(vm, scratch_dir='.'):
  """Returns whether Chromium_compile was installed via preprovisioned data."""

  # if the tarball is not in preprovisioned data return False
  if TARBALL not in PREPROVISIONED_DATA:
    return False
  if not vm.ShouldDownloadPreprovisionedData('chromium_compile', TARBALL):
    return False
  vm.InstallPreprovisionedPackageData(
      'chromium_compile', [TARBALL], scratch_dir, timeout=1800
  )
  vm.RemoteCommand(f'cd {scratch_dir} && tar xfz {TARBALL} && rm {TARBALL}')

  return True


def YumInstall(_):
  """Install the chromium_compile package on the VM using yum."""
  raise NotImplementedError(
      'Installation of chromium_compile is only supported on Debian.'
  )
