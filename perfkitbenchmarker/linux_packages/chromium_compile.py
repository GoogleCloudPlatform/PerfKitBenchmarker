"""Module for installing chromium_compile_benchmark dependencies."""

import json
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
    f'chromium_compile_working_directory_{DEFAULT_CHROMIUM_COMPILE_CHECKOUT}_ubuntu2404_x86_64.tar.gz': (
        '4885b7166f90c4cb0735d6e32ff0ed36104e7fd4f620032f4075d0dd2e2da93d'
    ),
    f'chromium_compile_working_directory_{DEFAULT_CHROMIUM_COMPILE_CHECKOUT}_ubuntu2404_aarch64.tar.gz': (
        'aaf4066ee70d49aaf4592d4b1f3a825ab1f2919095e47dad18608cc2757e30a6'
    ),
    f'chromium_compile_working_directory_{DEFAULT_CHROMIUM_COMPILE_CHECKOUT}_ubuntu2604_x86_64.tar.gz': (
        '07779c010c11bb9b0b86693cfffede5fe4cf2404fb01c664ea0723f1463b83e4'
    ),
    f'chromium_compile_working_directory_{DEFAULT_CHROMIUM_COMPILE_CHECKOUT}_ubuntu2604_aarch64.tar.gz': (
        'a350a9e270210ace9a94b0716a446b035d980b37f810bdbdf81953f47a739c4b'
    ),
}


def _GetTarballName(vm) -> str:
  return (
      f'chromium_compile_working_directory_{CHROMIUM_COMPILE_CHECKOUT.value}'
      f'_{vm.OS_TYPE}_{vm.cpu_arch}.tar.gz'
  )


def _InstallArmToolchain(vm, local_working_directory: str) -> None:
  """Replaces x86_64 prebuilt binaries with aarch64 equivalents.

  Chromium's tools/clang/scripts/build.py and tools/rust/build_rust.py hardcode
  x86_64 Linux host/stage0 binaries and perform full multi-target/multi-stage
  bootstraps. Instead, we replace the prebuilts directly. Note that on aarch64
  Clang is built in a single-stage Release config (without PGO/ThinLTO/BOLT)
  and mold is wrapped with ld.lld.

  NOTE: When upgrading DEFAULT_CHROMIUM_COMPILE_CHECKOUT, the pinned versions
  below can be looked up from the Chromium checkout (or made dynamic):
  - Node (v24.12.0): see 'node-linux-x64' in DEPS.
  - TypeScript (7.0.2): see third_party/typescript/linux-amd64/package.json.
  - Clang: dynamically read below from tools/clang/scripts/update.py.
  - Rust nightly (2026-08-12): commit date (+1 day) of RUST_REVISION in
    tools/rust/update_rust.py; libLLVM symlinks can glob ../../../libLLVM*.so*.
  - Bindgen (0.72.1): see BINDGEN_GIT_VERSION in tools/rust/build_bindgen.py
    (or install via --git BINDGEN_GIT_REPO --rev BINDGEN_GIT_VERSION).

  Args:
    vm: The VM to install the toolchain on.
    local_working_directory: The path to the local working directory.
  """
  src_path = os.path.join(local_working_directory, 'src')
  vm.InstallPackages('clang lld cmake libzstd-dev libxml2-dev')

  # 1. Copy system gperf and wrap ld.lld in place of the x86_64 mold binary.
  vm.RemoteCommand(
      f'mkdir -p {src_path}/third_party/gperf/cipd/bin && '
      f'cp -f /usr/bin/gperf {src_path}/third_party/gperf/cipd/bin/gperf && '
      f'rm -f {src_path}/buildtools/third_party/mold/cipd/mold && '
      "printf '#!/bin/sh\\nexec "
      '"$(dirname "$0")/../../../../third_party/llvm-build/'
      'Release+Asserts/bin/ld.lld" "$@"\\n\' '
      f'> {src_path}/buildtools/third_party/mold/cipd/mold && '
      f'chmod +x {src_path}/buildtools/third_party/mold/cipd/mold'
  )

  # 2. Replace node-linux-x64 with the matching linux-arm64 Node binary.
  vm.RemoteCommand(
      f'cd {src_path} && '
      'rm -rf third_party/node/linux/node-linux-x64 && '
      'mkdir -p third_party/node/linux/node-linux-x64 && '
      'curl -fsSL https://nodejs.org/dist/v24.12.0/'
      'node-v24.12.0-linux-arm64.tar.xz | '
      'tar -xJ --strip-components=1 -C third_party/node/linux/node-linux-x64'
  )

  # 3. Replace typescript/linux-amd64 tsc with the linux-arm64 binary.
  vm.RemoteCommand(
      f'cd {src_path} && '
      'chmod -R u+w third_party/typescript/linux-amd64/src/lib && '
      'curl -fsSL https://registry.npmjs.org/@typescript/'
      'typescript-linux-arm64/-/typescript-linux-arm64-7.0.2.tgz | '
      'tar -xz --strip-components=2 '
      '-C third_party/typescript/linux-amd64/src/lib package/lib/tsc'
  )

  # 4. Build AArch64 Clang (with Chromium plugins), LLD, LLVM binutils,
  # clang-format, and libclang at the pinned CLANG_REVISION.
  clang_rev, _ = vm.RemoteCommand(
      'python3 -c "import sys; '
      f"sys.path.append('{src_path}/tools/clang/scripts'); "
      'import update; print(update.CLANG_REVISION)"'
  )
  clang_short_sha = clang_rev.strip().split('-g')[-1]
  stdout, _ = vm.RemoteCommand(
      'curl -fsSL '
      '"https://chromium.googlesource.com/external/github.com/llvm/'
      f'llvm-project/+/{clang_short_sha}?format=JSON" | tail -n +2'
  )
  llvm_sha = json.loads(stdout)['commit']

  vm.RemoteCommand(
      'rm -rf /tmp/llvm-project /tmp/llvm-build && '
      'mkdir -p /tmp/llvm-project /tmp/llvm-build && '
      'cd /tmp/llvm-project && '
      'git init && '
      'git remote add origin '
      'https://chromium.googlesource.com/external/github.com/'
      'llvm/llvm-project && '
      f'git fetch --depth=1 origin {llvm_sha} && '
      'git checkout FETCH_HEAD'
  )

  llvm_bins = (
      'clang lld llvm-ar llvm-bcanalyzer llvm-cxxfilt llvm-dwarfdump '
      'llvm-dwp llvm-ml llvm-nm llvm-objcopy llvm-objdump llvm-pdbutil '
      'llvm-readobj llvm-symbolizer llvm-undname'
  )
  vm.RemoteCommand(
      'cd /tmp/llvm-build && '
      'cmake -GNinja '
      '-DCMAKE_BUILD_TYPE=Release '
      '-DCMAKE_POLICY_VERSION_MINIMUM=3.5 '
      '-DLLVM_ENABLE_ASSERTIONS=OFF '
      '-DLLVM_ENABLE_PROJECTS="clang;lld" '
      '-DLLVM_TARGETS_TO_BUILD="AArch64" '
      '-DLLVM_DEFAULT_TARGET_TRIPLE="aarch64-unknown-linux-gnu" '
      '-DLLVM_ENABLE_PER_TARGET_RUNTIME_DIR=ON '
      '-DLLVM_ENABLE_LLD=ON '
      '-DLLVM_ENABLE_Z3_SOLVER=OFF '
      '-DLLVM_ENABLE_UNWIND_TABLES=OFF '
      '-DLLVM_ENABLE_CURL=OFF '
      '-DLLVM_ENABLE_ZSTD=ON '
      '-DCLANG_PLUGIN_SUPPORT=OFF '
      '-DCLANG_ENABLE_STATIC_ANALYZER=OFF '
      '-DCLANG_ENABLE_ARCMT=OFF '
      '-DLLVM_EXTERNAL_PROJECTS=chrometools '
      f'-DLLVM_EXTERNAL_CHROMETOOLS_SOURCE_DIR={src_path}/tools/clang '
      '-DCHROMIUM_TOOLS="plugins;blink_gc_plugin;raw_ptr_plugin" '
      '-DCMAKE_C_COMPILER=clang '
      '-DCMAKE_CXX_COMPILER=clang++ '
      '/tmp/llvm-project/llvm && '
      f'ninja {llvm_bins} clang-format libclang'
  )

  vm.RemoteCommand(
      f'for bin in {llvm_bins}; do '
      'cp -f /tmp/llvm-build/bin/$bin '
      f'{src_path}/third_party/llvm-build/Release+Asserts/bin/$bin; '
      'done && '
      'cp -f /tmp/llvm-build/bin/clang-format '
      f'{src_path}/buildtools/linux64-format/clang-format && '
      f'rm -f {src_path}/third_party/rust-toolchain/lib/libclang.so* && '
      'cp -P /tmp/llvm-build/lib/libclang.so* '
      f'{src_path}/third_party/rust-toolchain/lib/ && '
      'rm -rf /tmp/llvm-project /tmp/llvm-build'
  )

  # 5. Replace third_party/rust-toolchain with aarch64 nightly + rustc-dev,
  # and build cc_bindings_from_rs (Crubit) and bindgen.
  rust_toolchain = f'{src_path}/third_party/rust-toolchain'
  vm.RemoteCommand(
      f'rm -rf {rust_toolchain}/lib/rustlib/rustc-src '
      f'{rust_toolchain}/lib/rustlib/components '
      f'{rust_toolchain}/lib/rustlib/x86_64-unknown-linux-gnu '
      f'{rust_toolchain}/.Linux_x64_* '
      '/tmp/rust-arm64 && '
      'mkdir -p /tmp/rust-arm64'
  )

  vm.RemoteCommand(
      'cd /tmp/rust-arm64 && '
      'curl -fsSL https://static.rust-lang.org/dist/2026-08-12/'
      'rust-nightly-aarch64-unknown-linux-gnu.tar.xz | tar -xJ && '
      './rust-nightly-aarch64-unknown-linux-gnu/install.sh '
      f'--prefix={rust_toolchain} --disable-ldconfig '
      '--components=rustc,cargo,rustfmt-preview,clippy-preview,'
      'rust-std-aarch64-unknown-linux-gnu'
  )

  vm.RemoteCommand(
      'cd /tmp/rust-arm64 && '
      'curl -fsSL https://static.rust-lang.org/dist/2026-08-12/'
      'rustc-dev-nightly-aarch64-unknown-linux-gnu.tar.xz | tar -xJ && '
      './rustc-dev-nightly-aarch64-unknown-linux-gnu/install.sh '
      f'--prefix={rust_toolchain} --disable-ldconfig && '
      'ln -sf ../../../libLLVM.so.23.1-rust-1.99.0-nightly '
      f'{rust_toolchain}/lib/rustlib/aarch64-unknown-linux-gnu/lib/ && '
      'ln -sf ../../../libLLVM-23-rust-1.99.0-nightly.so '
      f'{rust_toolchain}/lib/rustlib/aarch64-unknown-linux-gnu/lib/'
  )

  vm.RemoteCommand(
      f'cd {src_path} && '
      'python3 tools/rust/build_crubit.py && '
      f'PATH="{rust_toolchain}/bin:$PATH" '
      f'cargo install --root {rust_toolchain} '
      '--force --locked bindgen-cli@0.72.1 && '
      'rm -rf /tmp/rust-arm64 '
      f'{src_path}/third_party/rust-toolchain-intermediate'
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
    if vm.cpu_arch == 'aarch64' and (
        CHROMIUM_COMPILE_TOOLS_CHECKOUT.value
        != DEFAULT_CHROMIUM_COMPILE_TOOLS_CHECKOUT
        or CHROMIUM_COMPILE_CHECKOUT.value != DEFAULT_CHROMIUM_COMPILE_CHECKOUT
    ):
      raise ValueError(
          'ARM toolchain installation is only supported for the default '
          'checkout of depot_tools and chromium_compile.'
      )

    vm.RemoteCommand('mkdir -p {}'.format(local_working_directory))

    vm.RemoteCommand(
        'cd {} && git clone {} && cd depot_tools && git checkout {}'.format(
            local_working_directory,
            DEPOT_TOOLS_GIT_REPO,
            CHROMIUM_COMPILE_TOOLS_CHECKOUT.value,
        )
    )
    if vm.cpu_arch == 'aarch64':
      # infra/3pp/tools/gperf/linux-arm64 is missing from CIPD; skip it in
      # gclient and copy system /usr/bin/gperf in _InstallArmToolchain.
      vm.RemoteCommand(
          "sed -i 's/if not self.should_process:/"
          'if not self.should_process or "gperf" in self.name:/\' '
          f'{depot_tools_path}/gclient.py'
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
    if vm.cpu_arch == 'aarch64':
      _InstallArmToolchain(vm, local_working_directory)
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
