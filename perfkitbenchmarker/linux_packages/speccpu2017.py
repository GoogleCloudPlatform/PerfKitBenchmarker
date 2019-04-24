# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module containing installation functions for SPEC CPU 2017."""

from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import INSTALL_DIR
from perfkitbenchmarker.linux_packages import speccpu

FLAGS = flags.FLAGS

LLVM_TAR = 'clang+llvm-3.9.0-x86_64-linux-gnu-ubuntu-16.04.tar.xz'
LLVM_TAR_URL = 'http://releases.llvm.org/3.9.0/{0}'.format(LLVM_TAR)
OPENMP_TAR = 'libomp_20160808_oss.tgz'
OPENMP_TAR_URL = 'https://www.openmprtl.org/sites/default/files/{0}'.format(
    OPENMP_TAR)
_PACKAGE_NAME = 'speccpu2017'
_SPECCPU2017_DIR = 'cpu2017'
_SPECCPU2017_TAR = 'speccpu2017.tgz'
_TAR_REQUIRED_MEMBERS = 'cpu2017', 'cpu2017/bin/runcpu'
_LOG_FORMAT = r'Est. (SPEC.*2017_.*_base)\s*(\S*)'
_DEFAULT_RUNSPEC_CONFIG = 'pkb-crosstool-llvm-linux-x86-fdo.cfg'
PREPROVISIONED_DATA = {_SPECCPU2017_TAR: None}


def Install(vm):
  """Installs SPECCPU 2017."""
  install_config = speccpu.SpecInstallConfigurations()
  install_config.package_name = _PACKAGE_NAME
  install_config.base_spec_dir = _SPECCPU2017_DIR
  install_config.base_tar_file_path = _SPECCPU2017_TAR
  install_config.required_members = _TAR_REQUIRED_MEMBERS
  install_config.log_format = _LOG_FORMAT
  install_config.runspec_config = (FLAGS.runspec_config or
                                   _DEFAULT_RUNSPEC_CONFIG)
  speccpu.InstallSPECCPU(vm, install_config)
  vm.RemoteCommand('cd {0} && wget {1} && tar xf {2}'.format(
      INSTALL_DIR, LLVM_TAR_URL, LLVM_TAR))
  vm.RemoteCommand('cd {0} && wget {1} && tar xf {2}'.format(
      INSTALL_DIR, OPENMP_TAR_URL, OPENMP_TAR))
  vm.RemoteCommand('sudo apt-get install libjemalloc1 libjemalloc-dev')
  vm.RemoteCommand('sudo apt-get update && sudo apt-get install -y libomp-dev')
  # spec17 tarball comes pre-packages with runner scripts for x86 architecture.
  # But because we may have x86 or arm architecture machines, just rerun the
  # install script to regenerate the runner scripts based on what spec detects
  # to be the vm architecture.
  vm.RemoteCommand('echo yes | /scratch/cpu2017/install.sh')
