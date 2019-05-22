# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing installation functions for SPEC CPU 2006."""

from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import speccpu

FLAGS = flags.FLAGS

_PACKAGE_NAME = 'speccpu2006'
_MOUNT_DIR = 'cpu2006_mnt'
_SPECCPU2006_DIR = 'cpu2006'
_SPECCPU2006_ISO = 'cpu2006-1.2.iso'
_SPECCPU2006_TAR = 'cpu2006v1.2.tgz'
_TAR_REQUIRED_MEMBERS = 'cpu2006', 'cpu2006/bin/runspec'
_LOG_FORMAT = r'Est. (SPEC.*_base2006)\s*(\S*)'
_DEFAULT_RUNSPEC_CONFIG = 'linux64-x64-gcc47.cfg'
# This benchmark can be run with an .iso file in the data directory, a tar file
# in the data directory, or a tar file preprovisioned in cloud storage. To run
# this benchmark with tar file preprovisioned in cloud storage, update the
# following dict with md5sum of the file in cloud storage.
PREPROVISIONED_DATA = {_SPECCPU2006_TAR: None}


def GetSpecInstallConfig(scratch_dir):
  """Returns a SpecInstallConfigurations() for SPEC CPU 2006.

  Args:
    scratch_dir: The scratch directory on the VM that SPEC is installed on.
  """
  install_config = speccpu.SpecInstallConfigurations()
  install_config.package_name = _PACKAGE_NAME
  install_config.base_mount_dir = _MOUNT_DIR
  install_config.base_spec_dir = _SPECCPU2006_DIR
  install_config.base_iso_file_path = _SPECCPU2006_ISO
  install_config.base_tar_file_path = _SPECCPU2006_TAR
  install_config.required_members = _TAR_REQUIRED_MEMBERS
  install_config.log_format = _LOG_FORMAT
  install_config.runspec_config = (FLAGS.runspec_config or
                                   _DEFAULT_RUNSPEC_CONFIG)
  install_config.UpdateConfig(scratch_dir)
  return install_config


def Install(vm):
  """Installs SPECCPU 2006."""
  speccpu.InstallSPECCPU(vm, GetSpecInstallConfig(vm.GetScratchDir()))
