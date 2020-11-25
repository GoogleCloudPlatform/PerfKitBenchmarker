# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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

"""Installs the Intel apt/yum repo for MKL and other packages."""

import posixpath
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util

# The Intel yum/apt repositories are not included in the base OS and need to be
# added in.  The key file (data/intel_repo_key.txt) is the APT/YUM key
# associated with the Intel repositories as documented and releases listed here:
# https://software.intel.com/en-us/articles/installing-intel-free-libs-and-python-apt-repo
_INTEL_KEY_FILE = 'intel_repo_key.txt'
_REMOTE_KEY_FILE = posixpath.join(vm_util.VM_TMP_DIR, _INTEL_KEY_FILE)


# APT constants
# The local text file of the Intel repo entries.
_APT_REPO_FILE = 'intel_repo_list.txt'
# The remote file for the repo list.
_APT_REMOTE_REPO_FILE = posixpath.join(vm_util.VM_TMP_DIR, 'intel.list')
# Command to add the GPG key and update the repo list.
_APT_INSTALL_REPO_CMD = ';'.join([
    f'sudo apt-key add {_REMOTE_KEY_FILE}', f'rm {_REMOTE_KEY_FILE}',
    f'sudo mv {_APT_REMOTE_REPO_FILE} /etc/apt/sources.list.d/',
    'sudo apt-get update'
])

# YUM constants
_YUM_REPO_URL = 'https://yum.repos.intel.com/setup/intelproducts.repo'
# The current Intel GPG key.
_YUM_REPO_KEY = 'https://yum.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS-2019.PUB'
# Command to add the Intel repo.
_YUM_INSTALL_REPO_CMD = f'sudo yum-config-manager --add-repo {_YUM_REPO_URL}'
# The remote path to the downloaded GPG key from the repo
_YUM_DOWNLOAD_KEY = posixpath.join(vm_util.VM_TMP_DIR, 'mpi.yumkey')
# Command to download the current Intel GPG key.
_YUM_DOWNLOAD_KEY_CMD = f'curl -o {_YUM_DOWNLOAD_KEY} {_YUM_REPO_KEY}'
# Command to compare the current Intel key to our copy in the data/ directory.
_YUM_DIFF_KEY_CMD = f'diff {_REMOTE_KEY_FILE} {_YUM_DOWNLOAD_KEY}'


def AptPrepare(vm):
  """Configuration for APT install."""
  vm.PushDataFile(_INTEL_KEY_FILE, _REMOTE_KEY_FILE)
  vm.PushDataFile(_APT_REPO_FILE, _APT_REMOTE_REPO_FILE)
  vm.RemoteCommand(_APT_INSTALL_REPO_CMD)
  vm.InstallPackages('libgomp1')


def YumPrepare(vm):
  """Configuration for YUM install."""
  vm.PushDataFile(_INTEL_KEY_FILE, _REMOTE_KEY_FILE)
  vm.InstallPackages('yum-utils')
  vm.RemoteCommand(_YUM_INSTALL_REPO_CMD)
  # the /etc/yum.repos.d/intelproducts.repo file has the gpgkey listed as the
  # _YUM_REPO_KEY, confirm that it is the same as our local copy
  vm.RemoteCommand(_YUM_DOWNLOAD_KEY_CMD)
  diff, _, retcode = vm.RemoteCommandWithReturnCode(_YUM_DIFF_KEY_CMD)
  if retcode:
    raise errors.Setup.InvalidConfigurationError(
        f'Intel GPG key does not match local key: {diff}')
  vm.RemoteCommand(f'rm {_YUM_DOWNLOAD_KEY}')
  # need to update with -y to force import of known GPG key
  vm.RemoteCommand('sudo yum update -y')
