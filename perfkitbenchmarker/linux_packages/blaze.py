# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing blaze installation and cleanup functions."""

import os
from perfkitbenchmarker import data
from perfkitbenchmarker import vm_util

BLAZE_VERSION = '3.0'
BLAZE_TAR = 'blaze-%s.tar.gz' % BLAZE_VERSION
BLAZE_DIR = '%s/blaze-%s' % (vm_util.VM_TMP_DIR, BLAZE_VERSION)
BLAZE_TAR_URL = (
    'https://bitbucket.org/blaze-lib/blaze/downloads/%s' % BLAZE_TAR)
CONFIG_TEMPLATE = 'blaze_config.j2'
CONFIG = 'config'
MAX_BLAZE_CACHE_SIZE_IN_B = 100000000


def _Configure(vm):
  """Configure and build blaze library.

  See https://bitbucket.org/blaze-lib/blaze/wiki/Configuration%20Files
  for more details.
  """
  vm.RenderTemplate(
      data.ResourcePath(CONFIG_TEMPLATE),
      os.path.join(BLAZE_DIR, CONFIG),
      {'compiler': 'g++-5',
       'compile_flags': ' -DBLAZE_USE_BOOST_THREADS --std=c++14'})
  # Adjust cache size
  cache_in_KB, _ = vm.RemoteCommand(
      'cat /proc/cpuinfo | grep "cache size" | awk \'{print $4}\'')
  cache_in_B = int(1024 * float(cache_in_KB.split()[0]))
  vm.RemoteCommand(
      'sed -i \'s/constexpr size_t cacheSize = 3145728UL;/constexpr '
      'size_t cacheSize = %sUL;/g\' %s' % (
          min(cache_in_B, MAX_BLAZE_CACHE_SIZE_IN_B - 1), os.path.join(
              BLAZE_DIR, 'blaze', 'config', 'CacheSize.h')))
  vm.RemoteCommand('cd %s; ./configure %s; make -j %s' % (
      BLAZE_DIR, CONFIG, vm.num_cpus))


def _Install(vm):
  """Installs the blaze package on the VM."""
  vm.RemoteCommand(
      'cd {tmp_dir}; wget {tar_url}; tar xzvf {tar}'.format(
          tmp_dir=vm_util.VM_TMP_DIR,
          tar_url=BLAZE_TAR_URL,
          tar=BLAZE_TAR))
  vm.RemoteCommand('sudo cp -r {blaze_dir}/blaze /usr/local/include'.format(
      blaze_dir=BLAZE_DIR))
  _Configure(vm)


def YumInstall(vm):
  """Installs the OpenBLAS package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the OpenBLAS package on the VM."""
  _Install(vm)
