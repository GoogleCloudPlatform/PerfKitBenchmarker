# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing php installation and cleanup functions."""

import posixpath

from perfkitbenchmarker import vm_util

PHP_HOME_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'php-5.3.9')
PHP_TAR_URL = 'museum.php.net/php5/php-5.3.9.tar.gz'

MYSQL_TAR_URL = ('downloads.mysql.com/archives/get/file/'
                 'mysql-5.5.20-linux2.6-x86_64.tar.gz')
MYSQL_HOME_DIR = posixpath.join(vm_util.VM_TMP_DIR,
                                'mysql-5.5.20-linux2.6-x86_64')

APC_TAR_URL = 'pecl.php.net/get/APC-3.1.9.tgz'


def _Install(vm):
  """Installs the php package on the VM."""
  vm.Install('wget')
  vm.Install('curl')
  vm.Install('libcurl')
  vm.Install('libjpeg')
  vm.Install('libpng')
  vm.Install('libxml2')
  vm.RemoteCommand('cd %s && '
                   'wget %s && '
                   'tar zxf php-5.3.9.tar.gz'
                   % (vm_util.VM_TMP_DIR, PHP_TAR_URL))


def YumInstall(vm):
  """Installs the php package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the php package on the VM."""
  _Install(vm)


def ConfigureAndBuild(vm, config_file_path, connect_to_mysql=False):
  if connect_to_mysql:
    vm.RemoteCommand('cd %s && '
                     'wget %s && '
                     'tar zxf mysql-5.5.20-linux2.6-x86_64.tar.gz'
                     % (vm_util.VM_TMP_DIR, MYSQL_TAR_URL))
    vm.RemoteCommand('cd %s && '
                     './configure --enable-fpm --with-curl --with-pdo-mysql=%s'
                     ' --with-gd --with-jpeg-dir --with-png-dir '
                     '--with-config-file-path=%s'
                     % (PHP_HOME_DIR, MYSQL_HOME_DIR, config_file_path))
  else:
    vm.RemoteCommand('cd %s && '
                     './configure --enable-fpm --with-curl --with-gd '
                     '--with-jpeg-dir --with-png-dir --with-config-file-path=%s'
                     % PHP_HOME_DIR, config_file_path)
  vm.RemoteCommand('cd %s && '
                   'make && '
                   'sudo make install'
                   % PHP_HOME_DIR)


def InstallAPC(vm):
  vm.Install('autoconf')
  vm.RemoteCommand('cd %s && '
                   'wget %s && '
                   'tar zxf APC-3.1.9.tgz && '
                   'cd APC-3.1.9 && '
                   'phpize && '
                   './configure --enable-apc --enable-apc-mmap '
                   '--with-php-config=/usr/local/bin/php-config && '
                   'make -j && '
                   'sudo make -j install'
                   % (vm_util.VM_TMP_DIR, APC_TAR_URL))
