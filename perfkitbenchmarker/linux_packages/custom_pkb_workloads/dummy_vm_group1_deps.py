# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
# limitations under the License

import posixpath
import logging

from absl import flags
from perfkitbenchmarker.linux_packages import INSTALL_DIR


BENCHMARK_NAME = "dummy"
BENCHMARK_DATA_DIR = "dummy_benchmark"
BENCHMARK_DIR = posixpath.join(INSTALL_DIR, BENCHMARK_DATA_DIR)
GROUP_NAME = "vm_group1"
FLAGS = flags.FLAGS



flags.DEFINE_string("dummy_deps_ubuntu_make_vm_group1_ver", "4.0",
                    "Version of make package")
flags.DEFINE_string("dummy_deps_centos_make_vm_group1_ver", "4.0",
                    "Version of make package")


def AptInstall(vm):
  from perfkitbenchmarker.linux_benchmarks.dummy_benchmark import INSTALLED_PACKAGES
  pkg = "make"
  pkg_dict = {"name": pkg}
  if FLAGS.dummy_deps_ubuntu_make_vm_group1_ver:
    ver = FLAGS.dummy_deps_ubuntu_make_vm_group1_ver
    pkg_dict["version"] = ver
  INSTALLED_PACKAGES[GROUP_NAME].append(pkg_dict)
  vm.RemoteCommand("cd {0} && ".format(BENCHMARK_DIR) + "./dummy_prepare_ubuntu2004.sh install")


def AptUninstall(vm):
  """
  Uninstall Packages
  """
  vm.RemoteCommand("cd {0} && ".format(BENCHMARK_DIR) + "./dummy_prepare_ubuntu2004.sh remove")



def YumInstall(vm):
  from perfkitbenchmarker.linux_benchmarks.dummy_benchmark import INSTALLED_PACKAGES
  pkg = "make"
  pkg_dict = {"name": pkg}
  if FLAGS.dummy_deps_centos_make_vm_group1_ver:
    ver = FLAGS.dummy_deps_centos_make_vm_group1_ver
    pkg_dict["version"] = ver
  INSTALLED_PACKAGES[GROUP_NAME].append(pkg_dict)
  vm.RemoteCommand("cd {0} && ".format(BENCHMARK_DIR) + "./dummy_prepare_centos7.sh install")


def YumUninstall(vm):
  """
  Uninstall Packages
  """
  vm.RemoteCommand("cd {0} && ".format(BENCHMARK_DIR) + "./dummy_prepare_centos7.sh remove")
