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
"""Module containing installation for NFSv4 Server.

Network File System (NFS) is a distributed file system protocol that allows a
user on a client computer to access files over a computer network much like
local storage is accessed.

This is mainly used for scientific-computing distributed workloads that
require file copying between master and worker nodes. Server can be used on the
master node.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


PACKAGE_NAME = 'nfs_server'


def YumInstall(vm):
  vm.InstallPackages('nfs-utils')


def AptInstall(vm):
  vm.InstallPackages('nfs-kernel-server')
