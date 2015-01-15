# Copyright 2014 Google Inc. All rights reserved.
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


"""Module containing mongodb installation and cleanup functions."""


def YumInstall(vm):
  """Installs the mongodb package on the VM."""
  vm.InstallEpelRepo()
  vm.InstallPackages('mongodb-server')


def AptInstall(vm):
  """Installs the mongodb package on the VM."""
  vm.InstallPackages('mongodb-server')


def YumGetServiceName(vm):
  """Returns the name of the mongodb service."""
  return 'mongod'


def AptGetServiceName(vm):
  """Returns the name of the mongodb service."""
  return 'mongodb'
