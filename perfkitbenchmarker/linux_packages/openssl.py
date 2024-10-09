# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing OpenSSL installation and cleanup functions."""


def YumInstall(vm):
  """Installs OpenSSL on the VM."""
  vm.InstallPackages('openssl openssl-devel')


def AptInstall(vm):
  """Installs OpenSSL on the VM."""
  vm.InstallPackages('openssl libssl-dev')


def AptInstallQAT(vm):
  """Installs QAT engine for accelerated OpenSSL."""

  vm.InstallPackages('autoconf build-essential libtool cmake cpuid libssl-dev pkg-config nasm')

  # Install Intel® Integrated Performance Primitives Cryptography lib
  vm.RemoteCommand('git clone https://github.com/intel/ipp-crypto.git && cd ipp-crypto && git checkout ippcp_2021.7.1 && cd sources/ippcp/crypto_mb && cmake . -Bbuild -DCMAKE_INSTALL_PREFIX=/usr')

  # Install Intel® Multi-Buffer Crypto for IPsec Library lib
  vm.RemoteCommand('git clone https://github.com/intel/intel-ipsec-mb.git && cd intel-ipsec-mb && git checkout v1.3 && make -j && sudo make install NOLDCONFIG=y')

  # Install QAT_Engine
  vm.RemoteCommand('git clone https://github.com/intel/QAT_Engine.git && cd QAT_Engine && git checkout v1.2.0 && ./autogen.sh && ./configure --enable-qat_sw && make -j && sudo make install')