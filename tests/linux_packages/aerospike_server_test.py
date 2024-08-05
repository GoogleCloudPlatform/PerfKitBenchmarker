# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.packages.aerospike_client."""

import unittest

from absl import flags
from absl.testing import flagsaver
import mock

# pylint:disable=unused-import
from perfkitbenchmarker.linux_benchmarks import aerospike_benchmark
from perfkitbenchmarker.linux_packages import aerospike_server

FLAGS = flags.FLAGS


class AerospikeServerTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.vm = mock.Mock()
    self.vm.RemoteCommand.return_value = ('', '')
    self.vm.DownloadPreprovisionedData.return_value = ('', '')
    FLAGS.mark_as_parsed()

  @flagsaver.flagsaver(aerospike_instances=1)
  def testInstallFromPackage(self):
    aerospike_server._InstallFromPackage(self.vm)
    self.vm.DownloadPreprovisionedData.assert_called_once()
    self.vm.RemoteCommand.assert_has_calls([
        mock.call(
            'wget -O aerospike.tgz'
            ' https://enterprise.aerospike.com/enterprise/download/server/6.2.0/artifact/ubuntu20_amd64'
        ),
        mock.call('sudo mkdir -p /var/log/aerospike'),
        mock.call('mkdir -p aerospike'),
        mock.call('tar -xvf aerospike.tgz -C aerospike --strip-components=1'),
        mock.call('cd ./aerospike && sudo ./asinstall'),
        mock.call(
            'sudo mv ./aerospike/features.conf /etc/aerospike/features.conf'
        ),
    ])


if __name__ == '__main__':
  unittest.main()
