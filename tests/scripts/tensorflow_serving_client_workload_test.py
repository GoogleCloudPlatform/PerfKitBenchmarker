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
"""Tests for the tensorflow_serving_client_workload script."""

import datetime
import sys
import unittest

import mock
from six import StringIO

# These imports are mocked so that we don't need to add them to the
# test dependencies. The script under test for this test module is
# expected to execute only on a client VM which has built tensorflow
# from source.
sys.modules['grpc'] = mock.Mock()
sys.modules['grpc.beta'] = mock.Mock()
sys.modules['grpc.framework'] = mock.Mock()
sys.modules['grpc.framework.interfaces'] = mock.Mock()
sys.modules['grpc.framework.interfaces.face'] = mock.Mock()
sys.modules['grpc.framework.interfaces.face.face'] = mock.Mock()
sys.modules['tensorflow'] = mock.Mock()
sys.modules['tensorflow_serving'] = mock.Mock()
sys.modules['tensorflow_serving.apis'] = mock.Mock()

from perfkitbenchmarker.scripts import tensorflow_serving_client_workload  # pylint: disable=g-import-not-at-top,g-bad-import-order


class TestTensorflowServingClientWorkload(unittest.TestCase):

  def setUp(self):
    flag_values = {
        'server': '123:456',
        'image_directory': '/fake',
        'num_threads': 16,
        'runtime': 20,
    }

    p = mock.patch(tensorflow_serving_client_workload.__name__ + '.FLAGS')
    flags_mock = p.start()
    flags_mock.configure_mock(**flag_values)
    self.addCleanup(p.stop)

    os_patch = mock.patch(tensorflow_serving_client_workload.__name__ + '.os')
    os_patch.start()
    self.addCleanup(os_patch.stop)

    self.client_workload = (
        tensorflow_serving_client_workload.TfServingClientWorkload())

  def testPrintOutput(self):
    self.client_workload.num_completed_requests = 10
    self.client_workload.num_failed_requests = 2
    self.client_workload.latencies = [1.1, 2.2, 3.3]

    # Set start_time to an arbitarty datetime, and set end_time to 20 seconds
    # after start_time.
    self.client_workload.start_time = datetime.datetime(2000, 1, 1, 1, 1, 1, 1)
    self.client_workload.end_time = datetime.datetime(2000, 1, 1, 1, 1, 21, 1)

    expected_output = """
Completed requests: 10
Failed requests: 2
Runtime: 20.0
Number of threads: 16
Throughput: 0.5
Latency:
1.1
2.2
3.3""".strip()

    out = StringIO()
    self.client_workload.print_results(out=out)
    actual_output = out.getvalue().strip()
    self.assertEqual(expected_output, actual_output)


if __name__ == '__main__':
  unittest.main()
