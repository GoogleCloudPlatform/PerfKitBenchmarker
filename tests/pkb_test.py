# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for pkb.py"""

from perfkitbenchmarker import pkb
from perfkitbenchmarker import stages
import unittest
import mock


class TestCreateFailedRunSampleFlag(unittest.TestCase):

  def patchPkbFunction(self, function_name):
    patcher = mock.patch(pkb.__name__ + '.' + function_name)
    mock_function = patcher.start()
    self.addCleanup(patcher.stop)
    return mock_function

  def setUp(self):
    self.flags_mock = self.patchPkbFunction('FLAGS')
    self.provision_mock = self.patchPkbFunction('DoProvisionPhase')
    self.prepare_mock = self.patchPkbFunction('DoPreparePhase')
    self.run_mock = self.patchPkbFunction('DoRunPhase')
    self.cleanup_mock = self.patchPkbFunction('DoCleanupPhase')
    self.teardown_mock = self.patchPkbFunction('DoTeardownPhase')
    self.make_failed_run_sample_mock = self.patchPkbFunction(
        'MakeFailedRunSample')

    self.flags_mock.run_stage = [
        stages.PROVISION, stages.PREPARE, stages.RUN, stages.CLEANUP,
        stages.TEARDOWN
    ]

    self.spec = mock.MagicMock()
    self.collector = mock.Mock()

  def testCreateProvisionFailedSample(self):
    self.flags_mock.create_failed_run_samples = True
    error_msg = 'error'
    self.provision_mock.side_effect = Exception(error_msg)

    self.assertRaises(Exception, pkb.RunBenchmark, self.spec, self.collector)
    self.make_failed_run_sample_mock.assert_called_once_with(
        error_msg, stages.PROVISION)

  def testCreatePrepareFailedSample(self):
    self.flags_mock.create_failed_run_samples = True
    error_msg = 'error'
    self.prepare_mock.side_effect = Exception(error_msg)

    self.assertRaises(Exception, pkb.RunBenchmark, self.spec, self.collector)
    self.make_failed_run_sample_mock.assert_called_once_with(
        error_msg, stages.PREPARE)

  def testCreateRunFailedSample(self):
    self.flags_mock.create_failed_run_samples = True
    error_msg = 'error'
    self.run_mock.side_effect = Exception(error_msg)

    self.assertRaises(Exception, pkb.RunBenchmark, self.spec, self.collector)
    self.make_failed_run_sample_mock.assert_called_once_with(
        error_msg, stages.RUN)

  def testCreateCleanupFailedSample(self):
    self.flags_mock.create_failed_run_samples = True
    error_msg = 'error'
    self.cleanup_mock.side_effect = Exception(error_msg)

    self.assertRaises(Exception, pkb.RunBenchmark, self.spec, self.collector)
    self.make_failed_run_sample_mock.assert_called_once_with(
        error_msg, stages.CLEANUP)

  def testCreateTeardownFailedSample(self):
    self.flags_mock.create_failed_run_samples = True
    error_msg = 'error'
    self.teardown_mock.side_effect = Exception(error_msg)

    self.assertRaises(Exception, pkb.RunBenchmark, self.spec, self.collector)
    self.make_failed_run_sample_mock.assert_called_once_with(
        error_msg, stages.TEARDOWN)

  def testDontCreateFailedRunSample(self):
    self.flags_mock.create_failed_run_samples = False
    self.run_mock.side_effect = Exception('error')

    self.assertRaises(Exception, pkb.RunBenchmark, self.spec, self.collector)
    self.make_failed_run_sample_mock.assert_not_called()


class TestMakeFailedRunSample(unittest.TestCase):

  @mock.patch('perfkitbenchmarker.sample.Sample')
  def testMakeFailedRunSample(self, sample_mock):
    error_msg = 'error'
    pkb.MakeFailedRunSample(error_msg, stages.PROVISION)

    sample_mock.assert_called_once()
    sample_mock.assert_called_with('Run Failed', 1, 'Run Failed', {
        'error_message': error_msg,
        'run_stage': stages.PROVISION,
        'flags': '{}'
    })

  @mock.patch('perfkitbenchmarker.sample.Sample')
  def testMakeFailedRunSampleWithTruncation(self, sample_mock):
    error_msg = 'This is a long error message that should be truncated.'
    pkb.FLAGS.failed_run_samples_error_length = 7

    pkb.MakeFailedRunSample(error_msg, stages.PROVISION)

    sample_mock.assert_called_once()
    sample_mock.assert_called_with('Run Failed', 1, 'Run Failed', {
        'error_message': "This is",
        'run_stage': stages.PROVISION,
        'flags': '{}'
    })
