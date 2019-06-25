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

"""Tests for beam_benchmark_helper."""

import mock
import tempfile
import unittest

from perfkitbenchmarker import beam_benchmark_helper
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS


class BeamBenchmarkHelperTestCase(unittest.TestCase):

  def setUp(self):
    # TODO(ferneyhough): See exactly why this is needed and find a better way
    # to do this. Unittests in PKB should not have to add this call manually.
    FLAGS.mark_as_parsed()
    super(BeamBenchmarkHelperTestCase, self).setUp()

  def test_runner_option_override_use_override(self):
    test_option_val = 'TestVal'
    actual_options = []
    beam_benchmark_helper.AddRunnerPipelineOption(actual_options, None,
                                                  test_option_val)
    self.assertListEqual(['--runner=' + test_option_val], actual_options)

  def test_runner_option_override_empty_override(self):
    test_option_val = ''
    actual_options = []
    beam_benchmark_helper.AddRunnerPipelineOption(actual_options, None,
                                                  test_option_val)
    self.assertListEqual([], actual_options)

  def test_dataflow_runner_name_added(self):
    test_option_val = 'dataflow'
    actual_command = []
    beam_benchmark_helper.AddRunnerArgument(actual_command, test_option_val)
    self.assertListEqual(['-DintegrationTestRunner=' + test_option_val],
                         actual_command)

  def test_direct_runner_name_added(self):
    test_option_val = 'direct'
    actual_command = []
    beam_benchmark_helper.AddRunnerArgument(actual_command, test_option_val)
    self.assertListEqual(['-DintegrationTestRunner=' + test_option_val],
                         actual_command)

  def test_runner_name_empty(self):
    test_option_val = ''
    actual_command = []
    beam_benchmark_helper.AddRunnerArgument(actual_command, test_option_val)
    self.assertListEqual([], actual_command)

  def test_extra_property_empty_property(self):
    test_option_val = ''
    actual_command = []
    beam_benchmark_helper.AddExtraProperties(actual_command, test_option_val)
    self.assertListEqual([], actual_command)

  def test_extra_property_single_property(self):
    test_option_val = '[key=value]'
    actual_mvn_command = []
    beam_benchmark_helper.AddExtraProperties(actual_mvn_command,
                                             test_option_val)
    self.assertListEqual(['-Dkey=value'], actual_mvn_command)

  def test_extra_property_single_property_quoted(self):
    test_option_val = '["key=value"]'
    actual_mvn_command = []
    beam_benchmark_helper.AddExtraProperties(actual_mvn_command,
                                             test_option_val)
    self.assertListEqual(['-Dkey=value'], actual_mvn_command)

  def test_extra_property_multiple_properties(self):
    test_option_val = '["key=value", "key2=value2"]'
    actual_mvn_command = []
    beam_benchmark_helper.AddExtraProperties(actual_mvn_command,
                                             test_option_val)
    self.assertListEqual(['-Dkey=value', '-Dkey2=value2'], actual_mvn_command)

  def test_integrationPipelineOptions_rejection(self):
    test_option_val = '["integrationTestPipelineOptions=..."]'
    actual_mvn_command = []
    with self.assertRaises(ValueError):
      beam_benchmark_helper.AddExtraProperties(actual_mvn_command,
                                               test_option_val)

  def test_hdfs_filesystem_addition(self):
    test_option_val = 'hdfs'
    actual_command = []
    beam_benchmark_helper.AddFilesystemArgument(actual_command, test_option_val)
    self.assertListEqual(['-Dfilesystem=hdfs'], actual_command)

  def test_empty_filesystem(self):
    test_option_val = ''
    actual_command = []
    beam_benchmark_helper.AddFilesystemArgument(actual_command, test_option_val)
    self.assertListEqual([], actual_command)

  def test_add_task(self):
    test_module_val = ':sdks:java:io'
    test_task_val = 'tests'
    actual_command = []
    beam_benchmark_helper.AddTaskArgument(actual_command, test_task_val,
                                          test_module_val)
    self.assertListEqual([':sdks:java:io:tests'], actual_command)

  def test_add_empty_task(self):
    test_option_val = ''
    actual_command = []
    with self.assertRaises(ValueError):
      beam_benchmark_helper.AddTaskArgument(actual_command, test_option_val,
                                            test_option_val)

  def test_initialize_beam_repo_beam_exists(self):
    FLAGS.beam_location = tempfile.mkdtemp()

    with mock.patch.object(beam_benchmark_helper, '_PrebuildBeam') as mock_prebuild, \
        mock.patch.object(vm_util, 'GenTempDir'):

      mock_spec = mock.MagicMock()
      mock_spec.dpb_service.SERVICE_TYPE = dpb_service.DATAFLOW

      beam_benchmark_helper.InitializeBeamRepo(mock_spec)
      mock_prebuild.assert_called_once()

  def test_initialize_beam_repo_beam_not_exists(self):
    FLAGS.beam_location = None

    with mock.patch.object(beam_benchmark_helper, '_PrebuildBeam') as mock_prebuild, \
        mock.patch.object(vm_util, 'GenTempDir'), \
        mock.patch.object(vm_util, 'GetTempDir'), \
        mock.patch.object(vm_util, 'IssueCommand') as mock_run:

      mock_spec = mock.MagicMock()
      mock_spec.dpb_service.SERVICE_TYPE = dpb_service.DATAFLOW

      beam_benchmark_helper.InitializeBeamRepo(mock_spec)

      expected_cmd = ['git', 'clone', 'https://github.com/apache/beam.git']
      mock_run.assert_called_once_with(expected_cmd,
                                       cwd=vm_util.GetTempDir())
      mock_prebuild.assert_called_once()

  def test_beam_prebuild(self):
    FLAGS.beam_prebuilt = False
    FLAGS.beam_it_module = ':sdks:java'
    FLAGS.beam_runner = 'dataflow'
    FLAGS.beam_filesystem = 'hdfs'
    FLAGS.beam_extra_properties = '[extra_key=extra_value]'

    with mock.patch.object(beam_benchmark_helper, '_GetGradleCommand') as mock_gradle, \
        mock.patch.object(beam_benchmark_helper, '_GetBeamDir'), \
        mock.patch.object(vm_util, 'IssueCommand') as mock_run:

      mock_gradle.return_value = 'gradlew'
      beam_benchmark_helper._PrebuildBeam()

      expected_cmd = [
          'gradlew',
          '--stacktrace',
          '--info',
          ':sdks:java:clean',
          ':sdks:java:assemble',
          '-DintegrationTestRunner=dataflow',
          '-Dfilesystem=hdfs',
          '-Dextra_key=extra_value'
      ]
      mock_run.assert_called_once_with(expected_cmd,
                                       cwd=beam_benchmark_helper._GetBeamDir(),
                                       timeout=1500)

  def test_build_python_gradle_command(self):
    FLAGS.beam_python_attr = 'IT'
    FLAGS.beam_it_module = ':sdks:python'
    FLAGS.beam_runner = 'TestRunner'
    FLAGS.beam_python_sdk_location = 'py/location.tar'
    FLAGS.beam_sdk = beam_benchmark_helper.BEAM_PYTHON_SDK

    with mock.patch.object(beam_benchmark_helper, '_GetGradleCommand') as mock_gradle, \
        mock.patch.object(beam_benchmark_helper, '_GetBeamDir'), \
        mock.patch.object(vm_util, 'ExecutableOnPath', return_value=True) as exec_check:

      mock_gradle.return_value = 'gradlew'
      mock_spec = mock.MagicMock()
      mock_spec.service_type = dpb_service.DATAFLOW

      actual_cmd, _ = beam_benchmark_helper.BuildBeamCommand(mock_spec,
                                                             'apache_beam.py',
                                                             ['--args'])
      expected_cmd = [
          'gradlew',
          ':sdks:python:integrationTest',
          '-Dtests=apache_beam.py',
          '-Dattr=IT',
          '-DpipelineOptions=--args "--runner=TestRunner" '
          '"--sdk_location=py/location.tar"',
          '--info',
          '--scan',
      ]
      self.assertListEqual(expected_cmd, actual_cmd)
      exec_check.assert_called_once()

  def test_build_java_gradle_command(self):
    FLAGS.beam_it_module = ':sdks:java'
    FLAGS.beam_runner = 'dataflow'
    FLAGS.beam_filesystem = 'hdfs'
    FLAGS.beam_extra_properties = '["extra_key=extra_value"]'
    FLAGS.beam_sdk = beam_benchmark_helper.BEAM_JAVA_SDK

    with mock.patch.object(beam_benchmark_helper, '_GetGradleCommand') as mock_gradle, \
        mock.patch.object(beam_benchmark_helper, '_GetBeamDir'), \
        mock.patch.object(vm_util, 'ExecutableOnPath', return_value=True) as exec_check:

      mock_gradle.return_value = 'gradlew'
      mock_spec = mock.MagicMock()
      mock_spec.service_type = dpb_service.DATAFLOW

      actual_cmd, _ = beam_benchmark_helper.BuildBeamCommand(mock_spec,
                                                             'org.apache.beam.sdk.java',
                                                             ['--args'])
      expected_cmd = [
          'gradlew',
          ':sdks:java:integrationTest',
          '--tests=org.apache.beam.sdk.java',
          '-DintegrationTestRunner=dataflow',
          '-Dfilesystem=hdfs',
          '-Dextra_key=extra_value',
          '-DintegrationTestPipelineOptions=[--args,"--runner=TestDataflowRunner"]',
          '--stacktrace',
          '--info',
          '--scan',
      ]
      self.assertListEqual(expected_cmd, actual_cmd)
      exec_check.assert_called_once()


if __name__ == '__main__':
  unittest.main()
