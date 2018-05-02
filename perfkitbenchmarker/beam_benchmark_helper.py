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

"""Helper methods for Apache Beam benchmarks.

This file contains methods which are common to all Beam benchmarks and
executions.
"""

import fnmatch
import os

from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

BEAM_JAVA_SDK = 'java'
BEAM_PYTHON_SDK = 'python'

# TODO: For backwards compatibility only. To be deleted when not used.
flags.DEFINE_string('maven_binary', 'mvn',
                    'Set to use a different mvn binary than is on the PATH.')

# TODO: For backwards compatibility only. To be deleted when not used.
flags.DEFINE_string('beam_it_profile', None, 'Profile to activate integration test.')

flags.DEFINE_string('gradle_binary', None,
                    'Set to use a different gradle binary than gradle wrapper from the repository')
flags.DEFINE_string('python_binary', 'python',
                    'Set to use a different python binary that is on the PATH.')
flags.DEFINE_string('beam_location', None, 'Location of already checked out Beam codebase.')
flags.DEFINE_string('beam_it_module', None,
                    'Module containing integration test. For Python SDK, use '
                    'comma-separated list to include multiple modules.')
flags.DEFINE_boolean('beam_prebuilt', False, 'Set this to indicate that the repo in beam_location '
                                             'does not need to be rebuilt before being used')
flags.DEFINE_integer('beam_it_timeout', 600, 'Integration Test Timeout.')
flags.DEFINE_string('git_binary', 'git', 'Path to git binary.')
flags.DEFINE_string('beam_version', None, 'Version of Beam to download. Use tag from Github '
                                          'as value. If not specified, will use HEAD.')
flags.DEFINE_enum('beam_sdk', None, [BEAM_JAVA_SDK, BEAM_PYTHON_SDK],
                  'Which BEAM SDK is used to build the benchmark pipeline.')
flags.DEFINE_string('beam_python_attr', 'IT',
                    'Test decorator that is used in Beam Python to filter a specific category.')

flags.DEFINE_string('beam_extra_properties', None,
                    'Allows to specify list of key-value pairs that will be '
                    'forwarded to target mvn command as system properties')

# TODO: For backwards compatibility only. To be replaced by beam_extra_properties flag.
flags.DEFINE_string('beam_extra_mvn_properties', None,
                    'Allows to specify list of key-value pairs that will be '
                    'forwarded to target mvn command as system properties')

flags.DEFINE_string('beam_runner', 'dataflow', 'Defines runner which will be used in tests')
flags.DEFINE_string('beam_runner_option', None,
                    'Overrides any pipeline options to specify the runner.')

flags.DEFINE_string('beam_filesystem', None, 'Defines filesystem which will be used in tests. '
                                             'If not specified it will use runner\'s local filesystem.')

FLAGS = flags.FLAGS

SUPPORTED_RUNNERS = [dpb_service.DATAFLOW]

BEAM_REPO_LOCATION = 'https://github.com/apache/beam.git'
GRADLE_INSTALL_COMMAND_ARGS = ['clean', 'install', '-xcheck', '--stacktrace']


def AddRunnerArgument(command, runner_name):
  if runner_name is None or runner_name == 'direct':
    command.append('-DintegrationTestRunner=direct')

  if runner_name == 'dataflow':
    command.append('-DintegrationTestRunner=dataflow')


def AddRunnerPipelineOption(beam_pipeline_options, runner_name, runner_option_override):
  runner_pipeline_option = ''

  if runner_name == 'dataflow':
    runner_pipeline_option = ('"--runner=TestDataflowRunner"')

  if runner_name == 'direct':
    runner_pipeline_option = ('"--runner=DirectRunner"')

  if runner_option_override:
    runner_pipeline_option = '--runner=' + runner_option_override

  if len(runner_pipeline_option) > 0:
    beam_pipeline_options.append(runner_pipeline_option)


def AddFilesystemArgument(command, filesystem_name):

  if filesystem_name == 'hdfs':
    command.append('-Dfilesystem=hdfs')


def AddExtraProperties(command, extra_properties):

  # TODO: For backwards compatibility only. Delete when beam_extra_mvn_properties is not used.
  if extra_properties is None:
    extra_properties = FLAGS.beam_extra_mvn_properties

  if not extra_properties:
    return
  if 'integrationTestPipelineOptions=' in extra_properties:
    raise ValueError('integrationTestPipelineOptions must not be in beam_extra_properties')

  extra_properties = extra_properties.rstrip(']').lstrip('[').split(',')
  extra_properties = [p.rstrip('" ').lstrip('" ') for p in extra_properties]
  for p in extra_properties:
    command.append('-D{}'.format(p))


def InitializeBeamRepo(benchmark_spec):
  """Ensures environment is prepared for running Beam benchmarks.

  In the absence of FLAGS.beam_location, initializes the beam source code base
  by checking out the repository from github. Specific branch selection is
  supported.

  Args:
    benchmark_spec: The PKB spec for the benchmark to run.
  """
  if benchmark_spec.dpb_service.SERVICE_TYPE not in SUPPORTED_RUNNERS:
    raise NotImplementedError('Unsupported Runner')

  vm_util.GenTempDir()
  if FLAGS.beam_location is None:
    git_clone_command = [FLAGS.git_binary, 'clone', BEAM_REPO_LOCATION]
    if FLAGS.beam_version:
      git_clone_command.append('--branch={}'.format(FLAGS.beam_version))
      git_clone_command.append('--single-branch')

    vm_util.IssueCommand(git_clone_command, cwd=vm_util.GetTempDir())

  elif not os.path.exists(FLAGS.beam_location):
    raise errors.Config.InvalidValue('Directory indicated by beam_location does not exist: {}.'.format(FLAGS.beam_location))

  _PrebuildBeam()


def _PrebuildBeam():
  # Rebuild beam if it was not build earlier
  if not FLAGS.beam_prebuilt:
    build_command = [_GetGradleCommand()]
    build_command.extend(GRADLE_INSTALL_COMMAND_ARGS)

    AddRunnerArgument(build_command, FLAGS.beam_runner)
    AddFilesystemArgument(build_command, FLAGS.beam_filesystem)
    AddExtraProperties(build_command, FLAGS.beam_extra_properties)

    vm_util.IssueCommand(build_command, timeout=1500, cwd=_GetBeamDir())


def BuildBeamCommand(benchmark_spec, classname, job_arguments):
  """ Constructs a Beam execution command for the benchmark.

  Args:
    benchmark_spec: The PKB spec for the benchmark to run.
    classname: The classname of the class to run.
    job_arguments: The additional job arguments provided for the run.

  Returns:
    cmd: Array containing the built command.
    beam_dir: The directory in which to run the command.
  """
  if benchmark_spec.service_type not in SUPPORTED_RUNNERS:
    raise NotImplementedError('Unsupported Runner')

  base_dir = _GetBeamDir()

  if FLAGS.beam_sdk == BEAM_JAVA_SDK:
    cmd = _BuildGradleCommand(classname, job_arguments)
  elif FLAGS.beam_sdk == BEAM_PYTHON_SDK:
    cmd = _BuildPythonCommand(benchmark_spec, classname, job_arguments)
    base_dir = _GetBeamPythonDir()
  else:
    raise NotImplementedError('Unsupported Beam SDK: %s.' % FLAGS.beam_sdk)

  return cmd, base_dir


def _BuildGradleCommand(classname, job_arguments):
  """ Constructs a Gradle command for the benchmark.

  Args:
    classname: The classname of the class to run.
    job_arguments: The additional job arguments provided for the run.

  Returns:
    cmd: Array containing the built command.
  """
  cmd = []

  gradle_executable = _GetGradleCommand()

  if not vm_util.ExecutableOnPath(gradle_executable):
    raise errors.Setup.MissingExecutableError('Could not find required executable "%s"' % gradle_executable)

  cmd.append(gradle_executable)
  cmd.append('integrationTest')
  cmd.append('--tests={}'.format(classname))

  if FLAGS.beam_it_module:
    cmd.append('-p')
    cmd.append(FLAGS.beam_it_module)

  beam_args = job_arguments if job_arguments else []

  AddRunnerArgument(cmd, FLAGS.beam_runner)
  AddRunnerPipelineOption(beam_args, FLAGS.beam_runner, FLAGS.beam_runner_option)
  AddFilesystemArgument(cmd, FLAGS.beam_filesystem)
  AddExtraProperties(cmd, FLAGS.beam_extra_properties)

  cmd.append('-DintegrationTestPipelineOptions='
             '[{}]'.format(','.join(beam_args)))

  cmd.append('--stacktrace')
  cmd.append('--info')

  return cmd


def _BuildPythonCommand(benchmark_spec, modulename, job_arguments):
  """ Constructs a Python command for the benchmark.

  Args:
    benchmark_spec: The PKB spec for the benchmark to run.
    modulename: The name of the python module to run.
    job_arguments: The additional job arguments provided for the run.

  Returns:
    cmd: Array containg the built command.
  """

  cmd = []

  python_executable = FLAGS.python_binary
  if not vm_util.ExecutableOnPath(python_executable):
    raise errors.Setup.MissingExecutableError('Could not find required executable "%s"' % python_executable)
  cmd.append(python_executable)

  cmd.append('setup.py')
  cmd.append('nosetests')
  cmd.append('--tests={}'.format(modulename))
  cmd.append('--attr={}'.format(FLAGS.beam_python_attr))

  beam_args = job_arguments if job_arguments else []

  if benchmark_spec.service_type == dpb_service.DATAFLOW:
    python_binary = _FindFiles(os.path.join(_GetBeamPythonDir(), 'target'),
                               'apache-beam*.tar.gz')
    if len(python_binary) == 0:
      raise RuntimeError('No python binary is found')

    beam_args.append('--runner=TestDataflowRunner')
    beam_args.append('--sdk_location={}'.format(python_binary[0]))

  cmd.append('--test-pipeline-options='
             '{}'.format(' '.join(beam_args)))

  return cmd


def _GetGradleCommand():
  return FLAGS.gradle_binary or os.path.join(_GetBeamDir(), 'gradlew')


def _GetBeamDir():
  # TODO: This is temporary, find a better way.
  return FLAGS.beam_location or os.path.join(vm_util.GetTempDir(), 'beam')


def _GetBeamPythonDir():
  return os.path.join(_GetBeamDir(), 'sdks/python')


def _FindFiles(base_path, pattern):
  if not os.path.exists(base_path):
    raise RuntimeError('No such directory: %s' % base_path)

  results = []
  for root, dirname, files in os.walk(base_path):
    for file in files:
      if fnmatch.fnmatch(file, pattern):
        results.append(os.path.join(root, file))
  return results
