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
import logging
import os

from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

BEAM_JAVA_SDK = 'java'
BEAM_PYTHON_SDK = 'python'

# TODO: Find a better place for the maven_binary flag.
flags.DEFINE_string('maven_binary', 'mvn',
                    'Set to use a different mvn binary than is on the PATH.')
flags.DEFINE_string('python_binary', 'python',
                    'Set to use a different python binary that is on the PATH.')
flags.DEFINE_string('beam_location', None,
                    'Location of already checked out Beam codebase.')
flags.DEFINE_string('beam_it_module', None,
                    'Module containing integration test. For Python SDK, use '
                    'comma-separated list to include multiple modules.')
flags.DEFINE_string('beam_it_profile', None,
                    'Profile to activate integration test.')
flags.DEFINE_integer('beam_it_timeout', 600, 'Integration Test Timeout.')
flags.DEFINE_string('git_binary', 'git', 'Path to git binary.')
flags.DEFINE_string('beam_version', None, 'Version of Beam to download. Use'
                                          ' tag from Github as value. If not'
                                          ' specified, will use HEAD.')
flags.DEFINE_enum('beam_sdk', None, [BEAM_JAVA_SDK, BEAM_PYTHON_SDK],
                  'Which BEAM SDK is used to build the benchmark pipeline.')
flags.DEFINE_string('beam_python_attr', 'IT',
                    'Test decorator that is used in Beam Python to filter a '
                    'specific category.')

FLAGS = flags.FLAGS

SUPPORTED_RUNNERS = [
    dpb_service.DATAFLOW,
]

BEAM_REPO_LOCATION = 'https://github.com/apache/beam.git'
INSTALL_COMMAND_ARGS = ["clean", "install", "-DskipTests",
                        "-Dcheckstyle.skip=true"]


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
    clone_command = [
        FLAGS.git_binary,
        'clone',
        BEAM_REPO_LOCATION,
    ]
    if FLAGS.beam_version:
      clone_command.append('--branch={}'.format(FLAGS.beam_version))
      clone_command.append('--single-branch')
    vm_util.IssueCommand(clone_command, cwd=vm_util.GetTempDir())
  elif not os.path.exists(FLAGS.beam_location):
    raise errors.Config.InvalidValue(
        'Directory indicated by beam_location does not exist: '
        '{}.'.format(FLAGS.beam_location))

  if benchmark_spec.dpb_service.SERVICE_TYPE == dpb_service.DATAFLOW:
    mvn_command = [FLAGS.maven_binary]
    mvn_command.extend(INSTALL_COMMAND_ARGS)
    mvn_command.append('-Pdataflow-runner')
    logging.info("Running: %s", mvn_command)
    vm_util.IssueCommand(mvn_command, cwd=_GetBeamDir())


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
    cmd = _BuildMavenCommand(benchmark_spec, classname, job_arguments)
  elif FLAGS.beam_sdk == BEAM_PYTHON_SDK:
    cmd = _BuildPythonCommand(benchmark_spec, classname, job_arguments)
    base_dir = os.path.join(base_dir, 'sdks/python')
  else:
    raise NotImplementedError('Unsupported Beam SDK: %s.' % FLAGS.beam_sdk)

  return cmd, base_dir


def _BuildMavenCommand(benchmark_spec, classname, job_arguments):
  """ Constructs a maven command for the benchmark.

  Args:
    benchmark_spec: The PKB spec for the benchmark to run.
    classname: The classname of the class to run.
    job_arguments: The additional job arguments provided for the run.

  Returns:
    cmd: Array containing the built command.
  """
  cmd = []
  maven_executable = FLAGS.maven_binary

  if not vm_util.ExecutableOnPath(maven_executable):
    raise errors.Setup.MissingExecutableError(
        'Could not find required executable "%s"' % maven_executable)
  cmd.append(maven_executable)

  cmd.append('-e')
  cmd.append('verify')
  cmd.append('-Dit.test={}'.format(classname))
  cmd.append('-DskipITs=false')

  if FLAGS.beam_it_module:
    cmd.append('-pl')
    cmd.append(FLAGS.beam_it_module)

  if FLAGS.beam_it_profile:
    cmd.append('-P{}'.format(FLAGS.beam_it_profile))

  beam_args = job_arguments if job_arguments else []

  if benchmark_spec.service_type == dpb_service.DATAFLOW:
    cmd.append('-P{}'.format('dataflow-runner'))
    beam_args.append('"--runner=org.apache.beam.runners.'
                     'dataflow.testing.TestDataflowRunner"')
    beam_args.append('"--defaultWorkerLogLevel={}"'.format(FLAGS.dpb_log_level))

  cmd.append("-DintegrationTestPipelineOptions="
             "[{}]".format(','.join(beam_args)))

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
    raise errors.Setup.MissingExecutableError(
        'Could not find required executable "%s"' % python_executable)
  cmd.append(python_executable)

  cmd.append('setup.py')
  cmd.append('nosetests')
  cmd.append('--tests={}'.format(modulename))
  cmd.append('--attr={}'.format(FLAGS.beam_python_attr))

  beam_args = job_arguments if job_arguments else []

  if benchmark_spec.service_type == dpb_service.DATAFLOW:
    python_binary = _FindFiles(os.path.join(_GetBeamDir(),
                                            'sdks/python/target'),
                               'apache-beam*.tar.gz')
    if len(python_binary) == 0:
      raise RuntimeError('No python binary is found')

    beam_args.append('--runner=TestDataflowRunner')
    beam_args.append('--sdk_location={}'.format(python_binary[0]))

  cmd.append('--test-pipeline-options='
             '{}'.format(' '.join(beam_args)))

  return cmd


def _GetBeamDir():
  # TODO: This is temporary, find a better way.
  return FLAGS.beam_location if FLAGS.beam_location else os.path.join(
      vm_util.GetTempDir(), 'beam')


def _FindFiles(base_path, pattern):
  if not os.path.exists(base_path):
    raise RuntimeError('No such directory: %s' % base_path)

  results = []
  for root, dirname, files in os.walk(base_path):
    for file in files:
      if fnmatch.fnmatch(file, pattern):
        results.append(os.path.join(root, file))
  return results
