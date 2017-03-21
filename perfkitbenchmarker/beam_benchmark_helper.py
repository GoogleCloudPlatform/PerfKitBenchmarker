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

import os

from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util


# TODO: Find a better place for the maven_binary flag.
flags.DEFINE_string('maven_binary', 'mvn',
                    'Set to use a different mvn binary than is on the PATH.')
flags.DEFINE_string('beam_location', None,
                    'Location of already checked out Beam codebase.')
flags.DEFINE_string('beam_it_module', None,
                    'Module containing integration test.')
flags.DEFINE_string('beam_it_profile', None,
                    'Profile to activate integration test.')
flags.DEFINE_string('git_binary', 'git', 'Path to git binary.')
flags.DEFINE_string('beam_version', None, 'Version of Beam to download. Use'
                                          ' tag from Github as value.')

FLAGS = flags.FLAGS

SUPPORTED_RUNNERS = [
    dpb_service.DATAFLOW,
]

BEAM_REPO_LOCATION = 'https://github.com/apache/beam.git'
INSTALL_COMMAND = "{0} clean install -DskipTests -Dcheckstyle.skip=true -P{1}"


def InitializeBeamRepo(benchmark_spec):
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
    joined_cmd = ' '.join(clone_command)
    vm_util.IssueCommand([joined_cmd],
                         cwd=vm_util.GetTempDir(),
                         use_shell=True)
    beam_dir = os.path.join(vm_util.GetTempDir(), 'beam')
  else:
    beam_dir = FLAGS.beam_location

  if benchmark_spec.dpb_service.SERVICE_TYPE == dpb_service.DATAFLOW:
    vm_util.IssueCommand(
        [INSTALL_COMMAND.format(FLAGS.maven_binary, 'dataflow-runner')],
        cwd=beam_dir,
        use_shell=True)


def BuildMavenCommand(benchmark_spec, classname, job_arguments):
  if benchmark_spec.service_type not in SUPPORTED_RUNNERS:
    raise NotImplementedError('Unsupported Runner')

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
    cmd.append('-pl {}'.format(FLAGS.beam_it_module))

  if FLAGS.beam_it_profile:
    cmd.append('-P{}'.format(FLAGS.beam_it_profile))

  beam_args = job_arguments if job_arguments else []

  if benchmark_spec.service_type == dpb_service.DATAFLOW:
    cmd.append('-P{}'.format('dataflow-runner'))
    beam_args.append('"--runner=org.apache.beam.runners.'
                     'dataflow.testing.TestDataflowRunner"')
    beam_args.append('"--defaultWorkerLogLevel={}"'.format(FLAGS.dpb_log_level))

  cmd.append("-DintegrationTestPipelineOptions="
             "'[{}]'".format(','.join(beam_args)))
  full_cmd = ' '.join(cmd)

  # TODO: This is temporary, find a better way.
  beam_dir = FLAGS.beam_location if FLAGS.beam_location else os.path.join(
      vm_util.GetTempDir(), 'beam')
  return full_cmd, beam_dir
