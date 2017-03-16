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

FLAGS = flags.FLAGS

CLONE_COMMAND = '{0} clone https://github.com/apache/beam.git'
INSTALL_COMMAND = "{0} clean install -DskipTests -Dcheckstyle.skip=true -P{1}"


def InitializeBeamRepo(benchmark_spec):
  vm_util.GenTempDir()
  if FLAGS.beam_location is None:
    vm_util.IssueCommand([CLONE_COMMAND.format(FLAGS.git_binary)],
                         cwd=vm_util.GetTempDir(),
                         use_shell=True)
    beam_dir = os.path.join(vm_util.GetTempDir(), 'beam')
  else:
    beam_dir = FLAGS.beam_location

  if benchmark_spec.SERVICE_TYPE is dpb_service.DATAFLOW:
    vm_util.IssueCommand(
      [INSTALL_COMMAND.format(FLAGS.maven_binary, 'dataflow-runner')],
      cwd=beam_dir,
      use_shell=True)
  elif benchmark_spec.SERVICE_TYPE is dpb_service.DATAPROC:
    vm_util.IssueCommand(
      [INSTALL_COMMAND.format(FLAGS.maven_binary, 'spark-runner')],
      cwd=beam_dir,
      use_shell=True)


def BuildMavenCommand(benchmark_spec, classname, job_arguments):
  cmd = []
  dataflow_executable = FLAGS.maven_binary

  if not vm_util.ExecutableOnPath(dataflow_executable):
    raise errors.Setup.MissingExecutableError(
      'Could not find required executable "%s"' % dataflow_executable)
  cmd.append(dataflow_executable)

  cmd.append('-e')
  cmd.append('verify')
  cmd.append('-Dit.test={}'.format(classname))
  cmd.append('-DskipITs=false')

  if FLAGS.dpb_beam_it_module:
    cmd.append('-pl {}'.format(FLAGS.dpb_beam_it_module))

  if FLAGS.dpb_beam_it_profile:
    cmd.append('-P{}'.format(FLAGS.dpb_beam_it_profile))

  beam_args = job_arguments if job_arguments else []

  if benchmark_spec.service_type == dpb_service.DATAFLOW:
    cmd.append('-Pdataflow-runner')
    beam_args.append('"--runner=org.apache.beam.runners.'
                     'dataflow.testing.TestDataflowRunner"')
    beam_args.append('"--defaultWorkerLogLevel={}"'.format(FLAGS.dpb_log_level))
  else:
    raise NotImplementedError('Unsupported Runner Specified')

  cmd.append("-DintegrationTestPipelineOptions="
             "'[{}]'".format(','.join(beam_args)))
  full_cmd = ' '.join(cmd)

  # TODO: This is temporary, find a better way.
  beam_dir = FLAGS.beam_location if FLAGS.beam_location else os.path.join(vm_util.getTempDir(), 'beam')
  return full_cmd, beam_dir
