import posixpath

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ycsb


BENCHMARK_NAME = 'cloud_datastore_ycsb'
BENCHMARK_CONFIG = '''
cloud_datastore_ycsb:
  description: >
      Run YCSB agains Google Cloud Datastore.
      Configure the number of VMs via --num-vms.
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: null'''

YCSB_BINDING_TAR_URL = ('https://github.com/brianfrankcooper/YCSB/releases'
                        '/download/0.9.0/'
                        'ycsb-googledatastore-binding-0.9.0.tar.gz')
YCSB_BINDING_LIB_DIR = posixpath.join(ycsb.YCSB_DIR, 'lib')
PRIVATE_KEYFILE_DIR = '/tmp/key.p12'

FLAGS = flags.FLAGS
flags.DEFINE_string('google_datastore_keyfile',
                    None,
                    'The path to Google API P12 private key file')
flags.DEFINE_string('google_datastore_serviceAccount',
                    None,
                    'The service account email associated with'
                    'datastore private key file')
flags.DEFINE_string('google_datastore_datasetId',
                    None,
                    'The project ID that has Cloud Datastore service')
flags.DEFINE_string('google_datastore_debug',
                    'false',
                    'The logging level when running YCSB')


def GetConfig(user_config):
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites():
    # Before YCSB Cloud Datastore supports Application Default Credential,
    # we should always make sure valid credential flags are set.
    if not FLAGS.google_datastore_keyfile:
        raise ValueError("'google_datastore_keyfile' must be set")
    if not FLAGS.google_datastore_serviceAccount:
        raise ValueError("'google_datastore_serviceAccount' must be set")
    if not FLAGS.google_datastore_datasetId:
        raise ValueError("'google_datastore_datasetId' must be set ")


def Prepare(benchmark_spec):
    benchmark_spec.always_call_cleanup = True
    default_ycsb_tar_url = ycsb.YCSB_TAR_URL
    vms = benchmark_spec.vms

    # TODO: figure out a less hacky way to override.
    # Override so that we only need to download the required binding.
    ycsb.YCSB_TAR_URL = YCSB_BINDING_TAR_URL

    # Install required packages and copy credential files
    vm_util.RunThreaded(_Install, vms)

    # Restore YCSB_TAR_URL
    ycsb.YCSB_TAR_URL = default_ycsb_tar_url


def Run(benchmark_spec):
    vms = benchmark_spec.vms
    executor = ycsb.YCSBExecutor('googledatastore')
    run_kwargs = {
        'threads': 4,
        'googledatastore.datasetId': FLAGS.google_datastore_datasetId,
        'googledatastore.privateKeyFile': PRIVATE_KEYFILE_DIR,
        'googledatastore.serviceAccountEmail':
        FLAGS.google_datastore_serviceAccount,
            'googledatastore.debug': FLAGS.google_datastore_debug,
    }
    load_kwargs = run_kwargs.copy()
    # TODO: support more flags, like thread, operationcount, recordcount, etc.
    if FLAGS['ycsb_preload_threads'].present:
        load_kwargs['threads'] = FLAGS['ycsb_preload_threads']
    samples = list(executor.LoadAndRun(vms,
                                       load_kwargs=load_kwargs,
                                       run_kwargs=run_kwargs))
    return samples


def Cleanup(benchmark_spec):
    # For now, we can only manually delete all the entries via GCP portal.
    return


def _Install(vm):
    vm.Install('ycsb')

    # Copy private key file to VM
    vm.RemoteCopy(FLAGS.google_datastore_keyfile, PRIVATE_KEYFILE_DIR)
