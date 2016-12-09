import tempfile

from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import sample
from perfkitbenchmarker import flags

from perfkitbenchmarker.providers.gcp import gcp_dpb_dataproc
from perfkitbenchmarker.providers.gcp import gcp_dpb_dataflow

BENCHMARK_NAME = 'dp_df_wc_benchmark'

BENCHMARK_CONFIG = """
dp_df_wc_benchmark:
  description: Run word count on dataflow and dataproc
  dpb_service:
    service_type: dataproc
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-1
          boot_disk_size: 500
    worker_count: 2
"""

flags.DEFINE_string('gcs_input',
                    'gs://dataflow-samples/shakespeare/kinglear.txt',
                    'Input for word count')

FLAGS = flags.FLAGS

def GetConfig(user_config):
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
    pass

def Run(benchmark_spec):
    # Get handle to the dpb service
    dpb_service = benchmark_spec.dpb_service

    # TODO: Based on benchmark level config or a flag the storage will either be in gs or the hdfs created

    """
    Creating a file handle to contain the response from running the job on
    the dpb service
    """
    stdout_file = tempfile.NamedTemporaryFile(suffix='.stdout',
                                              prefix='spark_benchmark',
                                              delete=False)
    stdout_file.close()

    """
    Switch the parameters for submit job function of specific dpb service
    """
    if dpb_service.SERVICE_TYPE == 'dataproc':
        jarfile = gcp_dpb_dataproc.SPARK_SAMPLE_LOCATION
        classname = 'org.apache.spark.examples.JavaWordCount'
        job_arguments = [FLAGS.gcs_input]
        job_type = 'spark'
    elif dpb_service.SERVICE_TYPE == 'dataflow':
        jarfile = gcp_dpb_dataflow.DATAFLOW_WC_JAR
        classname = 'com.google.cloud.dataflow.examples.WordCount'
        job_arguments = ['--stagingLocation=gs://saksena-df/staging/', '--output=gs://saksena-df/output', '--runner=BlockingDataflowPipelineRunner']
        job_type = None
    else:
        raise NotImplementedError

    stats = dpb_service.SubmitJob(jarfile,
                                  classname,
                                  job_arguments=job_arguments,
                                  job_stdout_file=stdout_file,
                                  job_type=job_type)

    # TODO: What stats are we trying to gather
    print stats
    results = []

    # print 'Run based on', benchmark_spec.initialization_actions
    return results


def Cleanup(benchmark_spec):
    pass
