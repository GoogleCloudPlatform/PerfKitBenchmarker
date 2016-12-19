import tempfile

from perfkitbenchmarker import configs
from perfkitbenchmarker.dpb_service import BaseDpbService
from perfkitbenchmarker import flags
from perfkitbenchmarker.providers.aws import aws_dpb_emr
from perfkitbenchmarker.providers.gcp import gcp_dpb_dataproc
from perfkitbenchmarker.providers.gcp import gcp_dpb_dataflow

BENCHMARK_NAME = 'dpb_wordcount_benchmark'

BENCHMARK_CONFIG = """
dpb_wordcount_benchmark:
  description: Run word count on dataflow and dataproc
  flags:
    dpb_wordcount_out_fs: gs
    dpb_wordcount_out_base: saksena-df
  dpb_service:
    service_type: dataproc
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-1
          boot_disk_size: 500
        AWS:
          machine_type: m1.medium
      disk_spec:
        GCP:
          disk_type: nodisk
        AWS:
          disk_size: 500
          disk_type: gp2
    worker_count: 2
"""

flags.DEFINE_string('dpb_wordcount_gcs_input',
                    'gs://dataflow-samples/shakespeare/kinglear.txt',
                    'Input for word count')
flags.DEFINE_string('dpb_wordcount_s3_input',
                    's3://pkb-shakespeare/kinglear.txt',
                    'Input for word count')
flags.DEFINE_enum('dpb_wordcount_out_fs', BaseDpbService.HDFS_OUTPUT_FS,
                  [BaseDpbService.HDFS_OUTPUT_FS, BaseDpbService.GCS_OUTPUT_FS,
                   BaseDpbService.S3_OUTPUT_FS],
                  'File System to use for the job output')
flags.DEFINE_string('dpb_wordcount_out_base', None,
                    'Base directory for word count output')

FLAGS = flags.FLAGS


def GetConfig(user_config):
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
    pass


def Run(benchmark_spec):
    # Get handle to the dpb service
    dpb_service = benchmark_spec.dpb_service

    """
    Create a file handle to contain the response from running the job on
    the dpb service
    """
    stdout_file = tempfile.NamedTemporaryFile(suffix='.stdout',
                                              prefix='spark_benchmark',
                                              delete=False)
    stdout_file.close()

    """
    Switch the parameters for submit job function of specific dpb service
    """
    job_arguments = []
    if dpb_service.SERVICE_TYPE == 'dataproc':
        jarfile = gcp_dpb_dataproc.SPARK_SAMPLE_LOCATION
        classname = 'org.apache.spark.examples.JavaWordCount'
        job_arguments.append(FLAGS.dpb_wordcount_gcs_input)
        job_type = dpb_service.SPARK_JOB_TYPE
    elif dpb_service.SERVICE_TYPE == 'dataflow':
        jarfile = gcp_dpb_dataflow.DATAFLOW_WC_JAR
        classname = 'com.google.cloud.dataflow.examples.WordCount'
        """
        Validate and setup the output and staging directories for the job
        """
        if FLAGS.dpb_wordcount_out_fs != dpb_service.GCS_OUTPUT_FS:
          raise Exception('Invalid File System integration required for a '
                          'dataflow job')
        if not FLAGS.dpb_wordcount_out_base:
          raise Exception('Missing base output directory')
        base_gs_dataflow_dir = 'gs://{0}'.format(FLAGS.dpb_wordcount_out_base)
        job_arguments.append('--stagingLocation={0}/staging/'.format(
            base_gs_dataflow_dir))
        job_arguments.append('--output={0}/output/'.format(
            base_gs_dataflow_dir))
        """Set the runner for the data flow job"""
        job_arguments.append('--runner={0}'.format(
            gcp_dpb_dataflow.DATAFLOW_BLOCKING_RUNNER))
        job_type = dpb_service.DATAFLOW_JOB_TYPE
    elif dpb_service.SERVICE_TYPE == 'emr':
        jarfile = aws_dpb_emr.SPARK_SAMPLE_LOCATION
        classname = 'org.apache.spark.examples.JavaWordCount'
        job_arguments = [FLAGS.dpb_wordcount_s3_input]
        job_type = dpb_service.SPARK_JOB_TYPE
    else:
        raise NotImplementedError

    # TODO(saksena): Finalize stats and end to end run time
    dpb_service.SubmitJob(jarfile,
                          classname,
                          job_arguments=job_arguments,
                          job_stdout_file=stdout_file,
                          job_type=job_type)
    results = []
    return results


def Cleanup(benchmark_spec):
    pass
