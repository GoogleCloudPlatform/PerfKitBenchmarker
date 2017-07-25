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

"""Generic benchmark running Apache Beam Integration Tests as benchmarks.

This benchmark provides the piping necessary to run Apache Beam Integration
Tests as benchmarks. It provides the minimum additional configuration necessary
to get the benchmark going.
"""

import copy
import datetime
import tempfile

from perfkitbenchmarker import beam_benchmark_helper
from perfkitbenchmarker import beam_pipeline_options
from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import sample
from perfkitbenchmarker.dpb_service import BaseDpbService

BENCHMARK_NAME = 'beam_integration_benchmark'

BENCHMARK_CONFIG = """
beam_integration_benchmark:
  description: Run word count on dataflow and dataproc
  dpb_service:
    service_type: dataflow
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-1
          boot_disk_size: 500
        AWS:
          machine_type: m3.medium
      disk_spec:
        GCP:
          disk_type: nodisk
        AWS:
          disk_size: 500
          disk_type: gp2
    worker_count: 2
"""

DEFAULT_JAVA_IT_CLASS = 'org.apache.beam.examples.WordCountIT'
DEFAULT_PYTHON_IT_MODULE = 'apache_beam.examples.wordcount_it_test'

flags.DEFINE_string('beam_it_class', None, 'Path to IT class')
flags.DEFINE_string('beam_it_args', None, 'Args to provide to the IT.'
                    ' Deprecated & replaced by beam_it_options')
flags.DEFINE_string('beam_it_options', None, 'Pipeline Options sent to the'
                    ' integration test.')
flags.DEFINE_string('beam_kubernetes_scripts', None, 'A local path to the'
                    ' Kubernetes scripts to run which will instantiate a'
                    ' datastore.')
flags.DEFINE_string('beam_options_config_file', None, 'A local path to the'
                    ' yaml file defining static and dynamic pipeline options to'
                    ' use for this benchmark run.')

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config_spec):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.errors.Config.InvalidValue: If no Beam args are provided.
    NotImplementedError: If an invalid runner is specified.
  """
  if FLAGS.beam_it_options is None and FLAGS.beam_it_args is None:
    raise errors.Config.InvalidValue(
        'No options provided. To run with default class (WordCountIT), must'
        ' provide --beam_it_options=--tempRoot=<temp dir,'
        ' e.g. gs://my-dir/temp>.')
  if FLAGS.beam_sdk is None:
    raise errors.Config.InvalidValue(
        'No sdk provided. To run Beam integration benchmark, the test must'
        'specify which sdk is used in the pipeline. For example, java/python.')
  if benchmark_config_spec.dpb_service.service_type != dpb_service.DATAFLOW:
    raise NotImplementedError('Currently only works against Dataflow.')
  if (FLAGS.beam_it_options and
      (not FLAGS.beam_it_options.endswith(']') or
       not FLAGS.beam_it_options.startswith('['))):
    raise Exception("beam_it_options must be of form"
                    " [\"--option=value\",\"--option2=val2\"]")


def Prepare(benchmark_spec):
  beam_benchmark_helper.InitializeBeamRepo(benchmark_spec)
  benchmark_spec.always_call_cleanup = True
  kubernetes_helper.CreateAllFiles(getKubernetesScripts())
  pass


def getKubernetesScripts():
  if FLAGS.beam_kubernetes_scripts:
    scripts = FLAGS.beam_kubernetes_scripts.split(',')
    return scripts
  else:
    return []


def Run(benchmark_spec):
  # Get handle to the dpb service
  dpb_service_instance = benchmark_spec.dpb_service

  # Create a file handle to contain the response from running the job on
  # the dpb service
  stdout_file = tempfile.NamedTemporaryFile(suffix='.stdout',
                                            prefix='beam_integration_benchmark',
                                            delete=False)
  stdout_file.close()

  if FLAGS.beam_it_class is None:
    if FLAGS.beam_sdk == beam_benchmark_helper.BEAM_JAVA_SDK:
      classname = DEFAULT_JAVA_IT_CLASS
    elif FLAGS.beam_sdk == beam_benchmark_helper.BEAM_PYTHON_SDK:
      classname = DEFAULT_PYTHON_IT_MODULE
    else:
      raise NotImplementedError('Unsupported Beam SDK: %s.' % FLAGS.beam_sdk)
  else:
    classname = FLAGS.beam_it_class

  static_pipeline_options, dynamic_pipeline_options = \
      beam_pipeline_options.ReadPipelineOptionConfigFile()

  job_arguments = beam_pipeline_options.GenerateAllPipelineOptions(
      FLAGS.beam_it_args, FLAGS.beam_it_options,
      static_pipeline_options,
      dynamic_pipeline_options)

  job_type = BaseDpbService.BEAM_JOB_TYPE

  results = []
  metadata = copy.copy(dpb_service_instance.GetMetadata())

  start = datetime.datetime.now()
  dpb_service_instance.SubmitJob('', classname,
                                 job_arguments=job_arguments,
                                 job_stdout_file=stdout_file,
                                 job_type=job_type)
  end_time = datetime.datetime.now()
  run_time = (end_time - start).total_seconds()
  results.append(sample.Sample('run_time', run_time, 'seconds', metadata))
  return results


def Cleanup(benchmark_spec):
  kubernetes_helper.DeleteAllFiles(getKubernetesScripts())
  pass
