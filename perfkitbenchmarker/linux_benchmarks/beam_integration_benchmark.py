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
from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
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
flags.DEFINE_string('beam_it_args', None, 'Args to provide to the IT')

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_spec):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.errors.Config.InvalidValue: If no Beam args are provided.
    NotImplementedError: If an invalid runner is specified.
  """
  if FLAGS.beam_it_args is None:
    raise errors.Config.InvalidValue(
        'No args provided. To run with default class (WordCountIT), must'
        'provide --beam_it_args=--tempRoot=<temp dir, e.g. gs://my-dir/temp>.')
  if FLAGS.beam_sdk is None:
    raise errors.Config.InvalidValue(
        'No sdk provided. To run Beam integration benchmark, the test must'
        'specify which sdk is used in the pipeline. For example, java/python.'
    )
  if benchmark_spec.dpb_service.service_type != dpb_service.DATAFLOW:
    raise NotImplementedError('Currently only works against Dataflow.')


def Prepare(benchmark_spec):
  beam_benchmark_helper.InitializeBeamRepo(benchmark_spec)
  pass


def Run(benchmark_spec):
  # Get handle to the dpb service
  dpb_service_instance = benchmark_spec.dpb_service

  # Create a file handle to contain the response from running the job on
  # the dpb service
  stdout_file = tempfile.NamedTemporaryFile(suffix='.stdout',
                                            prefix='beam_integration_benchmark',
                                            delete=False)
  stdout_file.close()

  # Switch the parameters for submit job function of specific dpb service
  job_arguments = ['"{}"'.format(arg) for arg in FLAGS.beam_it_args.split(',')]

  if FLAGS.beam_it_class is None:
    if FLAGS.beam_sdk == beam_benchmark_helper.BEAM_JAVA_SDK:
      classname = DEFAULT_JAVA_IT_CLASS
    elif FLAGS.beam_sdk == beam_benchmark_helper.BEAM_PYTHON_SDK:
      classname = DEFAULT_PYTHON_IT_MODULE
    else:
      raise NotImplementedError('Unsupported Beam SDK: %s.' % FLAGS.beam_sdk)
  else:
    classname = FLAGS.beam_it_class

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
  pass
