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
"""Module containing pgbench installation, cleanup and run functions."""

import time
from perfkitbenchmarker import publisher
from perfkitbenchmarker import sample

APT_PACKAGES = (
    'postgresql-client-common',
    'postgresql-client',
    'postgresql-contrib',
)


def AptInstall(vm):
  """Installs pgbench on the Debian VM."""
  for package in APT_PACKAGES:
    vm.InstallPackages(package)


def YumInstall(vm):
  """Raises exception when trying to install on yum-based VMs."""
  raise NotImplementedError(
      'PKB currently only supports the installation of pgbench on '
      'Debian-based VMs')


def AptUninstall(vm):
  """Removes pgbench from the Debian VM."""
  remove_str = 'sudo apt-get --purge autoremove -y '
  for package in APT_PACKAGES:
    vm.RemoteCommand(remove_str + package)


def MakeSamplesFromOutput(pgbench_stderr, num_clients, num_jobs,
                          additional_metadata):
  """Creates sample objects from the given pgbench output and metadata.

  Two samples will be returned, one containing a latency list and
  the other a tps (transactions per second) list. Each will contain
  N floating point samples, where N = FLAGS.pgbench_seconds_per_test.

  Args:
    pgbench_stderr: stderr from the pgbench run command
    num_clients: number of pgbench clients used
    num_jobs: number of pgbench jobs (threads) used
    additional_metadata: additional metadata to add to each sample

  Returns:
    A list containing a latency sample and a tps sample. Each sample
    consists of a list of floats, sorted by time that were collected
    by running pgbench with the given client and job counts.
  """
  lines = pgbench_stderr.splitlines()[2:]
  tps_numbers = [float(line.split(' ')[3]) for line in lines]
  latency_numbers = [float(line.split(' ')[6]) for line in lines]

  metadata = additional_metadata.copy()
  metadata.update({'clients': num_clients, 'jobs': num_jobs})
  tps_metadata = metadata.copy()
  tps_metadata.update({'tps': tps_numbers})
  latency_metadata = metadata.copy()
  latency_metadata.update({'latency': latency_numbers})

  tps_sample = sample.Sample('tps_array', -1, 'tps', tps_metadata)
  latency_sample = sample.Sample('latency_array', -1, 'ms', latency_metadata)
  return [tps_sample, latency_sample]


def RunPgBench(benchmark_spec,
               relational_db,
               vm,
               test_db_name,
               client_counts,
               job_counts,
               seconds_to_pause,
               seconds_per_test,
               metadata,
               file=None,
               path=None):
  """Run Pgbench on the client VM.

  Args:
    benchmark_spec: Benchmark spec of the run
    relational_db: Relational database object
    vm: Client VM
    test_db_name: The name of the database
    client_counts: Number of client
    job_counts: Number of job
    seconds_to_pause: Seconds to pause between test
    seconds_per_test: Seconds per test
    metadata: Metadata of the benchmark
    file: Filename of the benchmark
    path: File path of the benchmar.
  """
  connection_string = relational_db.client_vm_query_tools.GetConnectionString(
      database_name=test_db_name)

  if file and path:
    metadata['pgbench_file'] = file

  samples = []
  if job_counts and len(client_counts) != len(job_counts):
    raise ValueError('Length of clients and jobs must be the same.')
  for i in range(len(client_counts)):
    time.sleep(seconds_to_pause)
    client = client_counts[i]
    if job_counts:
      jobs = job_counts[i]
    else:
      jobs = min(client, 16)
    command = (f'pgbench {connection_string} --client={client} '
               f'--jobs={jobs} --time={seconds_per_test} --progress=1 '
               '--report-latencies')
    if file and path:
      command = f'cd {path} && {command} --file={file}'
    _, stderr = vm.RobustRemoteCommand(command)
    samples = MakeSamplesFromOutput(stderr, client, jobs, metadata)
    publisher.PublishRunStageSamples(benchmark_spec, samples)
