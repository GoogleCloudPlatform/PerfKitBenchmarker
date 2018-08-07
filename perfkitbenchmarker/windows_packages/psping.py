# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing psping installation and cleanup functions.

psping is a tool made for benchmarking Windows networking.

"""

import json
import ntpath

from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

TEST_PORT = 5000
PSPING_OUTPUT_FILE = 'psping_out'

PSPING_DIR = 'PSTools'
PSPING_ZIP = PSPING_DIR + '.zip'
PSPING_URL = 'https://download.sysinternals.com/files/' + PSPING_ZIP

flags.DEFINE_integer('psping_packet_size', 1,
                     'The size of the packet to test the ping with.')

flags.DEFINE_integer('psping_bucket_count', 100,
                     'For the results histogram, number of columns')

flags.DEFINE_integer('psping_rr_count', 1000,
                     'The number of pings to attempt')

flags.DEFINE_integer('psping_timeout', 10,
                     'The time to allow psping to run')


def Install(vm):
  """Installs the psping package on the VM."""
  zip_path = ntpath.join(vm.temp_dir, PSPING_ZIP)
  vm.DownloadFile(PSPING_URL, zip_path)
  vm.UnzipFile(zip_path, vm.temp_dir)


def StartPspingServer(vm):
  server_command = (
      'Start-Job -ScriptBlock {{'
      '{psping_exec_dir}\\psping.exe /accepteula -s 0.0.0.0:{port};'
      '}}').format(
          psping_exec_dir=vm.temp_dir,
          port=TEST_PORT)
  vm.RemoteCommand(server_command)


def _RunPsping(vm, command):
  try:
    vm.RemoteCommand(command, timeout=FLAGS.psping_timeout)
  except errors.VirtualMachine.RemoteCommandError:
    # We expect psping to return an error here because the psping server has
    # to be killed with a CTRL+C.
    pass


@vm_util.Retry(max_retries=3)
def RunLatencyTest(sending_vm, receiving_vm, use_internal_ip=True):
  """Run the psping latency test.

  Uses a TCP request-response time to measure latency.

  Args:
    sending_vm: the vm to send the tcp request.
    receiving_vm: the vm acting as the server.
    use_internal_ip: whether or not to use the private IP or the public IP.

  Returns:
    list of samples representing latency between the two VMs.
  """
  server_ip = (receiving_vm.internal_ip if use_internal_ip
               else receiving_vm.ip_address)

  client_command = (
      'cd {psping_exec_dir}; '
      'sleep 2;'  # sleep to make sure the server starts first.
      '.\\psping.exe /accepteula -l {packet_size} -i 0 -q '
      '-n {rr_count} -h {bucket_count} {ip}:{port}'
      ' > {out_file}').format(
          psping_exec_dir=sending_vm.temp_dir,
          packet_size=FLAGS.psping_packet_size,
          rr_count=FLAGS.psping_rr_count,
          bucket_count=FLAGS.psping_bucket_count,
          ip=server_ip,
          port=TEST_PORT,
          out_file=PSPING_OUTPUT_FILE)

  # PSPing does not have a configurable timeout. To get around this, start the
  # server as a background job, then kill it after 10 seconds
  server_command = (
      '{psping_exec_dir}\\psping.exe /accepteula -s 0.0.0.0:{port};').format(
          psping_exec_dir=sending_vm.temp_dir,
          port=TEST_PORT)

  process_args = [(_RunPsping, (receiving_vm, server_command), {}),
                  (_RunPsping, (sending_vm, client_command), {})]

  background_tasks.RunParallelProcesses(process_args, 200, 1)

  cat_command = 'cd {psping_exec_dir}; cat {out_file}'.format(
      psping_exec_dir=sending_vm.temp_dir,
      out_file=PSPING_OUTPUT_FILE)

  output, _ = sending_vm.RemoteCommand(cat_command)
  return ParsePspingResults(output, sending_vm, receiving_vm, use_internal_ip)

# example output
# PsPing v2.10 - PsPing - ping, latency, bandwidth measurement utility
# Copyright (C) 2012-2016 Mark Russinovich
# Sysinternals - www.sysinternals.com
#
# TCP latency test connecting to 10.138.0.2:47001: Connected
# 15 iterations (warmup 5) sending 8192 bytes TCP latency test:   0%
# Connected
# 15 iterations (warmup 5) sending 8192 bytes TCP latency test: 100%
#
# TCP roundtrip latency statistics (post warmup):
#   Sent = 10, Size = 8192, Total Bytes: 81920,
#   Minimum = 0.19ms, Maxiumum = 0.58ms, Average = 0.27ms
#
# Latency\tCount
# 0.30\t688
# 0.51\t292
# 0.71\t15
# 0.92\t2
# 1.13\t0
# 1.33\t2
# 1.54\t0
# 1.75\t0
# 1.95\t0
# 2.16\t1


def ParsePspingResults(results, client_vm, server_vm, internal_ip_used):
  """Turn psping output into a list of samples.

  Args:
    results: string of the psping output
    client_vm: the VM performing the latency test
    server_vm: the VM serving the latency test
    internal_ip_used: whether or not the private IP was used.

  Returns:
    list of samples reflecting the psping results
  """

  output_list = [val.rstrip('\r') for val in results.split('\n')]

  # There should be exactly one line like this.
  data_line = [line for line in output_list if 'Minimum' in line][0]

  # split the line up by spaces
  data_line = [val for val in data_line.split(' ') if val]

  minimum = float(data_line[2].rstrip('ms,'))
  maximum = float(data_line[5].rstrip('ms,'))
  average = float(data_line[8].rstrip('ms,'))

  metadata = {
      'internal_ip_used': internal_ip_used,
      'sending_zone': client_vm.zone,
      'sending_machine_type': client_vm.machine_type,
      'receiving_zone': server_vm.zone,
      'receiving_machine_type': server_vm.machine_type,
  }

  samples = [
      sample.Sample('latency', average, 'ms', metadata),
      sample.Sample('latency:maximum', maximum, 'ms', metadata),
      sample.Sample('latency:minimum', minimum, 'ms', metadata),
  ]

  histogram = []
  index = 1

  for line in output_list:
    line_data = [val for val in line.split(' ') if val]

    # the line should look like ['entry\tvalue']
    if len(line_data) is not 1:
      continue

    entry_data = line_data[0].split('\t')

    if len(entry_data) is not 2:
      continue

    if 'Latency' in entry_data:
      continue

    # This is a histogram data line
    latency = float(entry_data[0])
    count = int(entry_data[1])

    histogram.append({'latency': latency,
                      'count': count,
                      'bucket_number': index})

    index += 1

  metadata.update({'histogram': json.dumps(histogram)})

  samples.append(sample.Sample('latency:histogram', 0, 'ms', metadata))

  return samples
