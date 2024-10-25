# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
"""Runs cpuid as a benchmark."""

import configparser
from typing import List, NamedTuple, OrderedDict

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample


BENCHMARK_NAME = 'cpuid_tool'
BENCHMARK_CONFIG = """
cpuid_tool:
  description: Runs cpuid as a benchmark
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n2-standard-2
          zone: us-central1-c
        Azure:
          machine_type: Standard_D2s_v5
          zone: eastus2-2
        AWS:
          machine_type: m5.large
          zone: us-east-1c
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(_):
  pass


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs the benchmark."""
  # TODO(arushigaur): Get bugs from /proc/cpuinfo and add them as metadata.
  vm = bm_spec.vm_groups['default'][0]
  vm.InstallPackages('cpuid')
  version, _ = vm.RemoteCommand('cpuid -v')
  stdout, _ = vm.RemoteCommand('taskset -c 0 cpuid -1')
  ascii_parsed_output = ParseCpuIdASCIIOutput(stdout)
  stdout, _ = vm.RemoteCommand('taskset -c 0 cpuid -r -1')
  hexadecimal_parsed_output = ParseCpuIdHexadecimalOutput(stdout)
  samples = []
  for section in ascii_parsed_output.sections():
    for key in dict(ascii_parsed_output[section]):
      updated_key = section + '-' + key
      samples.append(
          sample.Sample(
              metric=updated_key,
              value=-1,
              unit='',
              metadata={
                  'version': version,
                  'value': ascii_parsed_output[section][key],
              },
          )
      )
  for key, value in hexadecimal_parsed_output.items():
    samples.append(
        sample.Sample(
            metric=key,
            value=-1,
            unit='',
            metadata={
                'version': version,
                'value': value,
            },
        )
    )
  return samples


def Cleanup(_):
  pass


def ParseCpuIdHexadecimalOutput(output: str):
  """Parses the output of cpuid_tool key value pairs.

   Example line:
   0x00000000 0x00: eax=0x0000000d ebx=0x68747541 ecx=0x444d4163 edx=0x69746e65
   key = 0x00000000_0x00_eax
   value = 0x0000000d
   key = 0x00000000_0x00_ebx
   value = 0x68747541 (and so on)

  Args:
   output: hexadecimal output of cpuid_tool.

  Returns:
   A dictionary of key value pairs.
  """
  lines = output.splitlines()
  data = {}
  for line in lines:
    key_values_data = line.split(': ', 1)
    if len(key_values_data) != 2:
      continue
    key, values = key_values_data
    key = key.strip().replace(' ', '_')
    for register_value in values.split(' '):
      subkey, value = register_value.split('=', 1)
      complete_key = '_'.join([key, subkey])
      if complete_key in data:
        # Key is unique, adding this exception if key is not unique in future.
        raise ValueError('Duplicate key found: %s' % complete_key)
      data[complete_key] = value
  return data


def IsHeading(line: str):
  """Checks if the line is a heading.

  heading can be the following formats:
   1. CPU:
   2. version information (1/eax):
   3. --- cache 0 ---

  Args:
   line: line to check.

  Returns:
   True if the line is a heading.
  """
  if line.endswith(':') or ('---' in line):
    return True
  return False


def CountSpaces(line: str):
  return len(line) - len(line.lstrip())


def FormatKeyValueLine(line: str) -> str:
  """Formats the key value line to be compatible with configparser.

   Handling the following formats:
   1. VMPL: VM permission levels  = false
   2. 0x63: data TLB: 2M/4M pages, 4-way, 32 entries
   3. (multi-processing synth) = hyper-threaded (t=2)

  Args:
      line: line containing key value pair.

  Returns:
      Config parser compatible key value pair.
  """
  if ':' in line:
    if '=' not in line:
      line = line.replace(':', '=', 1)
    line = line.replace(':', '')
  line = line.replace('(', '').replace(')', '')
  key, value = line.split('=', 1)
  return key.strip().replace(' ', '_') + ' = ' + value.strip()


def FormatHeading(line: str) -> str:
  """Format the heading to be compatible with configparser.

  Args:
    line: line containing heading.

  Example headings:
    1. CPU:
    2. version information (1/eax):
    3. --- cache 0 ---

  Returns:
    Formatted heading
  """
  line = line.replace(':', '').replace('---', '')
  words = line.split(' ')
  words = [w.strip() for w in words if w.strip()]
  # removing the last section of the heading if it is inside parenthesis.
  # For example, in the heading "version information (1/eax)", "(1/eax)" is
  # removed and we get "version information".
  if len(words) > 1 and '(' in words[-1]:
    words = words[:-1]
  # joining the words with underscore: version_information
  line = '_'.join(words)
  line = line.replace('(', '').replace(')', '')
  return line


def BracketizeHeading(line: str) -> str:
  """Bracketize the heading to be compatible with configparser."""
  return ''.join(['[', line, ']'])


class MultiOrderedDict(OrderedDict):

  def __setitem__(self, key, value):
    if isinstance(value, list) and key in self:
      self[key].extend(value)
    else:
      super().__setitem__(key, value)


def ParseCpuIdASCIIOutput(output: str):
  """Parses the output of cpuid_tool into json."""
  formatted_lines = []
  # stack of headings to maintain the hierarchy based on the spaces
  # before the heading
  heading_stack = []
  HEADING_DETAILS = NamedTuple(
      'heading_details',
      [('heading', str), ('spaces', int)],
  )
  for line in output.splitlines():
    line = line.lower()
    if IsHeading(line):
      spaces = CountSpaces(line)
      # popping the headings till we reach the parent heading
      # (heading with lesser spaces in front than the current heading)
      while heading_stack and heading_stack[-1].spaces >= spaces:
        heading_stack.pop()
      formatted_current_heading = FormatHeading(line)
      if not heading_stack:
        complete_heading = formatted_current_heading.strip()
      else:
        complete_heading = '-'.join([
            heading_stack[-1].heading,
            formatted_current_heading.strip(),
        ])
      heading_stack.append(
          HEADING_DETAILS(
              complete_heading,
              spaces,
          )
      )
      # config parser supports heading in square parenthesis. For eg: [cpu]
      formatted_lines.append(BracketizeHeading(complete_heading))
    else:
      line = FormatKeyValueLine(line) if len(line.strip()) else line
      formatted_lines.append(line)
  data = '\n'.join(formatted_lines)
  parser = configparser.RawConfigParser(
      dict_type=MultiOrderedDict, strict=False
  )
  parser.read_string(data)
  return parser
