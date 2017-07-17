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

import logging
import yaml

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import kubernetes_helper

FLAGS = flags.FLAGS


def GetStaticPipelineOptions(options_list):
  """
  Takes the dictionary loaded from the yaml configuration file and returns it
  in a form consistent with the others in GenerateAllPipelineOptions: a list of
  (pipeline_option_name, pipline_option_value) tuples.

  The options in the options_list are a dict:
    Key is the name of the pipeline option to pass to beam
    Value is the value of the pipeline option to pass to beam
  """
  options = []
  for option in options_list:
    if not len(option.keys()) == 1:
      raise Exception('Each item in static_pipeline_options should only have'
                      ' 1 key/value')
    option_kv = option.items()[0]
    options.append((option_kv[0], option_kv[1]))
  return options


def EvaluateDynamicPipelineOptions(dynamic_options):
  """
  Takes the user's dynamic args and retrieves the information to fill them in.

  dynamic_args is a python map of argument name -> {type, kubernetesSelector}
  returns a list of tuples containing (argName, argValue)
  """
  filledOptions = []
  for optionDescriptor in dynamic_options:
    fillType = optionDescriptor['type']
    optionName = optionDescriptor['name']

    if not fillType:
      raise errors.Config.InvalidValue(
          'For dynamic arguments, you must provide a "type"')

    if fillType == 'NodePortIp':
      argValue = RetrieveNodePortIp(optionDescriptor)
    elif fillType == 'LoadBalancerIp':
      argValue = RetrieveLoadBalancerIp(optionDescriptor)
    elif fillType == 'TestValue':
      argValue = optionDescriptor['value']
    else:
      raise errors.Config.InvalidValue(
          'Unknown dynamic argument type: %s' % (fillType))

    filledOptions.append((optionName, argValue))

  return filledOptions


def GenerateAllPipelineOptions(it_args, it_options, static_pipeline_options,
                               dynamic_pipeline_options):
  """
  :param it_args: options list passed in via FLAGS.beam_it_args
  :param it_options: options list passed in via FLAGS.beam_it_options
  :param static_pipeline_options: options list loaded from the yaml config file
  :param dynamic_pipeline_options: options list loaded from the yaml config file
  :return: a list of values of the form "\"--option_name=value\""
  """
  # beam_it_options are in [--option=value,--option2=val2] form
  user_option_list = []
  if it_options is not None and len(it_options) > 0:
    user_option_list = it_options.rstrip(']').lstrip('[').split(',')
    user_option_list = [option.rstrip('" ').lstrip('" ')
                        for option in user_option_list]


  # Add static options from the benchmark_spec
  benchmark_spec_option_list = (
      EvaluateDynamicPipelineOptions(dynamic_pipeline_options))
  benchmark_spec_option_list.extend(
      GetStaticPipelineOptions(static_pipeline_options))
  option_list = ['--{}={}'.format(t[0], t[1])
                 for t in benchmark_spec_option_list]

  # beam_it_args is the old way of passing parameters
  args_list = []
  if it_args is not None and len(it_args) > 0:
    args_list = it_args.split(',')

  return ['"{}"'.format(arg)
          for arg in args_list + user_option_list + option_list]


def ReadPipelineOptionConfigFile():
  """
  Reads the path to the config file from FLAGS, then loads the static and
  dynamic pipeline options from it.
  """
  dynamic_pipeline_options = []
  static_pipeline_options = []
  if FLAGS.beam_options_config_file:
    with open(FLAGS.beam_options_config_file, 'r') as fileStream:
      config = yaml.load(fileStream)
      if config['static_pipeline_options']:
        static_pipeline_options = config['static_pipeline_options']
      if config['dynamic_pipeline_options']:
        dynamic_pipeline_options = config['dynamic_pipeline_options']
  return static_pipeline_options, dynamic_pipeline_options


def RetrieveNodePortIp(argDescriptor):
  jsonSelector = argDescriptor['podLabel']
  if not jsonSelector:
    raise errors.Config.InvalidValue('For NodePortIp arguments, you must'
                                     ' provide a "selector"')
  ip = kubernetes_helper.GetWithWaitForContents(
      'pods', '', jsonSelector, '.items[0].status.podIP')
  if len(ip) == 0:
    raise "Could not retrieve NodePort IP address"
  logging.info("Using NodePort IP Address: " + ip)
  return ip


def RetrieveLoadBalancerIp(argDescriptor):
  serviceName = argDescriptor['serviceName']
  if not serviceName:
    raise errors.Config.InvalidValue('For LoadBalancerIp arguments, you must'
                                     'provide a "serviceName"')
  ip = kubernetes_helper.GetWithWaitForContents(
      'svc', serviceName, '', '.status.loadBalancer.ingress[0].ip')
  if len(ip) == 0:
    raise "Could not retrieve LoadBalancer IP address"
  logging.info("Using LoadBalancer IP Address: " + ip)
  return ip
