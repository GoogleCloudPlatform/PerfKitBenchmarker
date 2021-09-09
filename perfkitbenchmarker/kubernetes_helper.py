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

import tempfile
import time

from absl import flags
import jinja2
from perfkitbenchmarker import data
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS
flags.DEFINE_integer('k8s_get_retry_count', 18,
                     'Maximum number of waits for getting LoadBalancer external IP')
flags.DEFINE_integer('k8s_get_wait_interval', 10,
                     'Wait interval for getting LoadBalancer external IP')


def checkKubernetesFlags():
  if not FLAGS.kubectl:
    raise Exception('Please provide path to kubectl tool using --kubectl '
                    'flag. Exiting.')
  if not FLAGS.kubeconfig:
    raise Exception('Please provide path to kubeconfig using --kubeconfig '
                    'flag. Exiting.')


def CreateFromFile(file_name):
  checkKubernetesFlags()
  create_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig, 'create',
                '-f', file_name]
  vm_util.IssueRetryableCommand(create_cmd)


def DeleteFromFile(file_name):
  checkKubernetesFlags()
  delete_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig, 'delete',
                '-f', file_name, '--ignore-not-found']
  vm_util.IssueRetryableCommand(delete_cmd)


def DeleteAllFiles(file_list):
  for file in file_list:
    DeleteFromFile(file)


def CreateAllFiles(file_list):
  for file in file_list:
    CreateFromFile(file)


def Get(resource, resourceInstanceName, labelFilter, jsonSelector):
  checkKubernetesFlags()
  get_pod_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
                 'get', resource]
  if len(resourceInstanceName) > 0:
    get_pod_cmd.append(resourceInstanceName)
  if len(labelFilter) > 0:
    get_pod_cmd.append('-l ' + labelFilter)
  get_pod_cmd.append('-ojsonpath={{{}}}'.format(jsonSelector))
  stdout, stderr, _ = vm_util.IssueCommand(get_pod_cmd, suppress_warning=True,
                                           raise_on_failure=False)
  if len(stderr) > 0:
    raise Exception("Error received from kubectl get: " + stderr)
  return stdout


def GetWithWaitForContents(resource, resourceInstanceName, filter, jsonFilter):
  ret = Get(resource, resourceInstanceName, filter, jsonFilter)
  numWaitsLeft = FLAGS.k8s_get_retry_count
  while len(ret) == 0 and numWaitsLeft > 0:
    time.sleep(FLAGS.k8s_get_wait_interval)
    ret = Get(resource, resourceInstanceName, filter, jsonFilter)
    numWaitsLeft -= 1
  return ret


def CreateResource(resource_body):
  with vm_util.NamedTemporaryFile(mode='w') as tf:
    tf.write(resource_body)
    tf.close()
    CreateFromFile(tf.name)


def DeleteResource(resource_body):
  with vm_util.NamedTemporaryFile() as tf:
    tf.write(resource_body)
    tf.close()
    DeleteFromFile(tf.name)


def CreateRenderedManifestFile(filename, config):
  """Returns a file containing a rendered Jinja manifest (.j2) template."""
  manifest_filename = data.ResourcePath(filename)
  environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
  with open(manifest_filename) as manifest_file:
    manifest_template = environment.from_string(manifest_file.read())
  rendered_yaml = tempfile.NamedTemporaryFile(mode='w')
  rendered_yaml.write(manifest_template.render(config))
  rendered_yaml.flush()
  return rendered_yaml
