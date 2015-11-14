# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
"""Utilities for working with DigitalOcean resources."""

import json
import logging
import os

from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

DIGITALOCEAN_API = 'https://api.digitalocean.com/v2/'

FLAGS = flags.FLAGS


def GetDefaultDigitalOceanCurlFlags():
  """Return common set of curl options for Digital Ocean.

  Returns:
    A common set of curl options.
  """
  options = [
      '--config', os.path.expanduser(FLAGS.digitalocean_curl_config),
  ]
  options.extend(FLAGS.additional_curl_flags)

  return options


def GetCurlCommand(action, url, args=None):
  """Returns a curl command line.

  Args:
    action: HTTP request type such as 'POST'
    url: URL location such as 'account/keys'
    args: key/value dict, used for JSON input
  Returns:
    Command as argv list.
  """

  cmd = [
      FLAGS.curl_path,
      '--request', action,
      '--url', DIGITALOCEAN_API + url,
      '--include',
      '--silent',
  ]

  if action == 'GET':
    # Disable pagination
    cmd.extend(['--form', 'per_page=999999999'])

  if args is not None:
    cmd.extend([
        '--header', 'Content-Type: application/json',
        '--data', json.dumps(args, separators=(',', ':'))])

  return cmd


def RunCurlCommand(action, url, args=None, suppress_warning=False):
  """Runs a curl command and processes the result.

  Args:
    action: HTTP request type such as 'POST'
    url: URL location such as 'account/keys'
    args: key/value dict, used for JSON input
    suppress_warning: if true, failures aren't interesting
  Returns:
    (stdout, ret) tuple, where stdout is a JSON string and
    ret is zero for success, the command exit code (1-255),
    or the HTTP status code (300 and up).
  """

  cmd = GetCurlCommand(action, url, args)

  cmd.extend(GetDefaultDigitalOceanCurlFlags())

  stdout, _, curl_ret, = vm_util.IssueCommand(
      cmd, suppress_warning=suppress_warning)
  if curl_ret:
    # Executing command failed - IssueCommand will have logged details.
    return stdout, curl_ret

  # Use last two \r\n separated blocks, there may be
  # extra blocks for HTTP "100 Continue" responses.
  meta, body = stdout.split('\r\n\r\n')[-2:]

  response, unused_header = meta.split('\r\n', 1)
  code = int(response.split()[1])
  status = 0  # Success.
  if code >= 300:
    # For non-success responses, use the HTTP code as return value.
    status = code

  if status and not suppress_warning:
    logging.info('Got HTTP status code (%s).\nRESPONSE: %s\n',
                 code, body)

  return body, status
