# Copyright 2015 Google Inc. All rights reserved.
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
import os
from StringIO import StringIO

from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

DIGITALOCEAN_API = 'https://api.digitalocean.com/v2/'

flags.DEFINE_string('digitalocean_curl_config',
                    os.getenv('DIGITALOCEAN_CURL_CONFIG',
                              '~/.config/digitalocean-oauth.curl'),
                    ('Path to Curl config file containing oauth header, '
                     'also settable via $DIGITALOCEAN_CURL_CONFIG env var.\n'
                     '\n'
                     'File format:\n'
                     '  header = "Authorization: Bearer 9876...ba98"'))

flags.DEFINE_string('curl_path',
                    'curl',
                    'The path for the curl utility.')

flags.DEFINE_list('additional_curl_flags',
                  [],
                  'Additional flags to pass to curl such as proxy settings.')

FLAGS = flags.FLAGS


def GetDefaultDigitaloceanCurlFlags():
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
  cmd = GetCurlCommand(action, url, args)

  cmd.extend(GetDefaultDigitaloceanCurlFlags())

  stdout, _, _, = vm_util.IssueCommand(cmd, suppress_warning=suppress_warning)

  # Use last two \r\n separated blocks, there may be
  # extra blocks for HTTP "100 Continue" responses.
  meta, body = stdout.split('\r\n\r\n')[-2:]

  response, unused_header = meta.split('\r\n', 1)
  code = int(response.split()[1])
  status = 0  # Success.
  if code >= 400:
    status = 22  # Match Curl exit code "HTTP page not retrieved".

  return body, status
