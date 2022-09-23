"""Scripts to poll http endpoint."""

import logging
import re
import subprocess
import sys
import time
import uuid
from absl import flags


FLAGS = flags.FLAGS

_ENDPOINT = flags.DEFINE_string('endpoint', None, 'Endpoint to use.')
_MAX_RETRIES = flags.DEFINE_integer('max_retries', 0, 'Number of retries.')
_RETRY_INTERVAL = flags.DEFINE_float('retry_interval', 0.5,
                                     'Gap in seconds between retries.')
_TIMEOUT = flags.DEFINE_integer('timeout', 600, 'Polling deadline in seconds.')
_EXPECTED_RESPONSE = flags.DEFINE_string('expected_response', '',
                                         'Expected response.')
_EXPECTED_RESPONSE_CODE = flags.DEFINE_string(
    'expected_response_code', r'2\d\d',
    'Expected http response code. Can be either a regexp or '
    'a integer in string format. '
    'By default, any 2xx response code is valid.')
_HEADERS = flags.DEFINE_list('headers', [], 'Headers applying to requests.')
_HTTP_RESPCODE_REGEXP = r'HTTPS?/[\d\.]*\s+(\d+)'
_CURL_METRICS = '<curl_time_total:%{time_total}>'
_CURL_METRICS_REGEXP = r'<curl_time_total:([\d\.]+)>'
# Poll succeed status
POLL_SUCCEED = 'succeed'
# Poll failure status
FAILURE_CURL = 'curl_failure'
FAILURE_INVALID_RESPONSE = 'invalid_response'
FAILURE_INVALID_RESPONSE_CODE = 'invalid_response_code'
POLL_STATUS = (POLL_SUCCEED, FAILURE_CURL,
               FAILURE_INVALID_RESPONSE, FAILURE_INVALID_RESPONSE_CODE)


def call_curl(endpoint, headers=None, expected_response_code=None,
              expected_response=None):
  """Curl endpoint once and return response.

  Args:
    endpoint: string. HTTP(S) endpoint to curl.
    headers: list. A list of strings, each representing a header.
    expected_response_code: string. Expected http response code. Can be either
      a string or a regex.
    expected_response: string. Expected http response. Can be either a string
      or a regex.

  Returns:
    A tuple of float, string, integer and string:
      Float indicates latency in second of the polling attempt.
      String indicates whether the HTTP endpoint is reachable and respond
        with expected responses or the reason of failure;
        List of current values are:
        [succeed, curl_failure, invalid_response_code, invalid_response]

      Integer indicates the actual HTTP response code;
      String indicates the actual HTTP response.
  """
  poll_stats = POLL_SUCCEED
  tmpid = str(uuid.uuid4())[:6]
  header = ''
  if headers:
    header = '-H %s ' % ' '.join(headers)

  curl_cmd = 'curl -w "{metrics}" -v {header}"{endpoint}"'.format(
      metrics=_CURL_METRICS,
      header=header,
      endpoint=endpoint)
  with open(tmpid + '.out', 'w+') as stdout:
    with open(tmpid + '.err', 'w+') as stderr:
      p = subprocess.Popen(
          curl_cmd, stdout=stdout, stderr=stderr, shell=True)
      retcode = p.wait()
  http_resp_code = None
  with open(tmpid + '.err', 'r') as stderr:
    http_stderr = stderr.read()
    for line in http_stderr.split('\n'):
      match = re.search(_HTTP_RESPCODE_REGEXP, line)
      if match:
        http_resp_code = match.group(1)
        if re.search(expected_response_code, http_resp_code) is None:
          poll_stats = FAILURE_INVALID_RESPONSE_CODE
  if retcode:
    poll_stats = FAILURE_CURL
    http_resp_code = '-1'
    logging.info(
        'Error: received non-zero return code running %s, stderr: %s',
        curl_cmd, http_stderr)
  latency = 0
  with open(tmpid + '.out', 'r') as stdout:
    http_stdout = stdout.read()
    if expected_response and (
        re.search(expected_response, http_stdout) is None):
      poll_stats = FAILURE_INVALID_RESPONSE
    else:
      latency = float(re.search(_CURL_METRICS_REGEXP, http_stdout).group(1))

    return latency or -1, poll_stats, int(http_resp_code), http_stdout


def poll(endpoint, headers=None, max_retries=0, retry_interval=0.5,
         timeout=600, expected_response_code=None, expected_response=None):
  """Poll HTTP endpoint until expected response code is received or time out.

  Args:
    endpoint: string. HTTP(S) endpoint to curl.
    headers: list. A list of strings, each representing a header.
    max_retries: int. Indicates how many retires before give up.
    retry_interval: float. Indicates interval in sec between retries.
    timeout: int. Deadline before giving up.
    expected_response_code: string. Expected http response code. Can be either
      a string or a regex.
    expected_response: string. Expected http response. Can be either a string
      or a regex.

  Returns:
    A tuple of float, float, string, string:
      First float indicates total time spent in seconds polling before
        giving up or succeed.
      Second float indicates latency for successful polls. If polling failed,
        latency is default to -1.
      String indicates if HTTP(S) endpoint is reachable and responds with
        expected response and code, or reasons of failure.
      String represents actual response.
  """
  start = time.time()
  end = start
  latency = 0
  attempt = 0
  poll_stats = None
  while end - start < timeout and attempt <= max_retries:
    attempt_latency, poll_stats, _, resp = call_curl(endpoint, headers,
                                                     expected_response_code,
                                                     expected_response)
    if poll_stats != POLL_SUCCEED:
      time.sleep(retry_interval)
      end = time.time()
      attempt += 1
    else:
      latency = attempt_latency
      break
  return time.time() - start - latency, latency or -1, poll_stats, resp


def main():
  if not _ENDPOINT.value:
    logging.info('Invalid arguments, no endpoint provided.')
    return 1
  print(
      poll(_ENDPOINT.value, _HEADERS.value, _MAX_RETRIES.value,
           _RETRY_INTERVAL.value, _TIMEOUT.value, _EXPECTED_RESPONSE_CODE.value,
           _EXPECTED_RESPONSE.value))
  return 0


if __name__ == '__main__':
  FLAGS(sys.argv)
  logging.basicConfig(
      level=logging.INFO,
      format='%(asctime)-15s %(message)s')
  sys.exit(main())
