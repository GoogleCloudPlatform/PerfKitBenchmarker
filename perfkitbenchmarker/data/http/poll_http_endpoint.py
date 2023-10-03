"""Scripts to poll http endpoint."""

import re
import subprocess
import sys
import time
import uuid
from absl import flags


FLAGS = flags.FLAGS

_ENDPOINT = flags.DEFINE_string('endpoint', None, 'Endpoint to use.')
_MAX_RETRIES = flags.DEFINE_integer('max_retries', 0, 'Number of retries.')
_RETRY_INTERVAL = flags.DEFINE_float(
    'retry_interval', 0.5, 'Gap in seconds between retries.'
)
_TIMEOUT = flags.DEFINE_integer('timeout', 600, 'Polling deadline in seconds.')
_EXPECTED_RESPONSE = flags.DEFINE_string(
    'expected_response', '', 'Expected response.'
)
_EXPECTED_RESPONSE_CODE = flags.DEFINE_string(
    'expected_response_code',
    r'2\d\d',
    'Expected http response code. Can be either a regexp or '
    'a integer in string format. '
    'By default, any 2xx response code is valid.',
)
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
POLL_STATUS = (
    POLL_SUCCEED,
    FAILURE_CURL,
    FAILURE_INVALID_RESPONSE,
    FAILURE_INVALID_RESPONSE_CODE,
)


def _generate_file_names():
  """Returns a file name as tuple (output, error).. mostly for mocking."""
  name_base = str(uuid.uuid4())[:6]
  return (name_base + '.out', name_base + '.err')


def _get_time() -> float:
  """Returns current time, mostly for mocking."""
  return time.time()


def _popen(curl_cmd, stdout, stderr, shell):
  """Wraps subprocess.Popen for mocking."""
  return subprocess.Popen(curl_cmd, stdout=stdout, stderr=stderr, shell=shell)


def call_curl(
    endpoint, headers=None, expected_response_code=None, expected_response=None
):
  """Curl endpoint once and return response.

  Args:
    endpoint: string. HTTP(S) endpoint to curl.
    headers: list. A list of strings, each representing a header.
    expected_response_code: string. Expected http response code. Can be either a
      string or a regex.
    expected_response: string. Expected http response. Can be either a string or
      a regex.

  Returns:
    A tuple of float, string, string, and string:
      Float indicates latency in second of the polling attempt.
      First string indicates whether the HTTP endpoint is reachable and respond
        with expected responses or the reason of failure;
        List of current values are:
        [succeed, curl_failure, invalid_response_code, invalid_response]

      Second string is the actual HTTP response.
      Third string is additional logged info.
  """
  poll_stats = POLL_SUCCEED
  outid, errid = _generate_file_names()
  header = ''
  logs = ''
  if headers:
    header = '-H %s ' % ' '.join(headers)

  curl_cmd = 'curl -w "{metrics}" -v {header}"{endpoint}"'.format(
      metrics=_CURL_METRICS, header=header, endpoint=endpoint
  )
  with open(outid, 'w+') as stdout:
    with open(errid, 'w+') as stderr:
      p = _popen(curl_cmd, stdout=stdout, stderr=stderr, shell=True)
      retcode = p.wait()
  http_resp_code = None
  with open(errid, 'r') as stderr:
    http_stderr = stderr.read()
    for line in http_stderr.split('\n'):
      match = re.search(_HTTP_RESPCODE_REGEXP, line)
      if expected_response_code and match:
        http_resp_code = match.group(1)
        if re.search(expected_response_code, http_resp_code) is None:
          poll_stats = FAILURE_INVALID_RESPONSE_CODE
  if retcode:
    poll_stats = FAILURE_CURL
    http_resp_code = '-1'
    logs += (
        f'Error: received non-zero return code running {curl_cmd}, '
        f'stderr: {http_stderr};'
    )
  latency = 0
  with open(outid, 'r') as stdout:
    http_stdout = stdout.read()
    if expected_response and (
        re.search(expected_response, http_stdout) is None
    ):
      poll_stats = FAILURE_INVALID_RESPONSE
    match = re.search(_CURL_METRICS_REGEXP, http_stdout)
    if match:
      latency = float(match.group(1))
      # Remove <curl_time_total:##> from the response.
      http_stdout = http_stdout.split(match.group(0))[0]

    logs += (
        f'call_curl values - latency: {latency}, pass/fail: {poll_stats}, '
        f'response code: {http_resp_code}, stdout: {http_stdout};'
    )
    return latency or -1, poll_stats, http_stdout, logs


def poll(
    endpoint,
    headers=None,
    max_retries=0,
    retry_interval=0.5,
    timeout=600,
    expected_response_code=None,
    expected_response=None,
):
  """Poll HTTP endpoint until expected response code is received or time out.

  Args:
    endpoint: string. HTTP(S) endpoint to curl.
    headers: list. A list of strings, each representing a header.
    max_retries: int. Indicates how many retires before give up.
    retry_interval: float. Indicates interval in sec between retries.
    timeout: int. Deadline before giving up.
    expected_response_code: string. Expected http response code. Can be either a
      string or a regex.
    expected_response: string. Expected http response. Can be either a string or
      a regex.

  Returns:
    A tuple of float, string, string, and string:
      Float indicates latency for successful polls. If polling failed,
        latency is default to -1.
      First string indicates if HTTP(S) endpoint is reachable and responds with
        expected response and code, or reasons of failure.
      Second string represents actual response.
      Third string has additional debugging info worth logging.
  """
  start = _get_time()
  end = start
  latency = 0
  attempt = 0
  poll_stats = None
  resp = ''
  logs = ''
  while end - start < timeout and attempt <= max_retries:
    attempt_latency, poll_stats, resp, logs = call_curl(
        endpoint, headers, expected_response_code, expected_response
    )
    if poll_stats != POLL_SUCCEED:
      time.sleep(retry_interval)
      end = _get_time()
      attempt += 1
    else:
      latency = attempt_latency
      break
  logs += f'Time before request started: {_get_time() - start - latency};'
  return latency or -1, poll_stats, resp, logs


def main():
  if not _ENDPOINT.value:
    print(
        -1, 'INVALID_ARGUMENTS', '', 'Invalid arguments, no endpoint provided.'
    )
    return 1
  print(
      poll(
          _ENDPOINT.value,
          _HEADERS.value,
          _MAX_RETRIES.value,
          _RETRY_INTERVAL.value,
          _TIMEOUT.value,
          _EXPECTED_RESPONSE_CODE.value,
          _EXPECTED_RESPONSE.value,
      )
  )
  return 0


if __name__ == '__main__':
  FLAGS(sys.argv)
  sys.exit(main())
