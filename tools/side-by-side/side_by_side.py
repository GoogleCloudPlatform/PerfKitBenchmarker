#!/usr/bin/env python

# Copyright 2014 Google Inc. All rights reserved.
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
# -*- coding: utf-8 -*-
"""Runs a side-by-side comparison of two PerfKitBenchmarker revisions.

Given a pair of revisions (e.g., 'dev', 'master'), this tool runs 'pkb.py' with
identical command line flags for both revisions and creates a report showing
the differences in the results between the two runs.
"""

import argparse
import collections
import contextlib
import difflib
import itertools
import json
import logging
import os
import pprint
import shlex
import shutil
import subprocess
import tempfile

import jinja2


# Keys in the sample JSON we expect to vary between runs.
# These will be removed prior to diffing samples.
VARYING_KEYS = 'run_uri', 'sample_uri', 'timestamp', 'value'
# Template name, in same directory as this file.
TEMPLATE = 'side_by_side.html.j2'


PerfKitBenchmarkerResult = collections.namedtuple(
    'PerfKitBenchmarkerResult',
    ['name', 'sha1', 'samples', 'flags'])


@contextlib.contextmanager
def TempDir(delete=True, **kwargs):
  """Directory equivalent of tempfile.NamedTemporaryFile.

  When used as a context manager, yields a temporary directory which by default
  is removed when the context manager goes our of scope.

  Example usage:

    >>> with TempDir(prefix='perfkit') as td:
    ...   shutil.copy('test.txt', td)

  Args:
    delete: Delete the directory on exit?
    **kwargs: Passed to tempfile.mkdtemp.

  Yields:
    String. Path to the temporary directory.
  """
  td = tempfile.mkdtemp(**kwargs)
  logging.info('Created %s', td)
  try:
    yield td
  finally:
    if delete:
      logging.info('Removing %s', td)
      shutil.rmtree(td)


def _GitCommandPrefix():
  """Prefix for all git commands.

  Returns:
    list of strings; 'git' with an appropriate '--git-dir' flag.
  """
  git_dir = os.path.join(os.path.dirname(__file__), '..', '..', '.git')
  return ['git', '--git-dir', git_dir]


def _GitRevParse(revision):
  """Returns the output of 'git rev-parse' for 'revision'."""
  output = subprocess.check_output(_GitCommandPrefix() +
                                   ['rev-parse', revision])
  return output.rstrip()


@contextlib.contextmanager
def PerfKitBenchmarkerCheckout(revision):
  """Yields a directory with PerfKitBenchmarker checked out to 'revision'."""
  archive_cmd = _GitCommandPrefix() + ['archive', revision]
  logging.info('Running: %s', archive_cmd)
  p_archive = subprocess.Popen(archive_cmd, stdout=subprocess.PIPE)
  with TempDir(prefix='pkb-test-', suffix=revision[:4]) as td:
    tar_cmd = ['tar', 'xf', '-']
    logging.info('Running %s in %s', tar_cmd, td)
    p_tar = subprocess.Popen(tar_cmd, stdin=p_archive.stdout, cwd=td)
    archive_status = p_archive.wait()
    tar_status = p_tar.wait()
    if archive_status:
      raise subprocess.CalledProcessError(archive_cmd, archive_status)
    if tar_status:
      raise subprocess.CalledProcessError(tar_status, tar_cmd)

    yield td


def RunPerfKitBenchmarker(revision, flags):
  """Runs perfkitbenchmarker, returning the results as parsed JSON.

  Args:
    revision: string. git commit identifier. Version of PerfKitBenchmarker to
      run.
    flags: list of strings. Default arguments to pass to `pkb.py.`

  Returns:
    List of dicts. Deserialized JSON output of running PerfKitBenchmarker with
      `--json_path`.
  """
  sha1 = _GitRevParse(revision)
  with PerfKitBenchmarkerCheckout(revision) as td:
    with tempfile.NamedTemporaryFile(suffix='.json') as tf:
      flags = flags + ['--json_path=' + tf.name]
      cmd = ['./pkb.py'] + flags
      logging.info('Running %s in %s', cmd, td)
      subprocess.check_call(cmd, cwd=td)
      samples = [json.loads(line) for line in tf]
      return PerfKitBenchmarkerResult(name=revision, sha1=sha1, flags=flags,
                                      samples=samples)


def _SplitLabels(labels):
  """Parse the 'labels' key from a PerfKitBenchmarker record.

  Labels are recorded in '|key:value|,|key:value|' form.
  This function transforms them to a dict.

  Args:
    labels: string. labels to parse.

  Returns:
    dict. Parsed 'labels'.
  """
  result = {}
  for item in labels.strip('|').split('|,|'):
    k, v = item.split(':', 1)
    result[k] = v
  return result


def _CompareSamples(a, b, context=True, numlines=1):
  """Generate an HTML table showing differences between 'a' and 'b'.

  Args:
    a: dict, as output by PerfKitBenchmarker.
    b: dict, as output by PerfKitBenchmarker.
    context: boolean. Show context in diff? If False, all lines are output, even
      those which are equal.
    numlines: int. Passed to difflib.Htmldiff.make_table.
  Returns:
    string or None. An HTML table, or None if there are no differences.
  """
  a = a.copy()
  b = b.copy()
  a['metadata'] = _SplitLabels(a.pop('labels', ''))
  b['metadata'] = _SplitLabels(b.pop('labels', ''))

  # Prune the keys in VARYING_KEYS prior to comparison to make the diff more
  # informative.
  for d in (a, b):
    for key in VARYING_KEYS:
      d.pop(key, None)

  astr = pprint.pformat(a).splitlines()
  bstr = pprint.pformat(b).splitlines()
  if astr == bstr and context:
    return None

  differ = difflib.HtmlDiff()
  return differ.make_table(astr, bstr, context=context, numlines=numlines)


def _MatchSamples(base_samples, head_samples):
  """Match items from base_samples with items from head_samples.

  Rows are matched using 'test', 'metric', and 'unit' fields.

  Args:
    base_samples: List of dicts.
    head_samples: List of dicts.

  Returns:
    List of pairs, each item of the pair containing either a dict or None.
  """
  def ExtractKeys(samples):
    return [(i['test'], i['metric'], i['unit']) for i in samples]

  base_keys = ExtractKeys(base_samples)
  head_keys = ExtractKeys(head_samples)

  sm = difflib.SequenceMatcher('', base_keys, head_keys)

  result = []

  for opcode, base_begin, base_end, head_begin, head_end in sm.get_opcodes():
    if opcode == 'equal':
      result.extend(zip(base_samples[base_begin:base_end],
                        head_samples[head_begin:head_end]))
    elif opcode == 'replace':
      result.extend(itertools.izip_longest(base_samples[base_begin:base_end],
                                           head_samples[head_begin:head_end]))
    elif opcode == 'delete':
      result.extend(zip(base_samples[base_begin:base_end],
                        [None] * (base_end - base_begin)))
    elif opcode == 'insert':
      result.extend(zip([None] * (head_end - head_begin),
                        head_samples[head_begin:head_end]))
    else:
      raise AssertionError('Unknown op: ' + opcode)
  return result


def RenderResults(base_result, head_result, flags, template_name=TEMPLATE,
                  **kwargs):
  """Render the results of a comparison as an HTML page.

  Args:
    base_result: PerfKitBenchmarkerResult. Result of running against base
      revision.
    head_result: PerfKitBenchmarkerResult. Result of running against head
      revision.
    template_name: string. The filename of the template.
    kwargs: Additional arguments to Template.render.

  Returns:
    String. The HTML template.
  """
  def _ClassForPercentDifference(percent_diff):
    """Crude highlighting of differences between runs.

    Samples varying by >25% are colored red.
    Samples varying by 5-25% are colored orange.
    Other samples are colored green.

    Args:
      percent_diff: float. percent difference between values.
    """
    percent_diff = abs(percent_diff)
    if percent_diff > 25:
      return 'bg-danger text-danger'
    elif percent_diff > 5:
      return 'bg-warning text-warning'
    else:
      return 'bg-success text-success'

  env = jinja2.Environment(
      loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
      undefined=jinja2.StrictUndefined)
  env.globals['class_for_percent_diff'] = _ClassForPercentDifference
  env.globals['izip_longest'] = itertools.izip_longest

  template = env.get_template('side_by_side.html.j2')

  matched = _MatchSamples(base_result.samples,
                          head_result.samples)

  # Generate sample diffs
  sample_context_diffs = []
  sample_diffs = []
  for base_sample, head_sample in matched:
    if not base_sample or not head_sample:
      # Sample inserted or deleted.
      continue
    sample_context_diffs.append(
        _CompareSamples(base_sample, head_sample))
    sample_diffs.append(
        _CompareSamples(base_sample, head_sample, context=False))

  return template.render(flags=flags,
                         base=base_result,
                         head=head_result,
                         matched_samples=matched,
                         sample_diffs=sample_diffs,
                         sample_context_diffs=sample_context_diffs,
                         **kwargs)


def main():
  p = argparse.ArgumentParser(
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      description=__doc__)
  p.add_argument('--base', default='master', help="""Base revision.""")
  p.add_argument('--head', default='dev', help="""Head revision.""")
  p.add_argument('-f', '--flags', help="""Command line flags""",
                 default=['--cloud=GCP', '--machine_type=n1-standard-4',
                          '--benchmarks=netperf_simple'],
                 type=shlex.split)
  p.add_argument('-p', '--parallel', default=False, action='store_true',
                 help="""Run concurrently""")
  p.add_argument('json_output', help="""JSON output path.""")
  p.add_argument('html_output', help="""HTML output path.""")
  a = p.parse_args()

  if a.parallel:
    from concurrent import futures
    with futures.ThreadPoolExecutor(max_workers=2) as executor:
      base_res_fut = executor.submit(RunPerfKitBenchmarker, a.base, a.flags)
      head_res_fut = executor.submit(RunPerfKitBenchmarker, a.head, a.flags)
      base_res = base_res_fut.result()
      head_res = head_res_fut.result()
  else:
      base_res = RunPerfKitBenchmarker(a.base, a.flags)
      head_res = RunPerfKitBenchmarker(a.head, a.flags)

  logging.info('Base result: %s', base_res)
  logging.info('Head result: %s', head_res)

  with argparse.FileType('w')(a.json_output) as json_fp:
    logging.info('Writing JSON to %s', a.json_output)
    json.dump({'head': head_res._asdict(),
               'base': base_res._asdict(),
               'flags': a.flags},
              json_fp,
              indent=2)
    json_fp.write('\n')

  with argparse.FileType('w')(a.html_output) as html_fp:
    logging.info('Writing HTML to %s', a.html_output)
    html_fp.write(RenderResults(base_result=base_res,
                                head_result=head_res,
                                flags=a.flags,
                                varying_keys=VARYING_KEYS))


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  main()
