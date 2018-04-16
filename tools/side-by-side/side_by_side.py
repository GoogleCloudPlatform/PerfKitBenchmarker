#!/usr/bin/env python

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
# -*- coding: utf-8 -*-
"""Runs a side-by-side comparison of two PerfKitBenchmarker revisions.

Given a pair of revisions (e.g., 'dev', 'master') and command-line arguments,
this tool runs 'pkb.py' with for each and creates a report showing the
differences in the results between the two runs.
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


DEFAULT_FLAGS = ('--cloud=GCP', '--machine_type=n1-standard-4',
                 '--benchmarks=netperf')
# Keys in the sample JSON we expect to vary between runs.
# These will be removed prior to diffing samples.
VARYING_KEYS = 'run_uri', 'sample_uri', 'timestamp', 'value'
# Template name, in same directory as this file.
TEMPLATE = 'side_by_side.html.j2'

# Thresholds for highlighting results
SMALL_CHANGE_THRESHOLD = 5
MEDIUM_CHANGE_THRESHOLD = 10
LARGE_CHANGE_THRESHOLD = 25


PerfKitBenchmarkerResult = collections.namedtuple(
    'PerfKitBenchmarkerResult',
    ['name', 'description', 'sha1', 'samples', 'flags'])


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


def _GitDescribe(revision):
  """Returns the output of 'git describe' for 'revision'."""
  output = subprocess.check_output(_GitCommandPrefix() +
                                   ['describe', '--always', revision])
  return output.rstrip()


@contextlib.contextmanager
def PerfKitBenchmarkerCheckout(revision):
  """Yields a directory with PerfKitBenchmarker checked out to 'revision'."""
  archive_cmd = _GitCommandPrefix() + ['archive', revision]
  logging.info('Running: %s', archive_cmd)
  p_archive = subprocess.Popen(archive_cmd, stdout=subprocess.PIPE)
  with TempDir(prefix='pkb-test-') as td:
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
  description = _GitDescribe(revision)
  with PerfKitBenchmarkerCheckout(revision) as td:
    with tempfile.NamedTemporaryFile(suffix='.json') as tf:
      flags = flags + ['--json_path=' + tf.name]
      cmd = ['./pkb.py'] + flags
      logging.info('Running %s in %s', cmd, td)
      subprocess.check_call(cmd, cwd=td)
      samples = [json.loads(line) for line in tf]
      return PerfKitBenchmarkerResult(name=revision, sha1=sha1, flags=flags,
                                      samples=samples, description=description)


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
      result.extend(zip(base_samples[base_begin:base_end],
                        [None] * (base_end - base_begin)))
      result.extend(zip([None] * (head_end - head_begin),
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


def RenderResults(base_result, head_result, template_name=TEMPLATE,
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
    if percent_diff < 0:
      direction = 'decrease'
    else:
      direction = 'increase'

    percent_diff = abs(percent_diff)
    if percent_diff > LARGE_CHANGE_THRESHOLD:
      size = 'large'
    elif percent_diff > MEDIUM_CHANGE_THRESHOLD:
      size = 'medium'
    elif percent_diff > SMALL_CHANGE_THRESHOLD:
      size = 'small'
    else:
      return ''

    return 'value-{0}-{1}'.format(direction, size)

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

  # Generate flag diffs
  flag_diffs = difflib.HtmlDiff().make_table(
      base_result.flags, head_result.flags, context=False)

  # Used for generating a chart with differences.
  matched_json = json.dumps(matched)\
      .replace(u'<', u'\\u003c') \
      .replace(u'>', u'\\u003e') \
      .replace(u'&', u'\\u0026') \
      .replace(u"'", u'\\u0027')

  return template.render(base=base_result,
                         head=head_result,
                         matched_samples=matched,
                         matched_samples_json=matched_json,
                         sample_diffs=sample_diffs,
                         sample_context_diffs=sample_context_diffs,
                         flag_diffs=flag_diffs,
                         infinity=float('inf'),
                         **kwargs)


def main():
  p = argparse.ArgumentParser(
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      description=__doc__)
  p.add_argument('-t', '--title', default='PerfKitBenchmarker Comparison',
                 help="""HTML report title""")
  p.add_argument('--base', default='master', help="""Base revision.""")
  p.add_argument('--head', default='dev', help="""Head revision.""")
  p.add_argument('--base-flags', default=None, help="""Flags for run against
                 '--base' revision. Will be combined with --flags.""",
                 type=shlex.split)
  p.add_argument('--head-flags', default=None, help="""Flags for run against
                 '--head' revision. Will be combined with --flags.""",
                 type=shlex.split)
  p.add_argument('-f', '--flags', type=shlex.split,
                 help="""Command line flags (Default: {0})""".format(
                     ' '.join(DEFAULT_FLAGS)))
  p.add_argument('-p', '--parallel', default=False, action='store_true',
                 help="""Run concurrently""")
  p.add_argument('--rerender', help="""Re-render the HTML report from a JSON
                 file [for developers].""", action='store_true')
  p.add_argument('json_output', help="""JSON output path.""")
  p.add_argument('html_output', help="""HTML output path.""")
  a = p.parse_args()

  if (a.base_flags or a.head_flags):
    if not (a.base_flags and a.head_flags):
      p.error('--base-flags and --head-flags must be specified together.\n'
              '\tbase flags={0}\n\thead flags={1}'.format(
                  a.base_flags, a.head_flags))
    a.base_flags = a.base_flags + (a.flags or [])
    a.head_flags = a.head_flags + (a.flags or [])
  else:
    # Just --flags
    assert not a.base_flags, a.base_flags
    assert not a.head_flags, a.head_flags
    a.base_flags = a.flags or list(DEFAULT_FLAGS)
    a.head_flags = a.flags or list(DEFAULT_FLAGS)

  if not a.rerender:
    if a.parallel:
      from concurrent import futures
      with futures.ThreadPoolExecutor(max_workers=2) as executor:
        base_res_fut = executor.submit(RunPerfKitBenchmarker, a.base,
                                       a.base_flags)
        head_res_fut = executor.submit(RunPerfKitBenchmarker, a.head,
                                       a.head_flags)
        base_res = base_res_fut.result()
        head_res = head_res_fut.result()
    else:
      base_res = RunPerfKitBenchmarker(a.base, a.base_flags)
      head_res = RunPerfKitBenchmarker(a.head, a.head_flags)

    logging.info('Base result: %s', base_res)
    logging.info('Head result: %s', head_res)

    with argparse.FileType('w')(a.json_output) as json_fp:
      logging.info('Writing JSON to %s', a.json_output)
      json.dump({'head': head_res._asdict(),
                 'base': base_res._asdict()},
                json_fp,
                indent=2)
      json_fp.write('\n')
  else:
    logging.info('Loading results from %s', a.json_output)
    with argparse.FileType('r')(a.json_output) as json_fp:
      d = json.load(json_fp)
      base_res = PerfKitBenchmarkerResult(**d['base'])
      head_res = PerfKitBenchmarkerResult(**d['head'])

  with argparse.FileType('w')(a.html_output) as html_fp:
    logging.info('Writing HTML to %s', a.html_output)
    html_fp.write(RenderResults(base_result=base_res,
                                head_result=head_res,
                                varying_keys=VARYING_KEYS,
                                title=a.title))


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  main()
