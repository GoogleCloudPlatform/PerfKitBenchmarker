#!/usr/bin/env python

import json
import re
import sys

def filter_by(samples, key, f):
  """
  Filter samples based on a field using a lambda. For each sample, the field's
  value will be passed into the lambda. If the lambda returns True, the sample
  will be kept. The list of samples is modified in-place.

  Args:
    samples: the list of samples
    key: the field to filter by
    f: the lambda
  """
  samples[:] = [s for s in samples if f(s[key])]

def select_filter(samples, keys, label_keys, f):
  """
  Filter samples based on several fields using a lambda. For each sample, the
  fields' values will be passed into the lambda. If the lambda returns True,
  the sample will be kept. The list of samples is modified in-place.

  Args:
    samples: the list of samples
    keys: the sample fields to filter by
    label_keys: the fields in sample['labels'] to filter by
    f: The lambda. The order of parameters must be as follows: keys, then
       label_keys
  """
  def select_f(s):
    values = [s[key] for key in keys]
    label_values = [get_label(s, label) for label in label_keys]
    return f(*(values + label_values))
  samples[:] = [s for s in samples if select_f(s)]

def filter_by_value(samples, key, value):
  """
  Keeps only samples where the value corresponding to the specified key is 
  equal to the specified value.
  """
  samples[:] = [s for s in samples if s[key] == value]

def filter_by_label_value(samples, label, value):
  """
  Keeps only samples where the value corresponding to the specified label in 
  sample['labels'] is equal to the specified value.
  """
  filter_by(samples, 'labels', lambda s: ('|%s:%s|' % (label, value)) in s)

def filter_by_label(samples, label, f):
  """
  Filter samples based on a label in sample['labels'] using a lambda. For each
  sample, the label's value will be passed into the lambda. If the lambda
  returns True, the sample will be kept. The list of samples is modified
  in-place.

  Args:
    samples: the list of samples
    label: the label to filter by
    f: the lambda
  """
  def label_search(s):
    match = re.search('\\|%s:([^\\|]*)\\|' % label, s)
    if not match:
      return False
    else:
      return f(match.group(1))
  filter_by(samples, 'labels', label_search)

def get_label(sample, label):
  """Get the value associated with a label in sample['lables']
  """
  match = re.search('\\|%s:([^\\|]*)\\|' % label, sample['labels'])
  if not match:
    return None
  else:
    return match.group(1)

def one(samples):
  """
  Assert that there is one and only one sample in the list of samples and
  return it
  """
  assert len(samples) == 1, "Expected exactly 1 sample, got %d" % len(samples)
  return samples[0]
