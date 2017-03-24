#!/usr/bin/env python3
import fcntl
import json
import multiprocessing as mp
import os
import socketserver
import statistics
import sys
import time
import traceback
import yaml

from collections import defaultdict
from itertools import accumulate

from filter_utils import select_filter
from service_util import ServiceConnection
from util import dict_inherit, js_date

########################################

def aggregate_data(data_dict, method):
  if method == 'sum':
    for group_data in data_dict.values():
      for values in group_data.values():
        values[:] = [sum(values)]
  elif method == 'sum_then_percent':
    total_sums = { group: 0 for group in data_dict.keys() }
    for group, group_data in data_dict.items():
      for values in group_data.values():
        total_sums[group] += sum(values)
    for group, group_data in data_dict.items():
      for values in group_data.values():
        values[:] = [sum(values) * 100.0 / total_sums[group]]
  elif method == 'coefficient_of_variation':
    for group, group_data in data_dict.items():
      for values in group_data.values():
        mean = abs(statistics.mean(values))
        if len(values) > 1 and mean > 0.0:
          values[:] = [statistics.stdev(values) / mean]
        else:
          values[:] = [0]
  elif method == 'average':
    for group_data in data_dict.values():
      for values in group_data.values():
        values[:] = [statistics.mean(values)]
  elif method == 'min':
    for group_data in data_dict.values():
      for values in group_data.values():
        values[:] = [min(values)]
  elif method == 'max':
    for group_data in data_dict.values():
      for values in group_data.values():
        values[:] = [max(values)]

def build_data_array(data_dict, column_to_group):
  def get_cell_type(cell):
    cell_type = type(cell)
    if cell_type is int or cell_type is float:
      return 'number'
    elif cell_type is bool:
      return 'boolean'
    elif cell_type is str:
      if cell.startswith('Date(') and cell.endswith(')'):
        return 'datetime'
      else:
        return 'string'
    return None

  # Get the column order
  data_array = []
  num_value_columns = len(data_dict)
  if num_value_columns != len(column_to_group):
    column_to_group = list(data_dict.keys())
    print(column_to_group)
    print("WARNING: failed to use column_order because the data is missing "
          "some columns.")
  assert num_value_columns == len(column_to_group)
  # Build the data array
  for column_num, group in enumerate(column_to_group):
    data = data_dict[group]
    for key, values in data.items():
      for value in values:
        row = [key]
        row += [None] * column_num
        row.append(value)
        row += [None] * (num_value_columns - column_num - 1)
        data_array.append(row)
  # Infer the columns and their types
  columns = [[None, 'keys']] + [[None, group] for group in column_to_group]
  for row in data_array:
    for i, cell in enumerate(row):
      if columns[i][0] is None:
        columns[i][0] = get_cell_type(cell)
    if None not in (column[0] for column in columns):
      break
  return data_array, columns

def sort_data_array(data_array, key_order):
  # Sort rows by key column
  data_array = sorted(data_array, key=lambda x: x[0])
  if key_order == 'decreasing':
    data_array = reversed(data_array)
  else:
    assert key_order == 'increasing', "Invalid key ordering: %s" % key_order
  return data_array

def collapse_data_array(data_array):
  # Collapse nulls in the data array
  collapsed_data = [data_array[0][:]]
  for row in data_array[1:]:
    # 'collapsed' keeps track of the number of cells that didn't find a home in
    # collapsed_data
    collapsed = 0
    for i in range(1, len(row)):
      if row[i] is not None:
        collapsed += 1 # Need to find an empty slot for this cell
        for c_row in reversed(range(0, len(collapsed_data))):
          if collapsed_data[c_row][0] == row[0] and \
              collapsed_data[-1][i] is None:
            # Found a row with an empty slot for this cell
            collapsed_data[-1][i] = row[i] 
            row[i] = None
            collapsed -= 1
            break;
    if collapsed != 0:
      assert collapsed > 0, "Something went terribly wrong"
      collapsed_data.append(row[:])
  return collapsed_data

class SelectThenParse:
  def __init__(self, select, parse):
    self._select = select
    if type(parse) is list:
      parse = ' '.join(parse)
    self._parse = parse

  def parse(self, sample):
    select_sample = self._select.get('sample') or []
    select_metadata = self._select.get('metadata') or []
    lambda_args = select_sample + select_metadata
    lambda_arg_values = [sample[key] for key in select_sample] + \
                        [sample['metadata'][key] for key in select_metadata]
    f = eval('lambda %s: %s' % (','.join(lambda_args), self._parse))
    return f(*lambda_arg_values)

class Extractor:
  def __init__(self, extract_type, group_by,
               aggregate=None, key_order=None, column_order=None):
    self.type = extract_type
    self.group_by = group_by
    self.aggregate = aggregate
    self.key_order = key_order
    self.column_order = column_order

  def extract_data_array(self, samples):
    data_dict = defaultdict() # {group: {key: [values_at_key]}}
    data_dict.default_factory = lambda: defaultdict(list)

    for sample in samples:
      group = self.group_by.parse(sample)
      for key, value in self.extract_from_sample(sample):
        data_dict[group][key].append(value)

    # Get column order
    column_order = self.column_order or list(data_dict.keys())

    if self.aggregate:
      aggregate_data(data_dict, self.aggregate)
    data_array, columns = build_data_array(data_dict, column_order)
    if self.key_order:
      data_array = sort_data_array(data_array, self.key_order)
      data_array = collapse_data_array(data_array)
    return data_array, columns

class KeyValueExtractor(Extractor):
  def __init__(self, group_by, key, value,
               aggregate=None, key_order=None, column_order=None):
    super().__init__('key_value', group_by, aggregate, key_order, column_order)
    self.key = key
    self.value = value

  def extract_from_sample(self, sample):
      yield (self.key.parse(sample), self.value.parse(sample))

class PairListExtractor(Extractor):
  def __init__(self, group_by, pair_list,
               aggregate=None, key_order=None, column_order=None):
    super().__init__('key_value', group_by, aggregate, key_order, column_order)
    self.pair_list = pair_list

  def extract_from_sample(self, sample):
    pair_list = self.pair_list.parse(sample)
    if type(pair_list) is not list:
      raise Exception("ERROR: pair_list.parse must produce a list of key "
                      "value pairs")
    for key, value in pair_list:
      yield (key, value)

def build_extractor(init_dict):
  extractor_type = init_dict['type']
  group_by = SelectThenParse(init_dict['group_by']['select'],
                             init_dict['group_by']['parse'])
  aggregate = init_dict.get('aggregate')
  key_order = init_dict.get('key_order')
  column_order = init_dict.get('column_order')
  if extractor_type == 'key_value':
    key = SelectThenParse(init_dict['key']['select'],
                          init_dict['key']['parse'])
    value = SelectThenParse(init_dict['value']['select'],
                            init_dict['value']['parse'])
    extractor = KeyValueExtractor(group_by, key, value, aggregate, key_order,
                                  column_order)
  elif extractor_type == 'pair_list':
    pair_list = SelectThenParse(init_dict['pair_list']['select'],
                                init_dict['pair_list']['parse'])
    extractor = PairListExtractor(group_by, pair_list, aggregate, key_order,
                                  column_order)
  else:
    raise Exception("Invalid data extractor type: %s" % extractor_type)
  return extractor

########################################

class Filter:
  def __init__(self, select, condition):
    if type(condition) is list:
      condition = ' '.join(condition)

    self.select_sample = select.get('sample') or []
    self.select_metadata = select.get('metadata') or []

    lambda_args = self.select_sample + self.select_metadata
    self._lambda = eval('lambda %s: %s' % (','.join(lambda_args), condition))

  def filter(self, samples):
    select_filter(samples, self.select_sample, self.select_metadata,
                  self._lambda)

def build_filter(init_dict):
  return Filter(init_dict['select'], init_dict['condition'])

########################################

class SampleSource:
  def __init__(self, path):
    self.path = path
    self.last_refresh = None
    self.next_seek = 0
    self.samples = []

  def maybe_refresh(self, full_reload=True):
    modified_time = os.path.getmtime(self.path)
    if modified_time != self.last_refresh:
      # Need to pull from this data source again
      print("Reloading %s" % self.path)
      samples = None
      with open(self.path) as samples_json:
        fcntl.flock(samples_json, fcntl.LOCK_EX)
        if full_reload or self.last_refresh is None:
          samples = [json.loads(s) for s in samples_json if s]
          self.next_seek = 0
        else:
          samples_json.seek(self.next_seek)
          samples_json_str = samples_json.read()
          self.next_seek += len(samples_json_str)
          samples = [json.loads(s) for s in samples_json_str.split('\n') if s]
      if samples is None:
        print("ERROR: Failed to reload %s" % self.path)
      else:
        print("Finished reading %s. Building samples..." % self.path)
        # Build the 'metadata' dict field from the 'labels' text field
        for sample in samples:
          # Chop of '|' at the beginning and end, split by '|,|', and remove
          # the labels field (which will be replaced by the metadata field)
          fields = sample.pop('labels')[1:-1].split('|,|')
          # Turn the fields into a [[key, value], ...]
          key_values = [field.split(':', 1) for field in fields]
          sample['metadata'] = { k: v for k, v in key_values }
        self.last_refresh = modified_time
        if full_reload:
          self.samples = samples
        else:
          self.samples += samples
        print("Finished reloading %s" % self.path)

class DataSource:
  def __init__(self, path, extractor, filters, full_reload=True):
    self.path = path
    self.last_refresh = None

    self.extractor = extractor
    self.filters = filters

    self.data = []
    self.columns = []

    self.filtered_samples = []

    # Stuff for processing only new samples
    self.full_reload = full_reload
    self.last_samples_checkpoint = 0

  def maybe_refresh_data(self, sample_sources, force_refresh=False):
    # Ensure the sample source exists and is up to date
    sample_source_path = self.path
    if sample_source_path not in sample_sources:
      # Sample source that we haven't seen yet - create it
      sample_sources[sample_source_path] = SampleSource(sample_source_path)
    sample_source = sample_sources[sample_source_path]
    sample_source.maybe_refresh(self.full_reload)

    if self.last_refresh == sample_source.last_refresh:
      # Data hasn't changed since last extraction, no need to extract it again
      return False

    # Extract the data
    data, columns = self.extract_data_array(sample_source.samples)
    self.data = data
    self.columns = columns
    self.last_refresh = sample_source.last_refresh
    return True

  def extract_data_array(self, samples):
    if self.full_reload:
      samples = samples[:] # Copy the array so we don't modify the original
      self.last_samples_checkpoint = 0
    else:
      samples = samples[self.last_samples_checkpoint:]
      self.last_samples_checkpoint += len(samples)

    # Filter out irrelevant samples
    for f in self.filters:
      f.filter(samples)

    if self.full_reload:
      self.filtered_samples = samples
    else:
      self.filtered_samples += samples

    # Extract and group data
    return self.extractor.extract_data_array(self.filtered_samples)

def build_data_source(init_dict):
  path = init_dict['path']
  extractor = build_extractor(init_dict['extract'])
  filters = [build_filter(f) for f in init_dict['filters']]
  full_reload = init_dict.get('full_reload')
  if full_reload is None:
    # Default to full reloads
    full_reload = True
  return DataSource(path, extractor, filters, full_reload)

########################################

def build_google_charts_data_array(columns, data):
  return {
    'cols': [{ 'label': col_name, 'type': col_type }
             for col_type, col_name in columns],
    'rows': [{'c': [{ 'v': value } for value in row]} for row in data]
  }

def run_data_refresh_loop(data_source_store, data_store):
  data_sources = {}
  data_source_specs = {}
  sample_sources = {}

  while True:
    # Maybe refresh specs
    for name, data_source_spec in data_source_store.items():
      data_source_spec = data_source_spec.copy()
      # Check if this data source is new or modified
      if name in data_source_specs and \
          data_source_spec == data_source_specs[name]:
        # Skip unchanged data source
        continue
      print("Data source %s has been modified" % name)
      data_source_specs[name] = data_source_spec
      data_sources[name] = build_data_source(data_source_spec)
    # Maybe refresh data sources and build data arrays
    for name, data_source in data_sources.items():
      try:
        if data_source.maybe_refresh_data(sample_sources):
          print("Refreshed data source: %s" % name)
          data_array = build_google_charts_data_array(data_source.columns,
                                                      data_source.data)
          data_store[name] = data_array
      except Exception as e:
        print("ERROR: Failed to refresh data source %s:" % name)
        traceback.print_exc()
    time.sleep(5)
