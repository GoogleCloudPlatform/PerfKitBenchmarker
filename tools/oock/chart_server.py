#!/usr/bin/env python3
import json
import os
import sys
import yaml

from collections import defaultdict
from http.server import BaseHTTPRequestHandler, HTTPServer
from itertools import accumulate

from filter_utils import select_filter
from page_gen import (build_chart_html, cell_to_js, build_data_table_js,
                      build_chart_js, build_chart_page, get_cell_type)

########################################

def aggregate_data(data_dict, method):
  if method == 'sum':
    for group_data in data_dict.values():
      for values in group_data.values():
        values[:] = [sum(values)]
  elif method == 'average':
    for group_data in data_dict.values():
      for values in group_data.values():
        values[:] = [sum(values) / len(values)]
  elif method == 'min':
    for group_data in data_dict.values():
      for values in group_data.values():
        values[:] = [min(values)]
  elif method == 'max':
    for group_data in data_dict.values():
      for values in group_data.values():
        values[:] = [max(values)]

def build_data_array(data_dict, column_to_group):
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
    self.samples = []

  def maybe_refresh(self):
    modified_time = os.path.getmtime(self.path)
    if modified_time != self.last_refresh:
      # Need to pull from this data source again
      print("Reloading %s" % self.path)
      samples = None
      with open(self.path) as samples_json:
        samples = [json.loads(s) for s in samples_json.read().split('\n') if s]
      if samples is None:
        print("ERROR: Failed to reload %s" % self.path)
      else:
        # Build the 'metadata' dict field from the 'labels' text field
        for sample in samples:
          # Chop of '|' at the beginning and end
          fields = sample['labels'][1:-1].split('|,|')
          # Turn the fields into a [[key, value], ...]
          key_values = [field.split(':', 1) for field in fields]
          sample['metadata'] = { k: v for k, v in key_values }
        self.samples = samples
        self.last_refresh = modified_time

class DataSource:
  def __init__(self, path, extractor, filters):
    self.path = path
    self.extractor = extractor
    self.filters = filters

  def extract_data_array(self, samples):
    samples = samples[:] # Copy the array so we don't modify the original
    # Filter out irrelevant samples
    for f in self.filters:
      f.filter(samples)
    # Extract and group data
    return self.extractor.extract_data_array(samples)

def build_data_source(init_dict):
  path = init_dict['path']
  extractor = build_extractor(init_dict['extract'])
  filters = [build_filter(f) for f in init_dict['filters']]
  return DataSource(path, extractor, filters)

class ChartSpec:
  def __init__(self, name, data_source, chart_type, options):
    self.name = name
    self.data_source = data_source
    self.data = []
    self.columns = []
    self.type = chart_type
    self.options = options
    self.last_extract = None

  def maybe_refresh_data(self, sample_sources, force_refresh=False):
    # Ensure the sample source exists and is up to date
    sample_source_path = self.data_source.path
    if sample_source_path not in sample_sources:
      # Sample source that we haven't seen yet - create it
      sample_sources[sample_source_path] = SampleSource(sample_source_path)
    sample_source = sample_sources[sample_source_path]
    sample_source_updated = sample_source.maybe_refresh()

    if self.last_extract == sample_source.last_refresh:
      # Data hasn't changed since last extraction, no need to extract it again
      return

    # Extract the data
    data, columns = self.data_source.extract_data_array(sample_source.samples)
    self.data = data
    self.columns = columns
    self.last_extract = sample_source.last_refresh

def build_chart_spec(name, init_dict):
  data_source = build_data_source(init_dict['data_source'])
  chart_type = init_dict['type']
  options = init_dict['options']
  return ChartSpec(name, data_source, chart_type, options)

class ChartPage:
  def __init__(self, page_spec_path):
    self.page_spec_path = page_spec_path
    self.last_page_spec_update = None
    self.chart_specs = {}
    self.chart_spec_dicts = {} # Chart specs dicts directly from the yaml
    self.chart_order = None
    self.sample_sources = {} # Cache sample sources
    
  def maybe_refresh_chart_data(self):
    self.maybe_refresh_page_spec()
    for chart in self.chart_specs.values():
      chart.maybe_refresh_data(self.sample_sources)

  def maybe_refresh_page_spec(self):
    modified_time = os.path.getmtime(self.page_spec_path)
    if modified_time != self.last_page_spec_update:
      print("Refreshing chart specs")
      self.last_page_spec_update = modified_time
      page_spec = None
      with open(self.page_spec_path) as page_spec_file:
        # Try to parse the chart page spec yaml
        try:
          page_spec = yaml.load(page_spec_file.read())
        except:
          sys.stderr.write("ERROR: Chart page spec yaml is not valid yaml\n")
      if page_spec is None:
        sys.stderr.write("ERROR: Failed to read the chart page spec yaml\n")
      else:
        chart_specs = page_spec['chart_specs']
        self.chart_order = page_spec.get('chart_order') or chart_specs.keys()
        # Build the chart specs that need to be built
        for chart_name in chart_specs.keys():
          if chart_name in self.chart_spec_dicts and \
             chart_specs[chart_name] == self.chart_spec_dicts[chart_name]:
            # Skip unchanged chart spec
            continue
          print("Rebuilding chart spec for %s" % chart_name)
          self.chart_specs[chart_name] = \
              build_chart_spec(chart_name, chart_specs[chart_name])
          self.chart_spec_dicts[chart_name] = chart_specs[chart_name]

def ChartServerFactory(chart_page):
  class ChartServer(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
      self.chart_page = chart_page
      super(ChartServer, self).__init__(*args, **kwargs)

    def _set_headers(self):
      self.send_response(200)
      self.send_header('Content-type', 'text/html')
      self.end_headers()

    def do_GET(self):
      self.chart_page.maybe_refresh_chart_data()
      self._set_headers()

      # Build list of chart specs in the proper order
      chart_specs = [self.chart_page.chart_specs[name]
                     for name in self.chart_page.chart_order]

      html = build_chart_page(chart_specs)
      self.wfile.write(str.encode(html))

    def do_HEAD(self):
      self._set_headers()
        
    def do_POST(self):
      # Doesn't do anything with posted data
      self._set_headers()
      self.wfile.write("<html><body><h1>POST!</h1></body></html>")

  return ChartServer
        
def main():
  if len(sys.argv) != 2:
    print("Usage: python3 chart_server.py <page_spec_yaml>")
    exit()

  port = 8080
  page_spec_path = sys.argv[1]

  chart_page = ChartPage(page_spec_path)

  server_address = ('0.0.0.0', port)
  chart_server = ChartServerFactory(chart_page)
  httpd = HTTPServer(server_address, chart_server)
  print('Starting chart server...')
  httpd.serve_forever()

########################################

if __name__ == "__main__":
  main()
