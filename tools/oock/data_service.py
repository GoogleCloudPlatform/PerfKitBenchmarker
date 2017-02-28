#!/usr/bin/env python3
import json
import multiprocessing as mp
import os
import socketserver
import sys
import time
import yaml

from collections import defaultdict
from itertools import accumulate

from filter_utils import select_filter
from service_util import ServiceConnection
from util import dict_inherit

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
  def get_cell_type(cell):
    cell_type = type(cell)
    if cell_type is int or cell_type is float:
      return 'number'
    elif cell_type is bool:
      return 'boolean'
    elif cell_type is str:
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
        if full_reload:
          samples = [json.loads(s)
                     for s in samples_json.read().split('\n') if s]
          self.next_seek = 0
        else:
          samples_json.seek(self.next_seek)
          samples_json_str = samples_json.read()
          self.next_seek += len(samples_json_str)
          samples = [json.loads(s) for s in samples_json_str.split('\n') if s]
      if samples is None:
        print("ERROR: Failed to reload %s" % self.path)
      else:
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
      return

    # Extract the data
    data, columns = self.extract_data_array(sample_source.samples)
    self.data = data
    self.columns = columns
    self.last_refresh = sample_source.last_refresh

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

class DataSourceManager:
  def __init__(self, spec_path):
    self.spec_path = spec_path
    self.last_spec_update = None

    self.sample_sources = {} # Cache sample sources
    self.data_sources = {}
    self.data_source_dicts = {} # Data source dicts directly from the yaml
    
  def maybe_refresh_data(self):
    self.maybe_refresh_spec()
    for data_source in self.data_sources.values():
      data_source.maybe_refresh_data(self.sample_sources)

  def maybe_refresh_spec(self):
    modified_time = os.path.getmtime(self.spec_path)
    if modified_time != self.last_spec_update:
      print("Refreshing data sources")
      self.last_spec_update = modified_time
      spec = None

      with open(self.spec_path) as spec_file:
        # Try to parse the data sources spec yaml
        try:
          spec = yaml.load(spec_file.read())
        except:
          sys.stderr.write("ERROR: Data sources spec yaml is not valid yaml\n")

      if spec is None:
        sys.stderr.write("ERROR: Failed to read the data sources spec yaml\n")
      else:
        data_sources = spec['data_sources']
        # Build the data sources that need to be built
        for name in data_sources.keys():
          # Apply inheritance
          base = data_sources[name].get('inherit')
          if base:
            data_sources[name] = dict_inherit(base, data_sources[name])
          if name in self.data_source_dicts and \
             data_sources[name] == self.data_source_dicts[name]:
            # Skip unchanged data source
            continue
          print("Rebuilding data source %s" % name)
          self.data_sources[name] = build_data_source(data_sources[name])
          self.data_source_dicts[name] = data_sources[name]

########################################

########################################

class DataServerHandler(socketserver.BaseRequestHandler):
  def handle(self):
    request = ServiceConnection(self.request)
    data_store = self.server.data_store

    data_source_name = request.recv_str()
    data_array = data_store[data_source_name]
    data_array_json = json.dumps(data_array)

    print("Sending data for %s to %s" % (data_source_name,
                                         self.request.getsockname()))
    request.send_str(data_array_json)

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
  pass

########################################

def build_google_charts_data_array(columns, data):
  return {
    'cols': [{ 'label': col_name, 'type': col_type }
             for col_type, col_name in columns],
    'rows': [{'c': [{ 'v': value } for value in row]} for row in data]
  }

def run_data_service(host, port, spec_path):
  data_source_mgr = DataSourceManager(spec_path)
  # Do the initial load of the data
  data_source_mgr.maybe_refresh_data()

  # Create the shared data store
  mp_manager = mp.Manager()
  data_store = mp_manager.dict()

  # Start the data refresher
  def _data_refresh():
    while True:
      data_source_mgr.maybe_refresh_data()
      for name, data_source in data_source_mgr.data_sources.items():
        data_array = build_google_charts_data_array(data_source.columns,
                                                    data_source.data)
        data_store[name] = data_array
      time.sleep(10)

  data_refresh_proc = mp.Process(target=_data_refresh)
  data_refresh_proc.daemon = True
  data_refresh_proc.start()

  # Start the data server
  data_server = ThreadedTCPServer((host, port), DataServerHandler)
  data_server.data_store = data_store
  data_server.serve_forever()
        
def main():
  if len(sys.argv) != 2:
    print("Usage: python3 data_service.py <data_sources_yaml>")
    exit()
  spec_path = sys.argv[1]
  run_data_service('localhost', 22422, spec_path)

########################################

if __name__ == "__main__":
  main()
