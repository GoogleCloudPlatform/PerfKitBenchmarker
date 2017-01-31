#!/usr/bin/env python3
import json
import os
import sys
import yaml

from collections import defaultdict
from http.server import BaseHTTPRequestHandler, HTTPServer
from itertools import accumulate

from page_gen import (build_chart_html, cell_to_js, build_data_table_js,
                      build_chart_js, build_chart_page, get_cell_type)

def pull_chart_data(data_source):
  from filter_utils import filter_by, filter_by_label, get_label, select_filter

  samples = None
  with open(data_source['path']) as samples_json:
    samples = [json.loads(s) for s in samples_json.read().split('\n') if s]
  if samples is None:
    return None

  def _filter_select_and_condition(samples, select, condition):
    select_sample = select.get('sample') or []
    select_metadata = select.get('metadata') or []
    lambda_args = select_sample + select_metadata
    select_filter(samples, select_sample, select_metadata,
                  eval('lambda %s: %s' % (','.join(lambda_args), condition)))
  
  def _select_then_parse(sample, select, parse):
    if type(parse) is list:
      parse = ''.join(parse)
    select_sample = select.get('sample') or []
    select_metadata = select.get('metadata') or []
    lambda_args = select_sample + select_metadata
    lambda_arg_values = [sample[key] for key in select_sample] + \
                        [get_label(sample, key) for key in select_metadata]
    f = eval('lambda %s: %s' % (','.join(lambda_args), parse))
    return f(*lambda_arg_values)

  # Filter by sample key/values
  for f in data_source['filters']:
    select = f['select']
    condition = ''.join(f['condition'])
    _filter_select_and_condition(samples, select, condition)

  # Group and extract data
  extract = data_source['extract']
  group_by_select = extract['group_by']['select']
  group_by_parse = extract['group_by']['parse']
  data_dict = defaultdict() # {group: {key: [values_at_key]}}
  data_dict.default_factory = lambda: defaultdict(list)
  if extract['type'] == 'key_value':
    extract_key_select = extract['key']['select']
    extract_key_parse = extract['key']['parse']
    extract_value_select = extract['value']['select']
    extract_value_parse = extract['value']['parse']
    for sample in samples:
      group = _select_then_parse(sample, group_by_select, group_by_parse)
      key = _select_then_parse(sample, extract_key_select, extract_key_parse)
      value = _select_then_parse(sample, extract_value_select,
                                 extract_value_parse)
      data_dict[group][key].append(value)
  elif extract['type'] == 'pair_list':
    extract_pl_select = extract['pair_list']['select']
    extract_pl_parse = extract['pair_list']['parse']
    for sample in samples:
      group = _select_then_parse(sample, group_by_select, group_by_parse)
      pair_list = _select_then_parse(sample, extract_pl_select,
                                     extract_pl_parse)
      for key, value in pair_list:
        data_dict[group][key].append(value)
  else:
    raise Exception('Invalid data extraction type: %s' % extract['type'])

  # Build the data array
  data_array = []
  num_value_columns = len(data_dict)
  column_to_group = []
  for column_num, (group, data) in enumerate(data_dict.items()):
    column_to_group.append(group)
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

  # Sort rows by key column
  if extract.get('key_order') is not None:
    data_array = sorted(data_array, key=lambda x: x[0])
    if extract['key_order'] == 'decreasing':
      data_array = reversed(data_array)
    else:
      assert extract['key_order'] == 'increasing', \
             "Invalid key ordering: %s" % label_column['key_order']
    # Collapse nulls in the data array
    collapsed_data = []
    work_row = None
    for i in range(len(data_array) - 1):
      if work_row is not None and work_row[0] != data_array[i][0]:
        collapsed_data.append(work_row)
        work_row = None
      if work_row is None:
        if data_array[i][0] == data_array[i+1][0]:
          work_row = data_array[i][:]
        else:
          collapsed_data.append(data_array[i][:])
        continue
      for j in range(len(work_row)):
        if work_row[j] is None and data_array[i][j] is not None:
          work_row[j] = data_array[i][j]
    data_array = collapsed_data

  return data_array, columns

class ChartPage:
  def __init__(self, page_spec_path):
    self.page_spec_path = page_spec_path
    self.last_page_spec_update = None
    self.chart_specs = None
    self.chart_order = None
    
  def refresh_chart_data(self):
    self.refresh_page_spec()
    for chart in self.chart_specs:
      modified_time = os.path.getmtime(chart['data_source']['path'])
      if modified_time != chart['data_source']['last_update']:
        # Need to pull from this data source again
        print("Refreshing chart data for: " + chart['name'])
        chart['data_source']['last_update'] = modified_time
        chart['data'], chart['columns'] = pull_chart_data(chart['data_source'])

  def refresh_page_spec(self):
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
        chart_order = page_spec.get('chart_order') or chart_specs.keys()
        for chart_name, chart in chart_specs.items():
          # Set some fields we need chart specs to have internally
          chart['name'] = chart_name
          chart['data_source']['last_update'] = None
          chart['data'] = []
        # Convert the chart_specs dictionary into a list
        self.chart_specs = [chart_specs[chart_name]
                            for chart_name in chart_order]

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
      self.chart_page.refresh_chart_data()
      self._set_headers()

      # Build chart_specs with only the fields required for build_chart_page
      keys = ['name', 'type', 'columns', 'data', 'options']
      chart_specs = [{key: c[key] for key in keys}
                     for c in self.chart_page.chart_specs]

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
