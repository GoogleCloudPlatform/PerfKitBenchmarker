#!/usr/bin/env python3
import json
import os
import sys
import yaml

from http.server import BaseHTTPRequestHandler, HTTPServer

from page_gen import (build_chart_html, cell_to_js, build_data_table_js,
                      build_chart_js, build_chart_page)

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

  # Filter by sample key/values
  for f in data_source['filters']:
    select = f['select']
    condition = ''.join(f['condition'])
    _filter_select_and_condition(samples, select, condition)

  # Build an empty data table with only labels filled out (first column)
  label_column = data_source['label_column']
  label_column_field = label_column['field']
  label_column_parse = eval('lambda %s: %s' % (label_column_field,
                                               label_column['parse']))
  if label_column['source'] == 'metadata':
    extract_label = lambda s: get_label(s, label_column_field)
  else:
    assert label_column['source'] == 'sample'
    extract_label = lambda s: sample[label_column_field]

  # Extract data by column
  data = []
  num_series_columns = len(data_source['series_columns']) - 1
  for i, series_column in enumerate(data_source['series_columns']):
    s = samples[:]
    # Create the lambda used to parse this series column's cells
    series_column_parse = eval('lambda value: ' + series_column['parse'])
    # Filter using this series column's condition
    select = series_column['select']
    condition = series_column['condition']
    if type(condition) is list:
      # If the condition is actually a list of strings, join them
      condition = ''.join(condition)
    _filter_select_and_condition(s, select, condition)
    # Build rows out of each remaining sample
    for sample in s:
      row = [label_column_parse(extract_label(sample))]
      row += [None] * i
      row.append(series_column_parse(sample['value']))
      row += [None] * (num_series_columns - i)
      data.append(row)
  # Sort rows by label if the label column contains numbers
  if label_column.get('order') is not None:
    data = sorted(data, key=lambda x: x[0])
    if label_column['order'] == 'decreasing':
      data = reversed(data)
    else:
      assert label_column['order'] == 'increasing'
  return data

class ChartPage:
  def __init__(self, chart_specs_path):
    self.chart_specs = None
    self.chart_specs_path = chart_specs_path
    self.last_chart_specs_update = None
    
  def refresh_chart_data(self):
    self.refresh_chart_specs()
    for chart in self.chart_specs:
      modified_time = os.path.getmtime(chart['data_source']['path'])
      if modified_time != chart['data_source']['last_update']:
        # Need to pull from this data source again
        print("Refreshing chart data for: " + chart['name'])
        chart['data_source']['last_update'] = modified_time
        chart['data'] = pull_chart_data(chart['data_source'])

  def refresh_chart_specs(self):
    modified_time = os.path.getmtime(self.chart_specs_path)
    if modified_time != self.last_chart_specs_update:
      print("Refreshing chart specs")
      self.last_chart_specs_update = modified_time
      chart_specs = None
      with open(self.chart_specs_path) as chart_specs_file:
        # Try to parse the chart page spec yaml
        try:
          chart_specs = yaml.load(chart_specs_file.read())
        except:
          sys.stderr.write("ERROR: Chart page spec yaml is not valid yaml\n")
      if chart_specs is None:
        sys.stderr.write("ERROR: Failed to read the chart page spec yaml\n")
      else:
        chart_specs = chart_specs['chart_specs']
        for chart_name, chart in chart_specs.items():
          # Set some fields we need chart specs to have internally
          chart['name'] = chart_name
          chart['data_source']['last_update'] = None
          chart['data'] = []
        # Convert the chart_specs dictionary into a list
        self.chart_specs = [chart_spec for chart_spec in chart_specs.values()]

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
    print("Usage: python3 chart_server.py <chart_specs_json>")
    exit()

  port = 8080
  chart_specs_path = sys.argv[1]

  chart_page = ChartPage(chart_specs_path)

  server_address = ('0.0.0.0', port)
  chart_server = ChartServerFactory(chart_page)
  httpd = HTTPServer(server_address, chart_server)
  print('Starting chart server...')
  httpd.serve_forever()

########################################

if __name__ == "__main__":
  main()
