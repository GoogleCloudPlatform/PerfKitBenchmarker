#!/usr/bin/env python3
import json
import multiprocessing as mp
import os
import pickle
import socketserver
import sys
import time
import yaml

from collections import deque

from data_service import run_data_refresh_loop
from service_util import ServiceConnection
from util import dict_inherit

########################################

class ChartPage:
  def __init__(self, page_spec_path):
    self.page_spec_path = page_spec_path
    self.last_page_spec_update = None
    self.name = ''
    self.title = ''
    self.chart_specs = {}
    self.charts_layout = []
    self.subpages = {} # { <relative path from parent> : ChartPage }
    self.data_sources = {}

  def maybe_refresh_page_spec(self):
    page_spec_dir = os.path.dirname(self.page_spec_path)
    # Get the list of subpage paths. This will be overwritten if the page spec
    # yaml ends up getting reloaded. At the end of this function, we maybe
    # refresh each subpage recursively
    subpages = [subpage_path for subpage_path in self.subpages.keys()]

    modified_time = os.path.getmtime(self.page_spec_path)
    reloaded = False
    if modified_time != self.last_page_spec_update:
      print("Refreshing page spec: %s" % self.page_spec_path)
      self.last_page_spec_update = modified_time

      page_spec = None
      with open(self.page_spec_path) as page_spec_file:
        # Try to parse the chart page spec yaml
        try:
          page_spec = yaml.load(page_spec_file.read())
        except:
          sys.stderr.write("ERROR: Chart page spec yaml is not valid yaml: %s\n" % self.page_spec_path)

      if page_spec is None:
        sys.stderr.write("ERROR: Failed to read the chart page spec yaml: %s\n" % self.page_spec_path)
      else:
        self.title = page_spec['title']
        self.name = self.title.lower().replace(' ', '_')

        subpages = page_spec.get('subpages') or []
        chart_specs = page_spec.get('chart_specs') or {}

        # Get charts layout
        self.charts_layout = page_spec.get('charts_layout') or \
            [[chart_name] for chart_name in chart_specs.keys()]
        # Make sure rows in charts_layout with only a single cell are lists also
        for i, row in enumerate(self.charts_layout):
          if type(row) is str:
            self.charts_layout[i] = [row]
          else:
            assert type(row) is list

        # Build the chart specs
        for chart_name in chart_specs.keys():
          chart = chart_specs[chart_name]
          # Apply inheritance
          base = chart.get('inherit')
          if base:
            chart = dict_inherit(base, chart_specs[chart_name])
          # Extract the data source
          self.data_sources[chart_name] = chart['data_source']
          # Build chart spec
          print("Building chart spec for %s" % chart_name)
          chart['data_source'] = chart_name
          chart['name'] = chart_name
          chart['width'] = chart['options'].get('width') or 400
          chart['height'] = chart['options'].get('height') or \
                            int(0.75 * chart['width'])
          options_json = json.dumps(chart['options'])
          chart['options'] = options_json

          self.chart_specs[chart_name] = chart
        reloaded = True
    # Only keep subpages that are still in the spec
    self.subpages = { key: self.subpages[key]
                      for key in subpages if key in self.subpages }
    # create and/or reload subpages
    for rel_subpage_path in subpages:
      subpage = self.subpages.get(rel_subpage_path)
      subpage_path = os.path.join(page_spec_dir, rel_subpage_path)
      if not subpage:
        subpage = ChartPage(subpage_path)
        self.subpages[rel_subpage_path] = subpage
      # Maybe refresh the subpage and update our 'reloaded' bool
      reloaded = subpage.maybe_refresh_page_spec() or reloaded
    return reloaded

########################################

class PageServerHandler(socketserver.BaseRequestHandler):
  def handle(self):
    page_store = self.server.page_store
    data_store = self.server.data_store
    request = ServiceConnection(self.request)

    cmd, args = request.recv_str().split(' ', 1)
    if cmd == 'page':
      page_path = args
      print("Sending page %s to %s" % (page_path or 'index',
                                       self.request.getsockname()))
      request.send(page_store[page_path])
    elif cmd == 'data_source':
      data_source = args
      print("Sending data source %s to %s" % (data_source or 'index',
                                              self.request.getsockname()))
      request.send_str(json.dumps(data_store[data_source]))

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
  pass

########################################

def run_page_service(host, port, page_spec_path):
  # Create the shared page store
  mp_manager = mp.Manager()
  page_store = mp_manager.dict()
  data_source_store = mp_manager.dict()
  data_store = mp_manager.dict()

  def _chart_page_refresh():
    root_page = ChartPage(page_spec_path)
    while True:
      if root_page.maybe_refresh_page_spec():
        # Page spec was updated - rebuild the pages
        page_queue = deque()
        page_queue.append(([], '', root_page)) # [(ancestors, path, page)]
        while len(page_queue) > 0:
          ancestors, path, page = page_queue.pop()
          path_prefix = '%s/' % path if path else ''
          # Add child pages to the queue
          for subpage in page.subpages.values():
            page_queue.append((ancestors + [(page.title, path)],
                               path_prefix + subpage.name, subpage))
          # Generate page HTML
          page_context = {
            'title': page.title,
            'charts': page.chart_specs,
            'charts_layout': page.charts_layout,
            'ancestors': ancestors,
            'subpages': [(path_prefix + subpage.name, subpage.title)
                         for subpage in page.subpages.values()]
          }
          page_store[path] = pickle.dumps(page_context)
          # Add the data sources to the data source store
          for name, data_source in page.data_sources.items():
            data_source_store[name] = data_source
      # Only try to refresh page spec every second
      time.sleep(1)

  # Start the page spec refresher
  chart_page_refresh_proc = mp.Process(target=_chart_page_refresh)
  chart_page_refresh_proc.daemon = True
  chart_page_refresh_proc.start()

  # Start the data refresher
  data_refresh_proc = mp.Process(target=run_data_refresh_loop,
                                 args=(data_source_store, data_store))
  data_refresh_proc.daemon = True
  data_refresh_proc.start()

  # Start the page server
  page_server = ThreadedTCPServer((host, port), PageServerHandler)
  page_server.page_store = page_store
  page_server.data_store = data_store
  page_server.serve_forever()
        
def main():
  if len(sys.argv) != 2:
    print("Usage: python3 page_service.py <page_spec_yaml>")
    exit()
  page_spec_path = sys.argv[1]
  run_page_service('localhost', 22422, page_spec_path)

########################################

if __name__ == "__main__":
  main()
