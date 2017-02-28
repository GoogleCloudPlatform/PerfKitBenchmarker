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

from service_util import ServiceConnection
from util import dict_inherit

########################################

class ChartPage:
  def __init__(self, page_spec_path):
    self.page_spec_path = page_spec_path
    self.last_page_spec_update = None
    self.chart_specs = {}
    self.chart_order = []
    self.subpages = {}

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
          chart = chart_specs[chart_name]
          # Apply inheritance
          base = chart.get('inherit')
          if base:
            chart = dict_inherit(base, chart_specs[chart_name])
          # Build chart spec
          print("Building chart spec for %s" % chart_name)
          chart['name'] = chart_name
          chart['width'] = chart['options'].get('width') or 400
          chart['height'] = chart['options'].get('height') or \
                            int(0.75 * chart['width'])
          options_json = json.dumps(chart['options'])
          chart['options'] = options_json

          self.chart_specs[chart_name] = chart
        return True
    return False

class PageServerHandler(socketserver.BaseRequestHandler):
  def handle(self):
    page_store = self.server.page_store
    request = ServiceConnection(self.request)

    page_path = request.recv_str()
    page_html = page_store[page_path]

    print("Sending page %s to %s" % (page_path or 'index',
                                     self.request.getsockname()))
    request.send(page_html)

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
  pass
        
def main():
  if len(sys.argv) != 2:
    print("Usage: python3 chart_server.py <page_spec_yaml>")
    exit()

  page_spec_path = sys.argv[1]

  # Create the shared page store
  mp_manager = mp.Manager()
  page_store = mp_manager.dict()

  root_page = ChartPage(page_spec_path)

  # Start the page spec refresher
  def _chart_page_refresh():
    while True:
      if root_page.maybe_refresh_page_spec():
        # Page spec was updated - rebuild the pages
        page_queue = deque()
        page_queue.append(('', root_page)) # [(path, page)]
        while len(page_queue) > 0:
          path, page = page_queue.pop()
          # Add child pages to the queue
          for subpage_name, subpage in page.subpages.items():
            page_queue.append(('%s/%s' % (path, subpage_name), subpage))
          # Generate page HTML
          chart_specs = [page.chart_specs[name]
                         for name in page.chart_order]
          page_context = {'charts': chart_specs}
          page_store[path] = pickle.dumps(page_context)

      # Only try to refresh page spec every second
      time.sleep(1)

  chart_page_refresh_proc = mp.Process(target=_chart_page_refresh)
  chart_page_refresh_proc.start()

  # Start the page server
  page_server = ThreadedTCPServer(('localhost', 22423), PageServerHandler)
  page_server.page_store = page_store
  page_server.serve_forever()

########################################

if __name__ == "__main__":
  main()
