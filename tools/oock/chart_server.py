#!/usr/bin/env python3
import multiprocessing as mp
import os
import pickle
import socket
import subprocess
import sys
import yaml

from flask import Flask, Response, make_response, render_template, g

from service_util import ServiceConnection
from page_service import run_page_service

app = Flask(__name__)

########################################
# app context stuff
########################################

def connect_service(host, port):
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.connect((host, port))
  return ServiceConnection(s)

def get_page_service_connection():
  if not hasattr(g, 'page_service'):
    g.page_service = connect_service('localhost', 22422)
  return g.page_service

@app.teardown_appcontext
def teardown_page_service_connection(error):
  if hasattr(g, 'page_service'):
    g.page_service.close()

########################################
# app serving stuff
########################################

@app.route('/')
@app.route('/charts', strict_slashes=False)
def index():
  return chart_page('')

@app.route('/charts/<path:path>')
def chart_page(path):
  # Remove path's trailing slash if it has one
  if path.endswith('/'):
    path = path[:-1]

  page_service = get_page_service_connection()

  page_service.send_str('page %s' % path)
  page_context = pickle.loads(page_service.recv())

  html = render_template('charts_page.html', **page_context)

  return make_response(html)

@app.route('/data/<string:data_source_name>')
def index_page(data_source_name):
  page_service = get_page_service_connection()

  page_service.send_str('data_source %s' % data_source_name)
  data_json = page_service.recv_str()

  return Response(response=data_json,
                  status=200,
                  mimetype='application/json')

########################################

def start_services(page_spec):
  # Start page service
  page_service_proc = mp.Process(target=run_page_service,
                                 args=('localhost', 22422, page_spec))
  page_service_proc.start()

  return page_service_proc

def main():
  if len(sys.argv) != 3:
    print("Usage: python3 chart_server.py <page_spec>")
    exit()

  page_spec = sys.argv[1]

  # Launch the page and data services
  page_service_proc = start_services(page_spec)

  # Start webserver
  try:
    app.run()
  except KeyboardInterrupt:
    page_service_proc.terminate()

########################################

if __name__ == "__main__":
  main()
