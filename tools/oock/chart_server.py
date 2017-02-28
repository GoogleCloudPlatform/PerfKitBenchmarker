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
from data_service import run_data_service
from page_service import run_page_service

app = Flask(__name__)

########################################
# app context stuff
########################################

def connect_service(host, port):
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.connect((host, port))
  return ServiceConnection(s)

def get_data_service_connection():
  if not hasattr(g, 'data_service'):
    g.data_service = connect_service('localhost', 22422)
  return g.data_service

@app.teardown_appcontext
def teardown_data_service_connection(error):
  if hasattr(g, 'data_service'):
    g.data_service.close()

def get_page_service_connection():
  if not hasattr(g, 'page_service'):
    g.page_service = connect_service('localhost', 22423)
  return g.page_service

@app.teardown_appcontext
def teardown_page_service_connection(error):
  if hasattr(g, 'page_service'):
    g.page_service.close()

########################################
# app serving stuff
########################################

@app.route('/')
def index():
  return chart_page('')

@app.route('/charts/<path:path>')
def chart_page(path):
  page_service = get_page_service_connection()

  page_service.send_str(path)
  page_context = pickle.loads(page_service.recv())

  html = render_template('charts_page.html', **page_context)

  return make_response(html)

@app.route('/data/<string:data_source_name>')
def index_page(data_source_name):
  data_service = get_data_service_connection()

  data_service.send_str(data_source_name)
  data_json = data_service.recv_str()

  return Response(response=data_json,
                  status=200,
                  mimetype='application/json')

########################################

def start_services(data_sources_spec, page_spec):
  # Start data service
  data_service_proc = mp.Process(target=run_data_service,
                                 args=('localhost', 22422, data_sources_spec))
  data_service_proc.start()
  # Start page service
  page_service_proc = mp.Process(target=run_page_service,
                                 args=('localhost', 22423, page_spec))
  page_service_proc.start()

  return data_service_proc, page_service_proc

def main():
  if len(sys.argv) != 3:
    print("Usage: python3 chart_server.py <data_sources_spec> <page_spec>")
    exit()

  data_sources_spec = sys.argv[1]
  page_spec = sys.argv[2]

  # Launch the page and data services
  data_service_proc, page_service_proc = \
      start_services(data_sources_spec, page_spec)

  # Start webserver
  try:
    app.run()
  except KeyboardInterrupt:
    data_service_proc.terminate()
    page_service_proc.terminate()

########################################

if __name__ == "__main__":
  main()
