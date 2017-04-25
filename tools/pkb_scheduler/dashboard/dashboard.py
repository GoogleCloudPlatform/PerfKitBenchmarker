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
from site_service import run_site_service

app = Flask(__name__)
app.jinja_env.add_extension("jinja2.ext.do")

SERVICE_PORT = 32422

########################################
# app context stuff
########################################

def connect_service(host, port):
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.connect((host, port))
  return ServiceConnection(s)

def get_site_service_connection():
  if not hasattr(g, "site_service"):
    g.site_service = connect_service("localhost", SERVICE_PORT)
  return g.site_service

@app.teardown_appcontext
def teardown_site_service_connection(error):
  if hasattr(g, "site_service"):
    g.site_service.close()

########################################
# app serving stuff
########################################

@app.route("/")
@app.route("/run", strict_slashes=False)
def index():
  site_service = get_site_service_connection()

  site_service.send_str("get_categories")
  categories = pickle.loads(site_service.recv())

  html = render_template("index.html", categories=categories)

  return make_response(html)

@app.route("/run/<benchmark>")
def benchmark_page(benchmark):
  site_service = get_site_service_connection()

  site_service.send_str("benchmark %s" % benchmark)
  page_context = pickle.loads(site_service.recv())

  html = render_template("benchmark_page.html", **page_context)

  return make_response(html)

@app.route("/run/<benchmark>/<timestamp>")
def benchmark_run_page(benchmark, timestamp):
  site_service = get_site_service_connection()

  site_service.send_str("run %s,%s" % (benchmark, timestamp))
  page_context = pickle.loads(site_service.recv())

  html = render_template("run_page.html", **page_context)

  return make_response(html)

########################################

def start_services(page_spec):
  # Start page service
  site_service_proc = mp.Process(target=run_site_service,
                                 args=("localhost", SERVICE_PORT, page_spec))
  site_service_proc.start()

  return site_service_proc

def main():
  if len(sys.argv) != 2:
    print("Usage: python3 chart_server.py <page_spec>")
    exit()

  page_spec = sys.argv[1]

  # Launch the page and data services
  site_service_proc = start_services(page_spec)

  # Start webserver
  try:
    app.run(host="0.0.0.0", port=8080)
  except KeyboardInterrupt:
    site_service_proc.terminate()

########################################

if __name__ == "__main__":
  main()
