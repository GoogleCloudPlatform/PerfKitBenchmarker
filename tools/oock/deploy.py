#!/usr/bin/env python3

import sys

from cheroot import wsgi
from chart_server import app, start_services

def main():
  if len(sys.argv) != 2:
    print("Usage: python3 deploy.py <page_spec>")
    exit()

  page_spec = sys.argv[1]

  # Launch the services
  page_service_proc = start_services(page_spec)

  dispatcher = wsgi.WSGIPathInfoDispatcher({'/': app})
  server = wsgi.WSGIServer(('0.0.0.0', 8080), wsgi_app=dispatcher)

  try:
    server.start()
  except KeyboardInterrupt:
    server.stop()
    page_service_proc.terminate()

########################################

if __name__ == '__main__':
  main()
