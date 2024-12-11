#!/bin/sh
gunicorn perf_server:app -w 4 --threads 2 --bind 0.0.0.0:5000
