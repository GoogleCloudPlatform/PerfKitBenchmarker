import datetime
import sys
import time

END_COLOR = '\033[0m'

def log_info(msg):
  OK_GREEN = '\033[92m'
  timestamp = datetime.datetime \
                      .fromtimestamp(time.time()) \
                      .strftime('%Y-%m-%d %H:%M:%S')
  print('%s%s INFO%s\t%s\n' % (OK_GREEN, timestamp, END_COLOR, msg))
  sys.stdout.flush()
  sys.stderr.flush()

def log_error(msg):
  FAIL = '\033[91m'
  timestamp = datetime.datetime \
                      .fromtimestamp(time.time()) \
                      .strftime('%Y-%m-%d %H:%M:%S')
  print('%s%s ERRO%s\t%s\n' % (FAIL, timestamp, END_COLOR, msg))
  sys.stdout.flush()
  sys.stderr.flush()

def log_warning(msg):
  WARNING = '\033[93m'
  timestamp = datetime.datetime \
                      .fromtimestamp(time.time()) \
                      .strftime('%Y-%m-%d %H:%M:%S')
  print('%s%s WARN%s\t%s\n' % (WARNING, timestamp, END_COLOR, msg))
  sys.stdout.flush()
  sys.stderr.flush()
