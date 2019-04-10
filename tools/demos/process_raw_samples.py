#!/usr/bin/env python2.7

"""Transform PKB json output into a csv format."""

from __future__ import print_function
import json
import re
import sys


def main():
  if len(sys.argv) != 4:
    print('usage: %s samples_file.json metric_name data_label' % sys.argv[0])
    sys.exit(1)
  with open(sys.argv[1]) as samples_file:
    for line in samples_file:
      sample = json.loads(line)
      if sample['metric'] == sys.argv[2]:
        regex = r'\|%s:(.*?)\|' % sys.argv[3]
        data_label = re.search(regex, sample['labels']).group(1)
        print(','.join((data_label, str(sample['value']))))


if __name__ == '__main__':
  main()
