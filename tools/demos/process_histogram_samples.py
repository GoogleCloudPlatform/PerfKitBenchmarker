#!/usr/bin/env python2.7

"""Transform PKB json output into a csv format."""

from __future__ import print_function
import collections
import json
import re
import sys


def main():
  if len(sys.argv) != 4:
    print('usage: %s samples_file.json metric_name data_label' % sys.argv[0])
    sys.exit(1)

  latency_histogram_by_label = collections.defaultdict(
      lambda: collections.defaultdict(int))
  total_samples = collections.defaultdict(int)
  with open(sys.argv[1]) as samples_file:
    for line in samples_file:
      sample = json.loads(line)
      if sample['metric'] == sys.argv[2]:
        labels = sample['labels']
        regex = r'\|%s:(.*?)\|' % sys.argv[3]
        label = re.search(regex, labels).group(1)
        histogram = json.loads(
            re.search(r'\|histogram:(.*?)\|', labels).group(1))
        for bucket, count in histogram.iteritems():
          latency_histogram_by_label[label][float(bucket)] += int(count)
          total_samples[label] += int(count)
  for label, histogram in latency_histogram_by_label.iteritems():
    running_count = 0
    for bucket in sorted(histogram):
      running_count += histogram[bucket]
      percentile = 100.0 * running_count / total_samples[label]
      print(','.join((label, str(bucket), str(percentile))))


if __name__ == '__main__':
  main()
