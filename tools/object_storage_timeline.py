#!/usr/bin/env python

# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import numpy as np
import matplotlib.collections as mplc
import matplotlib.pyplot as plt
import matplotlib.patches as mpl_patches
import sys


class DraggableXRange:
  def __init__(self, figure, updater):
    self.figure = figure
    self.span = None
    self.start = None
    self.end = None
    self.background = None
    self.updater = updater

  def connect(self):
    'connect to all the events we need'
    self.cidpress = self.figure.canvas.mpl_connect(
        'button_press_event', self.on_press)
    self.cidrelease = self.figure.canvas.mpl_connect(
        'button_release_event', self.on_release)
    self.cidmotion = self.figure.canvas.mpl_connect(
        'motion_notify_event', self.on_motion)

  def on_press(self, event):
    'on button press we will see if the mouse is over us and store some data'
    if event.button != 3:
      # Only continue for right mouse button
      return
    if self.span is not None:
      return

    self.start = event.xdata
    self.end = event.xdata
    self.span = plt.axvspan(self.start, self.end, color='blue', alpha=0.5)

    # draw everything but the selected rectangle and store the pixel buffer
    canvas = self.figure.canvas
    axes = self.span.axes
    canvas.draw()
    self.background = canvas.copy_from_bbox(self.span.axes.bbox)

    # now redraw just the rectangle
    axes.draw_artist(self.span)
    # and blit just the redrawn area
    canvas.blit(axes.bbox)

    self.updater.update(self.start, self.end)

  def on_motion(self, event):
    'on motion we will move the rect if the mouse is over us'
    if self.span is None:
      return

    self.span.remove()

    self.end = event.xdata
    self.span = plt.axvspan(self.start, self.end, color='blue', alpha=0.5)

    canvas = self.figure.canvas
    axes = self.span.axes
    # restore the background region
    canvas.restore_region(self.background)
    # Save the new background
    self.background = canvas.copy_from_bbox(self.span.axes.bbox)

    # redraw just the current rectangle
    axes.draw_artist(self.span)
    # blit just the redrawn area
    canvas.blit(axes.bbox)

    self.updater.update(self.start, self.end)

  def on_release(self, event):
    'on release we reset the press data'
    if event.button != 3:
      # Only continue for right mouse button
      return
    if self.span is None:
      return

    self.span.remove()

    self.start = None
    self.end = None
    self.span = None
    self.background = None

    # redraw the full figure
    self.figure.canvas.draw()

    self.updater.update(self.start, self.end)

  def disconnect(self):
    'disconnect all the stored connection ids'
    self.figure.canvas.mpl_disconnect(self.cidpress)
    self.figure.canvas.mpl_disconnect(self.cidrelease)
    self.figure.canvas.mpl_disconnect(self.cidmotion)


class SelectionUpdate:
  def __init__(self, figure, ax, start_times, latencies):
    self.text = None
    self.figure = figure
    self.ax = ax
    self.start_times = start_times
    self.latencies = latencies

  def update(self, start, end):
    if self.text is not None:
      axes = self.text.axes
      self.text.remove()
      self.text = None
      self.figure.canvas.blit(axes.bbox)
    if start is None:
      assert end is None
      return

    start, end = min(start, end), max(start, end)

    if start == end:
      return

    active_start_indexes = []
    for start_time in self.start_times:
      for i in xrange(len(start_time)):
        if start_time[i] >= start:
          active_start_indexes.append(i)
          break
    active_stop_indexes = []
    for start_time, latency in zip(self.start_times, self.latencies):
      for i in xrange(len(start_time) - 1, -1, -1):
        if start_time[i] + latency[i] <= end:
          active_stop_indexes.append(i + 1)
          break
    active_latencies = [
        self.latencies[i][active_start_indexes[i]:active_stop_indexes[i]]
        for i in range(len(self.latencies))]
    all_active_latencies = np.concatenate(active_latencies)

    qps = len(all_active_latencies) / (end - start)
    latency_min = min(all_active_latencies)
    latency_max = max(all_active_latencies)
    latency_avg = sum(all_active_latencies) / len(all_active_latencies)
    latency_stddev = np.std(all_active_latencies)

    text_str = ('Duration: %s\nQPS: %s\nlatency min: %s\nlatency max: %s\n'
                'latency avg: %s\nlatency stddev: %s'
                % (end - start, qps, latency_min, latency_max,
                   latency_avg, latency_stddev))

    # place a text box in upper left in axes coords
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    self.text = self.ax.text(0.05, 0.95, text_str,
                             transform=self.ax.transAxes, fontsize=14,
                             verticalalignment='top', bbox=props)
    # redraw just the text
    self.text.axes.draw_artist(self.text)
    # blit just the redrawn area
    self.figure.canvas.blit(self.text.axes.bbox)


def GenerateObjectTimeline(file_name, start_times, latencies):
  print("Generating object timeline")
  assert len(start_times) == len(latencies)
  rects = []
  for i, worker_times in enumerate(zip(start_times, latencies)):
    for j, (start_time, latency) in enumerate(np.vstack(worker_times).T):
      rect = mpl_patches.Rectangle((start_time, i + 0.5), latency, 1.0,
                                   color=(0.5 * (j % 2) + 0.5, 0, 0),
                                   linewidth=0)
      rects.append(rect)
  pc = mplc.PatchCollection(rects, match_original=True)
  fig, ax = plt.subplots(figsize=(30, 5))
  ax.add_collection(pc)
  ax.autoscale()
  ax.margins(0.1)
  print("Saving figure as %s" % file_name)
  plt.savefig(file_name, bbox_inches='tight', dpi=1200)
  print("Figured saved. Rendering figure...")

  selection = DraggableXRange(fig, SelectionUpdate(fig, ax, start_times,
                                                   latencies))
  selection.connect()
  plt.show()
  selection.disconnect()


def LoadWorkerOutput(output):
  """Load output from worker processes to our internal format.

  Args:
    output: list of strings. The stdouts of all worker processes.

  Returns:
    A tuple of start_time, latency, size. Each of these is a list of
    numpy arrays, one array per worker process. start_time[i],
    latency[i], and size[i] together form a table giving the start
    time, latency, and size (bytes transmitted or received) of all
    send/receive operations for worker i.

    start_time holds POSIX timestamps, stored as np.float64. latency
    holds times in seconds, stored as np.float64. size holds sizes in
    bytes, stored as np.int64.

    Example:
      start_time[i]  latency[i]  size[i]
      -------------  ----------  -------
               0.0         0.5      100
               1.0         0.7      200
               2.3         0.3      100

  Raises:
    AssertionError, if an individual worker doesn't have the same number of
    start_times, latencies, or sizes.
  """

  start_times = []
  latencies = []
  sizes = []

  for worker_out in output:
    json_out = json.loads(worker_out)

    for stream in json_out:
      assert len(stream['start_times']) == len(stream['latencies'])
      assert len(stream['latencies']) == len(stream['sizes'])

      start_times.append(np.asarray(stream['start_times'], dtype=np.float64))
      latencies.append(np.asarray(stream['latencies'], dtype=np.float64))
      sizes.append(np.asarray(stream['sizes'], dtype=np.int64))

  return start_times, latencies, sizes


def main():
  worker_output = None
  print("Reading worker output")
  with open(sys.argv[1], 'r') as worker_out_file:
    worker_output = json.loads(worker_out_file.read())
  print("Parsing worker output")
  start_times, latencies, _ = LoadWorkerOutput(worker_output)
  GenerateObjectTimeline(sys.argv[2], start_times, latencies)

########################################

if __name__ == '__main__':
  main()
