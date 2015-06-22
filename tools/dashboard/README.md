# README

This folder contains a list of dashboards for every benchmark in
PerfKitBenchmarker. The dashboards can be uploaded and used by PerfKitExplorer.

See:

`https://github.com/GoogleCloudPlatform/PerfKitExplorer` for How to setup
PerfKitExplorer.

## Setup dashboards
From this directory:
  sed -i  's/{{ project_id }}/PERFKIT_EXPLORER_PROJECT/g' *.json
  sed -i  's/{{ dataset_name }}/DATASET_NAME/g' *.json
  sed -i  's/{{ table_name }}/TABLE_NAME/g' *.json

