# README

This folder contains a list of pre-built dashboards for every benchmark in PerfKitBenchmarker and a summary dashboard for all benchmarks. The dashboards can be used by PerfKitExplorer.

See:

`https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/README.md` for setting up PerfKitExplorer and load data into Google BigQuery.

## Setup dashboards
* Once the PerfKitExplorer is deployed, config the the dashboards with your BIGQUERY_PROJECT_ID, BIGQUERY_DATASET and BIGQUERY_TABLE used by PerfKitExplorer by running the following command in this directory:

        sed -i  's/{{ project_id }}/BIGQUERY_PROJECT_ID/g' *.json
        sed -i  's/{{ dataset_name }}/BIGQUERY_DATASET/g' *.json
        sed -i  's/{{ table_name }}/BIGQUERY_TABLE/g' *.json

* Go to PerfKitExplorer site deployed by AppEngine: http://version-dot-EXPLORER_PROJECT_ID.appspot.com
* Click "Upload" button in the "PerfKit Dashboard Administration" to upload the dashboards.

