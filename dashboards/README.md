# README

This folder contains pre-built dashboards for every benchmark in PerfKitBenchmarker and a summary dashboard for all benchmarks. The dashboards can be used by [PerfKitExplorer]
(https://github.com/GoogleCloudPlatform/PerfKitExplorer/blob/master/README.md)
.


## Setup PerfKitExplorer
* See PerfKitExplorer [README.md]
(https://github.com/GoogleCloudPlatform/PerfKitExplorer/blob/master/README.md)
for how to setup PerfKitExplorer and how to setup Google BigQuery dataset and table. 


## Upload PerfKitBenchmarker results into BigQuery tables.
* Using bq command line tool, run (if you upload to a new BigQuery table, a [schema]
  (https://github.com/GoogleCloudPlatform/PerfKitExplorer/blob/master/data/samples_mart/results_table_schema.json)
  file is needed):

        bq load --source_format=NEWLINE_DELIMITED_JSON BIGQUERY_PROJECT_ID:BIGQUERY_DATASET.BIGQUERY_TABLE RESULTS_FILE (BIGQUERY_SCHEMA)

* Using PerfKitBenchmarker bigquery_table flag (only works if the BigQuery table already exists):

        ./pkb.py --benchmarks=BENCHMARK --bigquery_table=BIGQUERY_PROJECT_ID:BIGQUERY_DATASET.BIGQUERY_TABLE


## Setup dashboards
* Once PerfKitExplorer is deployed, configure the the dashboards with your BIGQUERY_PROJECT_ID, BIGQUERY_DATASET and BIGQUERY_TABLE used by PerfKitExplorer by running the following command in this directory:

        sed -i  's/{{ project_id }}/BIGQUERY_PROJECT_ID/g' *.json
        sed -i  's/{{ dataset_name }}/BIGQUERY_DATASET/g' *.json
        sed -i  's/{{ table_name }}/BIGQUERY_TABLE/g' *.json

* Go to PerfKitExplorer site deployed on AppEngine: http://version-dot-EXPLORER_PROJECT_ID.appspot.com
* Click "Upload" button in the "PerfKit Dashboard Administration" page to upload the dashboards.

        
