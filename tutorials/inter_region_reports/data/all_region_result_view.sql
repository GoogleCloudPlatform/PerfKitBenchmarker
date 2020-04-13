#standardSQL
# Use temp functions for more readable queries.
CREATE TEMPORARY FUNCTION re(source STRING, match STRING)
AS (REGEXP_EXTRACT(source, r'\|' || match || r':(.*?)\|'));

CREATE TEMPORARY FUNCTION re_zone(source STRING, match STRING)
AS (REGEXP_EXTRACT(source, r'\|' || match || r':(.*?-.*?)-.*?\|'));

SELECT
  value,
  unit,
  owner,
  run_uri,
  sample_uri,
  test,
  timestamp,
  official,
  metric,
  labels,
  product_name,
  TIMESTAMP_MICROS(CAST(timestamp * 1000000 AS int64)) AS thedate,
  re(labels, 'vm_1_cloud') AS vm_1_cloud,
  re(labels, 'vm_2_cloud') AS vm_2_cloud,
  re(labels, 'vm_1_zone') AS vm_1_zone,
  re(labels, 'vm_2_zone') AS vm_2_zone,
  re(labels, 'sending_zone') AS sending_zone,
  re(labels, 'receiving_zone') AS receiving_zone,
  re_zone(labels, 'sending_zone') AS sending_region,
  re_zone(labels, 'receiving_zone') AS receiving_region,
  re(labels, 'sending_machine_type') AS sending_machine_type,
  re(labels, 'receiving_machine_type') AS receiving_machine_type,
  re(labels, 'vm_1_machine_type') AS vm_1_machine_type,
  re(labels, 'vm_2_machine_type') AS vm_2_machine_type,
  re(labels, 'sending_thread_count') AS sending_thread_count,
  re(labels, 'ip_type') AS ip_type,
  re(labels, 'vm_1_gce_network_tier') AS vm_1_gce_network_tier,
  re(labels, 'vm_2_gce_network_tier') AS vm_2_gce_network_tier,
  re(labels, 'confidence_iter') AS confidence_iter,
  re(labels, 'confidence_width_percent') AS confidence_width_percent,
  re(labels, 'max_iter') AS max_iter,
  re(labels, 'runtime_in_seconds') AS runtime_in_seconds,
FROM `<PROJECT_ID>.pkb_results.all_region_results`
WHERE
  (
    REGEXP_EXTRACT(
      labels, r'\|sending_zone:(.*?-.*?)-.*?\|'
    ) != REGEXP_EXTRACT(labels, r'\|receiving_zone:(.*?-.*?)-.*?\|')
  )
