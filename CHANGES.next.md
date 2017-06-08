Breaking changes:
-

New features:
-

Enhancements:
- Added basic auth support for Mesos provider. (GH-1390)
- Add --failed_run_samples_error_length flag to limit failed run error length (GH-1391)
- Add option to specify latency histogram bucket granularity for fio benchmark (GH-1393)

Bug fixes and maintenance updates:
- Fix type of `--beam_it_timeout` flag (was string, must be integer) (GH-1375)
