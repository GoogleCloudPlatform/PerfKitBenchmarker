# README

`side_by_side.py` runs PerfKitBenchmarker under two different revisions, and
outputs a comparison of the results as JSON and HTML.

See:

* `sample.html` for sample HTML output.
* `sample.json` for sample JSON output.
* `./side_by_side.py --help` for a description of command line usage.

## Requirements

Listed in `requirements.txt` and `test-requirements.txt` at the root of this repository.

From this directory:

    pip install -r ../../requirements.txt
    pip install -r ../../test-requirements.txt

## Example: testing two different revisions

    ./side_by_side.py --base origin/master --head origin/dev \
      --flags='--cloud GCP --machine_type n1-standard-4 --benchmarks ping' \
      master_vs_dev.json master_vs_dev.html

Creates a JSON file with run results in `master_vs_dev.json`, and an HTML report in `master_vs_dev.html`.

## Example: comparing results between OS images

Here we fix the revision (`origin/dev`), and vary the operating system image
used:

    ./side_by_side.py --base origin/dev --head origin/dev \
      --flags='--cloud GCP --machine_type n1-standard-4 --benchmarks ping' \
      --base-flags='--image debian-7-backports' \
      --head-flags='--image ubuntu-14-04' \
      debian_vs_ubuntu.json debian_vs_ubuntu.html

The value of `--flags` is passed to both revisions. `--base-flags` and
`--head-flags` can be used to vary command-line options between runs.
