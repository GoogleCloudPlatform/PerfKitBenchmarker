---
title: PKB Website Development
layout: page
---

# PKB Website Development

The PKB website uses [GitHub Pages](https://docs.github.com/en/pages) for
hosting and [GitHub Actions](https://github.com/features/actions) for build and
deploy. This uses source code in the `docs` folder to automatically deploy the
website (hosted in the `gh-pages` branch) upon each commit. See the
[config](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/.github/workflows/mkdocs-gh-pages.yml)
for the config.

<!-- copybara:strip_begin(internal) -->

Recommended development workflow is to use your cloudtop since package
installation on your laptop is subject to Santa rules. Normal go/jetski has
remote dev support for working with local non-workspace files on your cloudtop
and works well for this use case.

<!-- copybara:strip_end -->

## Setup and Running MkDocs

Clone PKB and then install prerequisites with the following command:

```bash
pip install -r requirements-docs.txt
```

From `~/PerfKitBenchmarker` (or your local repo root), run `mkdocs`:

```bash
mkdocs serve --livereload -w .
```

<!-- copybara:strip_begin(internal) -->

Then on a separate tab on Mac/Linux, set up SSH port forwarding to your remote
machine:

```bash
ssh -L8000:localhost:8000 liubrandon-us-east5.c.googlers.com
```

<!-- copybara:strip_end -->
