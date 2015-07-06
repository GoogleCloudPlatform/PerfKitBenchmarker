# README

This folder contains [Git hooks][1] for use in developing PerfKitBenchmarker,
mostly adapted from [kubernetes][2].
Currently they ensure that all Python and shell scripts contain an Apache
2 license header, and that [`flake8`][3] runs without generating lint errors.
Install them by running:

    hooks/install.sh

from the root of the repository. This will symlink the hooks into `.git/hooks/`.

The pre-push hook is currently opt-in. To enable it, set the `PKB_BASE`
environment variable to the name of a base branch, for example
`PKB_BASE=upstream/dev` for a fork, or `PKB_BASE=origin/dev` when
working on the main repository.

You can also run checks manually:

```bash
# Run all checks on all files under version control.
hooks/check-everything

# Run all checks on a specific set of files.
hooks/check FILE ...
```

[1]: http://git-scm.com/docs/githooks
[2]: http://github.com/GoogleCloudPlatform/kubernetes
[3]: https://pypi.python.org/pypi/flake8


# Implementation notes

The hook implementation is split into three layers:

- Toplevel hook scripts determine which files need to be checked, run the
  `hooks/check` script on them as needed, and report back errors. They use a
  nonzero return code when actions should be blocked (`commit-msg` or
  `pre-push`), and do other actions where needed. (`prepare-commit-msg` modifies
  the message.)

- The `hooks/check` script expects a list of files to check as arguments, and
  runs individual `lib/check-*` scripts on them. It prints optional
  human-readable diagnostics on STDERR, a formatted report on STDOUT (including
  offending filenames where available), and returns a nonzero exit code if
  any checks failed.

- `lib/check-*` scripts take a list of files to check as arguments. They can
  report file-specific issues by printing a list of bad files on STDOUT along
  with a zero exit code, or exit with a nonzero exit code if they find issues
  that can't be associated with a specific file such as a failed unit test or
  invalid testing setup.
