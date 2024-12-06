Want to contribute? Great! First, read this page (including the small print at
the end).

### Before you contribute

Before we can use your code, you must sign the
[Google Individual Contributor License Agreement](https://developers.google.com/open-source/cla/individual?csw=1)
(CLA), which you can do online. The CLA is necessary mainly because you own the
copyright to your changes, even after your contribution becomes part of our
codebase, so we need your permission to use and distribute your code. We also
need to be sure of various other things—for instance that you'll tell us if you
know that your code infringes on other people's patents. You don't have to sign
the CLA until after you've submitted your code for review and a member has
approved it, but you must do it before we can put your code into our codebase.
Before you start working on a larger contribution, you should get in touch with
us first through the issue tracker with your idea so that we can help out and
possibly guide you. Coordinating up front makes it much easier to avoid
frustration later on.

### Code reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose.

### Configuring a local development environment

Reference the setup section in the [README](README.md#installation-and-setup) or
the [beginner tutorial](tutorials/beginner_walkthrough/README.md) for an example
of how to setup a local development environment.

### Fork the repository

If you plan on contributing, create your own fork of PerfKitBenchmarker to
publish your changes. GitHub has
[a great tutorial](https://help.github.com/articles/fork-a-repo/) (we follow the
*Fork & pull* model).

1.  Fork the repository:

    ```
    gh repo fork GoogleCloudPlatform/PerfKitBenchmarker
    ```

2.  Clone the repository:

    ```
    cd $HOME && git clone https://github.com/{github_username}/PerfKitBenchmarker
    ```

    ```
    cd PerfKitBenchmarker/
    ```

3.  Install Python 3 and pip (activate virtualenvs if needed).

4.  Install PerfKitBenchmarker's Python dependencies:

    ```
    pip install -r requirements.txt
    ```

5.  Install PerfKitBenchmarker's test dependencies:

    ```
    pip install -r requirements-testing.txt
    ```

### Create a branch

Start from the master branch of the repository. This is the default.

1.  Create a branch to contain your changes.

    ```
    git checkout -b <your-branch-name>
    ```

2.  Make your modifications to the code in one or more commits
    [(guidelines for useful git commit messages)](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html).

3.  Update the appropriate sections in CHANGES.next.md with a summary of your
    changes. For example, under "Bug fixes and maintenance updates" you might
    put something like: `- Fix crazy bug X (GH-<insert PR number here> from
    @<insert your username here>)`

4.  Push your changes to GitHub

    ```
    git push origin <your-branch-name>
    ```

5.  Create a pull request to integrate your change into the `master` branch.
    This can be done on github.com, or through CLIs. See GitHub's documentation
    [here](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)
    for more details. When making a pull request on GitHub, the base branch of
    the Pull Request should be `master`. This is usually the default.

### Developer documentation

We have added a lot of comments into the code to make it easy to;

*   Add new benchmarks (eg: --benchmarks=<new benchmark>)
*   Add new package/os type support (eg: --os_type=<new os type>)
*   Add new providers (eg: --cloud=<new provider>)
*   etc...

Even with lots of comments we make to support more detailed documentation. You
will find the documentation we have on the
[Wiki pages](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki).
Missing documentation you want? Start a page and/or open an
[issue](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/issues) to get
it added.

### Updating the GitHub Pages site:

The site requires
[Jekyll](https://docs.github.com/en/pages/setting-up-a-github-pages-site-with-jekyll/about-github-pages-and-jekyll)
in order to run. Use `bundle install` and `bundle update` to update the
dependencies in `Gemfile.lock`. `bundle exec jekyll serve` can be used to test
the site locally.

### The small print

Contributions made by corporations are covered by a different agreement than the
one above, the Software Grant and Corporate Contributor License Agreement. ~~~
