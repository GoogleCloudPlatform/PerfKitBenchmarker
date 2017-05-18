Want to contribute? Great! First, read this page (including the small print at the end).

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

### The small print
Contributions made by corporations are covered by a different agreement than
the one above, the Software Grant and Corporate Contributor License Agreement.

### Change log
After creating a pull request, update the appropriate sections in CHANGES.next.md with a summary of your changes, then commit and push to the branch you are trying to merge.

For example, under "Bug fixes and maintenance updates" you might put something like:
```
- Fix crazy bug X (GH-<insert PR number here> from @<insert your username here>)
```

Then:
```
git commit -m "Updated CHANGES.next.md"
git push origin fix_crazy_bug_x
```

### Configuring a local development environment
This can optionally done within a [`virtualenv`](https://virtualenv.pypa.io/en/latest/).

### Fork the repository

If you plan on contributing, create your own fork of PerfKitBenchmarker to publish your changes.
GitHub has [a great tutorial](https://help.github.com/articles/fork-a-repo/) (we follow the *Fork & pull* model).

- Clone the repository:
```
$ git clone git@github.com:<your-user-name>/PerfKitBenchmarker.git && cd PerfKitBenchmarker
```
- Install Python 2.7.
- Install [pip](https://pypi.python.org/pypi/pip):
```
$ sudo apt-get install -y python-pip
```
- Install PerfKitBenchmarker's Python dependencies:
```
$ [sudo] pip install -r requirements.txt
```
- Install PerfKitBenchmarker's test dependencies:
```
$ [sudo] pip install tox
```
- Install git pre-commit hooks, to enable linting and copyright checks:
```
$ hooks/install.sh
```

### Create a branch

Start from the master branch of the repository.  This is the default. 

- Create a branch to contain your changes.
```
$ git checkout -b <your-branch-name>
```

- Make your modifications to the code in one or more commits [(guidelines for useful git commit messages)](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html).
- Push your changes to GitHub
```
$ git push origin <your-branch-name>
```
- Create a pull request to integrate your change into the `master` branch.
This can be done on github.com, or by installing and using the command line tool [`hub`](https://github.com/github/hub).
```
$ hub pull-request -h <your-branch-name>
```
When making a pull request on GitHub, please the base branch of the Pull Request should be `master`.  This is usually the default.  

### Developer documentation
We have added a lot of comments into the code to make it easy to;
* Add new benchmarks (eg: --benchmarks=<new benchmark>)
* Add new package/os type support (eg: --os_type=<new os type>)
* Add new providers (eg: --cloud=<new provider>)
* etc...

Even with lots of comments we make to support more detailed documention.  You will find the documatation we have on the [Wiki pages] (https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki).  Missing documentation you want?  Start a page and/or open an [issue] (https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/issues) to get it added.
