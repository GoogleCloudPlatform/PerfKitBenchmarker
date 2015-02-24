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

### Configuring a local development environment
This can optionally done within a virtualenv.

- Clone the repository:
```
$ git clone git@github.com:GoogleCloudPlatform/PerfKitBenchmarker.git -b dev && cd PerfKitBenchmarker
```
- Install Python 2.7.
- Install pip: 
```
$ sudo apt-get install -y python-pip
```
- Install PerfKitBenchmarker python dependencies:
```
$ [sudo] pip install -r requirements.txt
```
- Install PerfKitBenchmarker test dependencies:
```
$ [sudo] pip install -r test-requirements.txt
```
- Install git pre-commit hooks, to enable linting and copyright checks:
```
$ hooks/install.sh
```

### Create a change list (CL)
- Start on GitHub and create a fork off of the project.  This will remain private unit the top level project is public.
- From your new repository clone the code using the steps detailed in configuring a local dev environment above.
- Start from dev in your fork
```
$ git checkout dev
```
- Create a branch for your changes.
```
$ git checkout -b <your-branch-name>
```
- Make your modifications to the code in one or more commits (guidelines for useful git commit messages).
- Push your changes to GitHub
```
$ git push origin
```
- Create a pull-request to integrate your change into dev. This can be done on github.com, or by installing and using the command line tool gh.
```
$ gh pull-request -h <your-branch-name> -b dev
```
