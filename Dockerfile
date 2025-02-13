ARG PYTHON_VERSION=3.9

FROM python:${PYTHON_VERSION}

WORKDIR /pkb

SHELL ["/bin/bash", "-c"]
COPY requirements.txt /pkb

RUN pip install -r requirements.txt

COPY . /pkb

RUN pip install -r requirements-testing.txt

# Run tests in isolation
CMD set -Eeuo pipefail; for test in $(find tests/ | grep "test.py"); do echo ; echo "Running $test ...";  python -m unittest $test -v ; done
