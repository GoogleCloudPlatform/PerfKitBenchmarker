ARG PYTHON_VERSION=3.9

FROM python:${PYTHON_VERSION}

WORKDIR /pkb

COPY requirements.txt /pkb

RUN pip install -r requirements.txt

COPY . /pkb

RUN pip install -r requirements-testing.txt

CMD python -m unittest discover -s tests -p '*test.py' -v
