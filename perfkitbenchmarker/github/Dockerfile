FROM python:3.7

WORKDIR /pkb

COPY requirements.txt /pkb

RUN pip install -r requirements.txt

COPY requirements-testing.txt /pkb

RUN pip install -r requirements-testing.txt

COPY . /pkb

CMD python -m unittest discover -s tests -p '*test.py' -v
