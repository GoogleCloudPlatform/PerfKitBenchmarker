# Some combinations of python 3.13/C++17 cause build failures in pandas:
# https://github.com/cython/cython/issues/5790
# Avoid it by just picking 3.12.
FROM --platform=linux/amd64 python:3.12 as build

WORKDIR /
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
EXPOSE 5000
COPY . .
ENTRYPOINT [ "./entrypoint.sh" ]
