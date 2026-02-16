# Some combinations of python 3.13/C++17 cause build failures in pandas:
# https://github.com/cython/cython/issues/5790
# Avoid it by just picking 3.12.
FROM --platform=linux/amd64 python:3.12 as build

# 1. Add Debian Sid (Unstable) repos, install OpenSSL, then remove the repo to keep system stable
RUN echo "deb http://deb.debian.org/debian sid main" >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y -t sid openssl libssl3 libssl-dev && \
    sed -i '/sid/d' /etc/apt/sources.list && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
EXPOSE 5000
COPY . .
ENTRYPOINT [ "./entrypoint.sh" ]
