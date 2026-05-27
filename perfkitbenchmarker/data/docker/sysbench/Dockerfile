###
### Copied from https://github.com/akopytov/sysbench/blob/master/Dockerfile, updated to use Ubuntu 24.04 base image,
### and pinned to stable sysbench version via build arg.
###
ARG base_image=ubuntu:24.04

FROM $base_image

ARG sysbench_version=1.0.20

RUN apt-get update

RUN apt-get -y install make automake libtool pkg-config libaio-dev git

# For MySQL support
RUN apt-get -y install libmysqlclient-dev libssl-dev

# For PostgreSQL support
RUN apt-get -y install libpq-dev

RUN git clone --branch $sysbench_version https://github.com/akopytov/sysbench.git sysbench

WORKDIR sysbench
RUN ./autogen.sh
RUN ./configure --with-mysql --with-pgsql
RUN make -j
RUN make install

WORKDIR /root
RUN rm -rf sysbench

ENTRYPOINT sysbench
