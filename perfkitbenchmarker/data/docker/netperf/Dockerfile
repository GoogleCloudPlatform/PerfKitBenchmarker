FROM ubuntu:14.04

RUN apt-get update && apt-get install -y gcc make curl

RUN curl -LO https://github.com/HewlettPackard/netperf/archive/netperf-2.7.0.tar.gz && tar -xzf netperf-2.7.0.tar.gz
RUN cd netperf-netperf-2.7.0 && ./configure && make && make install

CMD ["/usr/local/bin/netserver", "-D"]
