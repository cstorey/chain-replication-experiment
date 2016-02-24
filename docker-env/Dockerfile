FROM ubuntu:14.04

RUN apt-get update
RUN apt-get -y install build-essential
RUN apt-get -y install wget
WORKDIR /tmp
RUN wget -O rocksdb-4.2.tar.gz https://github.com/facebook/rocksdb/archive/v4.2.tar.gz
RUN tar -zxvf rocksdb-4.2.tar.gz
WORKDIR /tmp/rocksdb-4.2
RUN make shared_lib
RUN make install-shared INSTALL_PATH=/usr
