FROM ubuntu:16.04
MAINTAINER Hasitha Dhananjaya <hasitha.10@cse.mrt.ac.lk>

RUN apt-get update \
    && apt-get install -y software-properties-common \
    && apt-get update \
    && apt-get install -y --allow-unauthenticated \
        wget \
        libpng-dev \
        build-essential \
        apt-utils \
        python3.6 \
        python3-pip

RUN mkdir distributed_hechms

WORKDIR distributed_hechms

CMD ['runner.sh']

VOLUME /home/distributed_hechms

