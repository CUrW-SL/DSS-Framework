FROM ubuntu:16.04
MAINTAINER Hasitha Dhananjaya <hasitha.10@cse.mrt.ac.lk>

WORKDIR /home

RUN apt-get update \
    && apt-get install -y software-properties-common \
    && apt-get update \
    && apt-get install -y --allow-unauthenticated \
            wget \
            python3.6 \
            python3-pip

ENTRYPOINT ["./runner.sh"]


