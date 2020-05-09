FROM ubuntu:16.04
MAINTAINER Hasitha Dhananjaya <hasitha.10@cse.mrt.ac.lk>

WORKDIR /home

RUN apt-get update \
    && apt-get install -y software-properties-common \
    && add-apt-repository ppa:ubuntugis/ppa \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y --allow-unauthenticated \
            wget \
            python3.6 \
            python3-pip

RUN pip3 install Flask \
                 Flask-JSON \
                 pandas \
                 geopandas \
                 shapely \
                 scipy \
                 mysql-connector

RUN mkdir curw

WORKDIR /home/curw

COPY distributed_hec.tar.gz /home/curw/distributed_hec.tar.gz

ENTRYPOINT ["./runner.sh"]


