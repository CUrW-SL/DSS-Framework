FROM ubuntu:16.04
MAINTAINER Hasitha Dhananjaya <hasitha.10@cse.mrt.ac.lk>

WORKDIR /home

RUN apt-get install -y --allow-unauthenticated\
    && libldb.i686 \
    && libXext.i686 \
    && libXrender.i686 \
    && libXtst.i686 \
    && libgcc.i686 \
    && libstdc++.i686 \
    && xorg-x11-server-Xvfb \
    && java-1.6.0-openjdk \
    && zip unzip \
    && glibc-devel \
    && net-tools \
    && gcc \
    && --enablerepo=extras install epel-release \
    && https://centos7.iuscommunity.org/ius-release.rpm \
    && python3.6 \
    && python3-pip \
    && build-dep python-psycopg2


RUN wget http://www.hec.usace.army.mil/software/hec-hms/downloads/hec-hms421-linux.tar.gz \
    && wget https://excellmedia.dl.sourceforge.net/project/jython/jython/2.5.0/jython_installer-2.5.0.jar \
    && wget http://www.hec.usace.army.mil/software/hec-dssvue/downloads/hec-dssvue201-linux.bin.zip

RUN java -jar jython_installer-2.5.0.jar -s -d /usr/share/jython

WORKDIR /usr/share/jython
RUN cp jython.jar jythonlib.jar \
    && zip -r jythonlib.jar Lib

WORKDIR /home
RUN unzip hec-dssvue201-linux.bin.zip \
    && yes | ./hec-dssvue201.bin

WORKDIR /home/hec-dssvue201
RUN rm -f hec-dssvue201-linux.bin.zip \
    && rm -f hec-dssvue201.bin

RUN rm -f /home/hec-dssvue201/jar/sys/jythonlib.jar \
    && mv /usr/share/jython/jythonlib.jar /home/hec-dssvue201/jar/sys

RUN pip install psycopg2

ENTRYPOINT ["./runner.sh"]


