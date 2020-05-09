# Start with CentOS 7
FROM centos:7

# Goto installation point
WORKDIR /home

#-------------------------------------Start install hec-hms------------------------------------------
# Install dependencies
RUN yum -y install \
    wget \
    libldb.i686 \
    libXext.i686 \
    libXrender.i686 \
    libXtst.i686 \
    libgcc.i686 \
    libstdc++.i686 \
    xorg-x11-server-Xvfb \
    git

RUN mkdir -p curw/distributed_hec \
    && mkdir -p curw/git

WORKDIR /home/curw/distributed_hec

# Download hec-hms421-linux.tar.gz
RUN wget 'http://www.hec.usace.army.mil/software/hec-hms/downloads/hec-hms421-linux.tar.gz' \
    && wget 'https://files.pythonhosted.org/packages/f3/e0/8949888568534046c5c847d26c89a05c05f3151ab06728dbeca2d1621002/simplejson-2.5.2.tar.gz'

# Unzip tar file
RUN tar -xvzf hec-hms421-linux.tar.gz \
    && tar -xvzf simplejson-2.5.2.tar.gz

#Remove tar file
RUN rm -f hec-hms421-linux.tar.gz

COPY hechms_run.sh /home/curw/hechms_run.sh

RUN chmod a+x /home/curw/hechms_run.sh

WORKDIR /home/curw/distributed_hec

#install java for jython
RUN yum -y install java-1.6.0-openjdk

#-----------Uncomment later usage---------#
RUN wget 'https://excellmedia.dl.sourceforge.net/project/jython/jython/2.5.0/jython_installer-2.5.0.jar'
#COPY jython_installer-2.5.0.jar /home/jython_installer-2.5.0.jar
RUN java -jar jython_installer-2.5.0.jar -s -d /usr/share/jython

RUN yum -y install zip unzip

WORKDIR /usr/share/jython
RUN cp jython.jar jythonlib.jar
RUN zip -r jythonlib.jar Lib

WORKDIR /home/curw/distributed_hec

#-----------Uncomment later usage---------#
RUN wget 'http://www.hec.usace.army.mil/software/hec-dssvue/downloads/hec-dssvue201-linux.bin.zip'
#COPY hec-dssvue201-linux.bin.zip /home/hec-dssvue201-linux.bin.zip
RUN unzip hec-dssvue201-linux.bin.zip

RUN yes | ./hec-dssvue201.bin
#ADD hec-dssvue201 /home/hec-dssvue201

WORKDIR /home/curw/distributed_hec/hec-dssvue201
RUN rm -f hec-dssvue201-linux.bin.zip \
    rm -f hec-dssvue201.bin

# Replace existing jythonlib.jar with newly created one
RUN rm -f jar/sys/jythonlib.jar
RUN mv /usr/share/jython/jythonlib.jar /home/curw/distributed_hec/hec-dssvue201/jar/sys

WORKDIR /home/curw/distributed_hec

RUN yum -y --enablerepo=extras install epel-release \
    yum -y install \
        glibc-devel \
        net-tools \
        gcc \
        https://centos7.iuscommunity.org/ius-release.rpm \
        python36u \
        python36u-libs \
        python36u-devel \
        python36u-pip \
        python36-setuptools

RUN easy_install-3.6 pip

RUN pip3 install Flask \
                 Flask-JSON \
                 pandas \
                 geopandas \
                 shapely \
                 scipy \
                 mysql-connector

WORKDIR /home/curw/git

RUN git clone https://github.com/CUrW-SL/distributed_hechms.git

WORKDIR /home/curw

ENTRYPOINT ["./hechms_run.sh"]