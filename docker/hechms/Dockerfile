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
    xorg-x11-server-Xvfb

# Download hec-hms421-linux.tar.gz
RUN wget 'http://www.hec.usace.army.mil/software/hec-hms/downloads/hec-hms421-linux.tar.gz'

# Unzip tar file
RUN tar -xvzf hec-hms421-linux.tar.gz

#Remove tar file
RUN rm -f hec-hms421-linux.tar.gz

COPY run.sh /home/run.sh

RUN chmod a+x /home/run.sh

WORKDIR /home

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

WORKDIR /home

#-----------Uncomment later usage---------#
RUN wget 'http://www.hec.usace.army.mil/software/hec-dssvue/downloads/hec-dssvue201-linux.bin.zip'
#COPY hec-dssvue201-linux.bin.zip /home/hec-dssvue201-linux.bin.zip
RUN unzip hec-dssvue201-linux.bin.zip

RUN yes | ./hec-dssvue201.bin
#ADD hec-dssvue201 /home/hec-dssvue201

WORKDIR /home/hec-dssvue201
RUN rm -f hec-dssvue201-linux.bin.zip \
    rm -f hec-dssvue201.bin

# Replace existing jythonlib.jar with newly created one
RUN rm -f /home/hec-dssvue201/jar/sys/jythonlib.jar \
    mv /usr/share/jython/jythonlib.jar /home/hec-dssvue201/jar/sys

WORKDIR /home

RUN yum -y --enablerepo=extras install epel-release \
    yum -y install \
        glibc-devel \
        net-tools \
        gcc
        https://centos7.iuscommunity.org/ius-release.rpm \
        python36u \
        python36u-devel \
        wget https://bootstrap.pypa.io/get-pip.py \
        python3.6 get-pip.py \
        yum -y install build-dep python-psycopg2 \
        pip install psycopg2

CMD ["hechms_run.sh"]