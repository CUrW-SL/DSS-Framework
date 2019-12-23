FROM ubuntu:18.04
MAINTAINER Hasitha Dhananjaya <hasitha.10@cse.mrt.ac.lk>

ENV WRF_VERSION 4.1.2
ENV WPS_VERSION 4.1
ENV WRF_CONFIGURE_OPTION 34
ENV WPS_CONFIGURE_OPTION 1

RUN apt-get update \
    && apt-get install -y software-properties-common \
    && add-apt-repository ppa:ubuntugis/ppa \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y --allow-unauthenticated \
        wget \
        zip \
        unzip \
        gcc \
        csh \
        gfortran \
        gfortran-5-multilib \
        libpng-dev \
        build-essential \
        apt-utils \
        file \
        m4 \
        nco \
        ncl-ncarg \
        python3.6 \
        python3-pip

RUN add-apt-repository "deb http://security.ubuntu.com/ubuntu xenial-security main" \
    && apt update \
    && apt install -y --allow-unauthenticated \
        libjasper1 \
        libjasper-dev

RUN echo "deb http://mirrors.kernel.org/ubuntu/ xenial main" | tee -a /etc/apt/sources.list \
    && apt-get update \
    && apt install -y --allow-unauthenticated libpng12-0

RUN mkdir Build_WRF

WORKDIR Build_WRF

RUN mkdir LIBRARIES \
    && mkdir -p /home/Build_WRF/code \
    && mkdir -p /home/Build_WRF/logs \
    && mkdir -p /home/Build_WRF/gfs \
    && mkdir -p /home/Build_WRF/nfs \
    && mkdir -p /home/Build_WRF/geog \
    && mkdir -p /home/Build_WRF/archive

WORKDIR /home/Build_WRF/LIBRARIES

RUN wget http://www2.mmm.ucar.edu/wrf/OnLineTutorial/compile_tutorial/tar_files/netcdf-4.1.3.tar.gz \
    && wget http://www2.mmm.ucar.edu/wrf/OnLineTutorial/compile_tutorial/tar_files/mpich-3.0.4.tar.gz \
    && wget http://www2.mmm.ucar.edu/wrf/OnLineTutorial/compile_tutorial/tar_files/zlib-1.2.7.tar.gz  \
    && wget http://www2.mmm.ucar.edu/wrf/OnLineTutorial/compile_tutorial/tar_files/libpng-1.2.50.tar.gz \
    && wget http://www2.mmm.ucar.edu/wrf/OnLineTutorial/compile_tutorial/tar_files/jasper-1.900.1.tar.gz

ENV DIR /home/Build_WRF/LIBRARIES
ENV CC gcc
ENV CPP /lib/cpp -P
ENV CXX g++
ENV FC gfortran
ENV FCFLAGS -m64
ENV F77 gfortran
ENV FFLAGS -m64

# Installing NetCDF
RUN tar xzvf netcdf-4.1.3.tar.gz \
    && cd netcdf-4.1.3 \
    && ./configure --prefix=$DIR/netcdf --disable-dap --disable-netcdf-4 --disable-shared \
    && make \
    && make install \
    && export PATH=$DIR/netcdf/bin:$PATH \
    && export NETCDF=$DIR/netcdf \
    && cd ..

# Installing MPICH
RUN tar xzvf mpich-3.0.4.tar.gz \
    && cd mpich-3.0.4 \
    && ./configure --prefix=$DIR/mpich \
    && make \
    && make install \
    && export PATH=$DIR/mpich/bin:$PATH \
    && cd ..

# Installing zlib
RUN  export LDFLAGS=-L$DIR/grib2/lib \
    && export CPPFLAGS=-I$DIR/grib2/include \
    && tar xzvf zlib-1.2.7.tar.gz \
    && cd zlib-1.2.7 \
    && ./configure --prefix=$DIR/grib2 \
    && make \
    && make install \
    && cd ..

# Installing libpng
RUN export LDFLAGS=-L$DIR/grib2/lib \
    && export CPPFLAGS=-I$DIR/grib2/include \
    && tar xzvf libpng-1.2.50.tar.gz \
    && cd libpng-1.2.50 \
    && ./configure --prefix=$DIR/grib2 \
    && make \
    && make install \
    && cd ..

# Installing JasPer
RUN tar xzvf jasper-1.900.1.tar.gz \
    && cd jasper-1.900.1 \
    && ./configure --prefix=$DIR/grib2 \
    && make \
    && make install \
    && cd ..

# Permanent Export
ENV PATH "$DIR/netcdf/bin:$PATH"
ENV NETCDF "$DIR/netcdf"
ENV PATH "$DIR/mpich/bin:$PATH"
ENV LDFLAGS "-L$DIR/grib2/lib"
ENV CPPFLAGS "-I$DIR/grib2/include"
ENV JASPERLIB "$DIR/grib2/lib"
ENV JASPERINC "$DIR/grib2/include"

WORKDIR /home/Build_WRF

RUN wget https://github.com/wrf-model/WRF/archive/v4.1.2.tar.gz \
    && wget https://github.com/wrf-model/WPS/archive/v4.1.tar.gz

# Installing & building WRF. WRF-4.1.2.tar.gz
RUN tar -xvzf v4.1.2.tar.gz \
    && mv WRF-4.1.2 WRF \
    && cd ./WRF \
    && echo $WRF_CONFIGURE_OPTION | ./configure  \
    && ./compile em_real >> log.compile 2>&1 \
    && cd ..

# Installing & building WPS
RUN tar -xvzf v4.1.tar.gz \
    && mv WPS-4.1 WPS \
    && cd ./WPS \
    && export JASPERLIB=$DIR/grib2/lib \
    && export JASPERINC=$DIR/grib2/include \
    && echo $WPS_CONFIGURE_OPTION | ./configure \
    && ./compile >> log.compile 2>&1 \
    && cd ..

# install gcsfuse
RUN apt-get install -y curl \
    && export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s` \
    && echo "deb http://packages.cloud.google.com/apt $GCSFUSE_REPO main" | tee /etc/apt/sources.list.d/gcsfuse.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
    && apt-get update \
    && apt-get install -y gcsfuse

COPY code /home/Build_WRF/code

WORKDIR /home/Build_WRF/WPS

# copy geogrid.exe output
COPY geo_em.zip /home/Build_WRF/WPS
RUN unzip geo_em.zip

WORKDIR /home/Build_WRF/code

#install required libs for python script
RUN pip3 install -r requirements.yml

COPY code/namelist.wps /home/Build_WRF
COPY code/namelist.input /home/Build_WRF
COPY uwcc-admin.json /home/Build_WRF/code/gcs.json

RUN chmod +x wrfv4_run.sh

CMD ["wrfv4_run.sh"]

VOLUME /home/Build_WRF