FROM ubuntu:16.04
MAINTAINER Hasitha Dhananjaya <hasitha.10@cse.mrt.ac.lk>

ENV WRF_VERSION 4.0
ENV WPS_VERSION 4.0

RUN apt-get update -y
RUN cd ~
RUN mkdir Build_WRF/LIBRARIES/
RUN cd Build_WRF/LIBRARIES/

RUN wget http://www2.mmm.ucar.edu/wrf/OnLineTutorial/compile_tutorial/tar_files/netcdf-4.1.3.tar.gz \
    && wget http://www2.mmm.ucar.edu/wrf/OnLineTutorial/compile_tutorial/tar_files/mpich-3.0.4.tar.gz \
    && wget http://www2.mmm.ucar.edu/wrf/OnLineTutorial/compile_tutorial/tar_files/zlib-1.2.7.tar.gz  \
    && wget http://www2.mmm.ucar.edu/wrf/OnLineTutorial/compile_tutorial/tar_files/libpng-1.2.50.tar.gz \
    && wget http://www2.mmm.ucar.edu/wrf/OnLineTutorial/compile_tutorial/tar_files/jasper-1.900.1.tar.gz

# Installing NetCDF
RUN  export DIR=$PWD \
    && export CC=gcc \
    && export CXX=g++ \
    && export FC=gfortran \
    && export FCFLAGS=-m64 \
    && export F77=gfortran \
    && export FFLAGS=-m64 \
    && tar xzvf netcdf-4.1.3.tar.gz \
    && cd netcdf-4.1.3 \
    && ./configure --prefix=$DIR/netcdf --disable-dap --disable-netcdf-4 --disable-shared \
    && make \
    && make install \
    && export PATH=$DIR/netcdf/bin:$PATH \
    && export NETCDF=$DIR/netcdf \
    && cd ..

# Installing MPICH
RUN tar xzvf mpi-3.0.4.tar.gz \
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
RUN tar xzvf libpng-1.2.50.tar.gz \
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

