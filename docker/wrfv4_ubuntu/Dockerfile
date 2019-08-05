FROM ubuntu:16.04
MAINTAINER Hasitha Dhananjaya <hasitha.10@cse.mrt.ac.lk>

ENV WRF_VERSION 4.0
ENV WPS_VERSION 4.0

RUN apt-get update -y
RUN cd ~
RUN mkdir Build_WRF/LIBRARIES/
WORKDIR Build_WRF
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
ENV DIR "~/Build_WRF/LIBRARIES"
ENV CC "gcc"
ENV CXX "g++"
ENV FC "gfortran"
ENV FCFLAGS "-m64"
ENV F77 "gfortran"
ENV FFLAGS "-m64"
ENV PATH "$DIR/netcdf/bin:$PATH"
ENV NETCDF "$DIR/netcdf"
ENV PATH "$DIR/mpich/bin:$PATH"
ENV LDFLAGS "-L$DIR/grib2/lib"
ENV CPPFLAGS "-I$DIR/grib2/include"
ENV JASPERLIB "$DIR/grib2/lib"
ENV JASPERINC "$DIR/grib2/include"

WORKDIR Build_WRF

# Installing & building WRS
RUN apt-get install m4 -y \
    && gunzip WRSV4.0.TAR.gz \
    && tar -xf WRSV4.0.TAR \
    && RUN cd ./WPS \
    && ./configure <<< $'1\r' \
    && ./compile em_real >& log.compile \
    && cd ..

# Installing & building WPF
RUN apt-get install m4 -y \
    && gunzip WPFV4.0.TAR.gz \
    && tar -xf WPFV4.0.TAR \
    && RUN cd ./WPF \
    && export JASPERLIB=$DIR/grib2/lib \
    && export JASPERINC=$DIR/grib2/include \
    && ./configure <<< $'1\r' \
    && ./compile >& log.compile \
    && cd ..


