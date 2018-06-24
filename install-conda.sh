#!/bin/bash

# Convenience script to install miniconda.

# N.B., assume this will be executed from the root directory of the repo.

# ensure script errors if any command fails
set -e

# installation directory
mkdir -pv opt

# install miniconda
if [ ! -f opt/miniconda.installed ]; then
    echo "installing miniconda..."

    cd opt

    # clean up any previous
    rm -rf miniconda

    # download miniconda
    wget --no-clobber https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh

    # install miniconda
    bash Miniconda3-latest-Linux-x86_64.sh -b -p miniconda

    # put conda on the path
    export PATH=./miniconda/bin:$PATH

    # create environment
    conda env create --name genomics-benchmarks --file ../environment.yml

    # mark success
    touch miniconda.installed

    cd ..

else
    echo "miniconda already installed"
fi

