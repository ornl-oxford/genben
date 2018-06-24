#!/bin/bash

# convenience script to activate the conda environment
# usage: source activate.sh

# add miniconda to the path
export PATH=./opt/miniconda/bin:$PATH

# activate conda environment
source activate genomics-benchmarks
