genomics-benchmarks
===================

Benchmarking of systems for storage and compute over large-scale genomic data using the `scikit-allel <https://scikit-allel.readthedocs.io/en/latest/>`_ library.

.. image:: https://api.travis-ci.com/ornl-oxford/genomics-benchmarks.svg?branch=master
   :target: https://travis-ci.com/ornl-oxford/genomics-benchmarks

Source Code: https://github.com/ornl-oxford/genomics-benchmarks

Setup and Execution
###################

Download genomics-benchmarks:
    ``git clone https://github.com/ornl-oxford/genomics-benchmarks.git``

Install genomics-benchmarks:
    ``python setup.py install``

Run genomics-benchmarks:
  1. Generate the initial, default configuration file:
      ``$ genomics-benchmarks config --output_config FILEPATH [-f]``
      and then edit the newly-created INI file for the benchmarking environment.

  2. Setup the benchmark data and the benchmark environment:
      ``$ genomics-benchmarks setup --config_file FILEPATH``

  3. Run the benchmark:
      ``$ genomics-benchmarks exec --config_file FILEPATH [--label LABEL]}``

Benchmark Structure
###################

Benchmark is structured to follow conventions of the `authoritative Python benchmark suite <http://pyperformance.readthedocs.io/index.html>`_.

Benchmark is executed as a command line utility, and configured with an INI configuration file.
It contains an FTP downloader module that will attempt to download the benchmark data files from the FTP server, if the module is enabled. If the FTP module is disabled or if the files are already present, the benchmark will read the files from the filesystem.
All the results will be recoded as .psv (pipe separated values) for easy loading into analytic suites, or the database.


Data Sources for Benchmarking
##############################

VCF files:

* 1000 Genomes Project: ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/
