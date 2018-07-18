# genomics-benchmarks
Benchmarking of systems for storage and compute over large-scale genomic data

## Setup and Execution  

pip install genomics-benchmark

Then:  

1. Generate the initial, default configuration file:  
$ genomics-benchmark config --output_config FILEPATH  
and then edit the config.ini file for the benchmarking environment.  

2. Setup the benchmark data and the benchmark environment:  
$ genomics-benchmark  setup --config_file FILEPATH  

3. Run the benchmark:   
$ genomics-benchmark exec --config_file FILEPATH [--label LABEL]}

## Benchmark Structure 

Benchmark is structured to follow conventions of the [authoritative Python benchmark suite](http://pyperformance.readthedocs.io/index.html).
Benchmark is executed as a command line utility, and configured with a conf file, a default one found in a doc directory.
It runs in a dynamic and static mode. In dynamic mode, benchmark will attempt to download the benchmark data files from the ftp server. In a static mode, it will assume that all the files are already present, and it will read the files from the files system, and execute the benchmarks. All the results will be recoded as .psv (pipe separated values) for easy loading into analytic suites, or the database. 


## Sources of data for benchmarking

VCF files:
