""" Main module for the benchmark. It reads the command line arguments, reads the benchmark configuration, 
determines the runtime mode (dynamic vs. static); if dynamic, gets the benchmark data from the server,
runs the benchmarks, and records the timer results. """

import allel
import zarr
import datetime
import time  # for benchmark timer
import csv  # for writing results
import logging
import os
import pandas as pd
from collections import OrderedDict
from genomics_benchmarks import config, data_service


class BenchmarkResultsData:
    run_number = None
    operation_name = None
    start_time = None
    exec_time = None

    def to_dict(self):
        return OrderedDict([("Log Timestamp", datetime.datetime.fromtimestamp(self.start_time)),
                            ("Run Number", self.run_number),
                            ("Operation", self.operation_name),
                            ("Execution Time", self.exec_time)])

    def to_pandas(self):
        data = self.to_dict()
        df = pd.DataFrame(data, index=[1])
        df.index.name = '#'
        return df


class BenchmarkProfiler:
    benchmark_running = False

    def __init__(self, benchmark_label):
        self.results = BenchmarkResultsData()
        self.benchmark_label = benchmark_label

    def set_run_number(self, run_number):
        if not self.benchmark_running:
            self.results.run_number = run_number

    def start_benchmark(self, operation_name):
        if not self.benchmark_running:
            self.results.operation_name = operation_name

            self.benchmark_running = True

            # Start the benchmark timer
            self.results.start_time = time.time()

    def end_benchmark(self):
        if self.benchmark_running:
            end_time = time.time()

            # Calculate the execution time from start and end times
            self.results.exec_time = end_time - self.results.start_time

            # Save benchmark results
            self._record_runtime(self.results, "{}.psv".format(self.benchmark_label))

            self.benchmark_running = False

    def get_benchmark_results(self):
        return self.results

    def _record_runtime(self, benchmark_results, output_filename):
        """
        Records the benchmark results data entry to the specified PSV file.
        :param benchmark_results: BenchmarkResultsData object containing the benchmark results data
        :param output_filename: Which file to output the benchmark results to
        :type benchmark_results: BenchmarkResultsData
        :type output_filename: str
        """
        output_filename = str(output_filename)

        psv_header = not os.path.isfile(output_filename)

        # Open the output file in append mode
        with open(output_filename, "a") as psv_file:
            pd_results = benchmark_results.to_pandas()
            pd_results.to_csv(psv_file, sep="|", header=psv_header, index=False)


class Benchmark:
    benchmark_zarr_dir = ""  # Directory for which to use data from for benchmark process
    benchmark_zarr_file = ""  # File within benchmark_zarr_dir for which to use for benchmark process

    def __init__(self, bench_conf, data_dirs, benchmark_label):
        """
        Sets up a Benchmark object which is used to execute benchmarks.
        :param bench_conf: Benchmark configuration data that controls the benchmark execution
        :param data_dirs: DataDirectoriesConfigurationRepresentation object that contains working data directories
        :param benchmark_label: label to use when saving benchmark results to file
        :type bench_conf: config.BenchmarkConfigurationRepresentation
        :type data_dirs: config.DataDirectoriesConfigurationRepresentation
        :type benchmark_label: str
        """
        self.bench_conf = bench_conf
        self.data_dirs = data_dirs
        self.benchmark_label = benchmark_label

        self.benchmark_profiler = BenchmarkProfiler(benchmark_label=self.benchmark_label)

    def run_benchmark(self):
        """
        Executes the benchmarking process.
        """
        if self.bench_conf is not None and self.data_dirs is not None:
            for run_number in range(1, self.bench_conf.benchmark_number_runs + 1):
                # Clear out existing files in Zarr benchmark directory
                # (Should be done every single run)
                data_service.remove_directory_tree(self.data_dirs.zarr_dir_benchmark)

                # Update run number in benchmark profiler (for results tracking)
                self.benchmark_profiler.set_run_number(run_number)

                # Prepare data directory and file locations for benchmarks
                if self.bench_conf.benchmark_data_input == "vcf":
                    self.benchmark_zarr_dir = self.data_dirs.zarr_dir_benchmark

                    # Convert VCF data to Zarr format as part of benchmark
                    self._benchmark_convert_to_zarr()

                elif self.bench_conf.benchmark_data_input == "zarr":
                    # Use pre-converted Zarr data which was done ahead of benchmark (i.e. in Setup mode)
                    self.benchmark_zarr_dir = self.data_dirs.zarr_dir_setup
                    self.benchmark_zarr_file = self.bench_conf.benchmark_dataset

                else:
                    print("[Exec] Error: Invalid option supplied for benchmark data input format.")
                    print("  - Expected data input formats: vcf, zarr")
                    print("  - Provided data input format: {}".format(self.bench_conf.benchmark_data_input))
                    exit(1)

                # Ensure Zarr dataset exists and can be used for upcoming benchmarks
                benchmark_zarr_path = os.path.join(self.benchmark_zarr_dir, self.benchmark_zarr_file)
                if (benchmark_zarr_path != "") and (os.path.isdir(benchmark_zarr_path)):
                    # Load Zarr dataset into memory
                    self._benchmark_load_zarr_dataset(benchmark_zarr_path)

                    if self.bench_conf.benchmark_aggregations:
                        self._benchmark_simple_aggregations(benchmark_zarr_path)
                else:
                    # Zarr dataset doesn't exist. Print error message and exit
                    print("[Exec] Error: Zarr dataset could not be found for benchmarking.")
                    print("  - Zarr dataset location: {}".format(benchmark_zarr_path))
                    exit(1)

    def _benchmark_convert_to_zarr(self):
        self.benchmark_zarr_dir = self.data_dirs.zarr_dir_benchmark
        input_vcf_file = self.bench_conf.benchmark_dataset
        input_vcf_path = os.path.join(self.data_dirs.vcf_dir, input_vcf_file)

        if os.path.isfile(input_vcf_path):
            output_zarr_file = input_vcf_file
            output_zarr_file = output_zarr_file[
                               0:len(output_zarr_file) - 4]  # Truncate *.vcf from input filename
            output_zarr_path = os.path.join(self.data_dirs.zarr_dir_benchmark, output_zarr_file)

            data_service.convert_to_zarr(input_vcf_path=input_vcf_path,
                                         output_zarr_path=output_zarr_path,
                                         conversion_config=self.bench_conf.vcf_to_zarr_config,
                                         benchmark_profiler=self.benchmark_profiler)

            self.benchmark_zarr_file = output_zarr_file
        else:
            print("[Exec] Error: Dataset specified in configuration file does not exist. Exiting...")
            print("  - Dataset file specified in configuration: {}".format(input_vcf_file))
            print("  - Expected file location: {}".format(input_vcf_path))
            exit(1)

    def _benchmark_load_zarr_dataset(self, zarr_path):
        self.benchmark_profiler.start_benchmark(operation_name="Load Zarr Dataset")
        store = zarr.DirectoryStore(zarr_path)
        callset = zarr.Group(store=store, read_only=True)
        self.benchmark_profiler.end_benchmark()

    def _benchmark_simple_aggregations(self, zarr_path):
        # Load Zarr dataset
        store = zarr.DirectoryStore(zarr_path)
        callset = zarr.Group(store=store, read_only=True)

        gtz = callset['calldata/GT']

        # Setup genotype Dask array for computations
        gt = allel.GenotypeDaskArray(gtz)

        # Run benchmark for allele count
        self.benchmark_profiler.start_benchmark(operation_name="Allele Count (All Samples)")
        gt.count_alleles().compute()
        self.benchmark_profiler.end_benchmark()

        # Run benchmark for genotype count (heterozygous per variant)
        self.benchmark_profiler.start_benchmark(operation_name="Genotype Count: Heterozygous per Variant")
        gt.count_het(axis=1).compute()
        self.benchmark_profiler.end_benchmark()

        # Run benchmark for genotype count (homozygous per variant)
        self.benchmark_profiler.start_benchmark(operation_name="Genotype Count: Homozygous per Variant")
        gt.count_hom(axis=1).compute()
        self.benchmark_profiler.end_benchmark()

        # Run benchmark for genotype count (heterozygous per sample)
        self.benchmark_profiler.start_benchmark(operation_name="Genotype Count: Heterozygous per Sample")
        gt.count_het(axis=0).compute()
        self.benchmark_profiler.end_benchmark()

        # Run benchmark for genotype count (homozygous per sample)
        self.benchmark_profiler.start_benchmark(operation_name="Genotype Count: Homozygous per Sample")
        gt.count_hom(axis=0).compute()
        self.benchmark_profiler.end_benchmark()
