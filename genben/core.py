""" Main module for the benchmark. It reads the command line arguments, reads the benchmark configuration, 
determines the runtime mode (dynamic vs. static); if dynamic, gets the benchmark data from the server,
runs the benchmarks, and records the timer results. """

import allel
import zarr
import datetime
import time  # for benchmark timer
import numpy as np
import dask.array as da
import os
import pandas as pd
from collections import OrderedDict
from genben import config, data_service
from influxdb import InfluxDBClient


class BenchmarkResultsData:
    run_number = None
    operation_name = None
    start_time = None
    exec_time = None

    def to_dict(self):
        return OrderedDict([("log_timestamp", self.start_time),
                            ("run_number", self.run_number),
                            ("operation", self.operation_name),
                            ("execution_time", self.exec_time)])

    def to_pandas(self):
        data = self.to_dict()
        df = pd.DataFrame(data, index=[1])
        df.index.name = '#'
        return df

    def to_csv(self, filename, delimiter='|'):
        filename = str(filename)

        psv_header = not os.path.isfile(filename)

        # Open the output file in append mode
        with open(filename, "a") as psv_file:
            pd_results = self.to_pandas()
            pd_results.to_csv(psv_file, sep=delimiter, header=psv_header, index=False)

    def to_influxdb(self, host='localhost', port=8086, username='root', password='root', db_name=None,
                    benchmark_label=None, additional_tags=None):
        influx_client = InfluxDBClient(host=host,
                                       port=port,
                                       username=username,
                                       password=password,
                                       database=db_name)

        json_body = [{
            'measurement': 'benchmark',
            'tags': {
                'operation_name': self.operation_name,
                'benchmark_label': benchmark_label
            },
            'time': self.start_time,
            'fields': {
                'run_number': self.run_number,
                'execution_time': self.exec_time
            }
        }]

        # Add any additional tags if they were provided
        if additional_tags is not None:
            if type(additional_tags) is dict:
                json_body[0]['tags'].update(additional_tags)
            else:
                raise TypeError('Additional tags should be within a dict object.')

        return influx_client.write_points(json_body)


class BenchmarkProfiler:
    benchmark_running = False

    def __init__(self, output_config, benchmark_label):
        self.results = BenchmarkResultsData()
        self.output_config = output_config
        self.benchmark_label = benchmark_label

    def set_run_number(self, run_number):
        if not self.benchmark_running:
            self.results.run_number = run_number

    def start_benchmark(self, operation_name):
        if not self.benchmark_running:
            print('Running benchmark: {}'.format(operation_name))
            self.results.operation_name = operation_name

            self.benchmark_running = True

            # Start the benchmark timer
            self.results.start_time = datetime.datetime.utcnow()

    def end_benchmark(self):
        if self.benchmark_running:
            end_time = datetime.datetime.utcnow()

            print('  - Done.')

            # Calculate the execution time from start and end times
            self.results.exec_time = (end_time - self.results.start_time).total_seconds()

            # Save benchmark results
            self._record_runtime()

            self.benchmark_running = False

    def get_benchmark_results(self):
        return self.results

    def _record_runtime(self):
        """
        Records the benchmark results data entry to the specified PSV file.
        """
        if self.output_config.output_csv_enabled:
            self.results.to_csv(filename='{}.csv'.format(self.benchmark_label),
                                delimiter=self.output_config.output_csv_delimiter)

        if self.output_config.output_influxdb_enabled:
            influxdb_additional_tags = {
                'benchmark_group': self.output_config.output_influxdb_benchmark_group,
                'device_name': self.output_config.output_influxdb_device_name
            }

            self.results.to_influxdb(host=self.output_config.output_influxdb_host,
                                     port=self.output_config.output_influxdb_port,
                                     username=self.output_config.output_influxdb_username,
                                     password=self.output_config.output_influxdb_password,
                                     db_name=self.output_config.output_influxdb_database_name,
                                     benchmark_label=self.benchmark_label,
                                     additional_tags=influxdb_additional_tags)


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

        self.benchmark_profiler = BenchmarkProfiler(output_config=self.bench_conf.results_output_config,
                                                    benchmark_label=self.benchmark_label)

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
                    # Ensure user didn't attempt to use concatenation along with vcf data input mode
                    if self.bench_conf.benchmark_dataset == '*':
                        print(
                            '[Exec] Error: benchmark_dataset has a value of *, which cannot be used with VCF to Zarr conversion.')
                        print('  - Please disable concatenation and specify a single data set to work with.')
                        print(
                            '  - Alternatively, benchmark_data_input can be set to zarr so that concatenation can be used.')
                        exit(1)

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

                callsets = []

                # Ensure Zarr dataset exists and can be used for upcoming benchmarks
                benchmark_zarr_path = os.path.join(self.benchmark_zarr_dir, self.benchmark_zarr_file)
                if self.benchmark_zarr_file == '*':
                    # User specified concatenation mode. Get all available datasets
                    zarr_datasets = os.listdir(self.benchmark_zarr_dir)
                    if len(zarr_datasets) == 0:
                        print('[Exec] Error: No zarr data sets could be found for concatenation.')
                        exit(1)
                    else:
                        zarr_paths = []
                        for zarr_dataset in zarr_datasets:
                            zarr_paths.append(os.path.join(self.benchmark_zarr_dir, zarr_dataset))

                        callsets = self._benchmark_load_zarr_datasets(zarr_paths)
                elif (benchmark_zarr_path != "") and (os.path.isdir(benchmark_zarr_path)):
                    # Load Zarr dataset into memory
                    callsets = self._benchmark_load_zarr_datasets([benchmark_zarr_path])
                else:
                    # Zarr dataset doesn't exist. Print error message and exit
                    print("[Exec] Error: Zarr dataset could not be found for benchmarking.")
                    print("  - Zarr dataset location: {}".format(benchmark_zarr_path))
                    exit(1)

                # Create genotype data from data set
                num_variants = self.bench_conf.benchmark_num_variants
                num_samples = self.bench_conf.benchmark_num_samples
                gt = self._benchmark_create_genotype_array(callsets, num_variants, num_samples)

                if self.bench_conf.benchmark_aggregations:
                    # Run simple aggregations benchmark
                    self._benchmark_simple_aggregations(gt)

                if self.bench_conf.benchmark_pca:
                    # Run PCA benchmark
                    self._benchmark_pca(gt)

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

    def _benchmark_load_zarr_datasets(self, zarr_paths):
        callsets = []
        self.benchmark_profiler.start_benchmark(operation_name="Load Zarr Dataset")
        for zarr_path in zarr_paths:
            store = zarr.DirectoryStore(zarr_path)
            callset = zarr.Group(store=store, read_only=True)
            callsets.append(callset)
        self.benchmark_profiler.end_benchmark()
        return callsets

    def _benchmark_create_genotype_array(self, callsets, num_variants=None, num_samples=None):
        genotype_array_type = self.bench_conf.genotype_array_type

        # Create the genotype array and benchmark its execution time
        self.benchmark_profiler.start_benchmark(operation_name="Create Genotype Array")
        gt = data_service.get_genotype_array_concat(callsets=callsets, genotype_array_type=genotype_array_type)
        self.benchmark_profiler.end_benchmark()

        # If the number of variants or samples were specified, limit the genotype data returned
        if num_variants is not None and num_variants != -1:
            print('[Exec][Create Genotype Array] Limiting number of variants to {}.'.format(num_variants))
            # Ensure specified number of variants is not larger than the number of variants available
            if num_variants > gt.n_variants:
                print(
                    '[Exec][Create Genotype Array] Warning: number of variants specified ({}) exceeds the number available ({}). Including all variants.'.format(
                        num_variants, gt.n_variants))
            else:
                gt = gt[:num_variants, :]
        else:
            print('[Exec][Create Genotype Array] Including all variants ({}).'.format(gt.n_variants))

        if num_samples is not None and num_samples != -1:
            print('[Exec][Create Genotype Array] Limiting number of samples to {}.'.format(num_samples))
            # Ensure specified number of samples is not larger than the number of samples available
            if num_samples > gt.n_samples:
                print(
                    '[Exec][Create Genotype Array] Warning: number of samples specified ({}) exceeds the number available ({}). Including all samples.'.format(
                        num_samples, gt.n_samples))
            else:
                gt = gt[:, :num_samples]
        else:
            print('[Exec][Create Genotype Array] Including all samples ({}).'.format(gt.n_samples))

        # Rechunk the genotype data if specified
        if self.bench_conf.genotype_array_type == config.GENOTYPE_ARRAY_DASK:
            new_chunk_length = self.bench_conf.dask_genotype_array_chunk_variants
            new_chunk_width = self.bench_conf.dask_genotype_array_chunk_samples
            if new_chunk_length != -1 or new_chunk_width != -1:
                # Rechunk the genotype array
                new_chunk_size = (new_chunk_length if new_chunk_length != -1 else gt.values.chunksize[0],
                                  new_chunk_width if new_chunk_width != -1 else gt.values.chunksize[1],
                                  gt.ploidy)
                print('[Exec][Create Genotype Array] Rechunking data to size {}.'.format(new_chunk_size))
                gt = gt.rechunk(new_chunk_size)

        return gt

    def _benchmark_simple_aggregations(self, gt):
        # Run benchmark for allele count
        benchmark_allele_count_name = "Allele Count (All Samples)"
        if self.bench_conf.genotype_array_type == config.GENOTYPE_ARRAY_DASK:
            self.benchmark_profiler.start_benchmark(operation_name=benchmark_allele_count_name)
            gt.count_alleles().compute()
        else:
            self.benchmark_profiler.start_benchmark(operation_name=benchmark_allele_count_name)
            gt.count_alleles()
        self.benchmark_profiler.end_benchmark()

        # Run benchmark for genotype count (heterozygous per variant)
        benchmark_gt_count_het_per_var_name = "Genotype Count: Heterozygous per Variant"
        if self.bench_conf.genotype_array_type == config.GENOTYPE_ARRAY_DASK:
            self.benchmark_profiler.start_benchmark(operation_name=benchmark_gt_count_het_per_var_name)
            gt.count_het(axis=1).compute()
        else:
            self.benchmark_profiler.start_benchmark(operation_name=benchmark_gt_count_het_per_var_name)
            gt.count_het(axis=1)
        self.benchmark_profiler.end_benchmark()

        # Run benchmark for genotype count (homozygous per variant)
        benchmark_gt_count_hom_per_var_name = "Genotype Count: Homozygous per Variant"
        if self.bench_conf.genotype_array_type == config.GENOTYPE_ARRAY_DASK:
            self.benchmark_profiler.start_benchmark(operation_name=benchmark_gt_count_hom_per_var_name)
            gt.count_hom(axis=1).compute()
        else:
            self.benchmark_profiler.start_benchmark(operation_name=benchmark_gt_count_hom_per_var_name)
            gt.count_hom(axis=1)
        self.benchmark_profiler.end_benchmark()

        # Run benchmark for genotype count (heterozygous per sample)
        benchmark_gt_count_het_per_sample = "Genotype Count: Heterozygous per Sample"
        if self.bench_conf.genotype_array_type == config.GENOTYPE_ARRAY_DASK:
            self.benchmark_profiler.start_benchmark(operation_name=benchmark_gt_count_het_per_sample)
            gt.count_het(axis=0).compute()
        else:
            self.benchmark_profiler.start_benchmark(operation_name=benchmark_gt_count_het_per_sample)
            gt.count_het(axis=0)
        self.benchmark_profiler.end_benchmark()

        # Run benchmark for genotype count (homozygous per sample)
        benchmark_gt_hom_per_sample = "Genotype Count: Homozygous per Sample"
        if self.bench_conf.genotype_array_type == config.GENOTYPE_ARRAY_DASK:
            self.benchmark_profiler.start_benchmark(operation_name=benchmark_gt_hom_per_sample)
            gt.count_hom(axis=0).compute()
        else:
            self.benchmark_profiler.start_benchmark(operation_name=benchmark_gt_hom_per_sample)
            gt.count_hom(axis=0)
        self.benchmark_profiler.end_benchmark()

    def _benchmark_pca(self, gt):
        # Count alleles at each variant
        self.benchmark_profiler.start_benchmark('PCA: Count alleles')
        ac = gt.count_alleles()
        self.benchmark_profiler.end_benchmark()

        # Count number of multiallelic SNPs
        self.benchmark_profiler.start_benchmark('PCA: Count multiallelic SNPs')
        if self.bench_conf.genotype_array_type == config.GENOTYPE_ARRAY_DASK:
            num_multiallelic_snps = da.count_nonzero(ac.max_allele() > 1).compute()
        else:
            num_multiallelic_snps = np.count_nonzero(ac.max_allele() > 1)
        self.benchmark_profiler.end_benchmark()
        del num_multiallelic_snps

        # Count number of biallelic singletons
        self.benchmark_profiler.start_benchmark('PCA: Count biallelic singletons')
        if self.bench_conf.genotype_array_type == config.GENOTYPE_ARRAY_DASK:
            num_biallelic_singletons = da.count_nonzero((ac.max_allele() == 1) & ac.is_singleton(1)).compute()
        else:
            num_biallelic_singletons = np.count_nonzero((ac.max_allele() == 1) & ac.is_singleton(1))
        self.benchmark_profiler.end_benchmark()
        del num_biallelic_singletons

        # Apply filtering to remove singletons and multiallelic SNPs
        flt = (ac.max_allele() == 1) & (ac[:, :2].min(axis=1) > 1)
        flt_count = np.count_nonzero(flt)
        self.benchmark_profiler.start_benchmark('PCA: Remove singletons and multiallelic SNPs')
        if flt_count > 0:
            if self.bench_conf.genotype_array_type == config.GENOTYPE_ARRAY_DASK:
                gf = gt.take(np.flatnonzero(flt), axis=0)
            else:
                gf = gt.compress(condition=flt, axis=0)
        else:
            # Don't apply filtering
            print('[Exec][PCA] Cannot remove singletons and multiallelic SNPs as no data would remain. Skipping...')
            gf = gt
        self.benchmark_profiler.end_benchmark()
        del ac, flt, flt_count

        # Transform genotype data into 2-dim matrix
        self.benchmark_profiler.start_benchmark('PCA: Transform genotype data for PCA')
        gn = gf.to_n_alt()
        self.benchmark_profiler.end_benchmark()
        del gf

        # Randomly choose subset of SNPs
        if self.bench_conf.pca_subset_size == -1:
            print('[Exec][PCA] Including all ({}) variants for PCA.'.format(gn.shape[0]))
            gnr = gn
        else:
            n = min(gn.shape[0], self.bench_conf.pca_subset_size)
            print('[Exec][PCA] Including {} random variants for PCA.'.format(n))
            vidx = np.random.choice(gn.shape[0], n, replace=False)
            vidx.sort()
            if self.bench_conf.genotype_array_type in [config.GENOTYPE_ARRAY_NORMAL, config.GENOTYPE_ARRAY_CHUNKED]:
                gnr = gn.take(vidx, axis=0)
            elif self.bench_conf.genotype_array_type == config.GENOTYPE_ARRAY_DASK:
                gnr = gn[vidx]  # Use indexing workaround since Dask Array's take() method is not working properly
            else:
                print('[Exec][PCA] Error: Unspecified genotype array type specified.')
                exit(1)
            del vidx

        if self.bench_conf.pca_ld_enabled:
            if self.bench_conf.genotype_array_type != config.GENOTYPE_ARRAY_DASK:
                # Apply LD pruning to subset of SNPs
                size = self.bench_conf.pca_ld_pruning_size
                step = self.bench_conf.pca_ld_pruning_step
                threshold = self.bench_conf.pca_ld_pruning_threshold
                n_iter = self.bench_conf.pca_ld_pruning_number_iterations

                self.benchmark_profiler.start_benchmark('PCA: Apply LD pruning')
                gnu = self._pca_ld_prune(gnr, size=size, step=step, threshold=threshold, n_iter=n_iter)
                self.benchmark_profiler.end_benchmark()
            else:
                print('[Exec][PCA] Cannot apply LD pruning because Dask genotype arrays do not support this operation.')
                gnu = gnr
        else:
            print('[Exec][PCA] LD pruning disabled. Skipping this operation.')
            gnu = gnr

        # Run PCA analysis
        pca_num_components = self.bench_conf.pca_number_components
        scaler = self.bench_conf.pca_data_scaler

        if self.bench_conf.genotype_array_type == config.GENOTYPE_ARRAY_DASK:
            # Rechunk Dask array to work with Dask's svd function (single chunk for transposed column)
            gnu_pca_conv = gnu.rechunk({0: -1, 1: gt.values.chunksize[1]})
        else:
            gnu_pca_conv = gnu

        # Run conventional PCA analysis
        self.benchmark_profiler.start_benchmark(
            'PCA: Run conventional PCA analysis (scaler: {})'.format(scaler if scaler is not None else 'none'))
        coords, model = allel.pca(gnu_pca_conv, n_components=pca_num_components, scaler=scaler)
        if self.bench_conf.genotype_array_type == config.GENOTYPE_ARRAY_DASK:
            coords.compute()
        self.benchmark_profiler.end_benchmark()
        del gnu_pca_conv, coords, model

        if self.bench_conf.genotype_array_type == config.GENOTYPE_ARRAY_DASK:
            # Rechunk Dask array to match original genotype chunk size
            gnu_pca_rand = gnu.rechunk((gt.values.chunksize[0], gt.values.chunksize[1]))
        else:
            gnu_pca_rand = gnu

        # Run randomized PCA analysis
        self.benchmark_profiler.start_benchmark(
            'PCA: Run randomized PCA analysis (scaler: {})'.format(scaler if scaler is not None else 'none'))
        coords, model = allel.randomized_pca(gnu_pca_rand, n_components=pca_num_components, scaler=scaler)
        if self.bench_conf.genotype_array_type == config.GENOTYPE_ARRAY_DASK:
            coords.compute()
        self.benchmark_profiler.end_benchmark()
        del gnu_pca_rand, coords, model

    @staticmethod
    def _pca_ld_prune(gn, size, step, threshold=.1, n_iter=1):
        blen = size * 10
        for i in range(n_iter):
            loc_unlinked = allel.locate_unlinked(gn, size=size, step=step, threshold=threshold, blen=blen)
            n = np.count_nonzero(loc_unlinked)
            n_remove = gn.shape[0] - n
            print(
                '[Exec][PCA][LD Prune] Iteration {}/{}: Retaining {} and removing {} variants.'.format(i + 1,
                                                                                                       n_iter,
                                                                                                       n,
                                                                                                       n_remove))
            gn = gn.compress(loc_unlinked, axis=0)
        return gn
