import unittest
from genomics_benchmarks.core import *
from genomics_benchmarks.config import \
    BenchmarkConfigurationRepresentation, \
    VCFtoZarrConfigurationRepresentation, \
    DataDirectoriesConfigurationRepresentation
from time import sleep
import os
import shutil


class TestCoreBenchmark(unittest.TestCase):
    def test_benchmark_profiler_results(self):
        # Setup Benchmark Profiler object
        profiler_label = 'test_benchmark_profiler_results'
        profiler = BenchmarkProfiler(profiler_label)

        # Run a few mock benchmarks
        benchmark_times = [1, 2, 10]
        i = 1
        for benchmark_time in benchmark_times:
            profiler.set_run_number(i)

            operation_name = 'Sleep {} seconds'.format(benchmark_time)

            # Run the mock benchmark, measuring time to run sleep command
            profiler.start_benchmark(operation_name)
            time.sleep(benchmark_time)
            profiler.end_benchmark()

            # Grab benchmark results
            results = profiler.get_benchmark_results()
            results_exec_time = int(results.exec_time)  # Convert to int to truncate decimals
            results_operation_name = results.operation_name
            results_run_number = results.run_number

            # Ensure benchmark results match expected values
            self.assertEqual(benchmark_time, results_exec_time, msg='Execution time is incorrect.')
            self.assertEqual(operation_name, results_operation_name, msg='Operation name is incorrect.')
            self.assertEqual(i, results_run_number, msg='Run number is incorrect.')

            i += 1

        # Delete *.psv file created when running benchmark
        psv_file = '{}.psv'.format(profiler_label)
        if os.path.exists(psv_file):
            os.remove(psv_file)

    def test_benchmark_results_psv(self):
        # Setup Benchmark Profiler object
        profiler_label = 'test_benchmark_results_psv'

        # Delete *.psv file created from any previous unit testing
        psv_file = '{}.psv'.format(profiler_label)
        if os.path.exists(psv_file):
            os.remove(psv_file)

        profiler = BenchmarkProfiler(profiler_label)

        operation_name_format = 'Sleep {} seconds'

        # Run a few mock benchmarks
        benchmark_times = [1, 2, 10]
        i = 1
        for benchmark_time in benchmark_times:
            profiler.set_run_number(i)

            operation_name = operation_name_format.format(benchmark_time)

            # Run the mock benchmark, measuring time to run sleep command
            profiler.start_benchmark(operation_name)
            time.sleep(benchmark_time)
            profiler.end_benchmark()

            i += 1

        # Read results psv file
        psv_file = '{}.psv'.format(profiler_label)

        # Ensure psv file was created
        if os.path.exists(psv_file):
            # Read file contents
            with open(psv_file, 'r') as f:
                psv_lines = [line.rstrip('\n') for line in f]

            # Check line count of psv file. Line count should be equal to number of benchmarks run + 1 (for header)
            num_lines = len(psv_lines)
            num_lines_expected = len(benchmark_times) + 1
            self.assertEqual(num_lines_expected, num_lines, msg='Line count in resulting psv file is incorrect.')

            # Ensure header (first line) of psv file is correct
            header_expected = 'Log Timestamp|Run Number|Operation|Execution Time'
            header_actual = psv_lines[0]
            self.assertEqual(header_expected, header_actual)

            # Ensure contents (benchmark data) of psv file is correct
            i = 1
            for line_number in range(1, num_lines):
                content = psv_lines[line_number].split('|')

                # Ensure column count is correct
                num_columns = len(content)
                num_columns_expected = 4
                self.assertEqual(num_columns_expected, num_columns, msg='Column count for psv data is incorrect.')

                # Ensure run number is correct
                run_number_psv = int(content[1])
                run_number_expected = i
                self.assertEqual(run_number_expected, run_number_psv, msg='Run number is incorrect.')

                # Ensure operation name is correct
                operation_name_psv = content[2]
                operation_name_expected = operation_name_format.format(benchmark_times[i - 1])
                self.assertEqual(operation_name_expected, operation_name_psv, msg='Operation name is incorrect.')

                # Ensure execution time is correct
                execution_time_psv = int(float(content[3]))  # Convert to int to truncate decimals
                execution_time_expected = benchmark_times[i - 1]
                self.assertEqual(execution_time_expected, execution_time_psv, msg='Execution time is incorrect')

                i += 1

        else:
            self.fail(msg='Resulting psv file could not be found.')

        # Delete *.psv file created when running benchmark
        if os.path.exists(psv_file):
            os.remove(psv_file)

    def test_benchmark_simple_aggregations(self):
        test_dir = './tests_temp/'
        benchmark_label = 'test_benchmark_simple_aggregations'
        psv_file = '{}.psv'.format(benchmark_label)

        # Remove the test data directory from any previous unit tests
        if os.path.isdir(test_dir):
            shutil.rmtree(test_dir)

        # Remove the PSV file from any previous unit tests
        if os.path.isfile(psv_file):
            os.remove(psv_file)

        vcf_to_zar_config = VCFtoZarrConfigurationRepresentation()
        vcf_to_zar_config.enabled = True

        bench_conf = BenchmarkConfigurationRepresentation()
        bench_conf.vcf_to_zarr_config = vcf_to_zar_config
        bench_conf.benchmark_number_runs = 1
        bench_conf.benchmark_data_input = 'vcf'
        bench_conf.benchmark_dataset = 'trio.2010_06.ychr.genotypes.vcf'
        bench_conf.benchmark_aggregations = True

        data_dirs = DataDirectoriesConfigurationRepresentation()
        data_dirs.vcf_dir = './tests/data/'
        data_dirs.zarr_dir_setup = './tests_temp/zarr/'
        data_dirs.zarr_dir_benchmark = './tests_temp/zarr_benchmark/'
        data_dirs.temp_dir = './tests_temp/temp/'

        # Run the benchmark and ensure nothing fails
        benchmark = Benchmark(bench_conf=bench_conf,
                              data_dirs=data_dirs,
                              benchmark_label='test_benchmark_simple_aggregations')
        benchmark.run_benchmark()

        # Ensure psv file was created
        if os.path.exists(psv_file):
            # Read file contents
            with open(psv_file, 'r') as f:
                psv_lines = [line.rstrip('\n') for line in f]

            # Check line count of psv file
            num_lines = len(psv_lines)
            num_lines_expected = 10
            self.assertEqual(num_lines_expected, num_lines, msg='Unexpected line count in resulting psv file.')

            psv_operation_names = []

            for psv_line in psv_lines:
                line_split = psv_line.split('|')
                line_cols_actual = len(line_split)
                line_cols_expected = 4

                # Ensure correct number of data columns exist for current line of data
                self.assertEqual(line_cols_expected, line_cols_actual,
                                 msg='Unexpected number of columns in resulting psv file')

                operation_name = line_split[2]
                psv_operation_names.append(operation_name)

            # Ensure all aggregations were run
            test_operation_names = ['Allele Count (All Samples)',
                                    'Genotype Count: Heterozygous per Variant',
                                    'Genotype Count: Homozygous per Variant',
                                    'Genotype Count: Heterozygous per Sample',
                                    'Genotype Count: Homozygous per Sample']

            for test_operation_name in test_operation_names:
                if test_operation_name not in psv_operation_names:
                    self.fail(msg='Operation \"{}\" was not run during the benchmark.'.format(test_operation_name))
        else:
            self.fail(msg='Resulting psv file could not be found.')

        # Remove the test data directory from any previous unit tests
        if os.path.isdir(test_dir):
            shutil.rmtree(test_dir)

        # Remove the PSV file from this unit test
        if os.path.isfile(psv_file):
            os.remove(psv_file)

    def test_benchmark_pca(self):
        test_dir = './tests_temp/'
        benchmark_label = 'test_benchmark_pca'
        psv_file = '{}.psv'.format(benchmark_label)

        # Remove the test data directory from any previous unit tests
        if os.path.isdir(test_dir):
            shutil.rmtree(test_dir)

        # Remove the PSV file from any previous unit tests
        if os.path.isfile(psv_file):
            os.remove(psv_file)

        vcf_to_zar_config = VCFtoZarrConfigurationRepresentation()
        vcf_to_zar_config.enabled = True

        bench_conf = BenchmarkConfigurationRepresentation()
        bench_conf.vcf_to_zarr_config = vcf_to_zar_config
        bench_conf.benchmark_number_runs = 1
        bench_conf.benchmark_data_input = 'vcf'
        bench_conf.benchmark_dataset = 'trio.2010_06.ychr.genotypes.vcf'
        bench_conf.benchmark_pca = True
        bench_conf.pca_data_scaler = config.benchmark_pca_data_scaler_types[config.PCA_DATA_SCALER_PATTERSON]
        bench_conf.genotype_array_type = config.GENOTYPE_ARRAY_CHUNKED

        data_dirs = DataDirectoriesConfigurationRepresentation()
        data_dirs.vcf_dir = './tests/data/'
        data_dirs.zarr_dir_setup = './tests_temp/zarr/'
        data_dirs.zarr_dir_benchmark = './tests_temp/zarr_benchmark/'
        data_dirs.temp_dir = './tests_temp/temp/'

        # Run the benchmark and ensure nothing fails
        benchmark = Benchmark(bench_conf=bench_conf,
                              data_dirs=data_dirs,
                              benchmark_label=benchmark_label)
        benchmark.run_benchmark()

        # Ensure psv file was created
        if os.path.exists(psv_file):
            # Read file contents
            with open(psv_file, 'r') as f:
                psv_lines = [line.rstrip('\n') for line in f]

            # Check line count of psv file
            num_lines = len(psv_lines)
            num_lines_expected = 14
            self.assertEqual(num_lines_expected, num_lines, msg='Unexpected line count in resulting psv file.')

            psv_operation_names = []

            for psv_line in psv_lines:
                line_split = psv_line.split('|')
                line_cols_actual = len(line_split)
                line_cols_expected = 4

                # Ensure correct number of data columns exist for current line of data
                self.assertEqual(line_cols_expected, line_cols_actual,
                                 msg='Unexpected number of columns in resulting psv file')

                operation_name = line_split[2]
                psv_operation_names.append(operation_name)

            # Ensure all aggregations were run
            test_operation_names = ['PCA: Count alleles',
                                    'PCA: Count multiallelic SNPs',
                                    'PCA: Count biallelic singletons',
                                    'PCA: Remove singletons and multiallelic SNPs',
                                    'PCA: Transform genotype data for PCA',
                                    'PCA: Apply LD pruning',
                                    'PCA: Move data set to memory',
                                    'PCA: Run conventional PCA analysis (scaler: patterson)',
                                    'PCA: Run randomized PCA analysis (scaler: patterson)']

            for test_operation_name in test_operation_names:
                if test_operation_name not in psv_operation_names:
                    self.fail(msg='Operation \"{}\" was not run during the benchmark.'.format(test_operation_name))
        else:
            self.fail(msg='Resulting psv file could not be found.')

        # Remove the test data directory from any previous unit tests
        if os.path.isdir(test_dir):
            shutil.rmtree(test_dir)

        # Remove the PSV file from this unit test
        if os.path.isfile(psv_file):
            os.remove(psv_file)


if __name__ == "__main__":
    unittest.main()
