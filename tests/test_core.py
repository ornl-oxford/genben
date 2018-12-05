import unittest
from genomics_benchmarks.core import *
from time import sleep
import os


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


if __name__ == "__main__":
    unittest.main()
