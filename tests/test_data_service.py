""" Unit test for benchmark CLI functions. 
    To execute on a command line, run from the home directory:  
    python -m unittest tests.test_data_service
"""
import unittest
import os.path
import shutil
import zarr
import numpy as np
from ftplib import error_temp

from genben import data_service, config


class TestDataServices(unittest.TestCase):
    def test_fetch_data_via_ftp(self):
        local_directory = "./"

        test_config_location = "./tests/data/ftp_test_fetch_data.conf"
        runtime_config = config.read_configuration(location=test_config_location)
        ftp_config = config.FTPConfigurationRepresentation(runtime_config)

        # Attempt to remove local files in case a previous unit test failed to do so (avoid false positive)
        for file in ftp_config.files:
            if os.path.isfile(file):
                os.remove(file)
        try:
            data_service.fetch_data_via_ftp(ftp_config=ftp_config, local_directory=local_directory)

            flag = True
            for file in ftp_config.files:
                if not os.path.isfile(file):
                    flag = False
                    break
            self.assertTrue(flag)
        except (error_temp, IOError):
            pass  # Catch error with attempting to test FTP functionality on Travis CI

        # Remove the downloaded files
        for file in ftp_config.files:
            if os.path.isfile(file):
                os.remove(file)

    def test_decompress_gzip(self):
        """ Tests decompressing the fetched file. """
        local_file_gz = "./tests/data/trio.2010_06.ychr.genotypes.vcf.gz"
        local_filename = "trio.2010_06.ychr.genotypes.vcf"

        # Attempt to remove local file in case a previous unit test failed to do so (prevents false positive)
        if os.path.isfile(local_filename):
            os.remove(local_filename)

        data_service.decompress_gzip(local_file_gz, local_filename)
        self.assertTrue(os.path.isfile(local_filename), "No local file decompressed.")

        # Remove the downloaded file
        if os.path.isfile(local_filename):
            os.remove(local_filename)

    def test_read_file_contents_existing_file(self):
        local_filepath = "./tests/data/test_read_file_contents_data.txt"

        if os.path.isfile(local_filepath):
            results = data_service.read_file_contents(local_filepath)
            self.assertEqual(results, "test data")
        else:
            self.fail("Test data file does not exist. Please ensure the file exists and try running test again")

    def test_read_file_contents_missing_file(self):
        local_filepath = "./tests/data/test_read_file_contents_data_nonexistent.txt"

        if not os.path.isfile(local_filepath):
            results = data_service.read_file_contents(local_filepath)
            self.assertEqual(results, None)
        else:
            self.fail("File should not exist on filesystem. Please remove the file and try running test again.")

    def test_process_data_files(self):
        # Define test input files
        test_dir = "./tests/data/"
        test_files_input = ["trio.2010_06.ychr.genotypes.vcf.gz"]
        test_files_expected = ["trio.2010_06.ychr.genotypes.vcf"]

        # Setup test processing directories
        process_data_files_test_dir = "./data/unittest/"
        input_dir_test = process_data_files_test_dir + "input/"
        temp_dir_test = process_data_files_test_dir + "temp/"
        output_dir_test = process_data_files_test_dir + "vcf/"

        # Remove the test directory created for this unittest (from any previous unit testing)
        if os.path.exists(process_data_files_test_dir):
            shutil.rmtree(process_data_files_test_dir)

        # Create input directory
        data_service.create_directory_tree(input_dir_test)

        # Copy test files into input directory to test data processing
        for test_file in test_files_input:
            test_file_expected = test_dir + test_file
            test_file_output = input_dir_test + test_file
            if os.path.exists(test_file_expected):
                shutil.copy(test_file_expected, test_file_output)

        # Process the test files
        data_service.process_data_files(input_dir=input_dir_test, temp_dir=temp_dir_test, output_dir=output_dir_test)

        # Check the results to ensure corresponding vcf files exist in output directory
        error_flag = False
        for test_file_expected in test_files_expected:
            if not os.path.exists(output_dir_test + test_file_expected):
                error_flag = True

        # Remove the test directory created for this unittest
        shutil.rmtree(process_data_files_test_dir)

        # Return an error if the test failed
        if error_flag:
            self.fail(msg="One or more test files were not processed and placed in output directory.")

    def test_convert_to_zarr(self):
        input_vcf_path = "./tests/data/trio.2010_06.ychr.genotypes.vcf"
        output_zarr_path = "trio.2010_06.ychr.genotypes.zarr"

        # Attempt to remove local file in case a previous unit test failed to do so (prevents false positive)
        if os.path.isdir(output_zarr_path):
            shutil.rmtree(output_zarr_path)

        if os.path.isfile(input_vcf_path):
            # Setup test settings for Zarr conversion
            vcf_to_zarr_config = config.VCFtoZarrConfigurationRepresentation()
            vcf_to_zarr_config.fields = 'variants/numalt'
            vcf_to_zarr_config.enabled = True
            vcf_to_zarr_config.compressor = "Blosc"
            vcf_to_zarr_config.blosc_compression_algorithm = "zstd"
            vcf_to_zarr_config.blosc_compression_level = 1
            vcf_to_zarr_config.blosc_shuffle_mode = -1

            # Convert VCF file to Zarr
            data_service.convert_to_zarr(input_vcf_path=input_vcf_path,
                                         output_zarr_path=output_zarr_path,
                                         conversion_config=vcf_to_zarr_config)

            # Load the Zarr data from storage for testing
            callset = zarr.open_group(output_zarr_path, mode="r")
            numalt = callset['variants/numalt']
            self.assertEqual(np.size(numalt), 959)
            self.assertEqual(np.max(numalt), 1)
        else:
            self.fail("Test data file does not exist. Please ensure the file exists and try running test again")

        # Remove the Zarr test data
        if os.path.isdir(output_zarr_path):
            shutil.rmtree(output_zarr_path)


if __name__ == "__main__":
    unittest.main()
