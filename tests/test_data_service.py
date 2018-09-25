""" Unit test for benchmark CLI functions. 
    To execute on a command line, run from the home directory:  
    python -m unittest tests.test_data_service
"""
import unittest
import os.path
import shutil
import zarr
import numpy as np

from benchmark import data_service, config


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

        data_service.fetch_data_via_ftp(ftp_config=ftp_config, local_directory=local_directory)

        flag = True
        for file in ftp_config.files:
            if not os.path.isfile(file):
                flag = False
                break
        self.assertTrue(flag)

        # Remove the downloaded files
        for file in ftp_config.files:
            if os.path.isfile(file):
                os.remove(file)

    def test_fetch_file_from_url(self):
        """ Tests fetching a data to be used in benchmarking from a remote URL. """
        remote_file_url = "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/pilot_data/release/2010_07/trio/snps/trio.2010_06.ychr.sites.vcf.gz"
        local_filename = "trio.2010_06.ychr.sites.vcf.gz"

        # Attempt to remove local file in case a previous unit test failed to do so (prevents false positive)
        if os.path.isfile(local_filename):
            os.remove(local_filename)

        data_service.fetch_file_from_url(remote_file_url, local_filename)
        self.assertTrue(os.path.isfile(local_filename), "No local file retrieved")

        # Remove the downloaded file
        if os.path.isfile(local_filename):
            os.remove(local_filename)

    def test_decompress_gzip(self):
        """ Tests decompressing the fetched file. """
        local_file_gz = "./tests/data/trio.2010_06.ychr.sites.vcf.gz"
        local_filename = "trio.2010_06.ychr.sites.vcf"

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



if __name__ == "__main__":
    unittest.main()
