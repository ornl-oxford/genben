""" Unit test for benchmark CLI functions. 
    To execute on a command line, run from the home directory:  
    python -m unittest tests.test_data_service

"""
import unittest
import os.path


from benchmark import data_service

class TestDataServices(unittest.TestCase):
    def test_url_fetch(self):
    	""" Tests fetching a data to be used in benchmarking from a remote URL. """
    	remote_file_url = "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/pilot_data/release/2010_07/trio/snps/trio.2010_06.ychr.sites.vcf.gz"
    	local_name = "trio.2010_06.ychr.sites.vcf.gz"
    	data_service.fetch_file_from_url(remote_file_url,local_name)
    	self.assertTrue( os.path.isfile(local_name), "No local file retrieved")
    	if os.path.isfile(local_name):
    		os.remove(local_name)

    def test_gzip_decompression(self):
    	""" Tests decompressing the fetched file. """
    	local_file_gz = "./tests/data/trio.2010_06.ychr.sites.vcf.gz"
    	local_file = "trio.2010_06.ychr.sites.vcf"
    	data_service.decompress_gzip(local_file_gz, local_file)
    	self.assertTrue( os.path.isfile(local_file), "No local file decompressed.")
    	if os.path.isfile(local_file): 
        	os.remove(local_file)

if __name__ == '__main__':
    unittest.main()