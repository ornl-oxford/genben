""" Main module for the benchmark. It reads the command line arguments, reads the benchmark configuration, 
determines the runtime mode (dynamic vs. static); if dynamic, gets the benchmark data from the server,
runs the benchmarks, and records the timer results. """

import urllib.request 
import ftplib
import time # for benchmark timer
import csv # for writing results
import logging

import gzip
import shutil

def fetch_data_via_ftp(server, directory,remote_files,local_directory):
	""" Get benchmarking data from a remote ftp server. """
	ftp = FTP(server)
	ftp.login()
	ftp.cwd(directory)
	for remote_filename in remote_files:
		local_filename = remote_file
		with open(local_directory+local_filename, "wb") as local_file:
			ftp.retrbinary('RETR %s' % remote_filename, local_file.write)

def fetch_file_from_url(url, local_file):
	urllib.request.urlretrieve(url, local_file)

def decompress_gzip(local_file_gz, local_file):
	with open(local_file, 'wb') as file_out, gzip.open(local_file_gz, 'rb') as file_in:
		shutil.copyfileobj(file_in, file_out)
		
def convert_to_zarr(source_data,zarr_formatted_data):
	""" This function converts the original data (vcf) to a zarr format. """
	pass 
