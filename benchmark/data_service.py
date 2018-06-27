""" Main module for the benchmark. It reads the command line arguments, reads the benchmark configuration, 
determines the runtime mode (dynamic vs. static); if dynamic, gets the benchmark data from the server,
runs the benchmarks, and records the timer results. """

import ftp 
import time # for benchmark timer
import csv # for writing results
import logging

def get_benchmarking_data(server, directory,remote_file=None,local_file=None):
""" Get benchmarking data from a remote ftp server. """ 
    ftp = FTP('ngs.sanger.ac.uk')     # connect to host, default port
    ftp.login()                     # user anonymous, passwd anonymous
    ftp.cwd('/production/ag1000g/phase2/AR1/variation/main/vcf/all/')               # change into directory with unphased vcf dat
    ftp.retrlines('LIST')           # list directory contents

	benchmark_vcf_file = "ag1000g.phase2.ar1.UNKN.vcf"	
	local_gz_file = "ag1000g.phase2.ar1.UNKN.vcf.gz"	
	remote_filename = "ag1000g.phase2.ar1.UNKN.vcf.gz"
	print "Downloading file " + remote_filename + " as " + local_gz_file
	with open(local_gz_file, "wb") as local_file:
		ftp.retrbinary('RETR %s' % remote_filename, local_file.write)