""" Main module for the benchmark. It reads the command line arguments, reads the benchmark configuration, 
determines the runtime mode (dynamic vs. static); if dynamic, gets the benchmark data from the server,
runs the benchmarks, and records the timer results. """

import argparse # for command line parsing
import configparser # for config file parsing
import time # for benchmark timer
import csv # for writing results

def read_arguments():
  # Returns configuration dictionary
  pass

def read_configuration(location="/docs/benchmark.conf"):
  pass

def run_benchmark(bench_conf):
  pass

def run_dynamic(ftp_location):
  pass

def run_static():
  pass

def get_remote_files(ftp_server, ftp_directory,files=None):
  pass

def record_runtime(benchmark, timestamp):
  pass

# temporary here
def main():
  pass

