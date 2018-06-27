""" Main module for the benchmark. It reads the command line arguments, reads the benchmark configuration, 
determines the runtime mode (dynamic vs. static); if dynamic, gets the benchmark data from the server,
runs the benchmarks, and records the timer results. """

import argparse # for command line parsing
import ConfigParser # for config file parsing
import time # for benchmark timer
import csv # for writing results
import logging


def get_cli_arguments():
    """ Returns command line arguments. 

    Returns:
    args object from an ArgumentParses for fetch data (boolean, from a server), label (optional, for naming the benchmark run), 
    and config argument for where is the config file. """

    logging.debug('Getting cli arguments')

    parser = argparse.ArgumentParser(description="A benchmark for genomics routines in Python.")

    parser.add_argument("-f","--fetch_data", action='store_true', help="Run statically (do not download any file.)")
    parser.add_argument("-l","--label", type=str, default="run", metavar="RUN_LABEL", help="Label for the benchmark run.")
    #parser.add_argument("-c","--config", type=file, help="Specify the path to a configuration file.", metavar="FILEPATH")
    parser.add_argument("-c", "--config", type=str, default="../doc/benchmark.conf",
  	help="Specify the path to a configuration file.", metavar="FILEPATH")

    return parser.parse_args()

def parse_arguments(args):
  """ Parses command line arguments, used to control the behavior of the benchmark including location of the configuration files, 
  outputs, etc.
  Args:

  Returns:
   a dictionary with parameters: 
  """
  logging.debug('Parsing arguments')
  runtime_configuration = {"fetch_data":args.fetch_data}
  runtime_configuration["label"] = args.label
  runtime_configuration["config"] = args.config
  return runtime_configuration


def read_configuration(location="../doc/benchmark.conf"):
	""" Read the configuration file here, and store in a dictionary """
	logging.debug('Reading configuration')
	config = ConfigParser.SafeConfigParser()
	config.read(location)
	runtime_configuration = {"ftp_server":config.get('ftp', 'server')}
	runtime_configuration["directory"] = config.get('ftp', 'directory')
	runtime_configuration["files"] = config.get('ftp', 'files')
	runtime_configuration["output_results"] = config.get('output', 'output_results')
	return runtime_configuration


if __name__ == '__main__':
	args = get_cli_arguments()
	runtime_configuration = parse_arguments(args)
    #TODO: get here truthiness of the flag fetch data and store 
	runtime_configuration.update( read_configuration(location=runtime_configuration["config"]) )
	print runtime_configuration
