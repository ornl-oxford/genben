""" Main module for the benchmark. It reads the command line arguments, reads the benchmark configuration, 
determines the runtime mode (dynamic vs. static); if dynamic, gets the benchmark data from the server,
runs the benchmarks, and records the timer results. """

import argparse # for command line parsing
import time # for benchmark timer
import csv # for writing results
import logging

from configparser import ConfigParser

# Important -- use to dereference sections
section_names = 'output', 'ftp'

class ConfigurationRepresentation(object):
  """ A small utility class for object representation of a standard config. file. Requires list of section names. """

    def __init__(self, file_name):
      """ Initializes the configuration representation with a supplied file. """
        parser = ConfigParser()
        parser.optionxform = str  # make option names case sensitive
        found = parser.read(file_name)
        if not found:
            raise ValueError("Configuration file {0} not found".format(file_name) )
        for name in section_names:
            self.__dict__.update(parser.items(name))  # create dictionary representation  

def get_cli_arguments():
    """ Returns command line arguments. 

    Returns:
    args object from an ArgumentParses for fetch data (boolean, from a server), label (optional, for naming the benchmark run), 
    and config argument for where is the config file. """
    
    logging.debug('Getting cli arguments')

    parser = argparse.ArgumentParser(description="A benchmark for genomics routines in Python.")

    # Enable three exclusive groups of options (using subparsers)
    # https://stackoverflow.com/questions/17909294/python-argparse-mutual-exclusive-group/17909525

    subparser = parser.add_subparsers(title="commands", dest="command") 
    subparser.required = True

    config_parser = subparser.add_parser("config",
      help='Setting up the default configuration of the benchmark. It creates the default configuration file.')
    config_parser.add_argument("--output_config", type=str, required=True, help="Specify the output path to a configuration file.", metavar="FILEPATH")

    data_setup_parser = subparser.add_parser("setup",
      help='Preparation and setting up of the data for the benchmark. It requires a configuration file.')
    data_setup_parser.add_argument("--config_file", required=True, help="Location of the configuration file", metavar="FILEPATH")


    benchmark_exec_parser = subparser.add_parser("exec", 
      help='Execution of the benchmark modes. It requires a configuration file.')
    # TODO: use run_(timestamp) as default
    benchmark_exec_parser.add_argument("--label", type=str, default="run", metavar="RUN_LABEL", help="Label for the benchmark run.")
    benchmark_exec_parser.add_argument("--config_file", type=str,  required=True, help="Specify the path to a configuration file.", metavar="FILEPATH")

    runtime_configuration = vars(parser.parse_args()) 
    return runtime_configuration

def read_configuration(location):
    """
    Args: location of the configuration file, existing configuration dictionary
    Returns: a dictionary with keys of the form
    <section>.<option> and the corresponding values.
    """
    config = ConfigurationRepresentation(location)
    return config

if __name__ == '__main__':
    runtime_configuration = get_cli_arguments()
